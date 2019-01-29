# -*- coding: utf-8 -*
'''
A mini-pipeline just to convert Freesurfer aseg labels to MGDM labels
@author: atsuch
'''
from nipype.pipeline.engine import Workflow, Node
from nipype.interfaces.utility import IdentityInterface, Function
from nipype.interfaces.io import DataGrabber, FreeSurferSource, DataSink
from nipype.interfaces.freesurfer.preprocess import MRIConvert
from nipype.interfaces.ants.segmentation import N4BiasFieldCorrection
from nipype.interfaces.fsl.preprocess import FLIRT
from nipype.interfaces.fsl.maths import UnaryMaths, BinaryMaths, ApplyMask
from nipype.interfaces.fsl.utils import Reorient2Std, ImageStats
from lesion_tool.custom_fxns import getElementFromList, convertLabels



def FSlabels2MGDM(wf_name='Label_Converter',
                  base_dir='/beegfs_data/scratch/tsuchida-waimea/',
                  input_dir='/projects/waimea/mrishare/',
                  subjects=None,
                  fs_subjects_dir='/data/analyses/work_in_progress/freesurfer/fsmrishare-flair6.0/',
                  conversion_dict={'csv': '/homes_unix/tsuchida/nighres/lesion_tool/FS2MGDMlabels.csv',
                                   'src_val_colname': 'FS_label_val',
                                   'trg_val_colname': 'brain-atlas-quant_label_val'}):
    
    wf = Workflow(wf_name)
    wf.base_dir = base_dir
    

    '''
    ###############################
    #### Subject List and Data ####
    ###############################
    '''    
    # Subject List
    subjectList = Node(IdentityInterface(fields=['subject_id'],
                                         mandatory_inputs=True),
                       name="subList")
    subjectList.iterables = ('subject_id', [ sub for sub in subjects if sub != '' and sub !='\n' ] )
    
    # T1w and FLAIR
    scanList = Node(DataGrabber(infields=['subject_id'],
                                outfields=['T1', 'FLAIR']),
                    name="scanList")
    scanList.inputs.base_directory = input_dir
    scanList.inputs.ignore_exception = False
    scanList.inputs.raise_on_empty = True
    scanList.inputs.sort_filelist = True
    scanList.inputs.template = '%s/anat/%s'
    scanList.inputs.template_args = {'T1': [['subject_id','*_T1w.nii.gz']], 
                                     'FLAIR': [['subject_id','*_FLAIR.nii']]}  
    wf.connect(subjectList, "subject_id", scanList, "subject_id")
    
    # Freesurfer brainmask and aseg
    fsSource = Node(FreeSurferSource(), name='fsSource')
    fsSource.inputs.subjects_dir = fs_subjects_dir
    wf.connect(subjectList,'subject_id', fsSource, 'subject_id')
    
    '''
    ##################
    #### Reorient ####
    ##################
    '''
    # Reorient Volume
    T1Conv = Node(Reorient2Std(), name="ReorientVolume")
    T1Conv.inputs.ignore_exception = False
    T1Conv.inputs.out_file = "T1_reoriented.nii.gz"
    wf.connect(scanList, "T1", T1Conv, "in_file")
    
    # Reorient Volume (2)
    T2flairConv = Node(Reorient2Std(), name="ReorientVolume2")
    T2flairConv.inputs.ignore_exception = False
    T2flairConv.inputs.out_file = "FLAIR_reoriented.nii.gz"
    wf.connect(scanList, "FLAIR", T2flairConv, "in_file")

    '''
    ########################
    #### BF correction1 ####
    ########################
    '''    
    # N3 Correction
    T1NUC = Node(N4BiasFieldCorrection(), name="N3Correction")
    T1NUC.inputs.dimension = 3
    T1NUC.inputs.environ = {'NSLOTS': '1'}
    T1NUC.inputs.ignore_exception = False
    T1NUC.inputs.num_threads = 1
    T1NUC.inputs.save_bias = False
    wf.connect(T1Conv, "out_file", T1NUC , "input_image")
        
    # N3 Correction (2)
    T2flairNUC = Node(N4BiasFieldCorrection(), name="N3Correction2")
    T2flairNUC.inputs.dimension = 3
    T2flairNUC.inputs.environ = {'NSLOTS': '1'}
    T2flairNUC.inputs.ignore_exception = False
    T2flairNUC.inputs.num_threads = 1
    T2flairNUC.inputs.save_bias = False
    wf.connect(T2flairConv, "out_file", T2flairNUC, "input_image")
    
    '''
    #####################
    ### PRE-NORMALIZE ###
    #####################
    To make sure there's no outlier values (negative, or really high) to offset
    the initialization steps
    '''
    
    # Intensity Range Normalization
    getMaxT1NUC = Node(ImageStats(op_string= '-r'), name="getMaxT1NUC")
    wf.connect(T1NUC, 'output_image', getMaxT1NUC, 'in_file')
    
    T1NUCirn = Node(BinaryMaths(), name="IntensityNormalization")
    T1NUCirn.inputs.operation = "div"
    T1NUCirn.inputs.out_file = "normT1.nii.gz"
    wf.connect(T1NUC, 'output_image', T1NUCirn,'in_file')
    wf.connect(getMaxT1NUC, ('out_stat', getElementFromList, 1),
               T1NUCirn, "operand_value")
    
    # Intensity Range Normalization (2)
    getMaxT2NUC = Node(ImageStats(op_string= '-r'), name="getMaxT2")
    wf.connect(T2flairNUC, 'output_image', getMaxT2NUC, 'in_file')
    
    T2NUCirn = Node(BinaryMaths(), name="IntensityNormalization2")
    T2NUCirn.inputs.operation = "div"
    T2NUCirn.inputs.out_file = "normT2.nii.gz"
    wf.connect(T2flairNUC, 'output_image', T2NUCirn, 'in_file')
    wf.connect(getMaxT2NUC, ('out_stat', getElementFromList, 1),
               T2NUCirn, "operand_value")
    
    '''
    ########################
    #### COREGISTRATION ####
    ########################
    '''
    
    # FLIRT
    T2flairCoreg = Node(FLIRT(), name="T2flairCoreg")
    T2flairCoreg.inputs.output_type = 'NIFTI_GZ'
    wf.connect(T2NUCirn, "out_file", T2flairCoreg, "in_file")
    wf.connect(T1NUCirn, "out_file", T2flairCoreg, "reference")

    '''
    #################################################
    #### Reslice FS source images like main (T1) ####
    #################################################
    '''   
    
    fsAseg = Node(MRIConvert(), name="fsAseg")
    fsAseg.inputs.ignore_exception = False
    fsAseg.inputs.out_datatype = 'float'
    fsAseg.inputs.out_type = 'niigz'
    fsAseg.inputs.resample_type = 'nearest'
    fsAseg.inputs.subjects_dir = fs_subjects_dir
    wf.connect(fsSource, 'aseg', fsAseg, 'in_file')
    wf.connect(T1NUC, 'output_image', fsAseg, 'reslice_like')
    
    fsBrainmask = Node(MRIConvert(), name="fsBrainmask")
    fsBrainmask.inputs.ignore_exception = False
    fsBrainmask.inputs.out_datatype = 'float'
    fsBrainmask.inputs.out_type = 'niigz'
    fsBrainmask.inputs.resample_type = 'nearest'
    fsBrainmask.inputs.subjects_dir = fs_subjects_dir
    wf.connect(fsSource, 'brainmask', fsBrainmask, 'in_file')
    wf.connect(T1NUC, 'output_image', fsBrainmask, 'reslice_like')
    
    '''    
    #########################
    #### Apply mask      ####
    #########################
    '''
    
    # Since fsBrainmask is actually not a mask but a skull-stripped brain, make
    # mask by binarizing (note that in MRiSHARE pipe we also erode and dialate this...)
    binFsMask = Node(UnaryMaths(), name="binFsMask")
    binFsMask.inputs.operation = 'bin'
    binFsMask.inputs.output_type = 'NIFTI_GZ'
    binFsMask.inputs.nan2zeros = True
    binFsMask.inputs.out_file = 'brainmask_bin.nii.gz'
    wf.connect(fsBrainmask, "out_file", binFsMask, "in_file")
    
    T1ss = Node(ApplyMask(), name="T1ss")
    T1ss.inputs.out_file = 'normT1_masked.nii.gz'
    wf.connect(binFsMask, "out_file", T1ss, "mask_file")
    wf.connect(T1NUCirn, "out_file", T1ss, "in_file")
    
    # Image Calculator
    T2ss = Node(ApplyMask(), name="ImageCalculator")
    T2ss.inputs.out_file = 'normT2_flirt_masked.nii.gz'
    wf.connect(binFsMask, "out_file", T2ss, "mask_file")
    wf.connect(T2flairCoreg, "out_file", T2ss, "in_file")


    '''    
    ############################
    #### FS to MGDM labels  ####
    ############################
    '''   
    # To use fsAseg for tissue segmentation portion of the MGDM, convert the
    # FS labels to those of the atlas (brain-atlas-quant-3.0.8.txt)
    # see FS2MGDMlabels.csv for conversion
    
    FSlabels2MGDM = Node(Function(input_names=["src_label_img",
                                               "conversion_csv",
                                               "src_val_colname",
                                               "trg_val_colname",
                                               "new_label_img"],
                                  output_names=["new_label_img"],
                                  function=convertLabels),
                         name="FSlabels2MGDM")
    FSlabels2MGDM.inputs.conversion_csv = conversion_dict['csv']
    FSlabels2MGDM.inputs.src_val_colname = conversion_dict['src_val_colname']
    FSlabels2MGDM.inputs.trg_val_colname = conversion_dict['trg_val_colname']
    FSlabels2MGDM.inputs.new_label_img = 'converted_labels.nii.gz'
    wf.connect(fsAseg, "out_file", FSlabels2MGDM, "src_label_img")

    ## DataSink
    
    datasink = Node(DataSink(base_directory=base_dir,
                             container='%sSink' % wf_name),
                    name='Datasink')
    
    wf.connect(fsAseg, 'out_file', datasink, 'resliced_fsAseg')
    wf.connect(binFsMask, 'out_file', datasink, 'resliced_fsBrainmask')
    wf.connect(T1ss, 'out_file', datasink, 'skullstripped_T1')
    wf.connect(T2ss, 'out_file', datasink, 'skullstripped_coregT2')
    wf.connect(FSlabels2MGDM, 'new_label_img', datasink, 'FS2MGDMlabels')


    return wf
    
