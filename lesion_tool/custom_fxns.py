#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 28 16:08:33 2019

Custom functions used in the nypipe pipelines in lesion_tool

@author: tsuchida
"""


def getElementFromList(inlist,idx,slc=None):
    '''
    For selecting a particular element or slice from a list 
    within a nipype connect statement.
    If the slice is longer than the list, this returns the list
    '''
    if not slc:
        outlist=inlist[idx]
    else:
        if slc == -1:
            outlist=inlist[idx:]
        else:
            outlist=inlist[idx:slc]
    return outlist


def createOutputDir(sub,base,name,nodename):
    import os
    return os.path.join(base,name,'_subject_id_'+sub,nodename)


def getFirstElement(inlist):
    '''
    Get the first element from a list
    '''
    return inlist[0]


def convertLabels(src_label_img, conversion_csv, src_val_colname, trg_val_colname, new_label_img):
    '''
    Given the source label image and src to targ labels speciffied in a csv,
    convert all the src label to new labels specified in the dict.
    
    This function can merge but not split the original labels.
    '''
    import os.path as op
    import pandas as pd
    import numpy as np
    import nibabel as nib
    from six import string_types

    src_img = nib.load(src_label_img) if isinstance(src_label_img, string_types) else src_label_img
    src_im_dat = np.int16(np.asanyarray(src_img.dataobj))
    
    conversion_df = pd.read_csv(conversion_csv)
    try:
        src_to_targ_dict = dict(zip(conversion_df[src_val_colname], conversion_df[trg_val_colname]))
        
    except Exception as exc:
        print("Failed to generate conversion dict from the provided csv and specified column names")
        print(exc)
        
    trg_im_dat = np.zeros_like(src_im_dat)
    for src_val, trg_val in src_to_targ_dict.items():
        ijk = np.nonzero(src_im_dat == np.int16(src_val))
        if ijk:
            trg_im_dat[ijk] = np.int16(trg_val)
    
    header = src_img.header.copy()
    header['cal_min'], header['cal_max'] = trg_im_dat.min(), trg_im_dat.max()     
    nifti_out = nib.Nifti1Image(trg_im_dat, src_img.get_affine(), header=header)

    nifti_out.to_filename(new_label_img)
    
    return op.abspath(new_label_img)
    
    