"""
SpikeDB query and interface tools
"""

import pymongo
import numpy as np
import datetime
import operator
from pylab import *
import matplotlib.pyplot as plt
import scipy.io
from utils import parse_fname, compute_distance

def general_query(collection, key=None, start=None,end=None):
    """
    query with a certain key
    'timestamp': if end is not given, it will be set to one day after start 
    'fnhead': file header, like "20xx_xx_xx_"
    others: usually not used in query, but can also be used
    """
    if(key == 'timestamp'):
        if end == None:
            end = tuple(map(operator.add, start, (0,0,1)))  
        return find_within_time_range(collection, start, end)
    else:
        return collection.find( {key : start } )

def find_within_time_range(collection, start, end):
    """
    Find documents within specified time range
    """
    start_time = start + (0,0,0,0)
    end_time = end + (0,0,0,0)
    start_dt = datetime.datetime(*(start+(0,0,0,0)))
    end_dt = datetime.datetime(*(end+(0,0,0,0)))

    print start_dt,"~",end_dt
    result = collection.find( {'timestamp': {'$gte':start_dt, '$lt':end_dt} } )
    return result

def find_similar_input(collection, _result, r0, threshold=10, display=True):
    """
    find data set within result with similar input waveform to r
    """
    if r0.__class__ == dict:
        std = np.array(r0.get('input_signal'))
    elif r0.__class__ == str:
        (fnhead, fnum) = parse_fname(r0)
        r0_entry = collection.find_one( {'fnhead' : fnhead, 'fnum' : fnum } )
        std = np.array(r0_entry.get('input_signal'))
    filtered_result = []
    result = _result.clone()
    for r_candidate in result:
        try:
            comp = np.array(r_candidate.get('input_signal'))
            dist = compute_distance(comp, std)
            print dist
            if dist < threshold:
                if display == True:
                    plot(comp)
                    show()
                    print r_candidate.get('fnhead'), r_candidate.get('fnum')
                    print dist
                filtered_result = filtered_result + [r_candidate]

        except:
            print 'input signal not found'
    return filtered_result


def find_similar_input_file(collection,result, r):
    """
    find data set within result with similar input waveform to r, through reading mat file
    """
    r_input = read_data(r, 'c');
    for r_comp in result:
        try:
            r_input_comp = read_data(r_comp, 'c')
            dist = SpikeDB.compute_distance(r_input, r_input_comp)
            print dist
            if dist < 7000:
                try:
                    mat = scipy.io.loadmat(r_comp.get('mat_filepath'))
                    input_waveform = mat['c']
                    plot(input_waveform)
                    show()
                except:
                    print "not plottable"
                
        except:
            print "not comparable"


def plot_input_waveform(r):
    """
    Plot input stimulus waveform from query results
    """
    try:
        rf = r.get('mat_filepath')
        mat = scipy.io.loadmat(rf)
        input_waveform = mat['c']
        plot(input_waveform)
        show()
    except:
        print "input waveform not found in this entry"

def plot_input_waveforms(results):
    """
    Plot multiple input stimulus waveform from query results
    """
    for r in results:
        #plot_input_waveform(r)
        try:
            rf = r.get('mat_filepath')
            print rf, ">>"
            mat = scipy.io.loadmat(rf)
            input_waveform = mat['c']
            plot(input_waveform)
            show()
        except:
            print "input waveform not found in this entry"
            continue

def plot_downsampled_input_waveform(r):
    """
    Plot 1000-time downsampled signal
    """
    try:
        input_waveform  = r.get('input_signal')
        rf = r.get('mat_filepath')
        print rf, ">>"
        plot(input_waveform)
        show()
    except:
        print "input waveform not found in this entry"

def plot_downsampled_input_waveforms(results):
    """
    Plot multiple 1000-time downsampled signal
    """
    for r in results:
        try:
            input_waveform  = r.get('input_signal')
            rf = r.get('mat_filepath')
            print rf, ">>"
            plot(input_waveform)
            show()
        except:
            print "input waveform not found in this entry"
            continue
