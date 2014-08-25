# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import pymongo
from pymongo import MongoClient

# <codecell>

import os, os.path, time
import datetime
from os.path import join
import re
import scipy.io
%pylab inline
from pylab import *
import matplotlib.pyplot as plt

# <codecell>

# Plot the input wave form from query results
def plot_input_waveform(results):
    for r in results:
        rf = r.get('file_path')
        f_name, f_extension = os.path.splitext(rf)
        if(f_extension == '.mat'):
            try:
                mat = scipy.io.loadmat(rf)
                input_waveform = mat['c']
                print rf
                plot(input_waveform)
                show()
            except:
                print rf
                print "input waveform not found in this file"

# <codecell>

#Find documents within time range, inputs are tuples of yyyy,mm,dd, like (2014, 01, 01)
def find_within_time_range(collection, start, end):
    start_time = start + (0,0,0,0)
    end_time = end + (0,0,0,0)
    start_dt = datetime.datetime(*(start+(0,0,0,0)))
    end_dt = datetime.datetime(*(end+(0,0,0,0)))

    print start_dt
    result = collection.find( {'time': {'$gte':start_dt, '$lt':end_dt} } )
    return result

# <codecell>

# Insert new documents under the specified path into the database, auto-generating basic metadata, including filepath, timestamp, ...
def insert_documents(collection, data_path):
    for root,dirs, files in os.walk(data_path):
        for f in files:
            f_name, f_extension = os.path.splitext(f)
            # if file type is mat
            if(f_extension == '.mat'):
                fullpath = join(root, f)
                mdatetime = datetime.datetime.fromtimestamp(os.path.getmtime(fullpath))
                print mdatetime
                #print "last modified, %s" % time.ctime(os.path.getmtime(join(root,f)))
                #mat = scipy.io.loadmat(fullpath)
                #mat.update({"file_path" : fullpath,"time": mdatetime})
                #print mat
                collection.insert( {"file_path" : fullpath,"time": mdatetime} )
            else:
                print "file type incorrect"
            

# <codecell>

# demo
client = MongoClient()
db = client.bionet_database
collection = db.flylab_data

# <codecell>

collection.remove()

# <codecell>

data_path = "/Users/hanyuli/flylab_data/"
insert_documents(collection, data_path)

# <codecell>

#collection.update( {"file_path" : "/Users/hanyuli/flylab_data/2014_1_11_001.EDR"}, {"$set" : {"time":20140111} })

# <codecell>

start = (2014,1,10)
end = (2014,1,12)
result = find_within_time_range(collection, start, end)
#plot_input_waveform(result)
for r in result:
    print r

# <codecell>

result = collection.find()
plot_input_waveform(result)

# <codecell>

mat = scipy.io.loadmat('/Users/hanyuli/flylab_data/m/2014_1_11_001.mat')

# <codecell>

mat.__class__

# <codecell>

mat

# <codecell>

a = array([3,3,3])

# <codecell>

a.__class__

# <codecell>

if isinstance(a,np.ndarray):
    a = list(a)

# <codecell>

a

# <codecell>

if isinstante

# <codecell>


