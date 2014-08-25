"""
SpikeDB Core Functionalities and data insertion

Hanyu Li
Bionet Lab, Columbia University
hl2776@columbia.edu
"""
import pymongo
from pymongo import MongoClient
import os, os.path, time
from os.path import join
import re
import distutils.core
import xlrd
import numpy as np
from utils import *
from query import *
from animation import *


class SpikeDB(object):
    ## Initialize
    @staticmethod
    def start(databasename='bionet_database', collectionname='flylab_data'):
        """
        Initialize and return a collection to be worked on
        """
        client = MongoClient()
        db = client[databasename]
        collection = db[collectionname]
        return collection

    ## Default data insertion 
    @staticmethod
    def insert_and_process(collection, root_path):
        """
        Data insertion given a root path under which all documents are present
        """
        SpikeDB.insert_documents_default(collection, root_path)
        SpikeDB.insert_spikes(collection)
        SpikeDB.insert_input_signal(collection)





    ##Insert Previous Data Files into the database and Processing entrys

    #1. Yevgeniy's data
    @staticmethod
    def insert_documents_Yev(collection, root_path):
        """
        Insert Yev's data
        """
        log_dict = {}
        for root,dirs, files in os.walk(root_path):
            for f in files:
                f_name, f_extension = os.path.splitext(f)
                if(f == 'data_log.txt'):
                    log_path = join(root, f)
                    print log_path
                    
                    #H = analyze_log(log_path)
                    H = open(log_path, 'r').readlines()
                    log_dict[root] = H
                # if file type is mat

        
        for root,dirs, files in os.walk(root_path):
            for f in files:
                f_name, f_extension = os.path.splitext(f)
                if(f_extension == '.EDR'):
                    fullpath = join(root, f)
                    print fullpath
                    mdatetime = datetime.datetime.fromtimestamp(os.path.getmtime(fullpath))
                    try:
                        #print f_name
                        (fnhead, fnum) = parse_fname(f_name)
                        #entry = deepcopy(log_dict[root])
                        log = log_dict[root]
                        
                        entry = {}
                        entry['fnhead'] = fnhead
                        entry['fnum'] = fnum
                        entry['timestamp'] = mdatetime
                        entry['EDR_filepath'] = fullpath
                        #print "before", entry
                        SpikeDB.process_entry_Yev(entry, log)
                        #print "after ", entry
                        collection.insert(entry)
                        #print entry
                    except KeyError:
                        entry = {}
                        entry['fnhead'] = fnhead
                        entry['fnum'] = fnum
                        entry['timestamp'] = mdatetime
                        entry['EDR_filepath'] = fullpath
                        collection.insert(entry)
                    except ValueError:
                        print "parse error",sys.exc_info()
                elif(f_extension == '.mat'):
                    fullpath = join(root, f)
                    try:
                        (fnhead, fnum) = parse_fname(f_name)
                        collection.update({'fnhead' : fnhead, 'fnum' : fnum}, {"$set" : {"mat_filepath" : fullpath}})
                    except:
                        print "parse error",sys.exc_info()[0]

    @staticmethod
    def process_entry_Yev(entry, log):
        """
        process entry through analyzing log
        """
        #print entry
        num = entry['fnum']
        found = False
        #fly = 'Fly 1'
        for line in log:
            k = line.find(':')
            #print line
            #Fly = {}
            #Cell = {}
            
            if line[0:4] == 'Fly ':
                fly = line[0:5]
                fly_info = {}
                #Fly[fly] = fly_info
            
            elif found == False and line[0:6] == 'Neuron':
                key = line[0:7]
                #print key
                Cell = {}
                file_num = line[6]
                Cell['celltype'] = line[8:len(line)-2]
            elif k!=-1:
                key = line[0:k]
                if key == 'Line' or key == 'Sex' or key == 'Ages' or key == 'Concentration of ATR' or key == 'Recording' or key == 'Food':
                    if line[k+1] != ' ':
                        k = k-1
                    fly_info[key] = line[k+2:len(line)-2]
                elif key == 'File #':
                    substr = line[k+2:len(line)-2]
                    ind1 = substr.find(':')
                    ind2 = substr.find('---')
                    num1 = int(substr[0:ind1])
                    num2 = int(substr[ind1+1:ind2])
                    entry['stimtype'] = substr[ind2+4:]
                    if num >= num1 and num <= num2:
                        found = True
                elif found == True and (key == 'Light' or key == 'Protocol'):
                    entry[key] = line[k+2:len(line)-2]
                    if key == 'Protocol':
                        entry.update(fly_info)
                        entry.update(Cell)
                        #print fly_info
                        break
                elif found == True and key == 'File #':
                    entry.update(fly_info)
                    entry.update(Cell)
                    break
            elif found == True and (line[0:6] == 'Neuron'):
                
                entry.update(fly_info)
                entry.update(Cell)
                break
        return
                        
                
    

    #2. Anmo's data
    @staticmethod
    def insert_documents_Anmo(collection, root_path):
        SpikeDB.insert_by_info(collection, root_path)
        SpikeDB.insert_file_path(collection, root_path)


    #3. Ban's data
    @staticmethod
    def insert_documents_Ban(collection, root_path):
        """
        Insert Ban's data
        """
        log_dict = {}
        for root,dirs, files in os.walk(root_path):
            for f in files:
                f_name, f_extension = os.path.splitext(f)
                if(f == 'data_log.txt'):
                    log_path = join(root, f)
                    print log_path
                    
                    #H = analyze_log(log_path)
                    H = open(log_path, 'r').readlines()
                    log_dict[root] = H
                # if file type is mat
                
        #print log_dict['/Volumes/database_disk/Data/Ban/20130707']

        
        
        for root,dirs, files in os.walk(root_path):
            for f in files:
                f_name, f_extension = os.path.splitext(f)
                if(f_extension == '.EDR'):
                    fullpath = join(root, f)
                    print fullpath
                    mdatetime = datetime.datetime.fromtimestamp(os.path.getmtime(fullpath))
                    try:
                        #print f_name
                        (fnhead, fnum) = parse_fname(f_name)
                        #entry = deepcopy(log_dict[root])
                        log = log_dict[root]
                        
                        entry = {}
                        entry['fnhead'] = fnhead
                        entry['fnum'] = fnum
                        entry['timestamp'] = mdatetime
                        entry['EDR_filepath'] = fullpath
                        #print "before", entry
                        SpikeDB.process_entry_Ban(entry, log)
                        #print "after ", entry
                        collection.insert(entry)
                    except KeyError:
                        entry = {}
                        entry['fnhead'] = fnhead
                        entry['fnum'] = fnum
                        entry['timestamp'] = mdatetime
                        entry['EDR_filepath'] = fullpath
                        collection.insert(entry)
                        print "Key Error, data log not exist"
                    except ValueError:
                        print "parse error",sys.exc_info()
                elif(f_extension == '.mat'):
                    fullpath = join(root, f)
                    try:
                        (fnhead, fnum) = parse_fname(f_name)
                        collection.update({'fnhead' : fnhead, 'fnum' : fnum}, {"$set" : {"mat_filepath" : fullpath}})
                    except:
                        print "parse error",sys.exc_info()[0]
                        

    @staticmethod
    def process_entry_Ban(entry, log):
        """
        process entry through analyzing log
        """
        num = entry['fnum']
        found = False
        for line in log:
            k = line.find(':')
            
            if line[0:4] == 'Fly ':
                fly = line[0:5]
                fly_info = {}
            
            elif found == False and line[0:4] == 'Cell':
                key = line[0:7]
                #print key
                Cell = {}
                file_num = line[6]
                Cell['celltype'] = line[8:len(line)-2]
            elif k!=-1:
                key = line[0:k]
                if key == 'Line' or key == 'Sex' or key == 'Ages' or key == 'ATR Concentration' or key == 'Recording' or key == 'Food':
                    if line[k+1] != ' ':
                        k = k-1
                    fly_info[key] = line[k+2:len(line)-2]
                elif key == 'File #':
                    substr = line[k+2:len(line)-2]
                    ind1 = substr.find(':')
                    ind2 = substr.find('---')
                    num1 = int(substr[0:ind1])
                    num2 = int(substr[ind1+1:ind2])
                    entry['stimtype'] = substr[ind2+4:]
                    if num >= num1 and num <= num2:
                        found = True
                elif found == True and (key == 'Light' or key == 'Protocol'):
                    entry[key] = line[k+2:len(line)-2]
                    if key == 'Protocol':
                        entry.update(fly_info)
                        entry.update(Cell)
                        break
            elif found == True and (line[0:4] == 'Cell' or line[0:4] == 'File'):
                
                entry.update(fly_info)
                entry.update(Cell)
                break
        return
                        
                


    #4. Default data
    @staticmethod
    def insert_documents_default(collection, data_path):
        SpikeDB.insert_documents_Ban(collection, data_path)

    @staticmethod
    def insert_documents(collection, data_path):
        """
        Insert documents under the specified path into the database data collection
        """

        for root,dirs, files in os.walk(data_path):
            for f in files:
                f_name, f_extension = os.path.splitext(f)
                # if file type is mat
                if(f_extension == '.mat'):
                    fullpath = join(root, f)
                    mdatetime = datetime.datetime.fromtimestamp(os.path.getmtime(fullpath))
                    print f
                    #print mdatetime
                    collection.insert( {"file_path" : fullpath,"time": mdatetime} )
                else:
                    print "file type incorrect"




    @staticmethod
    def insert_by_info(collection, root_path):
        for root,dirs, files in os.walk(root_path):
            for f in files:
                f_name, f_extension = os.path.splitext(f)
                # if file type is mat
                if(f_extension == '.xls'):
                    fullpath = join(root, f)
                    print f
                    try:
                        SpikeDB.insert_by_workbook(collection, fullpath)
                    except:
                        print "xls parse error, not standard data record", sys.exc_info()
                    
        
    @staticmethod
    def insert_by_workbook(collection, workbook_path):
        workbook = xlrd.open_workbook(workbook_path)
        #worksheet = workbook.sheet_by_name('Sheet1')
        worksheet = workbook.sheet_by_index(0)
        attributes = worksheet.row_values(0,0,worksheet.ncols)
        num_rows = worksheet.nrows
        num_cols = worksheet.ncols
        curr_row = 2
#Insert each entry into collection
        while curr_row < num_rows:
            entry = {}
            row = worksheet.row(curr_row)
            curr_col = 0
            while curr_col < num_cols:
                v = worksheet.cell_value(curr_row, curr_col)
                #print v
                entry[attributes[curr_col]] = v
                curr_col +=1
            #print row
            curr_row += 1
            collection.insert(entry)


    @staticmethod
    def insert_file_path(collection, root_path):
        for root,dirs, files in os.walk(root_path):
            for f in files:
                f_name, f_extension = os.path.splitext(f)
                # if file type is mat
                if(f_extension == '.mat'):
                    fullpath = join(root, f)
                    mdatetime = datetime.datetime.fromtimestamp(os.path.getmtime(fullpath))
                    try:
                        (fnhead, fnum) = parse_fname(f_name)
                        result = collection.find_one({'fnhead': fnhead, 'fnum' : fnum})
                        if result != None:
                            result['mat_filepath'] = fullpath
                            result['timestamp'] = mdatetime
                            collection.save(result)
                            print fullpath
                        #result = collection.find({'fnhead': fnhead, 'fnum' : fnum})
                        #print r
                        #if(result.count() == 1):
                        #    collection.update({'fnhead' : fnhead, 'fnum' : fnum}, {"$set" : {"mat_filepath" : fullpath}})
                        #    collection.update({'fnhead' : fnhead, 'fnum' : fnum}, {"$set" : {"timestamp" : mdatetime}})
                    except ValueError:
                        try:
                            end = f_name[-1]
                            (fnhead, fnum) = parse_fname(f_name[:-1])
                            result = collection.find_one({'fnhead': fnhead, 'fnum' : fnum})
                            if result != None:
                                result['mat_filepath_'+end] = fullpath
                                #result['timestamp'] = mdatetime
                                collection.save(result)
                                print fullpath
                        except ValueError:
                            print "mat parse error, not standard data record", sys.exc_info()
                    
                    #print mdatetime
                    #collection.insert( {"file_path" : fullpath,"time": mdatetime} )
                elif(f_extension == '.abf'):
                    fullpath = join(root, f)
                    mdatetime = datetime.datetime.fromtimestamp(os.path.getmtime(fullpath))
                    print fullpath
                    try:
                        (fnhead, fnum) = parse_fname(f_name)
                        result = collection.find_one({'fnhead': fnhead, 'fnum' : fnum})
                        result['abf_filepath'] = fullpath
                        collection.save(result)
                        
                        #result = collection.find({'fnhead': fnhead, 'fnum' : fnum})
                        #if(result.count() == 1):
                        #    collection.update({'fnhead' : fnhead, 'fnum' : fnum}, {"$set" : {"abf_filepath" : fullpath}})
                    except:
                        print "abf parse error, not standard data record"
                    
                    #print mdatetime
                    #collection.insert( {"file_path" : fullpath,"time": mdatetime} )
                else:
                    print "file type incorrect"









    
    #Post insertion processing
    @staticmethod
    def insert_spikes(collection):
        """
        insert spikes into the documents
        """
        result = collection.find({'mat_filepath':{'$exists':True}, 'spkk':{'$exists':False}})
        i=0
        for r in result:
            try:
                spkk = read_data(r, 'spkk').tolist()
                r['spkk'] = spkk
                collection.save(r)
                print "spkk", i
                i += 1
            except:
                print "insert spkk error"
    @staticmethod
    def insert_input_signal(collection):
        """
        insert downsampled and normalized input signals into the documents
        """
        result = collection.find({'mat_filepath':{'$exists':True}, 'input_signal':{'$exists':False}})
        i=0
        for r in result:
            try:
                (d, normalize_rate) = SpikeDB.downsample(read_data(r, 'c'))
                r['input_signal']=d[:,0].tolist()
                r['normalize_rate']=normalize_rate
                collection.save(r)
                print "input_signal", i
                i += 1
            except:
                print "input signal not inserted"


    @staticmethod
    def remove_final_spike(c):
        """
        Remove the noise spike at the end of a session
        """
        end=c.size-1
        #find last spike
        window = c.size/25
        threshold = 50
        second_last_max = c[end-window:end].max()
        last_max = c[end-window*2:end-window].max()
        end -= 2*window
        while end > window:
            max_v=c[end-window:end].max()
            print max_v
            if abs(max_v - last_max)>abs(threshold*(last_max - second_last_max)):
                print 'spike'
                break;
            else:        
                last_max = max_v;
                
            end -= window
        
        print end   
        try:    
            c[end-window:end+window] = c[end+window:end+3*window]
        except:
            print 'final spike not removed'
        return c

    @staticmethod
    def remove_final_spike_bruteforce(c):
        """
        artificially set a threshold to filter out the spike
        """
        end=c.size-1
        #find last spike
        window = c.size/25
        threshold = 0.15
        last_max = c[end]
        while end > window:
            max_v=c[end-window:end].max()
            print max_v
            if max_v - last_max>threshold:
                print 'spike'
                break;
            else:        
                last_max = max_v;
                
            end -= window
        
        print end   
        try:    
            c[end-window:end+window] = c[end+window:end+3*window]
        except:
            print 'final spike not removed'
        return c
        
    @staticmethod
    def downsample(c):
        """
        Downsample and normalize
        """
        # final spike removal
        d = c[0:c.size:1000]
        normalize_rate = 1/d.max()
        d = d*normalize_rate
        return (d, normalize_rate)


