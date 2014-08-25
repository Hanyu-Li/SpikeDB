"""
SpikeDB utility functions
"""
import numpy as np
import scipy.io
def parse_fname(fname):
    div = fname.rfind('_')
    f_date = fname[0:div+1]
    f_num = fname[div+1:]
    f_num = int(f_num)
    return (f_date,f_num)

def compute_distance(a, b):
    return np.sum(np.abs(a-b)) 

def read_data(r, data_name):
    """
    read data with data_name inside the mat file
    """
    rf = r.get('mat_filepath')
    try:
        mat = scipy.io.loadmat(rf)
        data = mat[data_name]
        #print rf
        return data
    except:
        #print rf
        print "data not found in this file"

def read_data_b(r, data_name):
    """
    read data with data_name inside 20xx_xx_xx_xxb.mat file
    """
    rf = r.get('mat_filepath_b')
    try:
        mat = scipy.io.loadmat(rf)
        data = mat[data_name]
        #print rf
        return data
    except:
        #print rf
        print "data not found in this file"
        
def EDR_reader(edr_path):
    """
    Read EDR file
    """
    edr_file = open(edr_path,'r')
    h = {}
    for line in edr_file:
        k = line.find('=')
        #print line
        if k:
            key = line[0:k]
            #print key
            if key[0:2] == 'YN' or key[0:2] == 'YU' or key == 'TU' or key == 'ID' or key == 'BAK' or key == 'WCPFNAM' or key == 'CTIME' or key == 'MKTXT0':
                h[key] = line[k+1:len(line)-2]
            elif key == 'DETPOSPK' or key == 'DETBASST' or key == 'DETBASSUB':
                h[key] = line[k+1:len(line)]
            else:
                try:
                    h[key] = float(line[k+1:len(line)-2])
                    #print h.get(key)
                except:
                    break;
    edr_file.seek(h.get('NBH'), 0)
    data = np.fromfile(edr_file,np.short)
    #print data
    newsize = (h.get('NC'), h.get('NP')/h.get('NC'))
    data = np.reshape(data, newsize,2)
    #show()
    for Ch in range(0,int(h.get('NC'))):
        YCFCh = 'YCF'+str(Ch)
        data[Ch,:] = (h.get('AD') / ((h.get('ADCMAX')+1)*h.get(YCFCh)))*data[Ch,:]

    time = np.arange(0,(h.get('DT')*(h.get('NP')/h.get('NC') )), h.get('DT'))

    final_data =  np.vstack((time,data))
    #print final_data
    #plot(final_data[0,:])
    #show()
    edr_file.close();
    return final_data



