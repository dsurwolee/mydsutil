import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import subprocess
import csv
import os.path
import argparse
from sets import Set
import re
import json
#import glob
import os
# import pandas as pd
# from tqdm import tqdm
import random
import string
import io
import multiprocessing
from functools import partial

timeformat='%Y-%m-%dT%H'
filetype='Realtimesuccess.json'


def getTimeVariable(start,end):
    s = datetime.datetime.strptime(start, timeformat)
    e = datetime.datetime.strptime(end, timeformat)
    d = datetime.timedelta(hours=1)
    return s, e, d

def getDefaultHeaderList(fileList,peek_header,peek_value):
    headerListTmp=Set()
    count=0
    for filename in fileList[0]:
        if os.path.exists(filename):
            with open(filename, 'rb') as f:
                #print('Collecting headers from: ' + filename)
                for line in f:
                    try:
                        header = json.loads(line).keys()
                        headerListTmp.update(header)
                        count+=1
                        if peek_header:
                            if count >peek_value:
                                count=0
                                break
                    except ValueError:
                        print(line)
    return headerListTmp

def check(regex_true,given,field):
    if regex_true:
        return any(tmp in field for tmp in given)
    return (field in given)
                        
def cleanup_headers(headerListTmp, regex_true, include,cols):
    if cols:
        headerListGiven = cols.split(',')
    else:
        headerListGiven=headerListTmp[:]
    if include:
        headerList=[]
    else:
        headerList=headerListTmp[:]
    for each in headerListTmp:
        if check(regex_true,headerListGiven,each):
            if include:
                headerList.append(each)
            else:
                headerList.remove(each)
    return headerList

def createFileList(start, end, path):
    startTime, endTime, delta = getTimeVariable(start, end)
    fileList=[]
    while startTime<endTime:
        d = startTime.strftime(timeformat+'H')
        fileList.append((path+'/%s-'+filetype)%(d))
        startTime += delta
    return fileList

def mergeOutput(fileList,outputFile, headerList):
    with open(outputFile+'-'+str(fileList[1])+'.csv','w') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=headerList,quoting=csv.QUOTE_MINIMAL, quotechar='~',escapechar='\\', extrasaction='ignore', doublequote=False)
        writer.writerow(dict((fn,fn) for fn in headerList))
        for filename in fileList[0]:
            if os.path.exists(filename):
                with open(filename,'r') as f_in:
                    #print('Collecting data from: '+filename)
                    for row in f_in:
                        try:
                            line = json.loads(row)
                            writer.writerow(line)
                        except ValueError:
                            continue

def mergeOutputPandas(saveDir, output_file):
    '''
    merges all json files together using pandas
    :param saveDir:
    :param output_file:
    :return:
    '''
    path = saveDir
    allFiles = glob.glob(path + "/*.json")
    frame = pd.DataFrame()

    list_ = []
    for file_ in tqdm(allFiles):
        df = pd.read_json(file_, lines=True)
        list_.append(df)

    frame = pd.concat(list_)#, sort=False)

    print("Write file to disk ...")
    frame.to_csv(saveDir+'/' + output_file)

def getDownloadCmd(downloadCmd, unzipCmd, start, end):
    downloadCmdList = []
    unzipCmdList = []
    startTime, endTime, delta = getTimeVariable(start, end)
    while startTime < endTime:
        d = startTime.strftime(timeformat+'H')
        y = startTime.strftime('%Y')
        m = startTime.strftime('%m')
        d_ = startTime.strftime('%d')
        downloadCmdList.append(downloadCmd.format(d, y, m, d_))
        unzipCmdList.append(unzipCmd.format(d))
        startTime += delta
    return downloadCmdList,unzipCmdList

def downloadUnzipData(cmd):
    p = subprocess.Popen(cmd, shell=True)
    p.communicate()

def initMultiProcess(args):
    global counter
    counter = args

def getDefaultCmd(ip):
    cmdDict = {}
    cmdDict['google']='gsutil -m cp '+ip+'/{1}/{2}/{3}'
    cmdDict['aws']='aws sc cp '+ip+'/{1}/{2}/{3}'
    cmdDict['local']='cp '+ip
    return cmdDict


def getData(argObject):
    '''
    :param argObject:
    :return:
    '''
    use_pandas = False  # argObject['use_pandas']

    suffix= argObject['appPrefix']+'/'+argObject['entityName']
    if argObject['isCompressed']:
        pre='.gz'
    else:
        pre=''

    op=argObject['outputPath']+suffix
    ip=argObject['inputPath']+suffix
    pool = multiprocessing.Pool(argObject['max_threads'])

    cmd = getDefaultCmd(ip)[argObject['storageType']] + '/{0}-'+filetype+pre+' '+op+'/{0}-'+filetype+pre
    cmd2 = 'gzip -f -d '+op+'/{0}-'+filetype+pre
    final_= argObject['startDate']+'-'+argObject['endDate']
    fileList = createFileList(argObject['startDate'], argObject['endDate'], op)

    dwnld_cmd, unzip_cmd = getDownloadCmd(cmd,cmd2,argObject['startDate'],argObject['endDate'])
    chunkTime = datetime.datetime.now()
    if argObject['downloadData']:
        pool.map(downloadUnzipData,dwnld_cmd)
        if argObject['isCompressed']:
            pool.map(downloadUnzipData, unzip_cmd)
    print "Time taken to download and unzip files",datetime.datetime.now() - chunkTime

    size = len(fileList)/argObject['max_threads']
    fileListMulti = [(fileList[i:i + size], i/size) for i in
                     range(0, len(fileList), size)]


    print("Finished unzipping. Start merging ...")
    if use_pandas:
        mergeOutputPandas(op, final_)
    else:
        headerListFinal = []
        if argObject['include'] and not argObject['regex_true'] and argObject['cols']:
            headerListFinal = argObject['cols'].split(',')
        else:
            chunkTime = datetime.datetime.now()
            finalHeaderSet = Set()
            headerSetList = pool.map(partial(getDefaultHeaderList, peek_header=argObject['peek_header'], peek_value=argObject['peek_value']), fileListMulti)
            for x in headerSetList: finalHeaderSet.update(x)
            headerListFinal = cleanup_headers(list(finalHeaderSet), argObject['regex_true'],
                                              argObject['include'], argObject['cols'])
            print "Time taken to collect headers", datetime.datetime.now() - chunkTime
        pool.map(partial(mergeOutput,outputFile=op+'/'+final_,headerList=headerListFinal),fileListMulti)

    print("Finished!")

def str_to_bool(val):
    """Converts a string value to boolean.
    Args:
    val: [str] The string value to parse.
    Retruns:
    [bool] The boolean representation of the string.
    """
    false_opts = ['false', 'f', '0', 'no', 'n']
    true_opts = ['true', 't', '1', 'yes', 'y']
    clean_val = str(val).lower()
    if clean_val in false_opts:
        return False
    elif clean_val in true_opts:
        return True
    else:
        raise ArgumentError(
            "Option can only be one of: %s or %s" % (false_opts, true_opts))


def parse_args():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Merge json dumps of pipeline outputs given appPrefix, entityName, path to download from, and date range. Optionally also take include/exclude columns')
    parser.register('type',bool,str_to_bool)
    parser.add_argument(
        '--start_date',
        type=str,
        default=None,
        help="Start date inclusive (eg: '2018-02-01T02')")
    parser.add_argument(
        '--end_date',
        type=str,
        default=None,
        help="End date exclusive (eg: '2018-02-01T05')")
    parser.add_argument(
        '--app_prefix',
        type=str,
        default='simility',
        help='App prefix for which to do merge. Default=simility')
    parser.add_argument(
        '--entity_name',
        type=str,
        default='transactions',
        help='Entity name for which to do merge. Deafult=transactions')
    parser.add_argument(
        '--include',
        type=bool,
        default=True,
        help='Flag to include cols or exclude - true/false. (Default: true)')
    parser.add_argument(
        '--regex',
        type=bool,
        default=False,
        help='flag to set if providing regex for including/excluding columns - true/false. (Default: false)')
    parser.add_argument(
        '--cols',
        type=str,
        default=None,
        help='list of columns(or keywords in case of regex)')
    parser.add_argument(
        '--output_path',
        type=str,
        default='/mnt/druidtmp/merge-csv/',
        help='Output path where to put merge file. Default: /mnt/druidtmp/merge-csv/')
    parser.add_argument(
        '--storage_type',
        type=str,
        default='google',
        help='Where pipeline output json files are stored - options are google,aws,local. Default: google')
    parser.add_argument(
        '--is_compressed',
        type=bool,
        default=True,
        help='Are files compressed. Default: true')

    # parser.add_argument(
    #     '--use_pandas',
    #     type=bool,
    #     default=False,
    #     help='Merges jsons to csv using pandas. Default: false')

    parser.add_argument(
        '--max_threads',
        type=int,
        default=8,
        help='Number of parallel download processes. Default: 8')

    parser.add_argument(
        '--peek_header',
        type=bool,
        default=False,
        help='Instead of reading all json rows to get header list, peek over first x rows of each file to create final header list. Default: False')

    parser.add_argument(
        '--peek_value',
        type=int,
        default=50,
        help='Read first this many rows of each file to create final header list. Default: 50')

    parser.add_argument(
        '--input_path',
        type=str,
        default='gs://simility-druid-storage/pipelineOutput/',
        help='Where to procure data from. For google: it is gs_bucket, for AWS: it is s3 bucket, for local: it is local system filepath.\n Default: gs://simility-druid-storage/pipelineOutput/')
    parser.add_argument(
	'--download_data',
	type=bool,
	default=True,
	help='Is data to be downloaded. Default: True')
    return parser
        

def main(_):
    """
    main function
    """
    args = parse_args().parse_args()
    if len(sys.argv) < 7:
        parse_args().print_help()
        exit(0)
    argumentObject={}
    argumentObject['startDate'] = args.start_date
    argumentObject['endDate']= args.end_date
    argumentObject['appPrefix']=args.app_prefix
    argumentObject['entityName']=args.entity_name
    argumentObject['include']=args.include
    argumentObject['cols']=args.cols
    argumentObject['outputPath']=args.output_path
    argumentObject['regex_true']=args.regex
    argumentObject['storageType']=args.storage_type
    argumentObject['inputPath']=args.input_path
    argumentObject['isCompressed']=args.is_compressed
    argumentObject['downloadData']=args.download_data
    argumentObject['max_threads']=args.max_threads
    argumentObject['peek_header'] = args.max_threads
    argumentObject['peek_value'] = args.peek_value
    # argumentObject['use_pandas']=args.use_pandas

    chunkTime = datetime.datetime.now()
    getData(argumentObject)
    print "Total time taken", datetime.datetime.now()-chunkTime
    

if __name__=='__main__':
    main(sys.argv)
