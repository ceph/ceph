#!/usr/bin/python

import re
import os
import sys
import getopt
import requests
import argparse

def get_filepaths(directory):
    file_paths = []  
    for root, directories, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)
    return file_paths

def checkBrokenLink(url):
    if requests.get(url).ok == True:
        return False
    else:
        return True

def scrapLink(filePath):
    fo=open(filePath,'r')
    data=fo.read()
    urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', data)
    for url in urls:
        if checkBrokenLink(url):
            print("Found broken link : "+url+"\n\t in file: "+fo.name+"\n\t at:"+os.path.dirname(filePath)+"\n \n")
            if log == True:
                fs.write("Found broken link : "+url+"\n\t in file: "+fo.name+"\n\t at:"+os.path.dirname(filePath)+"\n \n")
        else:
            if verbose == True:
                print ("Checked working link : "+url+"\n\t in file: "+fo.name+"\n\t at:"+os.path.dirname(filePath)+"\n \n")
            if log == True:
                fs.write("Checked working link : "+url+"\n\t in file: "+fo.name+"\n\t at:"+os.path.dirname(filePath)+"\n \n")
 
def scanFiles(files):
    for file in files:
        filename, file_ext=os.path.splitext(file)
        if allFiles==True or file_ext in types:
            scrapLink(file)

parser=argparse.ArgumentParser()
parser.add_argument('-d','--directory',required=False,action='store',dest='path',default='.',help="Provide directory, where to look at")
parser.add_argument('-a','--all',required=False,action='store_true',dest='allFiles',help="Look at all files")
parser.add_argument('-v','--verbose',required=False,action='store_true',dest='verbose',help="Print all URLs being checked")
parser.add_argument('-l','--log',required=False,action='store_true',dest='log',help="Write the logs to a file")
parser.add_argument('-ld','--log_dir',required=False,action='store',dest='logPath',default=".",help="Specify the log directory/file")
parser.add_argument('-t','--types',required=False,action='append',dest='types',default=['.html','.css','.xml','.md','.txt',''],help="Provide directory, where to look at")

result=parser.parse_args()
allFiles=result.allFiles
log=result.log
logPath=result.logPath
verbose=result.verbose
path=result.path
types=result.types
files=[]
filename="BrokenLinks.log"

if os.path.isfile(logPath):
    try:
        fs.open(logPath,'w')
    except IOError:
        print('cannot open, probably incorrect path', logPath)
        fs.close()
else:
    if log == True:
        if os.path.exists(logPath) and os.path.isabs(logPath):
            fs=open(os.path.join(logPath,filename),'w')
        else:
            logPath=os.path.join(os.path.dirname(os.path.abspath(__file__)),logPath)
            if os.path.exists(logPath):
                fs=open(os.path.join(logPath,filename),'w')
            else:
                print("The log path you entered doesn't seem right.\n")
                sys.exit()

if os.path.exists(path) and os.path.isabs(path):
    files=get_filepaths(path)
    if files:
        scanFiles(files)
else:
    path=os.path.join(os.path.dirname(os.path.abspath(__file__)),path)
    if os.path.exists(path):
        files=get_filepaths(path)
        if files:
            scanFiles(files)
    else:
        print("The directory path you entered doesn't seem right.\n")
        sys.exit()
