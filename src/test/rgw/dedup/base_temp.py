#!/usr/bin/python3 -tt
import os
import sys
import subprocess
import json
import argparse
import random
import threading
import boto3
import logging
import time
import datetime
import pytz
import re
import pdb
import hashlib

OBJ_PART_SIZE = (4*1024*1024)
OUT_DIR="/tmp/dedup"
RADOSGW_COMMAND="cd ../../../build; bin/radosgw-admin "
OBJ_PUT_CMD=RADOSGW_COMMAND + "object put "
#-------------------------------------------------------------------------------
def print_stats(name, size_kb):
    print("%s %d KiB, %.2f MiB, %.2f GiB" %
          (name, size_kb, float(size_kb)/1024, float(size_kb)/(1024*1024)) )
    #print("%d::size =%d:: %.2f MiB" % (i, size,  float(size) /(1024*1024)) )
    #print("%d::size2=%d:: %.2f MiB" % (i, size2, float(size2)/(1024*1024)) )

#-----------------------------------------------
def write_random(size):
    #filename = OUT_DIR + "/OBJ_" + str(size) + ":" + str(i)
    filename = OUT_DIR + "/OBJ_" + str(size)
    print(filename)
    fout = open(filename, "wb")
    fout.write(os.urandom(size))
    fout.close()

#-----------------------------------------------
def gen_files(start_size, factor, out):

    size = start_size
    for f in range(1, factor+1):
        size2 = size + random.randint(1, OBJ_PART_SIZE)
        write_random(size)
        write_random(size2)
        size  = size * 2;
        print("==============================")

#-----------------------------------------------
def cleanup_local(out_dir):
    print("Removing old directory " + out_dir)
    subprocess.run(["/bin/rm", "-rf", out_dir])

#-----------------------------------------------
def simple_dedup(out_dir):
    cleanup_local(out_dir)
    os.mkdir(out_dir)
    gen_files(OBJ_PART_SIZE, 4, 3)

    command = OBJ_PUT_CMD + "--bucket=" + "bucket1" + " --object=" + "osmx4" + " --infile=" + "/tmp/dedup/OBJ_33554432"
    subprocess.check_output(command, shell=True)
    #subprocess.run(command)
    #bin/radosgw-admin object put  --bucket=bucket1 --object=osdmaptool --infile=bin/osdmaptool

#--------------------------------------
def main():
    print("calling simple_dedup")
    simple_dedup(OUT_DIR)

#--------------------------------------
if __name__ == '__main__':
    main()
