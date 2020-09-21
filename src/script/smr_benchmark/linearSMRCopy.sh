#! /usr/bin/env bash

# copy a linear file from srcFile to destination SMRDisk in a loop until writeSize MBs is written
# SMRDisk is the SMR Host Aware / Host Managed Disk eg. /dev/sdb

usage(){
	echo "linearSMRCopy.sh <srcFile> <SMRDisk> <writeSize (MB)>"
}

if [ "$#" -lt 3 ]; then
	usage
	exit
fi

if [ "$(id -u)" != "0" ]; then
	echo "Please run as sudo user"
	exit
fi

if which zbc_open_zone > /dev/null 2>&1 && which zbc_read_zone > /dev/null 2>&1 && which zbc_write_zone > /dev/null 2>&1 ; then
	echo "libzbc commands present... refreshing zones"
	# reset all write pointers before starting to write
	sudo zbc_reset_write_ptr /dev/sdb -1
else
	echo "libzbc commands not detected. Please install libzbc"
	exit
fi

srcFile=$1
SMRDisk=$2
writeSize=$3
iosize=10240

numberOfSectors=$(($writeSize * 2048))

smrZoneStart=33554432 # TODO query this from SMR drive

#dd if=$srcFile of=$destDisk seek=$smrZoneStart bs=512

fileSize=`stat --printf="%s" $srcFile`

if [ "$(($fileSize % 512))" -ne 0 ]; then
	echo "$srcFile not 512 byte aligned"
	exit
fi

sectorsLeftToWrite=$(($fileSize / 512))

znum=64 # TODO query this from SMR Drive

zoneLength=524288 # number of sectors in each zone TODO query from SMR drive

writeOffset=$smrZoneStart

sectorsLeftToWrite=$numberOfSectors

echo "write begin sectors Left = $sectorsLeftToWrite, writeOffset = $writeOffset zone Num = $znum"

while [ "$sectorsLeftToWrite" -gt 0 ]; 
do
	sudo zbc_open_zone $SMRDisk $znum 
        sudo time zbc_write_zone -f $srcFile -loop $SMRDisk $znum $iosize
	sudo zbc_close_zone /dev/sdb $znum
	writeOffset=$(($writeOffset+$zoneLength))
	znum=$(($znum+1))
	sectorsLeftToWrite=$(($sectorsLeftToWrite - $zoneLength))
done

echo "write end sectors Left = $sectorsLeftToWrite, writeOffset = $writeOffset zone Num = $znum"
