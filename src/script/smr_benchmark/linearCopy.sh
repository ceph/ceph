#!/usr/bin/env bash

# copy a linear file from srcFile to destination disk in a loop until writeSize MBs is written
# destinationDisk is a SMR Host Aware Disk eg. /dev/sdb

if [ "$#" -lt 3 ]; then
	echo "Usage ./linearCopy.sh srcFile destinationDisk writeSize(MB)"
	exit
fi

if [ "$(id -u)" != "0" ]; then
	echo "Please run as sudo user"
	exit
fi

srcFile=$1
destDisk=$2
writeSize=$3
verbose=true

if [ -f time ]; then
	rm -rf time
fi

#chunkSize=4096 # in bytes
chunkSize=1048576 # in bytes
fileSize=`stat --printf="%s" $srcFile`

numChunksInFile=`echo "$fileSize * (1048576 / $chunkSize)" | bc`
chunksLeft=$(( $(($writeSize * 1048576)) / $chunkSize))


echo "fileSize = $fileSize"

if [ "$(($fileSize % 512))" -ne 0 ]; then
	echo "$srcFile not 512 byte aligned"
	exit
fi

if [ "$(($chunkSize % 512))" -ne 0 ]; then
	echo "$chunkSize not 512 byte aligned"
	exit
fi

if [ "$fileSize" -lt "$chunkSize" ]; then
	echo "filesize $fileSize should be greater than chunkSize $chunkSize"
	exit
fi


numFileChunks=$(($fileSize / $chunkSize))
if [ $verbose == true ]; then
	echo "numFileChunks = $numFileChunks"
fi

smrLBAStart=33554432 # TODO query from SMR Drive
#smrLBAStart=37224448

offset=$(( $smrLBAStart / $(( $chunkSize / 512)) ))

if [ $verbose == true ]; then
	echo "chunksLeft = $chunksLeft, offset = $offset"
fi

chunkNum=0

while [ "$chunksLeft" -gt 0 ]; do
	chunkNum=$(($chunkNum + 1))
	if [ $verbose == true ]; then
		echo "CHUNK $chunkNum `date +%H:%M:%S`" >> time
	fi
	dd if=$srcFile of=$destDisk seek=$offset bs=$chunkSize 2> tmp 
	cat tmp | grep MB >> time # > /dev/null 2>&1
	if [ $verbose == true ]; then
		echo "chunksLeft = $chunksLeft, offset = $offset"
	fi
	chunksLeft=$(($chunksLeft - $numFileChunks))
	offset=$(($offset + $numFileChunks))
done

if [ -f tmp ]; then
	rm tmp
fi

if [ $verbose == false ]; then
	rm time
else
	echo "Time Stamp for Chunk Writes"
	cat time
	rm time
fi
