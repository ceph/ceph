#!/bin/bash -f

RGW_BASE="http://localhost:800"
INFLATOR=(7937)
FILE_COUNT=$((64*1024))
BUCKET_COUNT=5
OUT_DIR="/tmp/tmp_objs"
BASE_FILE_NAME_SHORT="file_"
BASE_FILE_NAME="$OUT_DIR/$BASE_FILE_NAME_SHORT"

if [[ $# -ne 5 ]]; then
    echo "usage: $0 <start_range>, <count>, <multiplier>, <seed>, <log-file>"
    exit 1
fi

start_range=$1
count=$2
multiplier=$3
seed=$4
logfile=$5
end_range=$(($start_range+$count))
MAX_COUNT=$(($count*$multiplier))
#echo "start_range = $start_range, end_range = $end_range, count=$count, max_count=$MAX_COUNT"
#RANDOM=$seed
objs_count=0
PRINT_RANGE=1000
MAX_COPIES=3
SINGLETON_COUNT=0
UNIQUE_COUNT=0;
DUP_COUNT=0
rgw=0
RGW_COUNT=3

#generate RGW objects
for i in `seq $start_range $(($end_range-1))`
do
    file_name="$BASE_FILE_NAME${i}"
    short_name=$BASE_FILE_NAME_SHORT${i}
    bucket_num=$(($RANDOM % $BUCKET_COUNT +1 ))

    #copies_count=$(( ($RANDOM % MAX_COPIES) + 1 ))
    val=$(($RANDOM % 7))
    if [ $val -lt 4 ]; then
	copies_count=1
    elif [ $val -lt 6 ]; then
	copies_count=2
    else
	copies_count=3
    fi
    
    if [ $copies_count -eq 1 ]; then
	SINGLETON_COUNT=$(($SINGLETON_COUNT + 1))
	#echo "singleton ${short_name}_"
    else
	#echo "dups (${copies_count}) ${short_name}_"
	UNIQUE_COUNT=$(($UNIQUE_COUNT + 1))
	tmp=$(($copies_count - 1))
	DUP_COUNT=$(($DUP_COUNT + $tmp))
    fi

    #rgw=$((($rgw + 1) % $RGW_COUNT))
    for j in `seq 1 $copies_count`
    do
	echo "$short_name"
	#echo "s3://bucket${bucket_num}/${short_name}_${j}_"
	s5cmd cp $file_name s3://bucket${bucket_num}/${short_name}_${j}_ > /dev/null

	objs_count=$(($objs_count + 1))
	if [ $(( $objs_count % $PRINT_RANGE )) == 0 ]; then
	    echo "$objs_count objects were created"
	fi
    done

    if [ $objs_count -ge $MAX_COUNT ]; then
	echo "objs_count ($objs_count) exceeds limit ($max_count), breaking.."
	break
    fi

    expected_singleton_count=$((($i-$start_range) + 1 - $UNIQUE_COUNT))
    if [ $SINGLETON_COUNT -ne $expected_singleton_count ]; then
	echo "i=$i, unique_count=$UNIQUE_COUNT, singleton_count=$SINGLETON_COUNT"
	break
    fi
done

#echo "=================================="
echo "@OBJS_COUNT=$objs_count"
echo "@SINGLETON_COUNT=$SINGLETON_COUNT"
echo "@UNIQUE_COUNT=$UNIQUE_COUNT"
echo "@DUP_COUNT=$DUP_COUNT"
#echo "=================================="
