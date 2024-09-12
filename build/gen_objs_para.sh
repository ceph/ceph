#!/bin/bash -f
export S3_ENDPOINT_URL="http://localhost:8000"
BUCKET_COUNT=5
INFLATOR=(5107)
FILE_COUNT=$((128*1024))
MAX_FILE_SIZE=$((4*1024))
OUT_DIR="/tmp/tmp_objs"
BASE_FILE_NAME_SHORT="file_"
BASE_FILE_NAME="$OUT_DIR/$BASE_FILE_NAME_SHORT"
PRINT_RANGE=(1000)
RGW_BASE="http://localhost:800"
THREAD_COUNT=16
range=$(($FILE_COUNT/$THREAD_COUNT))
offset=0
seed_array=( 17977 17981 17987 17989 18013 18041 18043 18047 18049 18059 18061 18077 18089 18097 18119 18121 )

if [ ! -e $OUT_DIR ]; then
    need_to_generate_files=1
    start_num=1
else
    start_num=`ls -l $OUT_DIR | wc -l`
    num=$(( $start_num -1 ))
    if [ $num -ge $FILE_COUNT ]; then
	need_to_generate_files=0
    else
	missing_files=$(( $FILE_COUNT - $num ))
	echo "existing count = $num, requested count = $FILE_COUNT"
	echo "need to create $missing_files more files"
	need_to_generate_files=1
    fi
fi
# first create out dir
if [ $need_to_generate_files -eq 1 ]; then
    if [ ! -e $OUT_DIR ]; then
	echo "mkdir $OUT_DIR"
	mkdir $OUT_DIR
    fi
    # generate random files into dir
    for ((i=$start_num;i<=$FILE_COUNT;i++));
    do
	# RANDOM range is 0-32K => multiply by INFLATOR to get bigger random range
	file_size=$(( ($RANDOM*$INFLATOR) % MAX_FILE_SIZE + 1 ))
	file_name="$BASE_FILE_NAME${i}"

	if [ $(( $i % $PRINT_RANGE )) == 0 ]; then
	    echo "$i files were created"
	fi

	dd if=/dev/urandom of=$file_name bs=$file_size count=1 >& /dev/null
    done
fi

OUT_FILE=/tmp/out
find $OUT_DIR -type f -exec md5sum {} + | sort | uniq -w32 -dD > $OUT_FILE
head $OUT_FILE
if [ -s $OUT_FILE ]; then
    # The file is not-empty.
    echo "files are not unique!!"
    exit 1
else
    # The file is empty.
    echo "files are unique"
fi

#generate RGW buckets
for bucket_num in `seq 1 $BUCKET_COUNT`
do
    #echo "s5cmd mb s3://bucket${bucket_num}"
    s5cmd mb s3://bucket${bucket_num}  >& /dev/null
done

s5cmd ls

multiplier=16
pids=()
for t in `seq 0 $(($THREAD_COUNT - 1))`
do
    seed=${seed_array[$t]}
    echo "thread=$t seed=$seed"
    #echo "gen_objs_range $(($t * $range + $offset)) $range $multiplier $seed > /tmp/thread${t}.log"
    ./gen_objs_range.sh  $(($t * $range + $offset)) $range $multiplier $seed "/tmp/thread${t}.log" > /tmp/thread${t}.log &
    pid=$!    
    pids[${t}]=$pid
    #echo "pids[$t]=$pid"
done


objs_count=0
singleton_count=0
unique_count=0
dup_count=0

# wait for all pids
t=0
for pid in ${pids[*]}; do
    #echo "wait $pid ..."
    wait $pid

    a=`grep OBJS_COUNT /tmp/thread${t}.log | cut -d= -f2`
    objs_count=$(($objs_count+$a))
    a=`grep SINGLETON_COUNT /tmp/thread${t}.log | cut -d= -f2`
    singleton_count=$(($singleton_count+$a))
    a=`grep UNIQUE_COUNT /tmp/thread${t}.log | cut -d= -f2`
    unique_count=$(($unique_count+$a))
    a=`grep DUP_COUNT /tmp/thread${t}.log | cut -d= -f2`
    dup_count=$(($dup_count+$a))

    t=$(($t+1))
done

echo "=================================="
echo "OBJS_COUNT      = $objs_count"
echo "SINGLETON_COUNT = $singleton_count"
echo "UNIQUE_COUNT    = $unique_count"
echo "DUP_COUNT       = $dup_count"
echo "=================================="
