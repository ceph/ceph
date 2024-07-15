iopool=$2
iosize=$4 # 4K
iotype=$3 # rand req
rw=$1

rbd bench --io-type ${rw} ${iopool}/rbd1 --io-size ${iosize} --io-threads 16 --io-total 500G --io-pattern ${iotype}
