#!/bin/sh

usage="usage: $0 <name> [vstart options]..\n"

usage_exit() {
	printf "$usage"
	exit
}

[ $# -lt 1 ] && usage_exit


instance=$1
shift

vstart_path=`dirname $0`

root_path=`dirname $0`
root_path=`(cd $root_path; pwd)`

if [ -e CMakeCache.txt ]; then
    root_path=$PWD
fi
RUN_ROOT_PATH=${root_path}/run
CLUSTERS_LIST=$RUN_ROOT_PATH/.clusters.list

mkdir -p $RUN_ROOT_PATH


if [ ! -f $CLUSTERS_LIST ]; then
touch $CLUSTERS_LIST
fi

pos=`grep -n -w $instance $CLUSTERS_LIST`
if [ $? -ne 0 ]; then
  echo $instance >> $CLUSTERS_LIST
  pos=`grep -n -w $instance $CLUSTERS_LIST`
fi

pos=`echo $pos | cut -d: -f1`
base_port=$((6800+pos*20))

export VSTART_DEST=$RUN_ROOT_PATH/$instance
export CEPH_PORT=$base_port

mkdir -p $VSTART_DEST

echo "Cluster dest path: $VSTART_DEST"
echo "monitors base port: $CEPH_PORT"

$vstart_path/vstart.sh "$@"
