#!/bin/sh

# Deploy a vstart.sh cluster in a named subdirectory. This makes it possible to
# start multiple clusters in different subdirectories. See mstop.sh for cleanup.
#
# Example:
#
# ~/ceph/build $ MON=1 OSD=1 RGW=1 MDS=0 MGR=0 ../src/mstart.sh c1 -n -d
# ~/ceph/build $ MON=1 OSD=1 RGW=1 MDS=0 MGR=0 ../src/mstart.sh c2 -n -d
#
# ~/ceph/build $ ls run
# c1  c2
# ~/ceph/build $ ls run/c1
# asok  ceph.conf  dev  keyring  out
#
# ~/ceph/build $ ../src/mrun c1 radosgw-admin user list
# [
#     "56789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234",
#     "testx$9876543210abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
#     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
#     "testacct1user",
#     "test",
#     "testacct2root",
#     "testacct1root",
#     "testid"
# ]
#
# ~/ceph/build $ ../src/mstop.sh c1
# ~/ceph/build $ ../src/mstop.sh c2

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

[ -z "$BUILD_DIR" ] && BUILD_DIR=build

if [ -e CMakeCache.txt ]; then
    root_path=$PWD
elif [ -e $root_path/../${BUILD_DIR}/CMakeCache.txt ]; then
    cd $root_path/../${BUILD_DIR}
    root_path=$PWD
fi
RUN_ROOT_PATH=${root_path}/run

mkdir -p $RUN_ROOT_PATH

if [ -z "$CLUSTERS_LIST" ]
then
  CLUSTERS_LIST=$RUN_ROOT_PATH/.clusters.list
fi

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
rgw_port=$((8000+pos*1))

[ -z "$VSTART_DEST" ] && export VSTART_DEST=$RUN_ROOT_PATH/$instance
[ -z "$CEPH_PORT" ] && export CEPH_PORT=$base_port
[ -z "$CEPH_RGW_PORT" ] && export CEPH_RGW_PORT=$rgw_port

mkdir -p $VSTART_DEST

echo "Cluster dest path: $VSTART_DEST"
echo "monitors base port: $CEPH_PORT"
echo "rgw base port: $CEPH_RGW_PORT"

$vstart_path/vstart.sh "$@"
