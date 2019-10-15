#!/usr/bin/env bash
set -ex

############################################
#			Helper functions
############################################
source $(dirname $0)/../ceph-helpers-root.sh

############################################
#			Install required tools
############################################
echo "Install required tools"
install git cmake

CURRENT_PATH=`pwd`

############################################
#			Compile&Start RocksDB
############################################
# install prerequisites
# for rocksdb
case $(distro_id) in
	ubuntu|debian|devuan)
		install g++ libsnappy-dev zlib1g-dev libbz2-dev librados-dev
		;;
	centos|fedora|rhel)
		install gcc-c++.x86_64 snappy-devel zlib zlib-devel bzip2 bzip2-devel librados2-devel.x86_64
		;;
	opensuse*|suse|sles)
		install gcc-c++ snappy-devel zlib-devel libbz2-devel
		;;
	*)
        echo "$(distro_id) is unknown, $@ will have to be installed manually."
        ;;
esac

# # gflags
# sudo yum install gflags-devel
# 
# wget https://github.com/schuhschuh/gflags/archive/master.zip
# unzip master.zip
# cd gflags-master
# mkdir build && cd build
# export CXXFLAGS="-fPIC" && cmake .. && make VERBOSE=1
# make && make install

# # snappy-devel


echo "Compile rocksdb"
if [ -e rocksdb ]; then
	rm -fr rocksdb
fi
git clone https://github.com/facebook/rocksdb.git --depth 1

# compile code
cd rocksdb
mkdir build && cd build && cmake -DWITH_LIBRADOS=ON -DWITH_SNAPPY=ON -DWITH_GFLAGS=OFF -DFAIL_ON_WARNINGS=OFF ..
make rocksdb_env_librados_test -j8

echo "Copy ceph.conf"
# prepare ceph.conf
mkdir -p ../ceph/src/
if [ -f "/etc/ceph/ceph.conf" ]; then
    cp /etc/ceph/ceph.conf ../ceph/src/
elif [ -f "/etc/ceph/ceph/ceph.conf" ]; then
	cp /etc/ceph/ceph/ceph.conf ../ceph/src/
else 
	echo "/etc/ceph/ceph/ceph.conf doesn't exist"
fi

echo "Run EnvLibrados test"
# run test
if [ -f "../ceph/src/ceph.conf" ]
	then
	cp env_librados_test ~/cephtest/archive
	./env_librados_test
else 
	echo "../ceph/src/ceph.conf doesn't exist"
fi
cd ${CURRENT_PATH}
