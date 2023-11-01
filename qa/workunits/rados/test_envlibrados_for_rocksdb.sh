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

CURRENT_PATH=`pwd`

############################################
#			Compile&Start RocksDB
############################################
# install prerequisites
# for rocksdb
case $(distro_id) in
	ubuntu|debian|devuan|softiron)
		install git g++ libsnappy-dev zlib1g-dev libbz2-dev libradospp-dev cmake
		;;
	centos|fedora|rhel)
        case $(distro_id) in
            rhel)
                # RHEL needs CRB repo for snappy-devel
                sudo dnf config-manager --set-enabled "codeready-builder-for-rhel-8-x86_64-rpms" 
                ;;
        esac
        install git gcc-c++.x86_64 snappy-devel zlib zlib-devel bzip2 bzip2-devel libradospp-devel.x86_64 cmake libarchive-3.3.3
        ;;
	opensuse*|suse|sles)
		install git gcc-c++ snappy-devel zlib-devel libbz2-devel libradospp-devel
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

pushd $(dirname /home/ubuntu/cephtest/clone.client.0/qa/workunits/rados/bash.sh)/../../../
git submodule update --init --progress src/rocksdb
popd
git clone $(dirname /home/ubuntu/cephtest/clone.client.0/qa/workunits/rados/bash.sh)/../../../src/rocksdb rocksdb

# compile code
cd rocksdb
if type cmake3 > /dev/null 2>&1 ; then
    CMAKE=cmake3
else
    CMAKE=cmake
fi

[ -z "$BUILD_DIR" ] && BUILD_DIR=build
mkdir ${BUILD_DIR} && cd ${BUILD_DIR} && ${CMAKE} -DCMAKE_BUILD_TYPE=Debug -DWITH_TESTS=ON -DWITH_LIBRADOS=ON -DWITH_SNAPPY=ON -DWITH_GFLAGS=OFF -DFAIL_ON_WARNINGS=OFF ..
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
