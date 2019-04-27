#!/usr/bin/env bash
set -ex

############################################
#			Helper functions
############################################
function install() {
    for package in "$@" ; do
        install_one $package
    done
    return 0
}

function install_one() {
    case $(lsb_release -si) in
        Ubuntu|Debian|Devuan)
            sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y "$@"
            ;;
        CentOS|Fedora|RedHatEnterpriseServer)
            sudo yum install -y "$@"
            ;;
        *SUSE*)
            sudo zypper --non-interactive install "$@"
            ;;
        *)
            echo "$(lsb_release -si) is unknown, $@ will have to be installed manually."
            ;;
    esac
}
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
case $(lsb_release -si) in
	Ubuntu|Debian|Devuan)
		install g++ libsnappy-dev zlib1g-dev libbz2-dev libradospp-dev
		;;
	CentOS|Fedora|RedHatEnterpriseServer)
		install gcc-c++.x86_64 snappy-devel zlib zlib-devel bzip2 bzip2-devel libradospp-devel.x86_64
		;;
	*)
        echo "$(lsb_release -si) is unknown, $@ will have to be installed manually."
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
