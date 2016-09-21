#!/bin/bash -ex
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
            sudo apt-get install -y "$@"
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
install git automake

CURRENT_PATH=`pwd`

############################################
#			Compile&Start RocksDB
############################################
# install prerequisites
# for rocksdb
case $(lsb_release -si) in
	Ubuntu|Debian|Devuan)
		install g++-4.7 libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev librados-dev
		;;
	CentOS|Fedora|RedHatEnterpriseServer)
		install gcc-c++.x86_64 gflags-devel snappy-devel zlib zlib-devel bzip2 bzip2-devel librados2-devel.x86_64
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
git clone https://github.com/facebook/rocksdb.git

# compile code
cd rocksdb
make env_librados_test ROCKSDB_USE_LIBRADOS=1 -j8

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
	./env_librados_test
else 
	echo "../ceph/src/ceph.conf doesn't exist"
fi
cd ${CURRENT_PATH}
