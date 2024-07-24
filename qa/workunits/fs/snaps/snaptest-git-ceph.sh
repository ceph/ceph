#!/bin/sh -x

set -e

# increase the cache size
sudo git config --global http.sslVerify false
sudo git config --global http.postBuffer 1048576000

# try it again if the clone is slow and the second time
retried=false
trap -- 'retry' EXIT
retry() {
    rm -rf ceph
    # double the timeout value
    timeout 3600 git clone https://git.ceph.com/ceph.git
}
rm -rf ceph
timeout 1800 git clone https://git.ceph.com/ceph.git
trap - EXIT
cd ceph

versions=`seq 1 90`

for v in $versions
do
    if [ $v -eq 48 ]; then
        continue
    fi
    ver="v0.$v"
    echo $ver
    git reset --hard $ver
    mkdir .snap/$ver
done

for v in $versions
do
    if [ $v -eq 48 ]; then
        continue
    fi
    ver="v0.$v"
    echo checking $ver
    cd .snap/$ver
    git diff --exit-code
    cd ../..
done

for v in $versions
do
    if [ $v -eq 48 ]; then
        continue
    fi
    ver="v0.$v"
    rmdir .snap/$ver
done

echo OK
