#!/bin/sh -x

set -e

# increase the cache size
sudo git config --global http.sslVerify false
sudo git config --global http.postBuffer 1024MB # default is 1MB
sudo git config --global http.maxRequestBuffer 100M # default is 10MB
sudo git config --global core.compression 0

# enable the debug logs for git clone
export GIT_TRACE_PACKET=1
export GIT_TRACE=1
export GIT_CURL_VERBOSE=1

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

# disable the debug logs for git clone
export GIT_TRACE_PACKET=0
export GIT_TRACE=0
export GIT_CURL_VERBOSE=0

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
