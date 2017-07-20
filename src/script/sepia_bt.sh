#!/usr/bin/env bash

function die() {
    echo $@ >&2
    exit 1
}

function usage() {
    echo "bt: $0 -c core_path [-d distro] [-C directory] [-v]"
    exit 1
}

function log() {
    if [ -n "$verbose" ]; then
        echo $*
    fi
}

function get_machine() {
    local core_path=$1
    local machine=${core_path%/coredump/*}
    echo $(basename $machine)
}

function get_t_dir() {
    local core_path=$1
    echo ${core_path%/remote/*}
}

while getopts  "c:C:d:v" opt
do
    case $opt in
        c) core_path=$OPTARG;;
        C) wd=$OPTARG;;
        d) codename=$OPTARG;;
        v) verbose=1;;
        *) usage;;
    esac
done

if [ -z $core_path ]; then
    usage
fi

sha1=$(strings $core_path | gawk 'BEGIN{ FS = "=" } /^CEPH_REF/{print $2}')
if [ -z $sha1 ]; then
    teuthology_log=$(get_t_dir $core_path)/teuthology.log
    sha1=$(grep -m1 -A1 "Running command: sudo ceph --version" ${teuthology_log} | tail -n1 | grep -oP "ceph version [^ ]+ \(\K[^\) ]+")
fi

if [ -z $distro ]; then
    machine=$(get_machine $core_path)
    teuthology_log=$(get_t_dir $core_path)/teuthology.log
    if [ ! -r ${teuthology_log} ]; then
        die "missing distro, and unable to read it from ${teuthology_log}"
    fi
    ld=$(grep -m1 -A1 "${machine}:Running.*linux_distribution" ${teuthology_log} | tail -n1 | grep -oP "\(\K[^\)]+")
    distro=$(echo $ld | gawk -F ", " '{print $1}' | sed s/\'//g)
    distro=$(echo $distro | tr '[:upper:]' '[:lower:]')
    distro_ver=$(echo $ld | gawk -F ", " '{print $2}' | sed s/\'//g)
    codename=$(echo $ld | gawk -F ", " '{print $3}' | sed s/\'//g)
    if [ "$distro" == "centos linux" ]; then
        # there is chance that it's actually something different,
        # but we take it as centos7 anyway.
        distro=centos
        distro_ver=7
    fi
else
    case $codename in
        xenial)
            distro=ubuntu
            distro_ver=16.04
            ;;
        trusty)
            distro=ubuntu
            distro_ver=14.04
            ;;
        centos7)
            distro=centos
            distro_ver=7
        ;;
        *)
            die "unknown distro: $distro"
            ;;
    esac
fi

# try to figure out a name for working directory
if [ -z $wd ]; then
    run=${core_path%/remote/*}
    job_id=${run#/a/}
    if [ $job_id != $core_path ]; then
        # use the run/job for the working dir
        wd=$job_id
    fi
fi

if [ -z $wd ]; then
   wd=$(basename $core_path)
   wd=${wd%.*}
   echo "unable to figure out the working directory, using ${wd}"
fi

log "creating ${wd}"
mkdir -p $wd
cd $wd

prog=$(file $core_path | grep -oP "from '\K[^']+" | cut -d' ' -f1)
case $prog in
    ceph_test_*)
        pkgs="ceph-test librados2"
        ;;
    ceph-osd|ceph-mon)
        pkgs=$prog
        ;;
    */python*)
        prog=$(basename $prog)
        pkgs=librados2
        ;;
    rados)
        pkgs="ceph-common librados2 libradosstriper1"
        ;;
    *)
        die "unknown prog: $prog"
        ;;
esac

flavor=default
arch=x86_64

release=$(strings $core_path | grep -m1  -oP '/build/ceph-\K[^/]+')
if [ -z $release ]; then
    teuthology_log=$(get_t_dir $core_path)/teuthology.log
    release=$(grep -m1 -A1 "Running command: sudo ceph --version" ${teuthology_log} | tail -n1 | grep -oP "ceph version \K[^ ]+")
fi

case $distro in
    ubuntu)
        pkg_path=pool/main/c/ceph/%s_%s-1${codename}_amd64.deb
        for p in $pkgs; do
            t="$t $p $p-dbg"
        done
        pkgs="$t"
        ;;
    centos)
        pkg_path=${arch}/%s-%s.el7.x86_64.rpm
        # 11.0.2-1022-g5b25cd3 => 11.0.2-1022.g5b25cd3
        release=$(echo $release | sed s/-/./2)
        pkgs="$pkgs ceph-debuginfo"
        ;;
    *)
        die "unknown distro: $distro"
        ;;
esac

query_url="https://shaman.ceph.com/api/search?status=ready&project=ceph&flavor=${flavor}&distros=${distro}%2F${distro_ver}%2F${arch}&sha1=${sha1}"
repo_url=`curl -L -s "${query_url}" | jq -r '.[0] | .url'`
pkg_url=${repo_url}/${pkg_path}
log "repo url is ${repo_url}"

for pkg in ${pkgs}; do
    url=`printf $pkg_url $pkg $release`
    log "downloading ${url}"
    curl -O -L -C - --silent --fail --insecure $url
    fname=`basename $url`
    case $fname in
        *.deb)
            ar p `basename $fname` data.tar.xz | tar xJv;;
        *.rpm)
            rpm2cpio < $fname | cpio -id;;
        *)
    esac
done

cat > preclude.gdb <<EOF
set sysroot .
set debug-file-directory ./usr/lib/debug
set solib-search-path ./usr/lib64
file ./usr/bin/$prog
core $core_path
EOF
gdb -x preclude.gdb
