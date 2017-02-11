#!/bin/bash

function die() {
    echo $@ >&2
    exit 1
}

function usage() {
    echo "bt: $0 -j job_name -c core_path -v version -d distro -s sha1"
    exit 1
}

while getopts  "j:c:v:d:s:" opt
do
    case $opt in
        j) run=$(dirname $OPTARG);
           job=$(basename $OPTARG);;
        c) core_path=$OPTARG;;
        v) release=$OPTARG;;
        s) sha1=$OPTARG;;
        d) distro=$OPTARG;;
        *) usage;;
    esac
done

if [ -z $run ] || [ -z $core_path ] || [ -z $release ] || [ -z $distro ] || [ -z $sha1 ]; then
    usage
fi

prog=`file $core_path | grep -oP "from '\K[^']+"`
case $prog in
    ceph_test_*)
        pkgs="ceph-test librados2"
        ;;
    ceph-osd|ceph-mon)
        pkgs=$prog
        ;;
    */python*)
        pkgs=librados2
        ;;
    rados)
        pkgs="ceph-common librados2 libradosstriper1"
    *)
        die "unknown prog: $prog"
        ;;
esac

flavor=default
arch=x86_64

case $distro in
    xenial)
        codename=$distro
        distro=ubuntu
        distro_ver=16.04
        ;;
    trusty)
        codename=$distro
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

case $distro in
    ubuntu)
        pkg_path=pool/main/c/ceph/%s_%s-1${codename}_amd64.deb
        for p in $pkgs; do
            t="$t $p $p-dbg"
        done
        pkgs="$t"
        ;;
    centos)
        pkg_path=${arch}/%s-%s.x86_64.rpm
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

mkdir -p $run/$job
cd $run/$job

for pkg in ${pkgs}; do
    url=`printf $pkg_url $pkg $release`
    wget $url
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
file ./usr/bin/$prog
core $core_path
EOF
gdb -x preclude.gdb
