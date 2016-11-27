#!/bin/sh

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

prog=`file $core_path | grep -oP "from '\K[^ ]+"`
case $prog in
    ceph-osd|ceph-mon)
        ;;
    *)
        die "unknown prog: $prog";;
esac

case $distro in
    ubuntu)
        base_url=http://gitbuilder.ceph.com/ceph-deb-trusty-x86_64-basic/sha1/%s/pool/main/c/ceph/%s_%s-1trusty_amd64.deb;
        pkgs="$prog $prog-dbg";;
    centos)
        # 11.0.2-1022-g5b25cd3 => 11.0.2-1022.g5b25cd3
        release=$(echo $release | sed s/-/./2)
        base_url=http://gitbuilder.ceph.com/ceph-rpm-centos7-x86_64-basic/sha1/%s/x86_64/%s-%s.x64_64.rpm;
        pkgs="$prog ceph-debuginfo"
    *)
        die "unknown distro: $distro";;
esac

mkdir -p $run/$job
cd $run/$job

for pkg in pkgs; do
    url=`printf $base_url $sha1 $pkg $release`
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
