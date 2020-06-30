#!/bin/sh

usage() {
    local prog_name=$1
    shift
    cat <<EOF
usage:
  $prog_name [options] <config-file>...

options:
  -a,--archive-dir    directory in which the test result is stored, default to $PWD/cbt-archive
  --build-dir         directory where CMakeCache.txt is located, default to $PWD
  --cbt               directory of cbt if you have already a copy of it. ceph/cbt:master will be cloned from github if not specified
  -h,--help           print this help message
  --source-dir        the path to the top level of Ceph source tree, default to $PWD/..
  --use-existing      do not setup/teardown a vstart cluster for testing

example:
  $prog_name --cbt ~/dev/cbt -a /tmp ../src/test/crimson/cbt/radosbench_4K_read.yaml
EOF
}

prog_name=$(basename $0)
archive_dir=$PWD/cbt-archive
build_dir=$PWD
source_dir=$(dirname $PWD)
use_existing=false
classical=false
opts=$(getopt --options "a:h" --longoptions "archive-dir:,build-dir:,source-dir:,cbt:,help,use-existing,classical" --name $prog_name -- "$@")
eval set -- "$opts"

while true; do
    case "$1" in
        -a|--archive-dir)
            archive_dir=$2
            shift 2
            ;;
        --build-dir)
            build_dir=$2
            shift 2
            ;;
        --source-dir)
            source_dir=$2
            shift 2
            ;;
        --cbt)
            cbt_dir=$2
            shift 2
            ;;
        --use-existing)
            use_existing=true
            shift
            ;;
        --classical)
            classical=true
            shift
            ;;
        -h|--help)
            usage $prog_name
            return 0
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "unexpected argument $1" 1>&2
            return 1
            ;;
    esac
done

if test $# -gt 0; then
    config_files="$@"
else
    echo "$prog_name: please specify one or more .yaml files" 1>&2
    usage $prog_name
    return 1
fi

if test -z "$cbt_dir"; then
    cbt_dir=$PWD/cbt
    git clone --depth 1 -b master https://github.com/ceph/cbt.git $cbt_dir
fi

# store absolute path before changing cwd
source_dir=$(readlink -f $source_dir)
if ! $use_existing; then
    cd $build_dir
    # seastar uses 128*8 aio in reactor for io and 10003 aio for events pooling
    # for each core, if it fails to enough aio context, the seastar application
    # bails out. and take other process into consideration, let's make it
    # 32768 per core
    max_io=$(expr 32768 \* $(nproc))
    if test $(/sbin/sysctl --values fs.aio-max-nr) -lt $max_io; then
        sudo /sbin/sysctl -q -w fs.aio-max-nr=$max_io
    fi
    if $classical; then
        MDS=0 MGR=1 OSD=3 MON=1 $source_dir/src/vstart.sh -n -X \
           --without-dashboard
    else
        MDS=0 MGR=1 OSD=3 MON=1 $source_dir/src/vstart.sh -n -X \
           --without-dashboard --memstore \
           -o "memstore_device_bytes=34359738368" \
           --crimson --nodaemon --redirect-output \
           --osd-args "--memory 4G"
    fi
    cd -
fi

for config_file in $config_files; do
    echo "testing $config_file"
    cbt_config=$(mktemp $config_file.XXXX.yaml)
    python3 $source_dir/src/test/crimson/cbt/t2c.py \
        --build-dir $build_dir \
        --input $config_file \
        --output $cbt_config
    python3 $cbt_dir/cbt.py \
        --archive $archive_dir \
        --conf $build_dir/ceph.conf \
        $cbt_config
    rm -f $cbt_config
done

if ! $use_existing; then
    cd $build_dir
    if $classical; then
      $source_dir/src/stop.sh
    else
      $source_dir/src/stop.sh --crimson
    fi
fi
