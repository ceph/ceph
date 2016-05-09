#!/bin/sh

eval set -- "$(getopt -o i: --long id:,cluster: -- $@)"

while true ; do
	case "$1" in
		-i|--id) id=$2; shift 2 ;;
		--cluster) cluster=$2; shift 2 ;;
		--) shift ; break ;;
	esac
done

if [ -z "$id"  ]; then
    echo "Usage: $0 [OPTIONS]"
    echo "--id/-i ID        set ID portion of my name"
    echo "--cluster NAME    set cluster name (default: ceph)"
    exit 1;
fi

data="/var/lib/ceph/osd/${cluster:-ceph}-$id"
journal="$data/journal"

if [ -L "$journal" -a ! -e "$journal" ]; then
    udevadm settle --timeout=5 || :
    if [ -L "$journal" -a ! -e "$journal" ]; then
        echo "ceph-osd($UPSTART_INSTANCE): journal not present, not starting yet." 1>&2
        stop
        exit 0
    fi
fi


# ensure ownership is correct
owner=`stat -c %U $data/.`
if [ $owner != 'ceph' -a $owner != 'root' ]; then
    echo "ceph-osd data dir $data is not owned by 'ceph' or 'root'"
    echo "you must 'chown -R ceph:ceph ...' or similar to fix ownership"
    exit 1
fi

exit 0
