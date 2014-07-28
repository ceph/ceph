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

update="$(ceph-conf --cluster=${cluster:-ceph} --name=osd.$id --lookup osd_crush_update_on_start || :)"

if [ "${update:-1}" = "1" -o "${update:-1}" = "true" ]; then
    # update location in crush
    hook="$(ceph-conf --cluster=${cluster:-ceph} --name=osd.$id --lookup osd_crush_location_hook || :)"
    if [ -z "$hook" ]; then
        hook="/usr/bin/ceph-crush-location"
    fi
    location="$($hook --cluster ${cluster:-ceph} --id $id --type osd)"
    weight="$(ceph-conf --cluster=${cluster:-ceph} --name=osd.$id --lookup osd_crush_initial_weight || :)"
    defaultweight=`df -P -k /var/lib/ceph/osd/${cluster:-ceph}-$id/ | tail -1 | awk '{ d= $2/1073741824 ; r = sprintf("%.2f", d); print r }'`
    ceph \
        --cluster="${cluster:-ceph}" \
        --name="osd.$id" \
        --keyring="/var/lib/ceph/osd/${cluster:-ceph}-$id/keyring" \
        osd crush create-or-move \
        -- \
        "$id" \
        "${weight:-${defaultweight:-1}}" \
        $location
fi

journal="/var/lib/ceph/osd/${cluster:-ceph}-$id/journal"
if [ -L "$journal" -a ! -e "$journal" ]; then
    udevadm settle --timeout=5 || :
    if [ -L "$journal" -a ! -e "$journal" ]; then
        echo "ceph-osd($UPSTART_INSTANCE): journal not present, not starting yet." 1>&2
        stop
        exit 0
    fi
fi
