#!/bin/sh

if [ `uname` = FreeBSD ]; then
  GETOPT=/usr/local/bin/getopt
else
  GETOPT=getopt
fi

eval set -- "$(${GETOPT} -o i: --long id:,cluster: -- $@)"

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

## should not be changed
conf_path=/etc/ceph
## in case that users have customized conf file name (e.g., ceph-x.conf)
conf_file="${conf_path}/`ls ${conf_path} | grep conf$`"
## customized data path?
data="`egrep 'osd data|osd_data' ${conf_file} | awk '{print $4}'`"
if [ ${data} ]
then
  path=`awk -F $ '{print $1}' <<< "${data}"`
  data=${path}${id}
else
  data="/var/lib/ceph/osd/${cluster:-ceph}-$id"
fi

# assert data directory exists - see http://tracker.ceph.com/issues/17091
if [ ! -d "$data" ]; then
    echo "OSD data directory $data does not exist; bailing out." 1>&2
    exit 1
fi

journal="$data/journal"

if [ -L "$journal" -a ! -e "$journal" ]; then
    udevadm settle --timeout=5 || :
    if [ -L "$journal" -a ! -e "$journal" ]; then
        echo "ceph-osd(${cluster:-ceph}-$id): journal not present, not starting yet." 1>&2
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
