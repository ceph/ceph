#!/bin/bash

eval set -- "$(getopt -o n: --long name:,cluster: -- $@)"

while true ; do
  case "$1" in
    -n|--name) name=$2; shift 2 ;;
    --cluster) cluster=$2; shift 2 ;;
    --) shift ; break ;;
  esac
done

CEPHCONF=`which ceph-conf`
RADOSGW=`which radosgw`

if [ -z "${CEPHCONF}"  ]; then
  CEPHCONF=/usr/bin/${CEPHCONF}
fi

if [ ! -x "${CEPHCONF}" ]; then
  echo "${CEPHCONF} could not start, it is not executable."
  exit 1
fi

if [ -z "$RADOSGW"  ]; then
  RADOSGW=/usr/bin/radosgw
fi

if [ ! -x "$RADOSGW" ]; then
  echo "$RADOSGW could not start, it is not executable."
  exit 1
fi

# prefix for radosgw instances in ceph.conf
PREFIX='client.radosgw.'

if [ -z "$name"  ]; then
  echo "no name paramter"
  exit 1
fi

if [ -z "$cluster"  ]; then
  cluster="ceph"
fi

ceph_conf_file="/etc/ceph/${cluster}.conf"

if [ ! -f "${ceph_conf_file}" ] ; then
  echo "ceph config file not found: $ceph_conf_file"
  exit 1
fi

longname=${PREFIX}${name}
testname=$(${CEPHCONF} -c ${ceph_conf_file} --list-sections $PREFIX | grep $longname )

if [ -z "$testname"  ]; then
  echo "error parsing '$name' : valid types are: $(echo $(${CEPHCONF} -c ${ceph_conf_file} --list-sections $PREFIX | sed s/$PREFIX//))"
  exit 1
fi

auto_start=`${CEPHCONF} -c ${ceph_conf_file} -n $longname 'auto start'`
if [ "$auto_start" = "no" ] || [ "$auto_start" = "false" ] || [ "$auto_start" = "0" ]; then
  echo "ceph.conf:[$longname], says not to start."
  exit 1
fi

# is the socket defined?  if it's not, this instance shouldn't run as a daemon.
rgw_socket=`$RADOSGW -c ${ceph_conf_file} -n $longname --show-config-value rgw_socket_path`
if [ -z "$rgw_socket" ]; then
  echo "socket $rgw_socket could not be found in ceph.conf:[$longname], not starting."
  exit 1
fi

# mapped to this host?
host=`${CEPHCONF} -c ${ceph_conf_file} -n $longname host`
hostname=`hostname -s`
if [ "$host" != "$hostname" ]; then
  echo "hostname $hostname could not be found in ceph.conf:[$longname], not starting."
  exit 1
fi

user=`${CEPHCONF} -c ${ceph_conf_file} -n $longname user`
if [ -n "$user" ]; then
  if [ "$USER" != "$user" ]; then
    echo "enviroment \$USER '$USER' does not match '$longname' user '$user'"
    exit 1
  fi
fi


log_file=`$RADOSGW -c ${ceph_conf_file} -n $longname --show-config-value log_file`
if [ -n "$log_file" ]; then
  if [ ! -f "$log_file" ]; then
    touch "$log_file"
    touchrc=$?
    if [ 0 != $touchrc ] ; then
      exit $touchrc
    fi
  fi
fi
