#!/bin/bash

rgw_flags="--debug-rgw=20 --debug-ms=1"

function _assert {
  src=$1; shift
  lineno=$1; shift
  [ "$@" ] || echo "$src: $lineno: assert failed: $@" || exit 1
}

assert="eval _assert \$BASH_SOURCE \$LINENO"

function var_to_python_json_index {
  echo "['$1']" | sed "s/\./'\]\['/g"
}

function json_extract {
var=""
[ "$1" != "" ] && var=$(var_to_python_json_index $1)
shift
python - <<END
import json
s='$@'
data = json.loads(s) 
print data$var
END
}

function python_array_len {
python - <<END
arr=$@
print len(arr)
END
}

function project_python_array_field {
var=$(var_to_python_json_index $1)
shift
python - <<END
arr=$@
s='( '
for x in arr:
    s += '"' + str(x$var) + '" '
s += ')'
print s
END
}


x() {
  # echo "x " "$@" >&2
  eval "$@"
}


script_dir=`dirname $0`
root_path=`(cd $script_dir/../..; pwd)`

mstart=$root_path/mstart.sh
mstop=$root_path/mstop.sh
mrun=$root_path/mrun

function start_ceph_cluster {
  [ $# -ne 2 ] && echo "start_ceph_cluster() needs 2 param" && exit 1

  echo "$mstart zg$1-c$2"
}

function rgw_admin {
  [ $# -lt 2 ] && echo "rgw_admin() needs 2 param" && exit 1

  echo "$mrun zg$1-c$2 radosgw-admin"
}

function rgw {
  [ $# -ne 3 ] && echo "rgw() needs 3 params" && exit 1

  echo "$root_path/mrgw.sh zg$1-c$2 $3 $rgw_flags"
}

function init_first_zone {
  [ $# -ne 8 ] && echo "init_first_zone() needs 8 params" && exit 1

  zgid=$1
  cid=$2
  realm=$3
  zg=$4
  zone=$5
  endpoints=$6

  access_key=$7
  secret=$8

# initialize realm
  x $(rgw_admin $zgid $cid) realm create --rgw-realm=$realm

# create zonegroup, zone
  x $(rgw_admin $zgid $cid) zonegroup create --rgw-zonegroup=$zg --master --default
  x $(rgw_admin $zgid $cid) zone create --rgw-zonegroup=$zg --rgw-zone=$zone --access-key=${access_key} --secret=${secret} --endpoints=$endpoints --default
  x $(rgw_admin $zgid $cid) user create --uid=zone.user --display-name="Zone User" --access-key=${access_key} --secret=${secret} --system

  x $(rgw_admin $zgid $cid) period update --commit
}

function init_zone_in_existing_zg {
  [ $# -ne 9 ] && echo "init_zone_in_existing_zg() needs 9 params" && exit 1

  zgid=$1
  cid=$2
  realm=$3
  zg=$4
  zone=$5
  master_zg_zone1_port=$6
  endpoints=$7

  access_key=$8
  secret=$9

  x $(rgw_admin $zgid $cid) realm pull --url=http://localhost:$master_zg_zone1_port --access-key=${access_key} --secret=${secret} --default
  x $(rgw_admin $zgid $cid) zonegroup default --rgw-zonegroup=$zg
  x $(rgw_admin $zgid $cid) zone create --rgw-zonegroup=$zg --rgw-zone=$zone --access-key=${access_key} --secret=${secret} --endpoints=$endpoints
  x $(rgw_admin $zgid $cid) period update --commit --url=http://localhost:$master_zg_zone1_port --access-key=${access_key} --secret=${secret}
}

function init_first_zone_in_slave_zg {
  [ $# -ne 9 ] && echo "init_first_zone_in_slave_zg() needs 9 params" && exit 1

  zgid=$1
  cid=$2
  realm=$3
  zg=$4
  zone=$5 
  master_zg_zone1_port=$6
  endpoints=$7

  access_key=$8
  secret=$9

# create zonegroup, zone
  x $(rgw_admin $zgid $cid) realm pull --url=http://localhost:$master_zg_zone1_port --access-key=${access_key} --secret=${secret}
  x $(rgw_admin $zgid $cid) realm default --rgw-realm=$realm
  x $(rgw_admin $zgid $cid) zonegroup create --rgw-realm=$realm --rgw-zonegroup=$zg --endpoints=$endpoints --default
  x $(rgw_admin $zgid $cid) zonegroup default --rgw-zonegroup=$zg

  x $(rgw_admin $zgid $cid) zone create --rgw-zonegroup=$zg --rgw-zone=$zone --access-key=${access_key} --secret=${secret} --endpoints=$endpoints
  x $(rgw_admin $zgid $cid) zone default --rgw-zone=$zone
  x $(rgw_admin $zgid $cid) zonegroup add --rgw-zonegroup=$zg --rgw-zone=$zone

  x $(rgw_admin $zgid $cid) user create --uid=zone.user --display-name="Zone User" --access-key=${access_key} --secret=${secret} --system
  x $(rgw_admin $zgid $cid) period update --commit --url=localhost:$master_zg_zone1_port --access-key=${access_key} --secret=${secret}

}

function call_rgw_admin {
  zgid=$1
  cid=$2
  shift 2
  x $(rgw_admin $zgid $cid) "$@"
}
