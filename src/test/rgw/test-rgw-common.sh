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
  # echo "x " "$@"
  eval "$@"
}


script_dir=`dirname $0`
root_path=$script_dir/../..

mstart=$root_path/mstart.sh
mstop=$root_path/mstop.sh
mrun=$root_path/mrun

function start_ceph_cluster {
  [ $# -ne 1 ] && echo "start_ceph_cluster() needs 1 param" && exit 1

  echo "$mstart c$1"
}

function rgw_admin {
  [ $# -lt 1 ] && echo "rgw_admin() needs 1 param" && exit 1

  echo "$mrun c$1 radosgw-admin"
}

function rgw {
  [ $# -ne 2 ] && echo "rgw() needs 2 params" && exit 1

  echo "$root_path/mrgw.sh c$1 $2 $rgw_flags"
}

function init_first_zone {
  [ $# -ne 7 ] && echo "init_first_zone() needs 7 params" && exit 1

  id=$1
  realm=$2
  zg=$3
  zone=$4
  port=$5

  access_key=$6
  secret=$7

# initialize realm
  x $(rgw_admin $id) realm create --rgw-realm=$realm

# create zonegroup, zone
  x $(rgw_admin $id) zonegroup create --rgw-zonegroup=$zg --endpoints=http://localhost:$port --master --default
  x $(rgw_admin $id) zone create --rgw-zonegroup=$zg --rgw-zone=$zone --access-key=${access_key} --secret=${secret} --endpoints=http://localhost:$port --default
  x $(rgw_admin $id) user create --uid=zone.user --display-name="Zone User" --access-key=${access_key} --secret=${secret} --system

  x $(rgw_admin $id) period update --commit

  x $(rgw $id $port) --rgw-zone=$zone
}

function init_zone_in_existing_zg {
  [ $# -ne 8 ] && echo "init_zone_in_existing_zg() needs 8 params" && exit 1

  id=$1
  realm=$2
  zg=$3
  zone=$4
  zone1_port=$5
  port=$6

  access_key=$7
  secret=$8

  x $(rgw_admin $id) realm pull --url=http://localhost:$zone1_port --access-key=${access_key} --secret=${secret} --default
  x $(rgw_admin $id) zonegroup default --rgw-zonegroup=$zg
  x $(rgw_admin $id) zone create --rgw-zonegroup=$zg --rgw-zone=$zone --access-key=${access_key} --secret=${secret} --endpoints=http://localhost:$port
  x $(rgw_admin $id) period update --commit

  x $(rgw $id $port) --rgw-zone=$zone
}

function call_rgw_admin {
  c=$1
  shift
  x $(rgw_admin $c) "$@"
}
