#!/bin/bash
#
# ganesha_smoke_test.sh
#
# runs on first client node
#
# args: --rgw, --mds

set -e

NFS_MOUNTPOINT=/root/mnt

TEMP=$(getopt -o h --long "mds,rgw" -n 'ganesha_smoke_test.sh' -- "$@")

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

# Note the quotes around TEMP': they are essential!
eval set -- "$TEMP"

MDS=""
RGW=""
while true ; do
    case "$1" in
        --mds) MDS="$1" ; shift ;;
        --rgw) RGW="$1" ; shift ;;
        --) shift ; break ;;
        *) echo "Internal error" ; false ;;
    esac
done

echo "Running ganesha_smoke_test.sh with option(s) $MDS $RGW"

MON_ADDR=$(ceph mon_status --format=json | jq -r .monmap.mons[0].public_addr | sed 's/\([0-9.]\+\):.*/\1/g')

if [ -n "$MDS" ]; then
  CEPHX_ADMIN_KEY=$(ceph auth get client.admin 2>/dev/null | grep key | sed 's/.*key = \(\w\+\)/\1/g')
fi
if [ -n "$RGW" ]; then
  echo "WWWW Applying awful kludge to work around http://tracker.ceph.com/issues/39252 !!!"
  set -x
  ceph dashboard set-rgw-api-host '[::]'
  ceph dashboard set-rgw-api-port 80
  set +x
fi

# preparing dashboard REST API access
API_URL=$(ceph mgr services 2>/dev/null | jq -r .dashboard | sed -e 's!/$!!g')

if [ ! -n "$API_URL" ]; then
  echo "ERROR: dashboard REST API is not available"
  false
fi

# set a password for the admin user
ceph dashboard ac-user-set-password admin admin >/dev/null

# getting authentication token to call the REST API
TOKEN=$(curl --insecure -s -H "Content-Type: application/json" -X POST \
            -d '{"username":"admin","password":"admin"}'  $API_URL/api/auth \
            | jq -r .token)

function rest_call {
  local method=$1
  local path=$2
  local data=$3

  result=$(curl --insecure -s -b /tmp/cd-cookie.txt -H "Authorization: Bearer $TOKEN " \
               -H "Content-Type: application/json" -X $method -d "$data" \
               ${API_URL}$path)
  echo "$result"
}

function get_ganesha_daemons {
  daemons=$(rest_call GET /api/nfs-ganesha/daemon)
  daemons=$(echo $daemons | jq -r .[].daemon_id)
  echo "$daemons" | sed ':a;N;$!ba;s/\n/,/g'
}

function array_to_json {
  local array=("$@")
  result="["
  result+=$(echo "${array[@]}" | sed 's/\(\w\+\)/"\1"/g' | sed 's/ /, /g')
  result+="]"
  echo $result
}

function create_cephfs_export {
  local path=$1
  local daemons=$2

  cat >/tmp/export.json <<EOF
{
  "cluster_id": "_default_",
  "path": "$path",
  "fsal": {
    "name": "CEPH",
    "user_id": "admin",
    "fs_name": "cephfs",
    "sec_label_xattr": null
  },
  "pseudo": "/cephfs$path",
  "tag": null,
  "access_type": "RW",
  "squash": "no_root_squash",
  "protocols": [3, 4],
  "transports": ["TCP"],
  "security_label": false,
  "daemons": ${daemons},
  "clients": []
}
EOF

  result=$(rest_call POST /api/nfs-ganesha/export "$(cat /tmp/export.json)")
  echo $result | jq -r .export_id
}

function create_rgw_export {
  local path=$1
  local daemons=$2

  cat >/tmp/export.json <<EOF
{
  "cluster_id": "_default_",
  "path": "$path",
  "fsal": {
    "name": "RGW",
    "rgw_user_id": "admin"
  },
  "pseudo": "/rgw/$path",
  "tag": null,
  "access_type": "RW",
  "squash": "no_root_squash",
  "protocols": [3, 4],
  "transports": ["TCP"],
  "security_label": false,
  "daemons": ${daemons},
  "clients": []
}
EOF

  result=$(rest_call POST /api/nfs-ganesha/export "$(cat /tmp/export.json)")
  echo $result | jq -r .export_id
}

function delete_export {
  local export_id=$1
  result=$(rest_call DELETE /api/nfs-ganesha/export/_default_/$export_id)
  echo $result
}

function mount_export {
  local daemon=$1
  local ex_path=$2
  local fs_path=$3

  test ! -e $fs_path
  mkdir -p $fs_path
  test -d $fs_path
  mount -t nfs $daemon:$ex_path $fs_path
}

function umount_export {
  local fs_path=$1
  test -d $fs_path
  umount $fs_path
  rmdir $fs_path
}

function test_cephfs_export {
  local ex_path=$1

  echo "Creating CephFS export"
  EXPORT_ID=$(create_cephfs_export "$ex_path" "$(array_to_json ${daemons[@]})")
  sleep 3

  for n in "${daemons[@]}"; do
    showmount -e $n
  done

  echo "Mount CephFS export path=$ex_path"
  mount_export ${daemons[0]} "/" "$NFS_MOUNTPOINT"
  echo "Writing into CephFS export"
  echo "Hello World!" > "$NFS_MOUNTPOINT/cephfs${ex_path}/hello.txt"
  echo "Unmount CephFS export"
  umount_export "$NFS_MOUNTPOINT"
  delete_export $EXPORT_ID
  sleep 3

  echo "Mounting CephFS filesystem using kernel client to check hello.txt"
  mkdir -p $NFS_MOUNTPOINT
  mount -t ceph $MON_ADDR:/ $NFS_MOUNTPOINT -o name=admin,secret=$CEPHX_ADMIN_KEY
  text=`cat ${NFS_MOUNTPOINT}${ex_path}/hello.txt`
  if [ ! -n "$text" ]; then
    echo "ERROR: hello world text not found"
    false
  fi
  umount $NFS_MOUNTPOINT
  rmdir $NFS_MOUNTPOINT
}

function test_rgw_export {
  local ex_path=$1

  echo "Creating RGW export"
  EXPORT_ID=$(create_rgw_export "$ex_path" "$(array_to_json ${daemons[@]})")
  sleep 3

  for n in "${daemons[@]}"; do
    showmount -e $n
  done

  echo "Mount RGW export path=$ex_path"
  mount_export ${daemons[0]} "/" "$NFS_MOUNTPOINT"
  echo "Writing into RGW export"
  echo "Hello World!" > "$NFS_MOUNTPOINT/rgw/${ex_path}/hello.txt"
  echo "Unmount RGW export"
  umount_export "$NFS_MOUNTPOINT"
  delete_export $EXPORT_ID
  sleep 3

  found=`radosgw-admin bi get --object=hello.txt --bucket=$ex_path | jq -r .entry.name`
  if [ ! -n "$found" ]; then
    echo "ERROR: hello.txt not found in bucket $ex_path"
    false
  fi
}


set -x

echo "install nfs-client RPM" >/dev/null
zypper --non-interactive --gpg-auto-import-keys install --no-recommends nfs-client

IFS="," read -r -a daemons <<< "$(get_ganesha_daemons)"
echo "Ganesha daemons: ${daemons[@]}"

if [ -n "$MDS" ]; then
  test_cephfs_export "/"

  test_cephfs_export "/test1"
fi

if [ -n "$RGW" ]; then
  test_rgw_export "bucket1"

  test_rgw_export "bucket2"
fi

echo OK>/devnull

