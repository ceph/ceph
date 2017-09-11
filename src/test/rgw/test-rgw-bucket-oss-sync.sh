#!/bin/bash

echo -e "usage: \n\$1:cloud_type\n\$2:domain_name\n\$3:public_key\n\$4:private_key\n\$5:prefix_bucket\n"
set -x

if [ $# -lt 5 ]
  then exit 1
fi

#. "`dirname $0`/test-rgw-common.sh"

cur_dir=`dirname $0`
cur_dir=$(pwd)
echo curdir=$cur_dir

out_path=$cur_dir/run
src_path=$cur_dir/../src

access_key=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 20 | head -n 1)
secret=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 40 | head -n 1)
realm=earth
zg=us
master_id=1
cloud_id=2
s3_id=3
bucket_name=c5-b
bucket_sync=$5$bucket_name
file_src="file_src_test"
s3cfg="s3cfg_ceph"
s3cfg_sync="s3cfg_s3cloud"

MYUID=$(id -u)
MYNAME=$(id -nu)

function stop_rgw {
    pg=`pgrep -u $MYUID -f radosgw`
    [ -n "$pg" ] && kill $pg
    killall -u $MYNAME radosgw
}

function start_ceph_cluster {
  #start ceph cluster
  $src_path/mstart.sh c$1 -n --mds_num 0 --osd_num 1 --mon_num 1
}

function stop_ceph_cluster {
  stop_rgw
  $src_path/stop.sh 
}

function http_endpoint {
  echo http://localhost:800$1
}

function endpoint {
  echo localhost:800$1
}

function init_s3cmd_config {
  #rm $1 >/dev/null 2>&1
  cat > $1 << EOF
[default]
access_key = $2
bucket_location = US
check_ssl_certificate = True
check_ssl_hostname = True
cloudfront_host = $4
default_mime_type = binary/octet-stream
delay_updates = False
delete_after = False
delete_after_fetch = False
delete_removed = False
dry_run = False
enable_multipart = True
encoding = UTF-8
encrypt = False
follow_symlinks = False
force = False
get_continue = False
gpg_command = /usr/bin/gpg
guess_mime_type = True
host_base = $4
host_bucket = $4
human_readable_sizes = False
invalidate_default_index_on_cf = False
invalidate_default_index_root_on_cf = True
invalidate_on_cf = False
limitrate = 0
list_md5 = False
long_listing = False
max_delete = -1
multipart_chunk_size_mb = 15
multipart_max_chunks = 10000
preserve_attrs = True
progress_meter = True
proxy_port = 0
put_continue = False
recursive = False
recv_chunk = 65536
reduced_redundancy = False
requester_pays = False
restore_days = 1
secret_key = $3
send_chunk = 65536
server_side_encryption = False
signature_v2 = False
skip_existing = False
socket_timeout = 300
stats = False
stop_on_error = False
urlencoding_mode = normal
use_https = False
use_mime_magic = True
verbosity = WARNING
EOF

}

function write_master_zone {
  #echo "test file in c$master_id ceph cluster!" > $out_path/$file_src
  dd if=/dev/zero of=$out_path/$file_src bs=1M count=16
  md5=`md5sum $out_path/$file_src | cut -d ' ' -f 1`
 
  init_s3cmd_config $out_path/$s3cfg admin admin $(endpoint $master_id)
  sleep 5s
  s3cmd -c $out_path/$s3cfg mb s3://$bucket_name
  for i in $(seq 1 10)
  do
    if [ $? -eq 0 ]
      then break;
    fi
    sleep 10s
    s3cmd -c $out_path/$s3cfg mb s3://$bucket_name
    echo "s3cmd creaet bucket $bucket_name"
  done
 
  s3cmd -c $out_path/$s3cfg put $out_path/$file_src s3://$bucket_name
 
}

function config_path {
  echo $out_path/c$1/ceph.conf
}

function create_master_zone {
  local zone=us-$master_id
  local endpoints=$(http_endpoint $master_id)
# initialize realm
  $cur_dir/bin/radosgw-admin -c $out_path/c$master_id/ceph.conf realm create --rgw-realm=$realm --default

# create zonegroup, zone
  $cur_dir/bin/radosgw-admin -c $out_path/c$master_id/ceph.conf zonegroup create --rgw-zonegroup=$zg --endpoints=$endpoints --master --default
  $cur_dir/bin/radosgw-admin -c $out_path/c$master_id/ceph.conf zone create --rgw-zonegroup=$zg --rgw-zone=$zone --access-key=${access_key} --secret=${secret} --endpoints=$endpoints --master --default
  $cur_dir/bin/radosgw-admin -c $out_path/c$master_id/ceph.conf user create --uid=zone.user --display-name="Zone User" --access-key=${access_key} --secret=${secret} --system
  $cur_dir/bin/radosgw-admin -c $out_path/c$master_id/ceph.conf user create --uid=admin --display-name="admin" --access-key=admin --secret=admin --system

  $cur_dir/bin/radosgw-admin -c $out_path/c$master_id/ceph.conf period update --commit
 
  $cur_dir/bin/radosgw -c $out_path/c$master_id/ceph.conf --rgw-zone=$zone --log-file=$out_path/c$master_id/out/rgw.log --debug-rgw=20
}

function create_cloud_zone {
  rm $out_path/c$cloud_id/ceph.conf
  mkdir -p $out_path/c$cloud_id/out
  cp $out_path/c$master_id/ceph.conf $out_path/c$cloud_id/
  local replace="rgw frontends = civetweb port=800$cloud_id"
  local line=`sed -n '/rgw frontends/=' $out_path/c$cloud_id/ceph.conf`
  sed -i "${line}c $replace" $out_path/c$cloud_id/ceph.conf
  sed -i "${line}a rgw_zone=us-$cloud_id" $out_path/c$cloud_id/ceph.conf

  replace="log file = $out_path/c$cloud_id/out/\$name.\$pid.log"
  line=`sed -n '/$name.$pid.log/=' $out_path/c$cloud_id/ceph.conf`
  sed -i "${line}c $replace" $out_path/c$cloud_id/ceph.conf

  replace="admin socket = $out_path/c$cloud_id/out/\$name.\$pid.asok"
  line=`sed -n '/$name.$pid.asok/=' $out_path/c$cloud_id/ceph.conf`
  sed -i "${line}c $replace" $out_path/c$cloud_id/ceph.conf

  local zone=us-$cloud_id
  local endpoints=$(http_endpoint $cloud_id)

  $cur_dir/bin/radosgw-admin -c $out_path/c$cloud_id/ceph.conf zone create --rgw-zonegroup=$zg --rgw-zone=$zone --endpoints=$endpoints --access-key=${access_key}  --secret=${secret} --tier-type=cloud
  $cur_dir/bin/radosgw-admin -c $out_path/c$cloud_id/ceph.conf zone modify --rgw-zonegroup=$zg --rgw-zone=$zone --access-key=${access_key}  --secret=${secret} --tier-config=cloud_type=$1,domain_name=$2,public_key=$3,private_key=$4,prefix_bucket=$5
  $cur_dir/bin/radosgw-admin -c $out_path/c$master_id/ceph.conf period update --commit

  $cur_dir/bin/radosgw -c $out_path/c$cloud_id/ceph.conf --rgw-zone=$zone --log-file=$out_path/c$cloud_id/out/rgw.log --debug-rgw=20
  sleep 10s
}

function init_cloud_config {
  rm $out_path/oss_config.cfg
  touch $out_path/oss_config.cfg
  cat > $out_path/oss_config.cfg << EOF
[Credentials]
language = EN
endpoint = $3
accessKeyID = $1
accessKeySecret = $2
EOF

}

function verify_bucket_sync_cloud {
  if [[ ! -f $out_path/ossutil64 ]]
    then
    wget -P $out_path http://docs-aliyun.cn-hangzhou.oss.aliyun-inc.com/assets/attach/50452/cn_zh/1500627059345/ossutil64
  fi
 
  init_cloud_config $3 $4 $2

  local file_download="file_download"
  
  for i in $(seq 1 60)
    do
    sleep 10s
    rm  $out_path/$file_download
    ./ossutil64 -c oss_config cp oss://oss-ucloud-test-c5-b/$file_src $out_path/$file_download
    if [ -f $out_path/$file_download ]
      then break
    fi
  done

  if [ ! -f $out_path/$file_download ]
    then echo "ossutil64 get $file_src failed."
    exit 1
  fi


  local md5_download=`md5sum $out_path/$file_download | cut -d ' ' -f 1`
  rm  $out_path/$file_download

  if [ "$md5_download" != "$md5" ]
    then echo "the file dowload from ufile cloud is not same with the other file from ceph cluster!"
    exit 1
  fi

  s3cmd -c $out_path/$s3cfg del s3://$bucket_name/$file_src
  for i in $(seq 1 60)
    do
    sleep 10s
    rm  $out_path/$file_download
    ./ossutil64 -c oss_config cp oss://oss-ucloud-test-c5-b/$file_src $out_path/$file_download
    if [ ! -f $out_path/$file_download ]
      then break
    fi
  done

  if [ -f $out_path/$file_download ]
    then echo "Test bucket remove sync failed!!!"
  fi

  echo "Test:bucket (data put&remove) sync to oss successed!"
}

stop_ceph_cluster
start_ceph_cluster $master_id
create_master_zone
create_cloud_zone $@
write_master_zone

verify_bucket_sync_cloud $@

stop_ceph_cluster
rm -rf $out_path
