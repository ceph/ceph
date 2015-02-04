#!/bin/bash
#
# Copyright (C) 2015 Ubuntu Kylin
#
# Author: Min Chen <minchen@ubuntukylin.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#

# unit test case for rbd-recover-tool

#prepare:
# - write config files: config/osd_host, config/mon_host, config/storage_path, config/mds_host if exist mds
#step 1. rbd export all images as you need
#step 2. stop all ceph services
#step 3. use ceph_rbd_recover_tool to recover all images
#step 4. compare md5sum of recover image with that of export image who has the same image name

ssh_opt="-o ConnectTimeout=1"
my_dir=$(dirname "$0")
tool_dir=$my_dir

#storage_path=$my_dir/config/storage_path
mon_host=$my_dir/config/mon_host
osd_host=$my_dir/config/osd_host
mds_host=$my_dir/config/mds_host

test_dir= # `cat $storage_path`
export_dir= #$test_dir/export
recover_dir= #$test_dir/recover
image_names= #$test_dir/image_names
online_images= #$test_dir/online_images, all images on ceph rbd pool
gen_db= #$test_dir/gen_db, label database if exist
pool=rbd
pool_id=2

function get_pool_id()
{
  local pool_id_file=/tmp/pool_id_file.$$$$
  ceph osd pool stats $pool|head -n 1|awk '{print $4}' >$pool_id_file
  if [ $? -ne 0 ];then
    echo "$func: get pool id failed: pool = $pool"
    rm -f $pool_id_file
    exit
  fi
  pool_id=`cat $pool_id_file`
  echo "$func: pool_id = $pool_id"
  rm -f $pool_id_file
}

function init()
{
  local func="init"
  if [ $# -eq 0 ];then
    echo "$func: must input <path> to storage images, enough disk space is good"
    exit
  fi
  if [ ! -s $osd_host ];then
    echo "$func: config/osd_host not exists or empty"
    exit
  fi
  if [ ! -s $mon_host ];then
    echo "$func: config/mon_host not exists or empty"
    exit
  fi
  if [ ! -e $mds_host ];then
    echo "$func: config/mds_host not exists"
    exit
  fi
  test_dir=$1
  export_dir=$test_dir/export
  recover_dir=$test_dir/recover
  image_names=$test_dir/image_names
  online_images=$test_dir/online_images
  gen_db=$test_dir/gen_db

  trap 'echo "ceph cluster is stopped ..."; exit;' INT
  ceph -s >/dev/null
  get_pool_id

  mkdir -p $test_dir
  mkdir -p $export_dir
  mkdir -p $recover_dir
  rm -rf $export_dir/*
  rm -rf $recover_dir/*
}

function do_gen_database()
{
  local func="do_gen_database"
  if [ -s $gen_db ] && [ `cat $gen_db` = 1 ];then
    echo "$func: database already existed"
    exit
  fi
  bash $tool_dir/rbd-recover-tool database
  echo 1 >$gen_db 
}

#check if all ceph processes are stopped
function check_ceph_service()
{
  local func="check_ceph_service"
  local res=`cat $osd_host $mon_host $mds_host|sort -u|tr -d [:blank:]|xargs -n 1 -I @ ssh $ssh_opt @ "ps aux|grep -E \"(ceph-osd|ceph-mon|ceph-mds)\"|grep -v grep"`
  if [ "$res"x != ""x ];then
    echo "$func: NOT all ceph services are stopped"
    return 1
    exit
  fi
  echo "$func: all ceph services are stopped"
  return 0
}

function stop_ceph()
{
  local func="stop_ceph"
  #cat osd_host|xargs -n 1 -I @ ssh $ssh_opt @ "killall ceph-osd" 
  while read osd
  do
  {
    osd=`echo $osd|tr -d [:blank:]`
    if [ "$osd"x = ""x ];then
      continue
    fi
    #ssh $ssh_opt $osd "killall ceph-osd ceph-mon ceph-mds" </dev/null
    ssh $ssh_opt $osd "killall ceph-osd" </dev/null
  } &
  done < $osd_host
  wait
  echo "waiting kill all osd ..."
  sleep 1
  #cat $mon_host|xargs -n 1 -I @ ssh $ssh_opt @ "killall ceph-mon ceph-osd ceph-mds" 
  cat $mon_host|xargs -n 1 -I @ ssh $ssh_opt @ "killall ceph-mon" 
  #cat $mds_host|xargs -n 1 -I @ ssh $ssh_opt @ "killall ceph-mds ceph-mon ceph-osd" 
  cat $mds_host|xargs -n 1 -I @ ssh $ssh_opt @ "killall ceph-mds" 
}

function create_image()
{
  local func="create_image"
  if [ ${#} -lt 3 ];then
    echo "create_image: parameters: <image_name> <size> <image_format>"
    exit
  fi
  local image_name=$1
  local size=$2
  local image_format=$3
  if [ $image_format -lt 1 ] || [ $image_format -gt 2 ];then
    echo "$func: image_format must be 1 or 2"
    exit
  fi
  local res=`rbd list|grep -E "^$1$"` 
  echo "$func $image_name ..."
  if [ "$res"x = ""x ];then
    rbd -p $pool create $image_name --size $size --image_format $image_format
  else
    if [ $image_format -eq 2 ];then
      rbd snap ls $image_name|tail -n +2|awk '{print $2}'|xargs -n 1 -I % rbd snap unprotect $image_name@%
    fi
    rbd snap purge $image_name
    #rbd rm $image_name
    rbd -p $pool resize --allow-shrink --size $size $image_name
  fi
}

function export_image()
{
  local func="export_image"

  if [ $# -lt 2 ];then
    echo "$func: parameters: <image_name> <image_format> [<image_size>]"
    exit
  fi

  local image_name=$1
  local format=$(($2)) 
  local size=$(($3)) #MB
  
  if [ $format -ne 1 ] && [ $format -ne 2 ];then
    echo "$func: image format must be 1 or 2"
    exit
  fi

  if [ $size -eq 0 ];then
    size=24 #MB
    echo "$func: size = $size"
  fi
  local mnt=/rbdfuse 

  mount |grep "rbd-fuse on /rbdfuse" &>/dev/null
  if [ $? -ne 0 ];then
    rbd-fuse $mnt
  fi
    
  create_image $image_name $size $format
 
  dd conv=notrunc if=/dev/urandom of=$mnt/$image_name bs=4M count=$(($size/4))
  
  local export_image_dir=$export_dir/pool_$pool_id/$image_name
  mkdir -p $export_image_dir
  local export_md5_nosnap=$export_image_dir/@md5_nosnap
  >$export_md5_nosnap
 
  local export_image_path=$export_image_dir/$image_name
  rm -f $export_image_path

  rbd export $pool/$image_name $export_image_path
  md5sum $export_image_path |awk '{print $1}' >$export_md5_nosnap 
}

function recover_image()
{
  local func="recover_snapshots"
  if [ $# -lt 1 ];then
    echo "$func: parameters: <image_name>"
    exit
  fi

  local image_name=$1
  #pool_id=29

  local recover_image_dir=$recover_dir/pool_$pool_id/$image_name
  mkdir -p $recover_image_dir
  local recover_md5_nosnap=$recover_image_dir/@md5_nosnap
  >$recover_md5_nosnap
  local snapshot=
  
  bash $tool_dir/rbd-recover-tool recover $pool_id/$image_name $recover_dir
  md5sum $recover_image_dir/$image_name|awk '{print $1}' >$recover_md5_nosnap
}

function make_snapshot()
{
  local func="make_snapshot"
  if [ $# -lt 5 ];then
    echo "$func: parameters: <ofile> <seek> <count> <snap> <export_image_dir>"
    exit
  fi
  local ofile=$1
  local seek=$(($2))
  local count=$(($3))
  local snap=$4
  local export_image_dir=$5

  if [ $seek -lt 0 ];then
    echo "$func: seek can not be minus"
    exit
  fi

  if [ $count -lt 1 ];then
    echo "$func: count must great than zero"
    exit
  fi

  echo "[$snap] $func ..."
  echo "$1 $2 $3 $4"
  rbd snap ls $image_name|grep $snap;
  
  local res=$?
  if [ $res -eq 0 ];then
    return $res
  fi

  dd conv=notrunc if=/dev/urandom of=$ofile bs=1M count=$count seek=$seek 2>/dev/null
  snapshot=$image_name@$snap 
  rbd snap create $snapshot
  rm -f $export_image_dir/$snapshot
  rbd export $pool/$image_name $export_image_dir/$snapshot
  pushd $export_image_dir >/dev/null
  md5sum $snapshot >> @md5
  popd >/dev/null
}

function recover_snapshots()
{
  local func="recover_snapshots"
  if [ $# -lt 1 ];then
    echo "$func: parameters: <image_name>"
    exit
  fi

  local image_name=$1
  #pool_id=29

  local recover_image_dir=$recover_dir/pool_$pool_id/$image_name
  mkdir -p $recover_image_dir
  local recover_md5=$recover_image_dir/@md5
  >$recover_md5
  local snapshot=

  
  # recover head
  bash $tool_dir/rbd-recover-tool recover $pool_id/$image_name $recover_dir

  # recover snapshots
  for((i=1; i<10; i++))
  do
    snapshot=snap$i
    bash $tool_dir/rbd-recover-tool recover $pool_id/$image_name@$snapshot $recover_dir
    pushd $recover_image_dir >/dev/null
    local chksum=`md5sum $image_name|awk '{print $1}'` 
    echo "$chksum  $image_name@$snapshot" >>@md5
    popd >/dev/null
  done
}

function export_snapshots()
{
  local func="export_snapshots"

  if [ $# -lt 2 ];then
    echo "$func: parameters: <image_name> <image_format> [<image_size>]"
    exit
  fi

  local image_name=$1
  local format=$(($2)) 
  local size=$(($3)) #MB
  
  if [ $format -ne 1 ] && [ $format -ne 2 ];then
    echo "$func: image format must be 1 or 2"
    exit
  fi

  if [ $size -eq 0 ];then
    size=24 #MB
    echo "$func: size = $size"
  fi
  local mnt=/rbdfuse 

  mount |grep "rbd-fuse on /rbdfuse" &>/dev/null
  if [ $? -ne 0 ];then
    rbd-fuse $mnt
  fi
    
  create_image $image_name $size $format
  
  local export_image_dir=$export_dir/pool_$pool_id/$image_name
  mkdir -p $export_image_dir
  local export_md5=$export_image_dir/@md5
  >$export_md5

  # create 9 snapshots
  # image = {object0, object1, object2, object3, object4, object5, ...}
  #
  # snap1 : init/write all objects 
  # snap2 : write object0
  # snap3 : write object1
  # snap4 : write object2
  # snap5 : write object3
  # snap6 : write object4
  # snap7 : write object5
  # snap8 : write object0
  # snap9 : write object3

  make_snapshot $mnt/$image_name 0 $size snap1 $export_image_dir
  make_snapshot $mnt/$image_name 0  1    snap2 $export_image_dir
  make_snapshot $mnt/$image_name 4  1    snap3 $export_image_dir
  make_snapshot $mnt/$image_name 8  1    snap4 $export_image_dir
  make_snapshot $mnt/$image_name 12 1    snap5 $export_image_dir
  make_snapshot $mnt/$image_name 16 1    snap6 $export_image_dir
  make_snapshot $mnt/$image_name 20 1    snap7 $export_image_dir
  make_snapshot $mnt/$image_name 1  1    snap8 $export_image_dir
  make_snapshot $mnt/$image_name 13 1    snap9 $export_image_dir
}

function check_recover_nosnap()
{
  local func="check_recover_nosnap"
  if [ $# -lt 3 ];then
    echo "$func: parameters: <export_md5_file> <recover_md5_file> <image_name>"
  fi
  local export_md5=$1
  local recover_md5=$2
  local image_name=$3

  local ifpassed="FAILED"
 
  echo "================ < $image_name nosnap > ================" 

  local export_md5sum=`cat $export_md5` 
  local recover_md5sum=`cat $recover_md5` 

  if [ "$export_md5sum"x != ""x ] && [ "$export_md5sum"x = "$recover_md5sum"x ];then
    ifpassed="PASSED"
  fi
  echo "export:  $export_md5sum"
  echo "recover: $recover_md5sum $ifpassed"
}

function check_recover_snapshots()
{
  local func="check_recover_snapshots"
  if [ $# -lt 3 ];then
    echo "$func: parameters: <export_md5_file> <recover_md5_file> <image_name>"
  fi
  local export_md5=$1
  local recover_md5=$2
  local image_name=$3

  local ifpassed="FAILED"
 
  echo "================ < $image_name snapshots > ================" 

  OIFS=$IFS
  IFS=$'\n'
  local export_md5s=(`cat $export_md5`)
  local recover_md5s=(`cat $recover_md5`)
  for((i=0; i<9; i++))
  do
    OOIFS=$IFS
    IFS=$'  '
    local x=$(($i+1))
    snapshot=snap$x

    local export_arr=(`echo ${export_md5s[$i]}`)
    local recover_arr=(`echo ${recover_md5s[$i]}`)
    echo "export:  ${export_md5s[$i]}"
    if [ "${export_arr[1]}"x != ""x ] && [ "${export_arr[1]}"x = "${recover_arr[1]}"x ];then
      ifpassed="PASSED"
    fi
    echo "recover: ${recover_md5s[$i]} $ifpassed"
    IFS=$OOIFS
  done
  IFS=$OIFS
}

# step 1: export image, snapshot
function do_export_nosnap()
{
  export_image image_v1_nosnap 1
  export_image image_v2_nosnap 2
}

function do_export_snap()
{
  export_snapshots  image_v1_snap 1
  export_snapshots  image_v2_snap 2
}

# step 2: stop ceph cluster and gen database
function stop_cluster_gen_database()
{
  trap 'echo stop ceph cluster failed; exit;' INT HUP
  stop_ceph 
  sleep 2
  check_ceph_service
  local res=$?
  while [ $res -ne 0 ]
  do
    stop_ceph
    sleep 2
    check_ceph_service
    res=$?
  done

  echo 0 >$gen_db
  do_gen_database
}

# step 3: recover image,snapshot
function do_recover_nosnap()
{
  recover_image image_v1_nosnap
  recover_image image_v2_nosnap
}

function do_recover_snap()
{
  recover_snapshots image_v1_snap
  recover_snapshots image_v2_snap
}

# step 4: check md5sum pair<export_md5sum, recover_md5sum>
function do_check_recover_nosnap()
{
  local image1=image_v1_nosnap
  local image2=image_v2_nosnap

  local export_md5_1=$export_dir/pool_$pool_id/$image1/@md5_nosnap
  local export_md5_2=$export_dir/pool_$pool_id/$image2/@md5_nosnap
  local recover_md5_1=$recover_dir/pool_$pool_id/$image1/@md5_nosnap
  local recover_md5_2=$recover_dir/pool_$pool_id/$image2/@md5_nosnap

  check_recover_nosnap $export_md5_1 $recover_md5_1 $image1 
  check_recover_nosnap $export_md5_2 $recover_md5_2 $image2
}

function do_check_recover_snap()
{
  local image1=image_v1_snap
  local image2=image_v2_snap

  local export_md5_1=$export_dir/pool_$pool_id/$image1/@md5
  local export_md5_2=$export_dir/pool_$pool_id/$image2/@md5
  local recover_md5_1=$recover_dir/pool_$pool_id/$image1/@md5
  local recover_md5_2=$recover_dir/pool_$pool_id/$image2/@md5

  check_recover_snapshots $export_md5_1 $recover_md5_1 $image1 
  check_recover_snapshots $export_md5_2 $recover_md5_2 $image2
}

function test_case_1()
{
  do_export_nosnap
  stop_cluster_gen_database
  do_recover_nosnap
  do_check_recover_nosnap
}

function test_case_2()
{
  do_export_snap
  stop_cluster_gen_database
  do_recover_snap
  do_check_recover_snap
}

function test_case_3()
{
  do_export_nosnap
  do_export_snap

  stop_cluster_gen_database

  do_recover_nosnap
  do_recover_snap

  do_check_recover_nosnap
  do_check_recover_snap
}


init $*
test_case_3
