#!/usr/bin/bash
#
#Runs to compare the workloads across 8,4 cpu cores for FIO (Single OSD)
#
#ALAS: ALWAYS LOOK AT lsblk after reboot the machine!
cd /ceph/build/
#BLUESTORE_DEVS='/dev/sdc,/dev/sde,/dev/sdf'
BLUESTORE_DEVS='/dev/sdf'
export NUM_RBD_IMAGES=8
#########################################
declare -A test_table
declare -A test_row

test_row["title"]='== 1 OSD 1 reactor default, FIO: unrestricted =='
test_row['fio']="0-31"
test_row['test']="crimson_1osd_default_fio_unrest"
string=$(declare -p test_row)
test_table["0"]=${string}

test_row["title"]='== 1 OSD 1 reactor default, FIO: 8 cores  =='
test_row['fio']="8-15"
test_row['test']="crimson_1osd_default_8fio"
string=$(declare -p test_row)
test_table["1"]=${string}

#########################################
function prefill_images(){
  local NUM_RBD_IMAGES=$1
  for (( i=0; i<${NUM_RBD_IMAGES}; i++ )); do
    RBD_NAME=fio_test_${i} RBD_SIZE="10G" fio /fio/examples/rbd_prefill.fio &
  done
  wait
  rbd du
}

for KEY in "${!test_table[@]}"; do
  eval "${test_table["$KEY"]}"
  #for k in "${!test_row[@]}"; do
  echo ${test_row["title"]}
  echo "MDS=0 MON=1 OSD=1 MGR=1 ../src/vstart.sh --new -x --localhost --without-dashboard --bluestore --redirect-output --bluestore-devs ${BLUESTORE_DEVS} --crimson --no-restart"
  MDS=0 MON=1 OSD=1 MGR=1 ../src/vstart.sh --new -x --localhost --without-dashboard --bluestore --redirect-output --bluestore-devs ${BLUESTORE_DEVS} --crimson --no-restart
  [ -f /ceph/build/vstart_environment.sh ] && source /ceph/build/vstart_environment.sh
  /root/bin/cephlogoff.sh 2>&1 > /dev/null
  /root/bin/cephmkrbd.sh
  #/root/bin/cpu-map.sh  -n osd -g "alien:4-31"
  prefill_images ${NUM_RBD_IMAGES}  && /root/bin/run_fio.sh -s -a -c "0-31" -f "${test_row["fio"]}" -p ${test_row["test"]} -k # w/o osd dump_metrics
  /root/bin/cephteardown.sh
  /ceph/src/stop.sh --crimson
  sleep 60
done
exit

#########################################
