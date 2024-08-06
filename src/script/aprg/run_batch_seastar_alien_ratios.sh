#!/usr/bin/bash
#
#Runs to compare the workloads across the seastar/alien core ratios -- I can't try these since using 8-15 for FIO
# so would need to think an alternative
#4 : 28 (0-3,4-31);8 : 24 (0-7,8-31); 12 : 20 (0-11, 12-31); 16 : 16 (0-15,16-31) = alien all HT siblings
#
#ALAS: ALWAYS LOOK AT lsblk after reboot the machine!
cd /ceph/build/
#BLUESTORE_DEVS='/dev/sdc,/dev/sde,/dev/sdf'
BLUESTORE_DEVS='/dev/sdf'
BEST_NUM_FIO_CORES="8-15"
#########################################
declare -A test_table
declare -A test_row

test_row["title"]='== 1 OSD 4 seastar cores =='
test_row['fio']=$BEST_NUM_FIO_CORES
test_row['smp']="4"
test_row['test']="crimson_1osd_4seastar"
string=$(declare -p test_row)
test_table["0"]=${string}

test_row["title"]='== 1 OSD 8 seastar cores =='
test_row['fio']=$BEST_NUM_FIO_CORES
test_row['smp']="8"
test_row['test']="crimson_1osd_8seastar"
string=$(declare -p test_row)
test_table["1"]=${string}

test_row["title"]='== 1 OSD 12 seastar cores =='
test_row['fio']=$BEST_NUM_FIO_CORES
test_row['smp']="12"
test_row['test']="crimson_1osd_12seastar"
string=$(declare -p test_row)
test_table["2"]=${string}

test_row["title"]='== 1 OSD 16 seastar cores =='
test_row['fio']=$BEST_NUM_FIO_CORES
test_row['smp']="16"
test_row['test']="crimson_1osd_16seastar"
string=$(declare -p test_row)
test_table["3"]=${string}
#########################################

for KEY in "${!test_table[@]}"; do
  eval "${test_table["$KEY"]}"
  echo ${test_row["title"]}
  echo "MDS=0 MON=1 OSD=1 MGR=1 ../src/vstart.sh --new -x --localhost --without-dashboard --bluestore --redirect-output --bluestore-devs ${BLUESTORE_DEVS} --crimson --crimson-smp ${test_row["smp"]} --no-restart"
  MDS=0 MON=1 OSD=1 MGR=1 ../src/vstart.sh --new -x --localhost --without-dashboard --bluestore --redirect-output --bluestore-devs ${BLUESTORE_DEVS} --crimson --crimson-smp ${test_row["smp"]} --no-restart
  [ -f /ceph/build/vstart_environment.sh ] && source /ceph/build/vstart_environment.sh
  /root/bin/cephlogoff.sh 2>&1 > /dev/null
  /root/bin/cephmkrbd.sh
  #/root/bin/cpu-map.sh  -n osd -g "alien:4-31"
  RBD_NAME=fio_test_0 RBD_SIZE="10G" fio /fio/examples/rbd_prefill.fio && rbd du fio_test_0 && /root/bin/run_fio.sh -s -a -c "0-31" -f "${test_row["fio"]}" -p ${test_row["test"]} -k # w/o osd dump_metrics
  /root/bin/cephteardown.sh 2>&1 > /dev/null
  /ceph/src/stop.sh --crimson
  sleep 60
done
exit

#########################################
