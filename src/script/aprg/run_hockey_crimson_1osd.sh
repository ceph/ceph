#!/usr/bin/bash
#
#Runs to collect response curves for Classic OSD
#
#ALAS: ALWAYS LOOK AT lsblk after reboot the machine!
cd /ceph/build/
#BLUESTORE_DEVS='/dev/sdc,/dev/sde,/dev/sdf'
BLUESTORE_DEVS='/dev/sde'
#########################################
declare -A test_table
declare -A test_row

test_row["title"]='== 1 OSD classic, FIO unrestricted, Response curves =='
test_row['fio']="0-31"
test_row['test']="classic_1osd_fio_unrest"
string=$(declare -p test_row)
test_table["0"]=${string}
########################################

for KEY in "${!test_table[@]}"; do
  eval "${test_table["$KEY"]}"
  echo ${test_row["title"]}
  echo "MDS=0 MON=1 OSD=1 MGR=1 ../src/vstart.sh --new -x --localhost --without-dashboard --bluestore --redirect-output --bluestore-devs ${BLUESTORE_DEVS} --no-restart"
  MDS=0 MON=1 OSD=1 MGR=1 ../src/vstart.sh --new -x --localhost --without-dashboard --bluestore --redirect-output --bluestore-devs ${BLUESTORE_DEVS} --no-restart
  [ -f /ceph/build/vstart_environment.sh ] && source /ceph/build/vstart_environment.sh
  /root/bin/cephlogoff.sh 2>&1 > /dev/null
  /root/bin/cephmkrbd.sh
  #/root/bin/cpu-map.sh  -n osd -g "alien:4-31"
  RBD_NAME=fio_test_0 RBD_SIZE="10G" fio /fio/examples/rbd_prefill.fio && rbd du fio_test_0 && /root/bin/run_fio.sh -s -w classic -a -c "0-31" -f "${test_row["fio"]}" -p ${test_row["test"]} -k -n # w/o osd dump_metrics
  /ceph/src/stop.sh
  sleep 60
done
exit

#########################################
