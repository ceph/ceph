#!/usr/bin/env bash
#
#ALAS: ALWAYS LOOK AT lsblk after reboot the machine!
cd /ceph/build/
#BLUESTORE_DEVS='/dev/sdc,/dev/sde,/dev/sdf'
BLUESTORE_DEVS='/dev/sdf'
BEST_NUM_FIO_CORES="8-15"
#########################################
declare -A test_table
declare -A test_row
test_row=(["title"]='== 1 OSD 1 reactor, 1 cpu core alien, manual ==' ['alien']="1-1" ['fio']=$BEST_NUM_FIO_CORES ['test']="crimson_1osd_1reactor_1alien_manual")
string=$(declare -p test_row)
test_table["0"]=${string}

test_row["title"]='== 1 OSD 1 reactor, 2 cpu cores alien, manual =='
test_row['test']="crimson_1osd_1reactor_2alien_manual"
test_row['alien']="1-2"
test_row['fio']=$BEST_NUM_FIO_CORES
string=$(declare -p test_row)
test_table["1"]=${string}

test_row["title"]='== 1 OSD 1 reactor, 4 cpu cores alien, manual =='
test_row['test']="crimson_1osd_1reactor_4alien_manual"
test_row['alien']="1-4"
test_row['fio']=$BEST_NUM_FIO_CORES
string=$(declare -p test_row)
test_table["2"]=${string}

test_row["title"]='== 1 OSD 1 reactor, 8 cpu cores alien, manual =='
test_row['test']="crimson_1osd_1reactor_8alien_manual"
test_row['alien']="1-8"
test_row['fio']=$BEST_NUM_FIO_CORES
string=$(declare -p test_row)
test_table["3"]=${string}

for KEY in "${!test_table[@]}"; do
  eval "${test_table["$KEY"]}"
  echo "${test_row["title"]}"
  MDS=0 MON=1 OSD=1  MGR=1 ../src/vstart.sh --debug --new -x --localhost --without-dashboard --bluestore --redirect-output --bluestore-devs ${BLUESTORE_DEVS} --crimson --crimson-alien-num-cores ${test_row["alien"]} --no-restart

  [ -f /ceph/build/vstart_environment.sh ] && source /ceph/build/vstart_environment.sh
  /root/bin/cephlogoff.sh 2>&1 > /dev/null
  /root/bin/cephmkrbd.sh
  #/root/bin/cpu-map.sh  -p $(pgrep osd) -g "alien:${test_row['alien']}"  2>&1 > /dev/null
  ps -L -o ppid,pid,psr,comm -p $(pgrep osd)
  RBD_NAME=fio_test_0 RBD_SIZE="10G" fio /fio/examples/rbd_prefill.fio && rbd du fio_test_0 && /root/bin/run_fio.sh -s -a -c "0-31" -f "${test_row["fio"]}" -p ${test_row["test"]} -k # w/o osd dump_metrics

  /root/bin/cephteardown.sh 2>&1 > /dev/null
  /ceph/src/stop.sh --crimson
  sleep 60
done
exit

#########################################

function _past_batch() {
	#######
	echo "== 3 OSD 8 reactor 4 alien core manual =="
	MDS=0 MON=1 OSD=3  MGR=1 ../src/vstart.sh --new -x --localhost --without-dashboard --bluestore --redirect-output --bluestore-devs /dev/sdd,/dev/sde,/dev/sdf --crimson --crimson-smp 8 --crimson-alien-num-cores 4 --no-restart # Crimson 3osd 8 reactor manual 4 aliencore
	/root/bin/cephlogoff.sh
	#/root/bin/cpu-map.sh  -n osd -g # need to modify this to make the range an argument
	RBD_NAME=fio_test_0 RBD_SIZE="10G" fio /fio/examples/rbd_prefill.fio && rbd du fio_test_0 && /root/bin/run_fio.sh -s -a -c "0-31" -p crimson_3osd_8reactor_4ac_manual -k # w/o osd dump_metrics
	/ceph/src/stop.sh --crimson
	sleep 60

	echo "== 3 OSD 8 reactor 4 alien core no HT manual =="
	MDS=0 MON=1 OSD=3  MGR=1 ../src/vstart.sh --new -x --localhost --without-dashboard --bluestore --redirect-output --bluestore-devs /dev/sdd,/dev/sde,/dev/sdf --crimson --crimson-smp 8 --crimson-alien-num-cores 4 --no-restart # Crimson 3osd 8 reactor manual 4 aliencore

	/root/bin/cephlogoff.sh
	/root/bin/cpu-map.sh  -n osd -g
	RBD_NAME=fio_test_0 RBD_SIZE="10G" fio /fio/examples/rbd_prefill.fio && rbd du fio_test_0 && /root/bin/run_fio.sh -s -a -c "0-31" -p crimson_3osd_8reactor_4ac_manual_noht -k # w/o osd dump_metrics
	/ceph/src/stop.sh --crimson
	sleep 60
}

#seastar/alien threads ratio scenarios:
#export PLAN=$(cat << END
#PLAN=<<END
#== 1 OSD 4 reactor manual 4 : 28 (0-3,4-31)  ==; 4 ; crimson_1osd_4reactor_4_31_manual
#== 1 OSD 8 reactor manual 8 : 24 (0-7,8-31)  ==; 8 ; crimson_1osd_8reactor_8_31_manual
#== 1 OSD 12 reactor manual 12 : 20 (0-11,12-31)  ==; 12; crimson_1osd_12reactor_12_31_manual
#== 1 OSD 16 reactor manual 16 : 16 (0-15,16-31)  ==; 16; crimson_1osd_16reactor_16_31_manual
#END
#
#while IFS='' read -a array <<< "$PLAN"; do
#while IFS='; ' read -r -a test_plan <<< "$PLAN"; do
#echo "${array[*]}"
#done
#exit
