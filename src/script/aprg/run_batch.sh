#!/usr/bin/bash
#
cd /ceph/build/

echo "== 1 OSD 4 reactor 2 alien core manual =="
MDS=0 MON=1 OSD=1  MGR=1 ../src/vstart.sh --new -x --localhost --without-dashboard --bluestore --redirect-output --bluestore-devs /dev/sdd,/dev/sde,/dev/sdf --crimson --crimson-smp 4 --crimson-alien-num-cores 2 --no-restart # Crimson 1osd 4 reactor manual 2 aliencore

/root/bin/cephlogoff.sh

RBD_NAME=fio_test_0 RBD_SIZE="10G" fio /fio/examples/rbd_prefill.fio && rbd du fio_test_0 && /root/bin/run_fio.sh -s -a -c "0-5" -p crimson_1osd_4reactor_2ac_manual -k # w/o osd dump_metrics

/ceph/src/stop.sh --crimson
sleep 60

echo "== 3 OSD 1 reactor 1 alien core manual =="
MDS=0 MON=1 OSD=3  MGR=1 ../src/vstart.sh --new -x --localhost --without-dashboard --bluestore --redirect-output --bluestore-devs /dev/sdd,/dev/sde,/dev/sdf --crimson --crimson-smp 1 --crimson-alien-num-cores 1 --no-restart # Crimson 3osd 1 reactor manual 1 aliencore

/root/bin/cephlogoff.sh

RBD_NAME=fio_test_0 RBD_SIZE="10G" fio /fio/examples/rbd_prefill.fio && rbd du fio_test_0 && /root/bin/run_fio.sh -s -a -c "0-3" -p crimson_3osd_1reactor_1ac_manual -k # w/o osd dump_metrics

/ceph/src/stop.sh --crimson
sleep 60

echo "== 3 OSD 2 reactor 2 alien core manual =="
MDS=0 MON=1 OSD=3  MGR=1 ../src/vstart.sh --new -x --localhost --without-dashboard --bluestore --redirect-output --bluestore-devs /dev/sdd,/dev/sde,/dev/sdf --crimson --crimson-smp 2 --crimson-alien-num-cores 2 --no-restart # Crimson 3osd 2 reactor manual 2 aliencore

/root/bin/cephlogoff.sh

RBD_NAME=fio_test_0 RBD_SIZE="10G" fio /fio/examples/rbd_prefill.fio && rbd du fio_test_0 && /root/bin/run_fio.sh -s -a -c "0-8" -p crimson_3osd_2reactor_2ac_manual -k # w/o osd dump_metrics

/ceph/src/stop.sh --crimson
sleep 60

echo "== 3 OSD 4 reactor 2 alien core manual =="
MDS=0 MON=1 OSD=3  MGR=1 ../src/vstart.sh --new -x --localhost --without-dashboard --bluestore --redirect-output --bluestore-devs /dev/sdd,/dev/sde,/dev/sdf --crimson --crimson-smp 4 --crimson-alien-num-cores 2 --no-restart # Crimson 3osd 4 reactor manual 2 aliencore

/root/bin/cephlogoff.sh

RBD_NAME=fio_test_0 RBD_SIZE="10G" fio /fio/examples/rbd_prefill.fio && rbd du fio_test_0 && /root/bin/run_fio.sh -s -a -c "0-13" -p crimson_3osd_4reactor_2ac_manual -k # w/o osd dump_metrics

/ceph/src/stop.sh --crimson
sleep 60

echo "== 3 OSD 8 reactor 2 alien core manual =="
MDS=0 MON=1 OSD=3  MGR=1 ../src/vstart.sh --new -x --localhost --without-dashboard --bluestore --redirect-output --bluestore-devs /dev/sdd,/dev/sde,/dev/sdf --crimson --crimson-smp 8 --crimson-alien-num-cores 2 --no-restart # Crimson 3osd 8 reactor manual 2 aliencore

/root/bin/cephlogoff.sh

RBD_NAME=fio_test_0 RBD_SIZE="10G" fio /fio/examples/rbd_prefill.fio && rbd du fio_test_0 && /root/bin/run_fio.sh -s -a -c "0-31" -p crimson_3osd_8reactor_2ac_manual -k # w/o osd dump_metrics

/ceph/src/stop.sh --crimson

