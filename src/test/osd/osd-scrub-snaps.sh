#! /bin/bash

TESTDATA="testdata.$$"

function wait_for_health() {
    echo -n "Wait for health_ok..."
    tries=0
    while ./ceph health 2> /dev/null |  grep -v 'HEALTH_OK' > /dev/null
    do
	tries=`expr $tries + 1`
        if [ $tries = "30" ];
	then
            echo "Time exceeded to go to health"
            exit 1
	fi
        sleep 5
    done
    echo "DONE"
}

# A single osd
rm -rf out dev
MDS=0 MON=1 OSD=1 ./vstart.sh -l -d  -n -o "osd_pool_default_size=1"
wait_for_health

# Create a pool with a single pg
./ceph osd pool create test 1 1

dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
for i in `seq 1 14`
do
    ./rados -p test put obj${i} $TESTDATA
done

SNAP=1
./rados -p test mksnap snap${SNAP}
dd if=/dev/urandom of=$TESTDATA bs=256 count=${SNAP}
./rados -p test put obj1 $TESTDATA
./rados -p test put obj5 $TESTDATA
./rados -p test put obj3 $TESTDATA
for i in `seq 6 14`
 do ./rados -p test put obj${i} $TESTDATA
done

SNAP=2
./rados -p test mksnap snap${SNAP}
dd if=/dev/urandom of=$TESTDATA bs=256 count=${SNAP}
./rados -p test put obj5 $TESTDATA

SNAP=3
./rados -p test mksnap snap${SNAP}
dd if=/dev/urandom of=$TESTDATA bs=256 count=${SNAP}
./rados -p test put obj3 $TESTDATA

SNAP=4
./rados -p test mksnap snap${SNAP}
dd if=/dev/urandom of=$TESTDATA bs=256 count=${SNAP}
./rados -p test put obj5 $TESTDATA
./rados -p test put obj2 $TESTDATA

SNAP=5
./rados -p test mksnap snap${SNAP}
SNAP=6
./rados -p test mksnap snap${SNAP}
dd if=/dev/urandom of=$TESTDATA bs=256 count=${SNAP}
./rados -p test put obj5 $TESTDATA

SNAP=7
./rados -p test mksnap snap${SNAP}

./rados -p test rm obj4
./rados -p test rm obj2

killall ceph-osd
sleep 5

JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj1 | grep \"snapid\":-2)"
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" remove

JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj5 | grep \"snapid\":2)"
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" remove

JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj5 | grep \"snapid\":1)"
OBJ5SAVE="$JSON"
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" remove

JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj5 | grep \"snapid\":4)"
dd if=/dev/urandom of=$TESTDATA bs=256 count=18
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" set-bytes $TESTDATA

JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj3 | grep \"snapid\":-2)"
dd if=/dev/urandom of=$TESTDATA bs=256 count=15
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" set-bytes $TESTDATA

JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj4 | grep \"snapid\":7)"
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" remove

JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj2 | grep \"snapid\":-1)"
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" rm-attr snapset

# Create a clone which isn't in snapset and doesn't have object info
JSON="$(echo "$OBJ5SAVE" | sed s/snapid\":1/snapid\":7/)"
dd if=/dev/urandom of=$TESTDATA bs=256 count=7
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" set-bytes $TESTDATA

rm -f $TESTDATA

JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj6 | grep \"snapid\":-2)"
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" clear-snapset
JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj7 | grep \"snapid\":-2)"
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" clear-snapset corrupt
JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj8 | grep \"snapid\":-2)"
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" clear-snapset seq
JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj9 | grep \"snapid\":-2)"
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" clear-snapset clone_size
JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj10 | grep \"snapid\":-2)"
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" clear-snapset clone_overlap
JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj11 | grep \"snapid\":-2)"
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" clear-snapset clones
JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj12 | grep \"snapid\":-2)"
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" clear-snapset head
JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj13 | grep \"snapid\":-2)"
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" clear-snapset snaps
JSON="$(./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal --op list obj14 | grep \"snapid\":-2)"
./ceph-objectstore-tool --data-path dev/osd0 --journal-path dev/osd0.journal "$JSON" clear-snapset size

#MDS=0 MON=1 OSD=1 ./vstart.sh -l -d  -o "osd_pool_default_size=1"
./ceph-osd -i 0 -c ceph.conf
wait_for_health

sleep 5
./ceph pg scrub 1.0
timeout 30 ./ceph -w

for i in `seq 1 7`
do
    ./rados -p test rmsnap snap$i
done

sleep 10

ERRORS=0

if ! killall ceph-osd
then
    echo "OSD crash occurred"
    ERRORS=$(expr $ERRORS + 1)
fi

./stop.sh

declare -a err_strings
err_strings[0]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/2acecc8b/obj10/1 is missing in clone_overlap"
err_strings[1]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/666934a3/obj5/7 no '_' attr"
err_strings[2]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/666934a3/obj5/7 is an unexpected clone"
err_strings[3]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/666934a3/obj5/4 on disk size [(]4608[)] does not match object info size [(]512[)] adjusted for ondisk to [(]512[)]"
err_strings[4]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/666934a3/obj5/head expected clone 1/666934a3/obj5/2"
err_strings[5]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/666934a3/obj5/head expected clone 1/666934a3/obj5/1"
err_strings[6]="log_channel[(]cluster[)] log [[]INF[]] : scrub 1.0 1/666934a3/obj5/head 1 missing clone[(]s[)]"
err_strings[7]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/d3a9faf5/obj12/head snapset.head_exists=false, but head exists"
err_strings[8]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/8df7eaa5/obj8/head snaps.seq not set"
err_strings[9]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/5c889059/obj7/head snapset.head_exists=false, but head exists"
err_strings[10]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/5c889059/obj7/1 is an unexpected clone"
err_strings[11]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/61f68bb1/obj3/head on disk size [(]3840[)] does not match object info size [(]768[)] adjusted for ondisk to [(]768[)]"
err_strings[12]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/83425cc4/obj6/1 is an unexpected clone"
err_strings[13]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/3f1ee208/obj2/snapdir no 'snapset' attr"
err_strings[14]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/3f1ee208/obj2/7 is an unexpected clone"
err_strings[15]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/3f1ee208/obj2/4 is an unexpected clone"
err_strings[16]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/a8759770/obj4/snapdir expected clone 1/a8759770/obj4/7"
err_strings[17]="log_channel[(]cluster[)] log [[]INF[]] : scrub 1.0 1/a8759770/obj4/snapdir 1 missing clone[(]s[)]"
err_strings[18]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/6cf8deff/obj1/1 is an unexpected clone"
err_strings[19]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/e478ac7f/obj9/1 is missing in clone_size"
err_strings[20]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/29547577/obj11/1 is an unexpected clone"
err_strings[21]="log_channel[(]cluster[)] log [[]ERR[]] : scrub 1.0 1/94122507/obj14/1 size 1032 != clone_size 1033"
err_strings[22]="log_channel[(]cluster[)] log [[]ERR[]] : 1.0 scrub 21 errors"

for i in `seq 0 ${#err_strings[@]}`
do
    if ! grep "${err_strings[$i]}" out/osd.0.log > /dev/null;
    then
	echo "Missing log message '${err_strings[$i]}'"
        ERRORS=$(expr $ERRORS + 1)
    fi
done

if [ $ERRORS != "0" ];
then
    echo "TEST FAILED WITH $ERRORS ERRORS"
    exit 1
fi

echo "TEST PASSED"
exit 0
