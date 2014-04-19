#! /bin/sh

wait_for_health() {
  echo -n "Wait for health_ok..."
  while( ./ceph health 2> /dev/null | grep -v HEALTH_OK > /dev/null)
  do
    sleep 5
  done
  echo DONE
}

REP_POOL="rep_pool"
REP_NAME="REPobject"
EC_POOL="ec_pool"
EC_NAME="ECobject"
NUM_OBJECTS=40
ERRORS=0
TESTDIR="/tmp/test.$$"
DATADIR="/tmp/data.$$"

echo -n "vstarting...."
OSD=4 ./vstart.sh -l -n -d -o "mon_pg_warn_min_per_osd=1" > /dev/null 2>&1
echo DONE

wait_for_health

./ceph osd pool create $REP_POOL 12 12 replicated  2> /dev/null
REPID=`./ceph osd pool stats $REP_POOL  2> /dev/null | grep ^pool | awk '{ print $4 }'`

echo "Created replicated pool #" $REPID

./ceph osd erasure-code-profile set testprofile ruleset-failure-domain=osd || return 1
./ceph osd erasure-code-profile get testprofile
./ceph osd pool create $EC_POOL 12 12 erasure testprofile || return 1
ECID=`./ceph osd pool stats $EC_POOL  2> /dev/null | grep ^pool | awk '{ print $4 }'`

echo "Created Erasure coded pool #" $ECID

echo "Creating $NUM_OBJECTS objects in replicated pool"
mkdir -p $DATADIR
for i in `seq 1 $NUM_OBJECTS`
do
  NAME="${REP_NAME}$i"
  rm -f $DATADIR/$NAME
  for i in `seq 1 10000`
  do
    echo "This is the replicated data for $NAME" >> $DATADIR/$NAME
  done
  ./rados -p $REP_POOL put $NAME $DATADIR/$NAME  2> /dev/null
done

echo "Creating $NUM_OBJECTS objects in erausre coded pool"
for i in `seq 1 $NUM_OBJECTS`
do
  NAME="${EC_NAME}$i"
  rm -f $DATADIR/$NAME
  for i in `seq 1 10000`
  do
    echo "This is the erasure coded data for $NAME" >> $DATADIR/$NAME
  done
  ./rados -p $EC_POOL put $NAME $DATADIR/$NAME  2> /dev/null
done

./stop.sh > /dev/null 2>&1

ALLREPPGS=`ls -d dev/*/current/${REPID}.*_head | awk -F / '{ print $4}' | sed 's/_head//' | sort -u`
#echo ALL REP PGS: $ALLREPPGS
ALLECPGS=`ls -d dev/*/current/${ECID}.*_head | awk -F / '{ print $4}' | sed 's/_head//' | sort -u`
#echo ALL EC PGS: $ALLECPGS

OBJREPPGS=`ls dev/*/current/${REPID}.*_head/${REP_NAME}* | awk -F / '{ print $4}' | sed 's/_head//' | sort -u`
#echo OBJECT REP PGS: $OBJREPPGS
OBJECPGS=`ls dev/*/current/${ECID}.*_head/${EC_NAME}* | awk -F / '{ print $4}' | sed 's/_head//' | sort -u`
#echo OBJECT EC PGS: $OBJECPGS

echo Checking pg info
for pg in $ALLREPPGS $ALLECPGS
do
  OSDS=`ls -d dev/*/current/${pg}_head | awk -F / '{ print $2 }'`
  for osd in $OSDS
  do
    ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal --type info --pgid $pg | grep "\"pgid\": \"$pg\"" > /dev/null
    if [ "$?" != "0" ];
    then
      echo "FAILURE: getting info for $pg"
      ERRORS=`expr $ERRORS + 1`
    fi
  done
done

echo Checking pg logs
for pg in $ALLREPPGS $ALLECPGS
do
  OSDS=`ls -d dev/*/current/${pg}_head | awk -F / '{ print $2 }'`
  for osd in $OSDS
  do
    ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal --type log --pgid $pg > /tmp/tmp.$$
    if [ "$?" != "0" ];
    then
      echo "FAILURE: getting log for $pg from $osd"
      ERRORS=`expr $ERRORS + 1`
      continue
    fi
    # Is this pg in the list of pgs with objects
    echo -e "\n$OBJREPPGS\n$OBJECPGS" | grep "^$pg$" > /dev/null
    HASOBJ=$?
    # Does the log have a modify
    grep modify /tmp/tmp.$$ > /dev/null
    MODLOG=$?
    if [ "$HASOBJ" != "$MODLOG" ];
    then
      echo "FAILURE: bad log for $pg from $osd"
      if [ "$HASOBJ" = "0" ];
      then
        MSG=""
      else
        MSG="NOT"
      fi
      echo "Log should $MSG have a modify entry"
      ERRORS=`expr $ERRORS + 1`
    fi
  done
done
rm /tmp/tmp.$$

echo Checking pg export
EXP_ERRORS=0
mkdir $TESTDIR
for pg in $ALLREPPGS $ALLECPGS
do
  OSDS=`ls -d dev/*/current/${pg}_head | awk -F / '{ print $2 }'`
  for osd in $OSDS
  do
     mkdir -p $TESTDIR/$osd
    ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal --type export --pgid $pg --file $TESTDIR/$osd/$pg.export.$$ > /tmp/tmp.$$
    if [ "$?" != "0" ];
    then
      echo "FAILURE: Exporting $pg on $osd"
      echo "ceph_filestore_dump output:"
      cat /tmp/tmp.$$
      EXP_ERRORS=`expr $EXP_ERRORS + 1`
    fi
    rm /tmp/tmp.$$
  done
done

ERRORS=`expr $ERRORS + $EXP_ERRORS`

echo Checking pg removal
RM_ERRORS=0
for pg in $ALLREPPGS $ALLECPGS
do
  OSDS=`ls -d dev/*/current/${pg}_head | awk -F / '{ print $2 }'`
  for osd in $OSDS
  do
    ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal --type remove --pgid $pg >  /tmp/tmp.$$
    if [ "$?" != "0" ];
    then
      echo "FAILURE: Removing $pg on $osd"
      echo "ceph_filestore_dump output:"
      cat /tmp/tmp.$$
      RM_ERRORS=`expr $RM_ERRORS + 1`
    fi
    rm /tmp/tmp.$$
  done
done

ERRORS=`expr $ERRORS + $RM_ERRORS`

IMP_ERRORS=0
if [ $EXP_ERRORS = "0" -a $RM_ERRORS = "0" ];
then
  for dir in $TESTDIR/*
  do
     osd=`basename $dir`
     for file in $dir/*
     do
      ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal --type import --file $file > /tmp/tmp.$$
      if [ "$?" != "0" ];
      then
        echo "FAILURE: Importing from $file"
        echo "ceph_filestore_dump output:"
        cat /tmp/tmp.$$
        IMP_ERRORS=`expr $IMP_ERRORS + 1`
      fi
      rm /tmp/tmp.$$
    done
  done
else
  echo "SKIPPING IMPORT TESTS DUE TO PREVIOUS FAILURES"
fi

ERRORS=`expr $ERRORS + $IMP_ERRORS`
rm -rf $TESTDIR

if [ $EXP_ERRORS = "0" -a $RM_ERRORS = "0" -a $IMP_ERRORS = "0" ];
then
  echo "Checking replicated import data"
  for path in ${DATADIR}/${REP_NAME}*
  do
    file=`basename $path`
    for obj_loc in `find dev -name "${file}_*"`
    do
      diff $path $obj_loc > /dev/null
      if [ $? != "0" ];
      then
        echo FAILURE: $file data not imported properly into $obj_loc
        ERRORS=`expr $ERRORS + 1`
      fi
    done
  done

  OSD=4 ./vstart.sh -l -d -o "mon_pg_warn_min_per_osd=1" > /dev/null 2>&1
  wait_for_health

  echo "Checking erasure coded import data"
  for file in ${DATADIR}/${EC_NAME}*
  do
    rm -f /tmp/tmp.$$
    ./rados -p $EC_POOL get `basename $file` /tmp/tmp.$$ > /dev/null 2>&1
    diff $file /tmp/tmp.$$ > /dev/null
    if [ $? != "0" ];
    then
      echo FAILURE: $file data not imported properly
      ERRORS=`expr $ERRORS + 1`
    fi
    rm -f /tmp/tmp.$$
  done

  ./stop.sh > /dev/null 2>&1
else
  echo "SKIPPING CHECKING IMPORT DATA DUE TO PREVIOUS FAILURES"
fi

rm -rf $DATADIR

if [ $ERRORS = "0" ];
then
  echo "TEST PASSED"
  exit 0
else
  echo "TEST FAILED WITH $ERRORS ERRORS"
  exit 1
fi

