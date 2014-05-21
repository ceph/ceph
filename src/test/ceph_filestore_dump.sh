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
JSONOBJ="/tmp/json.$$"

echo -n "vstarting...."
OSD=4 ./vstart.sh -l -n -d > /dev/null 2>&1
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
  for j in `seq 1 10000`
  do
    echo "This is the replicated data for $NAME" >> $DATADIR/$NAME
  done
  ./rados -p $REP_POOL put $NAME $DATADIR/$NAME  2> /dev/null
  ACNT=`expr $i - 1`
  for k in `seq 0 $ACNT`
  do
    if [ $k = "0" ];
    then
      continue
    fi
    ./rados -p $REP_POOL setxattr $NAME key${i}-${k} val${i}-${k}
  done
  # Create omap header in all objects but REPobject1
  if [ $i != "1" ];
  then
    ./rados -p $REP_POOL setomapheader $NAME hdr${i}
  fi
  for k in `seq 0 $ACNT`
  do
    if [ $k = "0" ];
    then
      continue
    fi
    ./rados -p $REP_POOL setomapval $NAME okey${i}-${k} oval${i}-${k}
  done
done

echo "Creating $NUM_OBJECTS objects in erausre coded pool"
for i in `seq 1 $NUM_OBJECTS`
do
  NAME="${EC_NAME}$i"
  rm -f $DATADIR/$NAME
  for j in `seq 1 10000`
  do
    echo "This is the erasure coded data for $NAME" >> $DATADIR/$NAME
  done
  ./rados -p $EC_POOL put $NAME $DATADIR/$NAME  2> /dev/null
  ACNT=`expr $i - 1`
  for k in `seq 0 $ACNT`
  do
    if [ $k = "0" ];
    then
      continue
    fi
    ./rados -p $EC_POOL setxattr $NAME key${i}-${k} val${i}-${k}
  done
  # Omap isn't supported in EC pools
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

ONEPG=`echo $ALLREPPGS | awk '{ print $1 }'`
#echo $ONEPG
ONEOSD=`ls -d dev/*/current/${ONEPG}_head | awk -F / '{ print $2 }' | head -1`
#echo $ONEOSD

# On export can't use stdout to a terminal
./ceph_filestore_dump --filestore-path dev/$ONEOSD --journal-path dev/$ONEOSD.journal --type export --pgid $ONEPG > /dev/tty 2> /tmp/tmp.$$
if [ $? = "0" ];
then
  echo Should have failed, but got exit 0
  ERRORS=`expr $ERRORS + 1`
fi
if head -1 /tmp/tmp.$$ | grep -- "stdout is a tty and no --file option specified" > /dev/null
then
  echo Correctly failed with message \"`head -1 /tmp/tmp.$$`\"
else
  echo Bad message to stderr \"`head -1 /tmp/tmp.$$`\"
  ERRORS=`expr $ERRORS + 1`
fi

# On import can't specify a PG
touch /tmp/foo.$$
./ceph_filestore_dump --filestore-path dev/$ONEOSD --journal-path dev/$ONEOSD.journal --type import --pgid $ONEPG --file /tmp/foo.$$ 2> /tmp/tmp.$$
if [ $? = "0" ];
then
  echo Should have failed, but got exit 0
  ERRORS=`expr $ERRORS + 1`
fi
if head -1 /tmp/tmp.$$ | grep -- "--pgid option invalid with import" > /dev/null
then
  echo Correctly failed with message \"`head -1 /tmp/tmp.$$`\"
else
  echo Bad message to stderr \"`head -1 /tmp/tmp.$$`\"
  ERRORS=`expr $ERRORS + 1`
fi
rm -f /tmp/foo.$$

# On import input file not found
./ceph_filestore_dump --filestore-path dev/$ONEOSD --journal-path dev/$ONEOSD.journal --type import --file /tmp/foo.$$ 2> /tmp/tmp.$$
if [ $? = "0" ];
then
  echo Should have failed, but got exit 0
  ERRORS=`expr $ERRORS + 1`
fi
if head -1 /tmp/tmp.$$ | grep -- "open: No such file or directory" > /dev/null
then
  echo Correctly failed with message \"`head -1 /tmp/tmp.$$`\"
else
  echo Bad message to stderr \"`head -1 /tmp/tmp.$$`\"
  ERRORS=`expr $ERRORS + 1`
fi

# On import can't use stdin from a terminal
./ceph_filestore_dump --filestore-path dev/$ONEOSD --journal-path dev/$ONEOSD.journal --type import --pgid $ONEPG < /dev/tty 2> /tmp/tmp.$$
if [ $? = "0" ];
then
  echo Should have failed, but got exit 0
  ERRORS=`expr $ERRORS + 1`
fi
if head -1 /tmp/tmp.$$ | grep -- "stdin is a tty and no --file option specified" > /dev/null
then
  echo Correctly failed with message \"`head -1 /tmp/tmp.$$`\"
else
  echo Bad message to stderr \"`head -1 /tmp/tmp.$$`\"
  ERRORS=`expr $ERRORS + 1`
fi

rm -f /tmp/tmp.$$

# Test --type list and generate json for all objects
echo "Testing --type list by generating json for all objects"
for pg in $OBJREPPGS $OBJECPGS
do
  OSDS=`ls -d dev/*/current/${pg}_head | awk -F / '{ print $2 }'`
  for osd in $OSDS
  do
    ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal --type list --pgid $pg >> /tmp/tmp.$$
    if [ "$?" != "0" ];
    then
      echo "Bad exit status $? from --type list request"
      ERRORS=`expr $ERRORS + 1`
    fi
  done
done

sort -u /tmp/tmp.$$ > $JSONOBJ
rm -f /tmp/tmp.$$

# Test get-bytes
echo "Testing get-bytes and set-bytes"
for file in ${DATADIR}/${REP_NAME}*
do
  rm -f /tmp/tmp.$$
  BASENAME=`basename $file`
  JSON=`grep \"$BASENAME\" $JSONOBJ`
  for pg in $OBJREPPGS
  do
    OSDS=`ls -d dev/*/current/${pg}_head | awk -F / '{ print $2 }'`
    for osd in $OSDS
    do
      if [ -e dev/$osd/current/${pg}_head/${BASENAME}_* ];
      then
        rm -f /tmp/getbytes.$$
        ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal  --pgid $pg "$JSON" get-bytes /tmp/getbytes.$$
        if [ $? != "0" ];
        then
          echo "Bad exit status $?"
          ERRORS=`expr $ERRORS + 1`
          continue
        fi
        diff -q $file /tmp/getbytes.$$ 
        if [ $? != "0" ];
        then
          echo "Data from get-bytes differ"
          echo "Got:"
          cat /tmp/getbyte.$$
          echo "Expected:"
          cat $file
          ERRORS=`expr $ERRORS + 1`
        fi
        echo "put-bytes going into $file" > /tmp/setbytes.$$
        ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal  --pgid $pg "$JSON" set-bytes - < /tmp/setbytes.$$
        if [ $? != "0" ];
        then
          echo "Bad exit status $? from set-bytes"
          ERRORS=`expr $ERRORS + 1`
        fi
        ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal  --pgid $pg "$JSON" get-bytes - > /tmp/testbytes.$$
        if [ $? != "0" ];
        then
          echo "Bad exit status $? from get-bytes"
          ERRORS=`expr $ERRORS + 1`
        fi
        diff -q /tmp/setbytes.$$ /tmp/testbytes.$$
        if [ $? != "0" ];
        then
          echo "Data after set-bytes differ"
          echo "Got:"
          cat /tmp/testbyte.$$
          echo "Expected:"
          cat /tmp/setbytes.$$
          ERRORS=`expr $ERRORS + 1`
        fi
        ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal  --pgid $pg "$JSON" set-bytes < $file
        if [ $? != "0" ];
        then
          echo "Bad exit status $? from set-bytes to restore object"
          ERRORS=`expr $ERRORS + 1`
        fi
      fi
    done
  done
done

rm -f /tmp/getbytes.$$ /tmp/testbytes.$$ /tmp/setbytes.$$

# Testing attrs
echo "Testing list-attrs get-attr"
for file in ${DATADIR}/${REP_NAME}*
do
  rm -f /tmp/tmp.$$
  BASENAME=`basename $file`
  JSON=`grep \"$BASENAME\" $JSONOBJ`
  for pg in $OBJREPPGS
  do
    OSDS=`ls -d dev/*/current/${pg}_head | awk -F / '{ print $2 }'`
    for osd in $OSDS
    do
      if [ -e dev/$osd/current/${pg}_head/${BASENAME}_* ];
      then
        ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal  --pgid $pg "$JSON" list-attrs > /tmp/attrs.$$
        if [ $? != "0" ];
        then
          echo "Bad exit status $?"
          ERRORS=`expr $ERRORS + 1`
          continue
        fi
        for key in `cat /tmp/attrs.$$`
        do
          ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal  --pgid $pg "$JSON" get-attr $key > /tmp/val.$$
          if [ $? != "0" ];
          then
            echo "Bad exit status $?"
            ERRORS=`expr $ERRORS + 1`
            continue
          fi
          if [ "$key" = "_" -o "$key" = "snapset" ];
          then
            continue
          fi
          OBJNUM=`echo $BASENAME | sed "s/$REP_NAME//"`
          echo -n $key | sed 's/_key//' > /tmp/checkval.$$
          cat /tmp/val.$$ | sed 's/val//' > /tmp/testval.$$
          diff -q /tmp/testval.$$ /tmp/checkval.$$
          if [ "$?" != "0" ];
          then
            echo Got `cat /tmp/val.$$` instead of val`cat /tmp/check.$$`
            ERRORS=`expr $ERRORS + 1`
          fi
        done
      fi
    done
  done
done

rm -rf /tmp/testval.$$ /tmp/checkval.$$ /tmp/val.$$ /tmp/attrs.$$

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

  OSD=4 ./vstart.sh -l -d > /dev/null 2>&1
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

rm -rf $DATADIR $JSONOBJ

if [ $ERRORS = "0" ];
then
  echo "TEST PASSED"
  exit 0
else
  echo "TEST FAILED WITH $ERRORS ERRORS"
  exit 1
fi

