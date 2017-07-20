#! /usr/bin/env bash

shopt -s nullglob

PLATFORM=`lsb_release -is`

TMPFILE1=`mktemp --tmpdir depA.XXXXXX` || exit 1
TMPFILE2=`mktemp --tmpdir depB.XXXXXX` || exit 2
TMPFILE3=`mktemp --tmpdir depB.XXXXXX` || exit 3

cleanup() {
    rm -f $TMPFILE1
    rm -f $TMPFILE2
    rm -f $TMPFILE3
}
trap cleanup INT EXIT

# find all the .deps directorys
DEPDIRS=`find . -name ".deps" -print`
if [ -z "$DEPDIRS" ] ; then
    echo "No depdirs found.  Ceph must be built before running dependecy check"
fi

# find all the headers
echo "Looking for headers ... " >&2
for DIR in $DEPDIRS
do
    for file in $DIR/*.Po $DIR/*.Plo
    do
        #echo "$DIR:  $file" >&2
	cut -d: -f1 $file | grep "^/" >> $TMPFILE1
    done
done

#  Add in required libraries
echo "Looking for libraries ... " >&2
LIB_PATHS="/lib64 /usr/lib64 /lib /usr/lib"
FIND=`which find`
autoconf --trace AC_CHECK_LIB | cut -d: -f4 | while read LIB
do
    for PATH in $LIB_PATHS
    do
        $FIND $PATH -name "lib$LIB.so*" -print  2> /dev/null >> $TMPFILE1
    done
done
autoconf --trace AC_SEARCH_LIBS | cut -d: -f5 | while read LIBLIST
do
    for LIB in $LIBLIST ; do
        for PATH in $LIB_PATHS ; do
            $FIND $PATH -name "lib$LIB.so*" -print  2> /dev/null >> $TMPFILE1
        done
    done
done
autoconf --trace PKG_CHECK_MODULES | cut -d: -f5 | cut -d' ' -f1 | while read PKG
do
    LIBLIST=`pkg-config --libs $PKG 2> /dev/null`
    for LIB in $LIBLIST ; do 
        LIB=${LIB#-l}
        for PATH in $LIB_PATHS
        do
            $FIND $PATH -name "lib$LIB.so*" -print  2> /dev/null >> $TMPFILE1
        done
    done
done

# path to package
echo "Looking up packages for hdr and lib paths ... " >&2
sort $TMPFILE1 | uniq > $TMPFILE2

rm $TMPFILE1
cat $TMPFILE2 | while read LINE
do
    package=`rpm -q --whatprovides $LINE`
    echo $package >> $TMPFILE1
done

#  Add in any libraries needed for the devel packages
echo "Adding libraries for devel packages ... " >&2
sort $TMPFILE1 | uniq > $TMPFILE3
cat $TMPFILE3 | grep devel | while read PACKAGE
do
    NAME=`rpm -q --qf %{NAME} $PACKAGE`
    NAME=${NAME%-devel}
    #echo "looking for matching $NAME ... " >&2
    LPACKAGE=`rpm -q $NAME 2> /dev/null` 
    if [ $? -eq 0 ] ; then
        #echo "Found $LPACKAGE ... " >&2
        echo $LPACKAGE >> $TMPFILE1
    else
        LPACKAGE=`rpm -q $NAME-libs 2> /dev/null` 
        if [ $? -eq 0 ] ; then
            #echo "Found $LPACKAGE ... " >&2
            echo $LPACKAGE >> $TMPFILE1
        fi
    fi
done
echo "Checking licenses ... " >&2

#  Read package list and generate report
sort $TMPFILE1 | uniq > $TMPFILE2

rm $TMPFILE1
echo -e "\nPackage Dependencies:\n"
cat $TMPFILE2 | while read PACKAGE
do
    LICENSE=`rpm -q --qf %{LICENSE} $PACKAGE`
    NAME=`rpm -q --qf %{NAME} $PACKAGE`
    echo "${PACKAGE}.rpm"
    echo "    Name:    $NAME"
    echo "    License: $LICENSE"
done

echo -e "\nSource Code Dependencies:\n"
echo "src/leveldb"
echo "    Name:    leveldb"
echo "    License: Google Public License"

echo "Done"
#echo "DEPDIRS: $DEPDIRS"

