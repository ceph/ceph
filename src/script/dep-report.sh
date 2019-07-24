#! /usr/bin/env bash

shopt -s nullglob

warn() 
{
  printf '%s\n' "$@" >&2
}

die() 
{
  local st="$?"
  warn "$@"
  exit "$st"
}

PLATFORM=$(lsb_release -is)

TMPFILE1=$(mktemp --tmpdir depA.XXXXXX)|| exit 1
TMPFILE2=$(`mktemp --tmpdir depB.XXXXXX) || exit 2
TMPFILE3=$(mktemp --tmpdir depB.XXXXXX) || exit 3

cleanup() {
    rm -f $TMPFILE1
    rm -f $TMPFILE2
    rm -f $TMPFILE3
}
trap cleanup INT EXIT

# find all the .deps directories
depdirs=()
while IFS= read -rd '' dir
  do 
    depdirs+=("$dir")
  done < <(find . -type d -name '*.deps' -print0)

if ((${#depdirs[@]} == 0)); then
  die "No depdirs found. Ceph must be built before running the dependency check"
fi

# find all the headers
echo "Looking for headers ... " >&2
find "${depdirs[@]}" -type f \( -name '*.Po' -o -name '*.Plo' \) -print | tee -a "$TMPFILE1"

#  Add in required libraries
echo "Looking for libraries ... " >&2
LIB_PATHS="/lib64 /usr/lib64 /lib /usr/lib"
FIND=$(command -v find)
autoconf --trace AC_CHECK_LIB | while IFS=: read -r _ _ _ LIB _
do
    for PATH in "$LIB_PATHS"
    do
        $FIND "$PATH" -name "lib$LIB.so*" -print  2> /dev/null >> "$TMPFILE1"
    done
done
autoconf --trace AC_SEARCH_LIBS | while IFS=: read -r _ _ _ _ LIBLIST _
do
    for LIB in "$LIBLIST" ; do
        for PATH in "$LIB_PATHS" ; do
            $FIND "$PATH" -name "lib$LIB.so*" -print  2> /dev/null >> "$TMPFILE1"
        done
    done
done
autoconf --trace PKG_CHECK_MODULES | cut -d: -f5 | cut -d' ' -f1 | while read PKG
do
    LIBLIST=(pkg-config --libs $PKG 2> /dev/null)
    for LIB in "$LIBLIST" ; do 
        LIB=${LIB#-l}
        for PATH in "$LIB_PATHS"
        do
            $FIND "$PATH" -name "lib$LIB.so*" -print  2> /dev/null >> "$TMPFILE1"
        done
    done
done

# path to package
echo "Looking up packages for hdr and lib paths ... " >&2
sort "$TMPFILE1" | uniq > "$TMPFILE2"

rm $TMPFILE1
while read -r LINE
  do
    package=$(rpm -q --whatprovides $LINE)
    echo "$package" >> "$TMPFILE1"
  done < "$TMPFILE2"

#  Add in any libraries needed for the devel packages
echo "Adding libraries for devel packages ... " >&2
sort $TMPFILE1 | uniq > $TMPFILE3
cat $TMPFILE3 | grep devel | while read PACKAGE
do
    NAME=$(rpm -q --qf %{NAME} "$PACKAGE")
    NAME=${NAME%-devel}
    #echo "looking for matching $NAME ... " >&2
    LPACKAGE=$(rpm -q $NAME 2> /dev/null)
    if [ $? -eq 0 ] ; then
        #echo "Found $LPACKAGE ... " >&2
        echo $LPACKAGE >> $TMPFILE1
    else
        LPACKAGE=$(rpm -q "$NAME-libs" 2> /dev/null)
        if [ $? -eq 0 ] ; then
            #echo "Found $LPACKAGE ... " >&2
            echo "$LPACKAGE" >> "$TMPFILE1"
        fi
    fi
done
echo "Checking licenses ... " >&2

#  Read package list and generate report
sort $TMPFILE1 | uniq > $TMPFILE2

rm $TMPFILE1
echo -e "\nPackage Dependencies:\n"
while read -r PACKAGE
do
    LICENSE=$(rpm -q --qf %{LICENSE} "$PACKAGE")
    NAME=(rpm -q --qf %{NAME} "$PACKAGE")
    echo "${PACKAGE}.rpm"
    echo "    Name:    $NAME"
    echo "    License: $LICENSE"
done < "$TMPFILE2"

cat <<EOF
Source code Dependencies:

src/leveldb
  Name:    leveldb
  License: Google Public License

Done
EOF
