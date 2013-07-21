#!/bin/sh

check_for_pkg_config() {
    which pkg-config >/dev/null && return

    echo
    echo "Error: could not find pkg-config"
    echo
    echo "Please make sure you have pkg-config installed."
    echo
    exit 1
}

if [ `which libtoolize` ]; then
    LIBTOOLIZE=libtoolize
elif [ `which glibtoolize` ]; then
    LIBTOOLIZE=glibtoolize
else
  echo "Error: could not find libtoolize"
  echo "  Please install libtoolize or glibtoolize."
  exit 1
fi

rm -f config.cache
aclocal -I m4 --install
check_for_pkg_config
$LIBTOOLIZE --force --copy
autoconf
autoheader
automake -a --add-missing -Wall
( cd src/gtest && autoreconf -fvi; )
exit
