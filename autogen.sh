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

rm -f config.cache
aclocal -I m4 --install
check_for_pkg_config
libtoolize --force --copy
autoconf
autoheader
automake -a --add-missing -Wall
( cd src/gtest && autoreconf -fvi; )
exit
