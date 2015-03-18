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

if test -d ".git" ; then
  force=$(if git submodule usage 2>&1 | grep --quiet 'update.*--force'; then echo --force ; fi)
  if ! git submodule sync || ! git submodule update $force --init --recursive; then
    echo "Error: could not initialize submodule projects"
    echo "  Network connectivity might be required."
    exit 1
  fi
fi

rm -f config.cache
aclocal -I m4 --install
check_for_pkg_config
libtoolize --force --copy
autoconf
autoheader
automake -a --add-missing -Wall
( cd src/gtest && autoreconf -fvi; )
exit
