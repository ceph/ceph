#!/bin/sh
rm -f config.cache
aclocal #-I m4
libtoolize --force --copy
autoconf
autoheader
automake -a --add-missing
exit
