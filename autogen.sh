#!/bin/sh
rm -f config.cache
libtoolize --force --copy
aclocal -I m4
autoconf
autoheader
automake -a --add-missing
exit
