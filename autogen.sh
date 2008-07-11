#!/bin/sh
rm -f config.cache
aclocal #-I m4
autoconf
autoheader
automake -a --add-missing
exit
