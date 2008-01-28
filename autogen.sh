#!/bin/sh
rm -f config.cache
aclocal
autoconf
autoheader
automake -a --add-missing
exit
