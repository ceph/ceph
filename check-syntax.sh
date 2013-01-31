#!/bin/bash

which pyflakes > /dev/null
if test $? != 0; then
    echo "$0 requires pyflakes (sudo apt-get install pyflakes)"
    exit 1
fi

d=$(dirname $0)
for f in $(find ${d}/teuthology | grep py$); do
    if test -n "${V}"; then
	echo "checking ${f}"
    fi
    pyflakes ${f} > >( \
	grep -v "'Lock' imported but unused" | \
	grep -v "'MachineLock' imported but unused" \
	)
done
