#!/bin/sh

cd rocksdb
git clean -dffx
cd ..
echo "EXTRA_DIST += \\" > /tmp/$$
for f in `find rocksdb -type f | grep -v -e /.git$ -e ^rocksdb/tools/rdb | sort`; do
		echo "  $f \\" >> /tmp/$$
done
echo "  rocksdb/AUTHORS" >> /tmp/$$
mv /tmp/$$ Makefile-rocksdb.am
