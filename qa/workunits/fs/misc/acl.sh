#!/bin/sh -x

set -e
mkdir -p testdir
cd testdir

set +e
setfacl -d -m u:nobody:rw .
if test $? != 0; then
	echo "Filesystem does not support ACL"
	exit 0
fi

set -e
c=0
while [ $c -lt 100  ]
do
	c=`expr $c + 1`
	# inherited ACL from parent directory's default ACL
	mkdir d1
	c1=`getfacl d1 | grep -c "nobody:rw"`
	echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
	c2=`getfacl d1 | grep -c "nobody:rw"`
	rmdir d1
	if [ $c1 -ne 2 ] || [ $c2 -ne 2 ]
	then
		echo "ERROR: incorrect ACLs"
		exit 1
	fi
done

cd ..
rmdir testdir
echo OK
