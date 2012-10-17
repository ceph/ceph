#!/bin/sh -x

set -e

check_perms() {

	file=$1
	r=$(ls -la ${file})
	if test $? != 0; then
		echo "ERROR: File listing/stat failed"
		exit 1
	fi

	perms=$2
	if test "${perms}" != $(echo ${r} | awk '{print $1}'); then
		echo "ERROR: Permissions should be ${perms}"
		exit 1
	fi
}

echo "foo" > foo
if test $? != 0; then
	echo "ERROR: Failed to create file foo"
	exit 1
fi

check_perms foo "-rw-r--r--"

chmod 400 foo
if test $? != 0; then
	echo "ERROR: Failed to change mode of foo"
	exit 1
fi

check_perms foo "-r--------"

set +e
echo "bar" >> foo
if test $? = 0; then
	echo "ERROR: Write to read-only file should Fail"
	exit 1
fi

set -e
chmod 600 foo
echo "bar" >> foo
if test $? != 0; then
	echo "ERROR: Write to writeable file failed"
	exit 1
fi

check_perms foo "-rw-------"

echo "foo" >> foo
if test $? != 0; then
	echo "ERROR: Failed to write to file"
	exit 1
fi
