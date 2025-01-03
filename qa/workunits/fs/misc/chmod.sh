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
	if test "${perms}" != $(echo ${r} | awk '{print $1}') && \
           test "${perms}." != $(echo ${r} | awk '{print $1}') && \
           test "${perms}+" != $(echo ${r} | awk '{print $1}'); then
		echo "ERROR: Permissions should be ${perms}"
		exit 1
	fi
}

file=test_chmod.$$

echo "foo" > ${file}
if test $? != 0; then
	echo "ERROR: Failed to create file ${file}"
	exit 1
fi

chmod 400 ${file}
if test $? != 0; then
	echo "ERROR: Failed to change mode of ${file}"
	exit 1
fi

check_perms ${file} "-r--------"

set +e
echo "bar" >> ${file}
if test $? = 0; then
	echo "ERROR: Write to read-only file should Fail"
	exit 1
fi

set -e
chmod 600 ${file}
echo "bar" >> ${file}
if test $? != 0; then
	echo "ERROR: Write to writeable file failed"
	exit 1
fi

check_perms ${file} "-rw-------"

echo "foo" >> ${file}
if test $? != 0; then
	echo "ERROR: Failed to write to file"
	exit 1
fi
