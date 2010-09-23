#!/bin/sh


origdir=`pwd`
# set -x

load_credentials() {
	if [ -e ~/.s3 ]; then
		source ~/.s3
	else
		echo "ERROR: Credentials not defined!"
		exit 1
	fi
}

if [ "$S3_ACCESS_KEY_ID" == "" ] ||
   [ "$S3_HOSTNAME" == "" ] ||
   [ "$S3_SECRET_ACCESS_KEY" == "" ]; then
	 load_credentials
fi

bindir=${origdir}/libs3/build/bin
libdir=${origdir}/libs3/build/lib
log=${origdir}/s3.log
export LD_LIBRARY_PATH=${libdir}
s3=${bindir}/s3

tmp_bucket="test-`date | sed 's/[: ]/-/g'`"

cleanup() {
	rm -fR libs3
}

build() {
	echo "Checking out source"
	log git clone git://github.com/wido/libs3.git
	echo "Building"
	log make -C libs3
}

init() {
	cleanup
	build
	mkdir -p tmp
}

log() {
	"$@" >> $log
}


do_op() {
	should_succeed=$1
	shift
	op=$1
	shift
	params="$@"
	echo "# $op" "$@" | tee -a $log
	$op "$@" > .cmd.log 2>&1
	log cat .cmd.log

	fail=`grep -c ERROR .cmd.log`
	[ $fail -eq 0 ] && success=1 || success=0
	if [ $success -ne $should_succeed ]; then
		[ $should_succeed -ne 0 ] && echo "Command failed:"
		[ $should_succeed -eq 0 ] && echo "Command succeeded unexpectedly:"
		echo "$op $params"
		cat .cmd.log
		exit 1
	fi
}

run_s3() {
	echo $s3 "$@"
	$s3 "$@"
}

create_bucket() {
	bucket_name=$1

	run_s3 create $tmp_bucket
}

delete_bucket() {
	bucket_name=$1

	run_s3 delete $tmp_bucket
}

create_file() {
	file_name=$1
	dd if=/dev/urandom of=tmp/$file_name bs=4096 count=2048
	run_s3 put $tmp_bucket/$file_name filename=tmp/$file_name
}

get_file() {
	file_name=$1
	dest_fname=$2
	run_s3 get $tmp_bucket/$file_name filename=tmp/$dest_fname
	do_op 1 diff tmp/$file_name tmp/$dest_fname
	rm -f tmp/foo.tmp
}

delete_file() {
	file_name=$1
	run_s3 delete $tmp_bucket/$file_name
}


main() {
	log echo "****************************************************************"
	log echo "* `date`" >> $log
	log echo "****************************************************************"
	init
	do_op 1 create_bucket $tmp_bucket
	do_op 0 create_bucket $tmp_bucket
	do_op 1 create_file foo
	do_op 1 get_file foo foo.tmp
	do_op 1 delete_file foo
	do_op 1 delete_bucket $tmp_bucket
}


main "$@"


