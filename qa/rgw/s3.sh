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

tmp_bucket="test-`date +%s`"
tmpdir="tmp"

cleanup() {
	rm -fR libs3 tmp
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

check_error() {
	should_succeed=$1
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

do_op() {
	should_succeed=$1
	shift
	op=$1
	shift
	params="$@"
	echo "# $op" "$@" | tee -a $log
	$op "$@" > .cmd.log 2>&1
	log cat .cmd.log
	check_error $should_succeed
}

run_s3() {
	echo $s3 "$@" >> .cmd.log
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
	filename=$1
	dd if=/dev/urandom of=$tmpdir/$filename bs=4096 count=2048
	run_s3 put $tmp_bucket/$filename filename=$tmpdir/$filename
}

get_file() {
	filename=$1
	dest_fname=$2
	run_s3 get $tmp_bucket/$filename filename=$tmpdir/$dest_fname
	do_op 1 diff $tmpdir/$filename $tmpdir/$dest_fname
	rm -f $tmpdir/foo.tmp
}

get_acl() {
	filename=$1
	dest_fname=$2
	run_s3 getacl $tmp_bucket/$filename filename=$tmpdir/$dest_fname
}

set_acl() {
	filename=$1
	src_fname=$2
	run_s3 setacl $tmp_bucket/$filename filename=$tmpdir/$src_fname
}

delete_file() {
	filename=$1
	run_s3 delete $tmp_bucket/$filename
}

get_anon() {
	should_succeed=$1
	bucket=$2
	fname=$3
	dest=$tmpdir/$4

	echo "# get_anon $@"

	url="http://$bucket.$S3_HOSTNAME/$fname"
	wget $url -O $dest > .cmd.log 2>&1
	res=$?
	log cat .cmd.log
	if [ $res -ne 0 ]; then
		echo "ERROR: Could not fetch file anonymously (url=$url)" > .cmd.log
	fi
	check_error $should_succeed
}

add_acl() {
	filename=$1
	acl=$2
	echo $acl >> $tmpdir/$filename
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

	do_op 1 get_acl foo foo.acl
	get_anon 0 $tmp_bucket foo foo.anon
	add_acl foo.acl "Group All Users READ"

	do_op 1 set_acl foo foo.acl
	get_anon 1 $tmp_bucket foo foo.anon

	do_op 1 delete_file foo
	do_op 1 delete_bucket $tmp_bucket
}


main "$@"


