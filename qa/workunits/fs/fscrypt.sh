#!/usr/bin/env bash

set -xe

mydir=`dirname $0`

if [ $# -ne 2 ]
then
	echo "2 parameters are required!\n"
	echo "Usage:"
	echo "  fscrypt.sh <type> <testdir>"
	echo "  type: should be any of 'none', 'unlocked' or 'locked'"
	echo "  testdir: the test direcotry name"
	exit 1
fi

fscrypt=$1
testcase=$2
testdir=fscrypt_test_${fscrypt}_${testcase}
mkdir $testdir

XFSPROGS_DIR='xfprogs-dev-dir'
XFSTESTS_DIR='xfstest-dev-dir'
export XFS_IO_PROG="$(type -P xfs_io)"

# Setup the xfstests env
setup_xfstests_env()
{
	git clone https://git.ceph.com/xfstests-dev.git $XFSTESTS_DIR --depth 1
	pushd $XFSTESTS_DIR
	. common/encrypt
	popd
}

install_deps()
{
	local system_value=$(sudo lsb_release -is | awk '{print tolower($0)}')
	case $system_value in
		"centos" | "centosstream" | "fedora")
			sudo yum install -y inih-devel userspace-rcu-devel \
				libblkid-devel gettext libedit-devel \
				libattr-devel device-mapper-devel libicu-devel
			;;
		"ubuntu" | "debian")
			sudo apt-get install -y libinih-dev liburcu-dev \
				libblkid-dev gettext libedit-dev libattr1-dev \
				libdevmapper-dev libicu-dev pkg-config
			;;
		*)
			echo "Unsupported distro $system_value"
			exit 1
			;;
	esac
}

# Install xfsprogs-dev from source to support "add_enckey" for xfs_io
install_xfsprogs()
{
	local install_xfsprogs=0

	xfs_io -c "help add_enckey" | grep -q 'not found' && install_xfsprogs=1

	if [ $install_xfsprogs -eq 1 ]; then
		install_deps

		git clone https://git.ceph.com/xfsprogs-dev.git $XFSPROGS_DIR --depth 1
		pushd $XFSPROGS_DIR
		make
		sudo make install
		popd
	fi
}

clean_up()
{
	rm -rf $XFSPROGS_DIR
	rm -rf $XFSTESTS_DIR
	rm -rf $testdir
}

# For now will test the V2 encryption policy only as the
# V1 encryption policy is deprecated

install_xfsprogs
setup_xfstests_env

# Generate a fixed keying identifier
raw_key=$(_generate_raw_encryption_key)
keyid=$(_add_enckey $testdir "$raw_key" | awk '{print $NF}')

case ${fscrypt} in
	"none")
		# do nothing for the test directory and will test it
		# as one non-encrypted directory.
		pushd $testdir
		${mydir}/../suites/${testcase}.sh
		popd
		clean_up
		;;
	"unlocked")
		# set encrypt policy with the key provided and then
		# the test directory will be encrypted & unlocked
		_set_encpolicy $testdir $keyid
		pushd $testdir
		${mydir}/../suites/${testcase}.sh
		popd
		clean_up
		;;
	"locked")
		# remove the key, then the test directory will be locked
		# and any modification will be denied by requiring the key
		_rm_enckey $testdir $keyid
		clean_up
		;;
	*)
		clean_up
		echo "Unknown parameter $1"
		exit 1
esac
