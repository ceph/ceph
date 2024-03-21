#!/usr/bin/env bash

set -xe

client_type=$1

case ${client_type} in
	"kclient")
		# test contents protection
		echo "hello" > fscrypt_test_file
		touch ../fscrypt_kclient_ready
		while [ -f ../fscrypt_kclient_ready ]
		do
			sleep 5
		done
		if grep -q "hello" fscrypt_test_file; then
			echo "Fscrypt contents protect test successfully!"
		else
			echo "Fscrypt contents protect test failed!"
			exit 1
		fi
		rm -f fscrypt_test_file

		# test creating directory dentries, the fuse client should
		# fail and there should be no new dentries to be created
		mkdir fscrypt_dir
		touch ../fscrypt_kclient_ready
		while [ -f ../fscrypt_kclient_ready ]
		do
			sleep 5
		done
		num=$(ls -l fscrypt_dir/ | wc -l)
		if [ $num -eq 1 ]; then
			echo "Fscrypt create dir dentries protect test successfully!"
		else
			echo "Fscrypt create dir dentries protect test failed!"
			exit 1
		fi
		touch ../fscrypt_kclient_ready

		;;
	"fuse")
		while [ ! -f ../fscrypt_kclient_ready ]
		do
			sleep 5
		done
		set +e
		echo > ./*
		set -e
		rm -f ../fscrypt_kclient_ready

		while [ ! -f ../fscrypt_kclient_ready ]
		do
			sleep 5
		done
		set +e
		touch `ls`/fscrypt_test_file
		mkdir `ls`/fscrypt_subdir
		ln -s . `ls`/fscrypt_symlink
		set -e
		rm -f ../fscrypt_kclient_ready

		# Break possibly dead loop just in case the kclient creates the
		# "../fscrypt_kclient_ready" file but removes it immediately when
		# unmounting
		cnt=20
		while [ ! -f ../fscrypt_kclient_ready -a $cnt != 0 ]
		do
			cnt=$((cnt-1))
			sleep 5
		done
		;;
	*)
		echo "Unknown client type $1"
		exit 1
esac
