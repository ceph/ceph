#!/usr/bin/env bash
set -xe

client_type=$1
TOP=`pwd`
TOP=$(dirname "$(pwd)")
KDIR=${TOP}/kclient
FDIR=${TOP}/fuse

setup_fscrypt_env() {
	DIR=$1
	if [ ! -f /etc/fscrypt.conf ]
	then
		sudo fscrypt setup --force --verbose
	fi

	if [ ! -d $TOP/.fscrypt ]
	then
		sudo fscrypt setup ${DIR} --force --verbose --all-users
		sudo fscrypt status --verbose
	fi
}

encrypt_dir() {
	DIR=$1
	KEY=${DIR}/../key
	mkdir -p ${DIR}

	if [ ! -f ${KEY} ]
	then
		sudo dd if=/dev/urandom of=${KEY} bs=32 count=1
	fi
	sudo fscrypt encrypt --verbose --source=raw_key --name=protector-${client_type} --no-recovery --skip-unlock --key=${KEY} ${DIR}
	#sudo fscrypt encrypt --verbose --source=raw_key --name=randprotector1 --no-recovery --skip-unlock --key=${KEY} ${DIR}
}

unlock_dir() {
	DIR=$1
	KEY=${DIR}/../key

	sudo fscrypt unlock --verbose --key=${KEY} ${DIR}
}

generate_tree() {
	DIR=$1
	cd $DIR
	wget -O linux.tar.xz http://download.ceph.com/qa/linux-6.5.11.tar.xz
	mkdir linux
	cd linux
	tar xJf ../linux.tar.xz
}

build_kernel() {
	DIR=$1
	cd ${DIR}
	cd linux*
	make defconfig
	make -j`grep -c processor /proc/cpuinfo`
}

setup_fscrypt_env ${TOP}
K_LINUX_ROOT=${KDIR}/linux
F_LINUX_ROOT=${FDIR}/linux
case ${client_type} in
	"kclient")
		mkdir -p ${KDIR}
                encrypt_dir ${KDIR}
		unlock_dir ${KDIR}
		generate_tree ${KDIR}
		K_LINUX_ROOT=${KDIR}/linux
		touch ${TOP}/k_ready
		while [ ! -f ${TOP}/f_ready ]
		do
			sleep 1s
		done
		unlock_dir ${FDIR}
		build_kernel ${F_LINUX_ROOT}
		;;
	"fuse")
		mkdir -p ${FDIR}
                encrypt_dir ${FDIR}
		unlock_dir ${FDIR}
		generate_tree ${FDIR}
		F_LINUX_ROOT=${FDIR}/linux
		touch ${TOP}/f_ready
		while [ ! -f ${TOP}/k_ready ]
		do
			sleep 1s
		done
		unlock_dir ${KDIR}
		build_kernel ${K_LINUX_ROOT}
		;;
	*)
		echo not found
		exit 1
esac
