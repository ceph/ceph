#!/bin/bash

die() {
        echo ${@}
        exit 1
}

usage()
{
        cat << EOF
$0: sets up a chroot environment for building the ceph server
usage:
-h                      Show this message

-r [install_dir]        location of the root filesystem to install to
                        example: -r /images/sepia/

-s [src_dir]            location of the directory with the source code
                        example: -s ./src/ceph
EOF
}

cleanup() {
        umount -l "${INSTALL_DIR}/mnt/tmp"
        umount -l "${INSTALL_DIR}/proc"
        umount -l "${INSTALL_DIR}/sys"
}

INSTALL_DIR=
SRC_DIR=
while getopts “hr:s:” OPTION; do
        case $OPTION in
        h) usage; exit 1 ;;
        r) INSTALL_DIR=$OPTARG ;;
        s) SRC_DIR=$OPTARG ;;
        ?) usage; exit
        ;;
        esac
done

[ $EUID -eq 0 ] || die "This script uses chroot, which requires root permissions."

[ -d "${INSTALL_DIR}" ] || die "No such directory as '${INSTALL_DIR}'. \
You must specify an install directory with -r"

[ -d "${SRC_DIR}" ] || die "no such directory as '${SRC_DIR}'. \
You must specify a source directory with -s"

readlink -f ${SRC_DIR} || die "readlink failed on ${SRC_DIR}"
ABS_SRC_DIR=`readlink -f ${SRC_DIR}`

trap cleanup INT TERM EXIT

mount --bind "${ABS_SRC_DIR}" "${INSTALL_DIR}/mnt/tmp" || die "bind mount failed"
mount -t proc none "${INSTALL_DIR}/proc" || die "mounting proc failed"
mount -t sysfs none "${INSTALL_DIR}/sys" || die "mounting sys failed"

echo "$0: starting chroot."
echo "cd /mnt/tmp before building"
echo
chroot ${INSTALL_DIR} env HOME=/mnt/tmp /bin/bash

echo "$0: exiting chroot."

exit 0
