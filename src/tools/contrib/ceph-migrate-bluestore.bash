#!/bin/bash
# https://tracker.ceph.com/issues/47839
# Signed-off-by: Chris Dunlop <chris@onthe.net.au>


######################################################################
function usage
{
	cat <<END
Usage: $0 osd device

Description:

Migrate an OSD from Filestore to BlueStore

Where:

osd    - OSD ID to migrate
device - raw device to migrate to, starting with /dev/disk/by-id/

E.g.:

ceph-migrate-bluestore 6 /dev/disk/by-id/ata-WDC_WD80EFZX-68UW8N0_VK0RKXTY

END
  exit 0
}
######################################################################

shopt -s -o errexit nounset pipefail
shopt -s extglob failglob inherit_errexit lastpipe

[[ $# -eq 2 ]] || usage
osd=$1
bluestore_device=$2

[[ $osd =~ ^[0-9]+$ ]] || error 'osd must be numeric'
[[
	-b $bluestore_device &&
	$bluestore_device =~ ^/dev/disk/by-id/ &&
	! $bluestore_device =~ -part[0-9]+$
]] || error "device must be a raw block device starting with /dev/disk/by-id/"

######################################################################
# Setup...
#

#
# VG used for block.db LVs
#
vgdb='vg-861d7200-578c-45c2-a44c-2f0c56427bf1'
vgs "${vgdb}" >& /dev/null || error "VG '${vgdb}' for block.db not found"

#
# Size of LV in $vgdb for the block.db
#
dblvsize=60G

#
# Prefix used for block LVs
#
block_prefix='osd-block'
 
#
# Some less(?) common we use - abort early if they're missing
#
cmds=(
	bc
	sgdisk
)

######################################################################
# Functions...
#
function runcmd
{
	local IFS=' '
	echo 1>&2 "$*"
	"$@"
}

function is_uuid
{
  [[ $1 =~ ^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$ ]]
}

#
# Compare the used size of the OSD with the new device
# (and arbitrarily 20% larger 'cos we don't want to fill it up)
#
function check-device-size
{
	# "ceph osd df" fields 7 and 8 - "RAW USE", size and units
	IFS=' ' read -r sz units <<< "$(ceph osd df | awk -v"id=${osd}" '$1==id { print $7, $8 }')"
	case $units in
	KiB) pow=1 ;;
	MiB) pow=2 ;;
	GiB) pow=3 ;;
	TiB) pow=4 ;;
	PiB) pow=5 ;;
	*) error "ceph df: units not recognized: ${units}" ;;
	esac
	osdbytes=$(printf '%.0f' "$(bc <<< "${sz} * 1024^${pow} * 1.2")")

	bdev=$(realpath "${bluestore_device}")
	bdev=${bdev##*/}
	[[ -e /sys/block/${bdev##*/}/size ]] || error "Can't find size for ${bluestore_device}"
	bdevbytes=$(($(<"/sys/block/${bdev##*/}/size") * 512))

	declare -p osdbytes bdevbytes

	((bdevbytes >= osdbytes)) || error "The block device isn't large enough"
}

#
# Check things look ok
#
# Is there a better way of checking, other than manually?
#
function check-ceph-ok
{
	local ans=r

	while [[ $ans = r ]]
	do
		runcmd ceph -s
		read -r -p $'\nCheck status above and press r to recheck or <Enter> to continue with scrub' ans
	done

	#
	# Run a scrub "to be sure, to be sure"
	#
	# For smaller OSDs we can see which PGs we need to watch for...
	#
	runcmd ceph pg ls-by-primary "${osd}" | awk '$1~/^[0-9]+\./ { print $1 }'
	runcmd ceph osd scrub "${osd}"

	hr
	tail -n0 -f "/var/log/ceph/ceph-osd.${osd}.log" &
	pid=$!
	sleep 2
	while ! read -r -t 10 -p $'\n\n\ntailing osd log file: press <Enter> to continue\n\n\n' ans
	do
		:
	done
	kill "${pid}"
	hr

	ans=r
	while [[ $ans = r ]]
	do
		runcmd ceph -s
		read -r -p $'\nCheck status above and press r to recheck or <Enter> to continue' ans
	done
}

#
# Disable the FileStore so it doesn't attempt to come back on reboot, but
# so we can revert back to it if necessary
#
# https://en.wikipedia.org/wiki/GUID_Partition_Table#Partition_type_GUIDs
# Partition GUID code: 4FBD7E29-9D25-41B8-AFD0-062C0CEFF05D (Ceph OSD)
# Partition GUID code: 0FC63DAF-8483-4772-8E79-3D69D8477DE4 (Linux filesystem data)
#
function disable-filestore
{
	#
	# Remove the original device from fstab if it's there
	# (it may be in here for xfs with logdev etc.)
	#
	if grep -qE '^[^#[:space:]]+[[:space:]]+'"${osddir}"'[[:space:]]' /etc/fstab
	then
		[[ -e /etc/fstab.${0##*/} ]] || cp -a /etc/fstab{,."${0##*/}"}
		sed -ri '/^[^#[:space:]]+[[:space:]]+'"${osddir//\//\\\/}"'[[:space:]]/ s/^/# /' /etc/fstab
	fi

	#
	# Change the partition type
	#
	[[ -e ${osd_json%.json}.part ]] ||
		runcmd sgdisk --backup="${osd_json%.json}.part" "${filestore_device}"
	part_guid=$(sgdisk -i1 "${filestore_device}" | sed -rn 's/^Partition GUID code: ([[:xdigit:]-]+) .*/\1/p')
	if [[ $part_guid = 4FBD7E29-9D25-41B8-AFD0-062C0CEFF05D ]]
	then
		runcmd sgdisk --typecode=1:0FC63DAF-8483-4772-8E79-3D69D8477DE4 "${filestore_device}"
		echo "${filestore_device} partition 1 changed to type 0FC63DAF-8483-4772-8E79-3D69D8477DE4 (Linux filesystem data)"
	fi
}

######################################################################
# Processing...
#

#
# Check we have the commands we need
#
for cmd in "${cmds[@]}"
do
	type "${cmd}" >& /dev/null || error "${cmd} utility required"
done

#
# Get/check OSD 
#
unit=ceph-osd@${osd}
runcmd systemctl is-enabled "${unit}" ||
	error "systemd unit ${unit} not enabled"

osddir=/var/lib/ceph/osd/ceph-${osd}
[[ -d $osddir ]] || error "No directory: ${osddir}"

fsid=$(< "${osddir}/fsid")
is_uuid "${fsid}" || error "fsid uuid not found in ${osddir}/fsid"

osd_json=/etc/ceph/osd/${osd}-${fsid}.json
[[ -f $osd_json ]] || error "File doesn't exist: ${osd_json}"

lvnewdb=${vgdb}/osd-db-${fsid}
authkey=$(sed -rn 's/^[[:space:]]+key[[:space:]]*=[[:space:]]*//p' "${osddir}/keyring")
[[ $authkey ]] || error "Can't get authkey from ${osddir}/keyring"

#
# We want the device containing the FileStore version of the OSD
# so we can disable it once the BlueStore version is up and running,
# so the FileStore doesn't contend with the BlueStore on reboot etc.
#
filestore_device=$(awk '$2=="'"${osddir}"'" { print $1; }' /etc/mtab)
[[ $filestore_device ]] || error "Can't find device currently mounted on ${osddir}"
[[ $filestore_device =~ ^/dev/sd[a-z]+[0-9]*$ ]] || error "Don't recognize device currently mounted on ${osddir}: ${filestore_device}"
filestore_device=${filestore_device%%+([0-9])}

declare -p unit block_prefix bluestore_device osd osddir fsid osd_json lvnewdb authkey filestore_device

runcmd check-device-size

#
# Create raw LV for block.db
#
runcmd lvcreate --yes -L "${dblvsize}" -n "${lvnewdb#*/}" "${lvnewdb%/*}"


#
# Prepare the new OSD
# osd-list.orig is so we can work out which osd was created
#
ceph osd ls > /tmp/osd-list.orig
runcmd ceph-volume lvm prepare --data "${bluestore_device}" --block.db "${lvnewdb}"

#
# Work out which OSD has been created
# Is there a better way of doing this?
#
ceph osd ls > /tmp/osd-list.new

new=$(comm -13 /tmp/osd-list.{orig,new})
[[ $new =~ ^[0-9]+$ ]] || error "New OSD id not found"

#
# remove the new OSD from the ceph database
# (it's left mounted)
#
runcmd ceph osd purge "${new}" --yes-i-really-mean-it

#
# Params for the newly created OSD
#
newdir=/var/lib/ceph/osd/ceph-${new}
lvnew=$(readlink "${newdir}/block"); lvnew=${lvnew#/dev/}

#
# lvfix is what we're going to rename the LV to so
# it ends in the (original) fsid
#
is_uuid "${lvnew#*/${block_prefix}-}" || error "LV not recognised: ${lvnew}"
lvfix=${lvnew%%/*}/${block_prefix}-${fsid}

declare -p new newdir lvnew lvfix

#
# the "dup" step only works if the destination has the same id and fsid
# as the source: fix 'em up
#
new_fsid=$(< "${newdir}/fsid")
args=(
	--deltag "ceph.osd_id=${new}"
	--addtag "ceph.osd_id=${osd}"

	--deltag "ceph.osd_fsid=${new_fsid}"
	--addtag "ceph.osd_fsid=${fsid}"

	--deltag "ceph.block_device=${lvnew}"
	--addtag "ceph.block_device=${lvfix}"
)
runcmd lvchange "${args[@]}" "${lvnew}"
runcmd lvchange "${args[@]}" "${lvnewdb}"

runcmd ceph-bluestore-tool set-label-key --dev "${newdir}/block"    --key whoami --value "${osd}"
runcmd ceph-bluestore-tool set-label-key --dev "${newdir}/block"    --key osd_uuid --value "${fsid}"
runcmd ceph-bluestore-tool set-label-key --dev "${newdir}/block.db" --key osd_uuid --value "${fsid}"

echo "${fsid}" > "${newdir}/fsid"

#
# Rename the LV so it ends in the (original) fsid
#
runcmd lvrename "${lvnew}" "${lvfix}"
runcmd ln -sf "/dev/${lvfix}" "${newdir}/block"
lvnew=$lvfix

#
# Remove the flags that mkfs has already been done - otherwise mkfs skips the actual mkfs!
#
runcmd ceph-bluestore-tool  rm-label-key --dev "${newdir}/block" --key mkfs_done
runcmd rm "${newdir}/mkfs_done"

#
# Empty out the new OSD filesystem
#
runcmd ceph-objectstore-tool --type bluestore --data-path "${newdir}" --fsid "${fsid}" --op mkfs --no-mon-config

#
# Stop the osd - the copy can't proceed if it's busy
#
runcmd systemctl is-active --quiet "ceph-osd@${osd}" &&
	runcmd systemctl stop "ceph-osd@${osd}"

#
# The actual copy...
#
runcmd time ceph-objectstore-tool --type filestore --data-path "/var/lib/ceph/osd/ceph-${osd}" --target-data-path "${newdir}" --op dup

#
# Fix up some keys from the copy
#
printf '[osd.%d]\n\tkey = %s\n' "${osd}" "${authkey}" > "${newdir}/key"
ceph-bluestore-tool set-label-key --dev "${newdir}/block" --key osd_key --value "${authkey}"
ceph-bluestore-tool  rm-label-key --dev "${newdir}/block" --key fsid

#
# Move the FileStore config file out of the way to avoid it being used on boot
#
runcmd mv "${osd_json}"{,.orig}

#
# prepare the mount points
#
runcmd umount "${osddir}"
runcmd umount "${newdir}"
runcmd rmdir "${newdir}"

#
# Start the new BlueStore version of the OSD 
#
runcmd ceph-volume lvm trigger "${osd}-${fsid}"

#
# Let things settle a little then check the new OSD is running
#
sleep 5
if ! systemctl is-active --quiet "${unit}"
then
	systemctl status "${unit}"
	exit 1
fi

runcmd check-ceph-ok

runcmd disable-filestore

exit 0
