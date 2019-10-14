set -ex

declare -a minion_fqdn="$1"

echo "### Getting random minion and its random OSD ###"
minion=${minion_fqdn%%.*}
random_osd=$(ceph osd tree | grep -A 1 $minion | grep -o "osd\.".* | awk '{print$1}')
osd_id=${random_osd#*.}

vg_name=$(salt $minion_fqdn cmd.run "find /var/lib/ceph/osd/ceph-$osd_id -type l -name block \
    -exec readlink {} \; | rev | cut -d / -f 2 | rev" | tail -1 | tr -d ' ')

minion_osd_disk_partition=$(salt $minion_fqdn cmd.run "pvdisplay -m 2>/dev/null | grep -B 1 $vg_name \
    | grep \"PV Name\" | awk '{print \$3}' | cut -d / -f 3" | tail -1 | tr -d ' ')

minion_osd_disk=$(echo $minion_osd_disk_partition | tr -d [:digit:])

salt-run disengage.safety >/dev/null 2>&1

salt-run osd.replace $osd_id

ceph osd rm $osd_id

ceph osd crush remove $random_osd

ceph osd tree

ceph health detail

echo "Adding new device to OSD"
