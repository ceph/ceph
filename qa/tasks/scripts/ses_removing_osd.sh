set -ex

declare -a minion_fqdn="$1"

function wait_for_server () {
    sleep 20
    while ! nc -zv $1 22 
    do
        sleep 20
    done
}

minion=${minion_fqdn%%.*} # target-ses-097.ecp.suse.de -> target-ses-097
random_osd=$(ceph osd tree | grep -A 1 $minion | grep -Eo "osd\.[[:digit:]]+")
osd_id=${random_osd##*.} # osd.0 -> 0

ceph osd out $random_osd
ceph osd tree
ceph osd in $random_osd
salt $minion_fqdn service.stop ceph-osd@${osd_id} 2>/dev/null
ceph osd crush remove $random_osd
ceph auth del $random_osd
ceph osd down $random_osd
ceph osd rm $random_osd
ceph osd tree
sleep 3
osd_systemdisk=$(salt \* cephdisks.find_by_osd_id ${osd_id} --out json 2> /dev/null | jq -j '.[][].path') # /dev/vdb

salt $minion_fqdn cmd.run "sgdisk -Z $osd_systemdisk" 2>/dev/null

salt $minion_fqdn cmd.run "sgdisk -o -g $osd_systemdisk" 2>/dev/null

salt $minion_fqdn system.reboot || true

wait_for_server $minion_fqdn

ceph health | grep HEALTH_OK
