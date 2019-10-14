set -ex

declare -a minion_fqdn="$1"

echo "### Getting random minion and its random OSD ###"
minion=$(echo $minion_fqdn | cut -d . -f 1)
random_osd=$(ceph osd tree | grep -A 1 $minion | grep -o "osd\.".* | awk '{print$1}')
osd_id=${random_osd#*.}

ceph health | grep HEALTH_OK

ceph osd tree | grep -w $osd_id

ceph osd pool rm replacedisk replacedisk --yes-i-really-really-mean-it

