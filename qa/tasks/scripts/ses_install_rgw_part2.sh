set -ex

for minion_fqdn in $@
do
    minion=${minion_fqdn%%.*}
    salt $minion_fqdn service.status ceph-radosgw@rgw.${minion}.service 2>/dev/null
    salt $minion_fqdn service.restart ceph-radosgw@rgw.${minion}.service 2>/dev/null
    salt $minion_fqdn service.status ceph-radosgw@rgw.${minion}.service | grep -i true 2>/dev/null
done

sed -i "s/^role-rgw\/cluster\/$minion_fqdn/#role-rgw\/cluster\/$minion_fqdn/g" /srv/pillar/ceph/proposals/policy.cfg
