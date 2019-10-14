set -ex

for minion in $@
do
    salt $minion service.status nfs-ganesha.service 2>/dev/null
    salt $minion service.restart nfs-ganesha.service 2>/dev/null
    sleep 15
    salt $minion service.status nfs-ganesha.service | grep -i true 2>/dev/null
done

sed -i "s/^role-ganesha\/cluster\/$minion/#role-ganesha\/cluster\/$minion/g" /srv/pillar/ceph/proposals/policy.cfg
