set -ex

minions=("$@")

for i in ${!minions[@]}
do
    minion=${minions[i]%%.*}
    salt ${minions[i]} cmd.run "systemctl status ceph-radosgw@us-east-$((i + 1)).${minion}.service"
done

ceph health | grep HEALTH_OK

sed -i "s/^role-us-east/#role-us-east/g" /srv/pillar/ceph/proposals/policy.cfg
