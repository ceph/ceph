set -ex

declare -a monitor_minions=("$@")

minion_fqdn=${monitor_minions[0]}
sed -i '/^role-mds/d' /srv/pillar/ceph/proposals/policy.cfg
echo "role-mds/cluster/${minion_fqdn}.sls" >> /srv/pillar/ceph/proposals/policy.cfg

