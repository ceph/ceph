set -ex

for minion in $@
do
    echo "role-ganesha/cluster/${minion}.sls" >> /srv/pillar/ceph/proposals/policy.cfg
done

echo "role-mds/cluster/${minion}.sls" >> /srv/pillar/ceph/proposals/policy.cfg
