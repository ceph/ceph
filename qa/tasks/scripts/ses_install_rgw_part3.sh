set -ex

minion_fqdn=$2

sed -i "s/^#role-rgw\/cluster\/$minion_fqdn/role-rgw\/cluster\/$minion_fqdn/g" /srv/pillar/ceph/proposals/policy.cfg
