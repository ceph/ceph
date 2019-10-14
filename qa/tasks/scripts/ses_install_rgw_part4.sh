set -ex

sed -i 's/^role-rgw/#role-rgw/g' /srv/pillar/ceph/proposals/policy.cfg
