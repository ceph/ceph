set -ex

cat << EOF >> /srv/pillar/ceph/rgw.sls
rgw_configurations:
  rgw:
    users:
      - { uid: "admin", name: "admin", email: "demo@demo.nil", system: True }
EOF

for minion_fqdn in $@
do 
    echo "role-rgw/cluster/${minion_fqdn}.sls" >> /srv/pillar/ceph/proposals/policy.cfg
done
