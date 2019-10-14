set -ex

declare -a storage_minions=("$@")

echo "### Getting random minion to install RGW on ###"
minion_fqdn=${storage_minions[0]}
minion=${minion_fqdn%%.*}

echo "### Getting second random minion to install RGW on ###"
minion2_fqdn=${storage_minions[1]}

minion2=${minion2_fqdn%%.*}

cat << EOF >> /srv/pillar/ceph/stack/global.yml

rgw_configurations:
  - us-east-1
  - us-east-2

EOF

for zone in us-east-1 us-east-2
do

    cat << EOF >> /srv/salt/ceph/configuration/files/ceph.conf.d/${zone}.conf

[client.{{ client }}]
rgw frontends = \"civetweb port=80\"
rgw dns name = {{ fqdn }}
rgw enable usage log = true
rgw zone=${zone}

EOF
    cp /srv/salt/ceph/rgw/files/rgw.j2 /srv/salt/ceph/rgw/files/${zone}.j2
done

echo "### Configuring 2 RGW zones ###"

SYSTEM_ACCESS_KEY=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 20 | head -n 1)
SYSTEM_SECRET_KEY=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 40 | head -n 1)

radosgw-admin realm create --rgw-realm=gold --default
radosgw-admin zonegroup delete --rgw-zonegroup=default || true
radosgw-admin zonegroup create --rgw-zonegroup=us --endpoints=http://rgw1:80 --master --default
radosgw-admin zone create --rgw-zonegroup=us --rgw-zone=us-east-1 --endpoints=http://${minion_fqdn}:80 \
    --access-key=$SYSTEM_ACCESS_KEY --secret=$SYSTEM_SECRET_KEY --default --master
radosgw-admin user create --uid=admin --display-name="Zone User" --access-key=$SYSTEM_ACCESS_KEY \
    --secret=$SYSTEM_SECRET_KEY --system
radosgw-admin period get
radosgw-admin period update --commit
radosgw-admin zone create --rgw-zonegroup=us --rgw-zone=us-east-2 --access-key=$SYSTEM_ACCESS_KEY \
    --secret=$SYSTEM_SECRET_KEY --endpoints=http://${minion2_fqdn}:80
radosgw-admin period update --commit

sed -i 's/^role-rgw/#role-rgw/g' /srv/pillar/ceph/proposals/policy.cfg

cat << EOF >> /srv/pillar/ceph/proposals/policy.cfg
role-us-east-1/cluster/${minion_fqdn}.sls
role-us-east-2/cluster/${minion2_fqdn}.sls
EOF
