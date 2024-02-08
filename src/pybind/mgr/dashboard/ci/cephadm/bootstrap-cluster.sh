#!/usr/bin/env bash

set -x

export PATH=/root/bin:$PATH
mkdir /root/bin

export CEPHADM_IMAGE='quay.ceph.io/ceph-ci/ceph:main'

CEPHADM="/root/bin/cephadm"
CEPHADM_SRC="/mnt/{{ ceph_dev_folder }}/src/cephadm/cephadm"

cp $CEPHADM_SRC $CEPHADM

mkdir -p /etc/ceph
mon_ip=$(ifconfig eth0  | grep 'inet ' | awk '{ print $2}')

bootstrap_extra_options='--allow-fqdn-hostname --dashboard-password-noupdate'

# commenting the below lines. Uncomment it when any extra options are
# needed for the bootstrap.
# bootstrap_extra_options_not_expanded=''
# {% if expanded_cluster is not defined %}
#   bootstrap_extra_options+=" ${bootstrap_extra_options_not_expanded}"
# {% endif %}

$CEPHADM bootstrap --mon-ip $mon_ip --initial-dashboard-password {{ admin_password }} --shared_ceph_folder /mnt/{{ ceph_dev_folder }} ${bootstrap_extra_options}

fsid=$(cat /etc/ceph/ceph.conf | grep fsid | awk '{ print $3}')
cephadm_shell="$CEPHADM shell --fsid ${fsid} -c /etc/ceph/ceph.conf -k /etc/ceph/ceph.client.admin.keyring"

{% for number in range(1, nodes) %}
  ssh-copy-id -f -i /etc/ceph/ceph.pub  -o StrictHostKeyChecking=no root@192.168.100.10{{ number }}
  {% if expanded_cluster is defined %}
    ${cephadm_shell} ceph orch host add {{ prefix }}-node-0{{ number }}
  {% endif %}
{% endfor %}

{% if expanded_cluster is defined %}
  ${cephadm_shell} ceph orch apply osd --all-available-devices
{% endif %}
