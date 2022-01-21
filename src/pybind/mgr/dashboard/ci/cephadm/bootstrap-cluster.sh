#!/usr/bin/env bash

set -x

export PATH=/root/bin:$PATH
mkdir /root/bin

cp /mnt/{{ ceph_dev_folder }}/src/cephadm/cephadm /root/bin/cephadm
chmod +x /root/bin/cephadm
mkdir -p /etc/ceph
mon_ip=$(ifconfig eth0  | grep 'inet ' | awk '{ print $2}')

bootstrap_extra_options='--allow-fqdn-hostname --dashboard-password-noupdate'
bootstrap_extra_options_not_expanded='--skip-monitoring-stack'
{% if expanded_cluster is not defined %}
  bootstrap_extra_options+=" ${bootstrap_extra_options_not_expanded}"
{% endif %}

cephadm bootstrap --mon-ip $mon_ip --initial-dashboard-password {{ admin_password }} --shared_ceph_folder /mnt/{{ ceph_dev_folder }} ${bootstrap_extra_options}

fsid=$(cat /etc/ceph/ceph.conf | grep fsid | awk '{ print $3}')
cephadm_shell="cephadm shell --fsid ${fsid} -c /etc/ceph/ceph.conf -k /etc/ceph/ceph.client.admin.keyring"

{% for number in range(1, nodes) %}
  ssh-copy-id -f -i /etc/ceph/ceph.pub  -o StrictHostKeyChecking=no root@{{ prefix }}-node-0{{ number }}
  {% if expanded_cluster is defined %}
    ${cephadm_shell} ceph orch host add {{ prefix }}-node-0{{ number }}
  {% endif %}
{% endfor %}

{% if expanded_cluster is defined %}
  ${cephadm_shell} ceph orch apply osd --all-available-devices
{% endif %}
