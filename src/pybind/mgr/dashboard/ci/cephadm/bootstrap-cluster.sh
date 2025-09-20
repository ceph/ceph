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
quick_install_options=''
{% if quick_install is defined %}
  quick_install_options="--image localhost:5000/ceph"
{% endif %}

if [[ ${NODES} -lt 2 ]]; then
  bootstrap_extra_options+=" --config /root/initial-ceph.conf"
fi

{% if prefix is not defined %}
  PREFIX="ceph"
{% else %}
  PREFIX="{{ prefix }}"
{% endif %}

{% if ceph_dev_folder is defined %}
  bootstrap_extra_options+=" --shared_ceph_folder /mnt/{{ ceph_dev_folder }}"
{% endif %}

$CEPHADM ${quick_install_options} bootstrap --mon-ip $mon_ip --initial-dashboard-password {{ admin_password }} ${bootstrap_extra_options}

fsid=$(cat /etc/ceph/ceph.conf | grep fsid | awk '{ print $3}')
cephadm_shell="$CEPHADM shell --fsid ${fsid} -c /etc/ceph/ceph.conf -k /etc/ceph/ceph.client.admin.keyring"

for number in $(seq 1 $((NODES - 1))); do
  LAST_OCTET=$((NODE_IP_OFFSET + $number))
  ssh-copy-id -f -i /etc/ceph/ceph.pub  -o StrictHostKeyChecking=no root@192.168.100.${LAST_OCTET}
  {% if expanded_cluster is defined %}
    ${cephadm_shell} ceph orch host add ${PREFIX}-node-0${number} 192.168.100.${LAST_OCTET}
  {% endif %}
done

{% if expanded_cluster is defined %}
  ${cephadm_shell} ceph orch apply osd --all-available-devices
{% endif %}
