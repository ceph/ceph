#!/usr/bin/env bash

set -x

export PATH=/root/bin:$PATH
mkdir /root/bin

export CEPHADM_IMAGE='quay.ceph.io/ceph-ci/ceph:main'

CEPHADM="/root/bin/cephadm"
CEPHADM_SRC="/mnt/{{ ceph_dev_folder }}/src/cephadm/cephadm"

cp $CEPHADM_SRC $CEPHADM

# default configs for the cluster to make it
# behave like a single node cluster
declare -A ceph_configs=(
  ["mon_allow_pool_size_one"]="true"
  ["mon_allow_pool_delete"]="true"
  ["osd_pool_default_min_size"]="1"
  ["osd_pool_default_size"]="1"
  ["mon_data_avail_crit"]="1"
  ["mon_data_avail_warn"]="1"
)

declare -A config_sections=(
  ["mon_allow_pool_size_one"]="mon"
  ["mon_allow_pool_delete"]="mon"
  ["osd_pool_default_min_size"]="global"
  ["osd_pool_default_size"]="global"
  ["mon_data_avail_crit"]="mon"
  ["mon_data_avail_warn"]="mon"
)

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

$CEPHADM ${quick_install_options} bootstrap --mon-ip $mon_ip --initial-dashboard-password {{ admin_password }} --shared_ceph_folder /mnt/{{ ceph_dev_folder }} ${bootstrap_extra_options}

fsid=$(cat /etc/ceph/ceph.conf | grep fsid | awk '{ print $3}')
cephadm_shell="$CEPHADM shell --fsid ${fsid} -c /etc/ceph/ceph.conf -k /etc/ceph/ceph.client.admin.keyring"

{% if nodes < 3 %}
  for config in "${!ceph_configs[@]}"; do
    section="${config_sections[$config]}"
    value="${ceph_configs[$config]}"
    ${cephadm_shell} ceph config set "$section" "$config" "$value"
  done
{% endif %}

{% for number in range(1, nodes) %}
  ssh-copy-id -f -i /etc/ceph/ceph.pub  -o StrictHostKeyChecking=no root@192.168.100.10{{ number }}
  {% if expanded_cluster is defined %}
    ${cephadm_shell} ceph orch host add {{ prefix }}-node-0{{ number }} 192.168.100.10{{ number }}
  {% endif %}
{% endfor %}

{% if expanded_cluster is defined %}
  ${cephadm_shell} ceph orch apply osd --all-available-devices
{% endif %}
