#!/bin/bash

# very basic set up of iscsi gw and client
# to make sure things are working

set -ex

if ! grep -q rhel /etc/*-release; then
    echo "The script only supports CentOS."
    exit 1
fi

# teuthology tends to put the cephadm binary built for our testing
# branch in /home/ubuntu/cephtest/. If it's there, lets just move it
# so we don't need to reference the full path.
if ! command -v cephadm && ls /home/ubuntu/cephtest/cephadm; then
    sudo cp /home/ubuntu/cephtest/cephadm /usr/sbin/
fi

# make sure we haven't already created luns
! sudo ls /dev/disk/by-path | grep iscsi

sudo dnf install jq -y

ISCSI_CONT_ID=$(sudo podman ps -qa --filter='name=iscsi' | head -n 1)
ISCSI_DAEMON_NAME=$(sudo cephadm ls --no-detail | jq -r '.[] | select(.name | startswith("iscsi")) | .name')
ISCSI_DAEMON_ID=$(cut -d '.' -f2- <<< "$ISCSI_DAEMON_NAME")
HOSTNAME=$(sudo cephadm shell -- ceph orch ps --daemon-id "$ISCSI_DAEMON_ID" -f json | jq -r '.[] | .hostname')
NODE_IP=$(sudo cephadm shell -- ceph orch host ls --format json | jq --arg HOSTNAME "$HOSTNAME" -r '.[] | select(.hostname == $HOSTNAME) | .addr')
# The result of this python line is what iscsi will expect for the first gateway name
FQDN=$(python3 -c 'import socket; print(socket.getfqdn())')
# I am running this twice on purpose. I don't know why but in my testing the first time this would
# run it would return a different result then all subsequent runs (and take significantly longer to run).
# The result from the first run would cause gateway creation to fail when the return value is used
# later on. It was likely specific to my env, but it doesn't hurt to run it twice anyway. This
# was the case whether I ran it through cephadm shell or directly on the host machine.
FQDN=$(python3 -c 'import socket; print(socket.getfqdn())')
ISCSI_POOL=$(sudo cephadm shell -- ceph orch ls iscsi --format json | jq -r '.[] | .spec | .pool')
ISCSI_USER="adminadmin"
ISCSI_PASSWORD="adminadminadmin"

# gateway setup
container_gwcli() {
    sudo podman exec -it ${ISCSI_CONT_ID} gwcli "$@"
}

container_gwcli /iscsi-targets create iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw
# I've seen this give a nonzero error code with an error message even when
# creating the gateway successfully, so this command is allowed to fail
# If it actually failed to make the gateway, some of the follow up commands will fail
container_gwcli /iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/gateways create ${FQDN} ${NODE_IP} || true
container_gwcli /disks create pool=${ISCSI_POOL} image=disk_1 size=2G
container_gwcli /disks create pool=${ISCSI_POOL} image=disk_2 size=2G
container_gwcli /iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/hosts create iqn.1994-05.com.redhat:client1
container_gwcli /iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/hosts/iqn.1994-05.com.redhat:client1 auth username=${ISCSI_USER}  password=${ISCSI_PASSWORD}
container_gwcli /iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/hosts/iqn.1994-05.com.redhat:client1 disk add ${ISCSI_POOL}/disk_1
container_gwcli /iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/hosts/iqn.1994-05.com.redhat:client1 disk add ${ISCSI_POOL}/disk_2

# set up multipath and some iscsi config options
sudo dnf install -y iscsi-initiator-utils device-mapper-multipath

# this next line is purposely being done without "-a" on the tee command to
# overwrite the current initiatorname.iscsi file if it is there
echo "GenerateName=no" | sudo tee /etc/iscsi/initiatorname.iscsi
echo "InitiatorName=iqn.1994-05.com.redhat:client1" | sudo tee -a /etc/iscsi/initiatorname.iscsi

echo "node.session.auth.authmethod = CHAP" | sudo tee -a /etc/iscsi/iscsid.conf
echo "node.session.auth.username = ${ISCSI_USER}" | sudo tee -a /etc/iscsi/iscsid.conf
echo "node.session.auth.password = ${ISCSI_PASSWORD}" | sudo tee -a /etc/iscsi/iscsid.conf

sudo tee -a /etc/multipath.conf > /dev/null << EOF
devices {
  device {
          vendor                 "LIO-ORG"
          product                "TCMU device"
          hardware_handler       "1 alua"
          path_grouping_policy   "failover"
          path_selector          "queue-length 0"
          failback               60
          path_checker           tur
          prio                   alua
          prio_args              exclusive_pref_bit
          fast_io_fail_tmo       25
          no_path_retry          queue
  }
}
EOF
sudo systemctl restart multipathd
sudo systemctl restart iscsid

# client setup
sudo iscsiadm -m discovery -t st -p ${NODE_IP}
sudo iscsiadm -m node -T iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw -l
sudo iscsiadm -m session --rescan

sleep 5

# make sure we can now see luns
sudo ls /dev/disk/by-path | grep iscsi
