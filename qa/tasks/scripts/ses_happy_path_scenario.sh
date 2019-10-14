set -ex

declare -a minion2run="$1"

ceph -s

salt $minion2run service.status ceph.target
salt $minion2run service.stop ceph.target
sleep 15
salt $minion2run service.start ceph.target
sleep 15
salt $minion2run service.status ceph.target
sleep 30
ceph -s
salt $minion2run service.status ceph.target
salt $minion2run cmd.run "systemctl reset-failed ceph*"
salt $minion2run service.restart ceph.target
salt $minion2run service.status ceph.target
ceph -s
ceph osd lspools
rados lspools | grep testpool1
ceph mon stat
ceph osd stat
ceph osd tree
ceph osd pool rename testpool1 testpool2
rados lspools | grep testpool2
ceph osd pool rm testpool2 testpool2 --yes-i-really-really-mean-it
sleep 10
ceph health | grep HEALTH_OK

