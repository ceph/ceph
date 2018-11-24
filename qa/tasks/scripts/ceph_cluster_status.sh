# ceph_cluster_status.sh
#
# Display ceph cluster status
#
# args: None
#
set -ex
ceph pg stat -f json-pretty
ceph health detail -f json-pretty
ceph osd tree
ceph osd pool ls detail -f json-pretty
ceph -s
echo "OK" >/dev/null
