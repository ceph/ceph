#
# This file is part of the DeepSea integration test suite
#

function ceph_conf_upstream_rbd_default_features {
  #
  # by removing this line, we ensure that there will be no "rbd default
  # features" setting in ceph.conf, so the default value will be used
  #
  sed -i '/^rbd default features =/d' \
      /srv/salt/ceph/configuration/files/rbd.conf
}

function ceph_test_librbd_can_be_run {
  local TESTSCRIPT=/tmp/rbd_api_test.sh
  local CLIENTNODE=$(_client_node)
  cat << 'EOF' > $TESTSCRIPT
set -e
trap 'echo "Result: NOT_OK"' ERR
for delay in 60 60 60 60 ; do
    sudo zypper --non-interactive --gpg-auto-import-keys refresh && break
    sleep $delay
done
set -x
zypper --non-interactive install --no-recommends ceph-test
rpm -V ceph-test
type ceph_test_librbd
echo "Result: OK"
EOF
  _run_test_script_on_node $TESTSCRIPT $CLIENTNODE
  echo "You can now run ceph_test_librbd on the client node"
}
