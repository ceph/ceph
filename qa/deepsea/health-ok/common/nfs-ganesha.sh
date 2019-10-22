#
# This file is part of the DeepSea integration test suite
#

NFS_MOUNTPOINT=/root/mnt

function _nfs_ganesha_node {
  _first_x_node ganesha
}

function nfs_ganesha_no_root_squash {
  local GANESHAJ2=/srv/salt/ceph/ganesha/files/ganesha.conf.j2
  sed -i '/Access_Type = RW;/a \\tSquash = No_root_squash;' $GANESHAJ2
}

function nfs_ganesha_no_grace_period {
  local GANESHAJ2=/srv/salt/ceph/ganesha/files/ganesha.conf.j2
  cat <<EOF >>$GANESHAJ2
NFSv4 {Graceless = True}
EOF
}

function nfs_ganesha_debug_log {
  local GANESHANODE=$(_nfs_ganesha_node)
  local TESTSCRIPT=/tmp/test-nfs-ganesha.sh
  cat <<EOF > $TESTSCRIPT
set -ex
trap 'echo "Result: NOT_OK"' ERR
echo "nfs-ganesha debug log script running as $(whoami) on $(hostname --fqdn)"
sed -i 's/NIV_EVENT/NIV_DEBUG/g' /etc/sysconfig/nfs-ganesha
cat /etc/sysconfig/nfs-ganesha
rm -rf /var/log/ganesha/ganesha.log
systemctl restart nfs-ganesha.service
systemctl is-active nfs-ganesha.service
rpm -q nfs-ganesha
echo "Result: OK"
EOF
  _run_test_script_on_node $TESTSCRIPT $GANESHANODE
}

function nfs_ganesha_cat_config_file {
  salt -C 'I@roles:ganesha' cmd.run 'cat /etc/ganesha/ganesha.conf'
}

#function nfs_ganesha_showmount_loop {
#  local TESTSCRIPT=/tmp/test-nfs-ganesha.sh
#  salt -C 'I@roles:ganesha' cmd.run "while true ; do showmount -e $GANESHANODE | tee /tmp/showmount.log || true ; grep -q 'Timed out' /tmp/showmount.log || break ; done"
#}

function nfs_ganesha_mount {
  #
  # creates a mount point and mounts NFS-Ganesha export in it
  #
  local NFSVERSION=$1   # can be "3", "4", or ""
  local ASUSER=$2
  local CLIENTNODE=$(_client_node)
  local GANESHANODE=$(_nfs_ganesha_node)
  local TESTSCRIPT=/tmp/test-nfs-ganesha.sh
  salt "$CLIENTNODE" pillar.get roles
  salt "$CLIENTNODE" pkg.install nfs-client # FIXME: only works on SUSE
  cat <<EOF > $TESTSCRIPT
set -ex
trap 'echo "Result: NOT_OK"' ERR
echo "nfs-ganesha mount test script"
test ! -e $NFS_MOUNTPOINT
mkdir $NFS_MOUNTPOINT
test -d $NFS_MOUNTPOINT
#mount -t nfs -o nfsvers=4 ${GANESHANODE}:/ $NFS_MOUNTPOINT
mount -t nfs -o ##OPTIONS## ${GANESHANODE}:/ $NFS_MOUNTPOINT
ls -lR $NFS_MOUNTPOINT
echo "Result: OK"
EOF
  if test -z $NFSVERSION ; then
      sed -i 's/##OPTIONS##/sync/' $TESTSCRIPT
  elif [ "$NFSVERSION" = "3" -o "$NFSVERSION" = "4" ] ; then
      sed -i 's/##OPTIONS##/sync,nfsvers='$NFSVERSION'/' $TESTSCRIPT
  else
      echo "Bad NFS version ->$NFS_VERSION<- Bailing out!"
      exit 1
  fi
  _run_test_script_on_node $TESTSCRIPT $CLIENTNODE $ASUSER
}

function nfs_ganesha_umount {
  local ASUSER=$1
  local CLIENTNODE=$(_client_node)
  local TESTSCRIPT=/tmp/test-nfs-ganesha-umount.sh
  cat <<EOF > $TESTSCRIPT
set -ex
trap 'echo "Result: NOT_OK"' ERR
echo "nfs-ganesha umount test script running as $(whoami) on $(hostname --fqdn)"
umount $NFS_MOUNTPOINT
rm -rf $NFS_MOUNTPOINT
echo "Result: OK"
EOF
  _run_test_script_on_node $TESTSCRIPT $CLIENTNODE $ASUSER
}

function nfs_ganesha_write_test {
  #
  # NFS-Ganesha FSAL write test
  #
  local FSAL=$1
  local NFSVERSION=$2
  local CLIENTNODE=$(_client_node)
  local TESTSCRIPT=/tmp/test-nfs-ganesha-write.sh
  local APPENDAGE=""
  if [ "$FSAL" = "cephfs" ] ; then
      if [ "$NFSVERSION" = "3" ] ; then
          APPENDAGE=""
      else
          APPENDAGE="/cephfs"
      fi
  else
      APPENDAGE="/demo/demo-demo"
  fi
  local TOUCHFILE=$NFS_MOUNTPOINT$APPENDAGE/saturn
  cat <<EOF > $TESTSCRIPT
set -ex
trap 'echo "Result: NOT_OK"' ERR
echo "nfs-ganesha write test script"
! test -e $TOUCHFILE
touch $TOUCHFILE
test -f $TOUCHFILE
rm -f $TOUCHFILE
echo "Result: OK"
EOF
  _run_test_script_on_node $TESTSCRIPT $CLIENTNODE
}

function nfs_ganesha_pynfs_test {
  #
  # NFS-Ganesha PyNFS test
  #
  local CLIENTNODE=$(_client_node)
  local GANESHANODE=$(_nfs_ganesha_node)
  local TESTSCRIPT=/tmp/test-nfs-ganesha-pynfs.sh
  cat <<'EOF' > $TESTSCRIPT
set -ex
trap 'echo "Result: NOT_OK"' ERR

function assert_success {
    local PYNFS_OUTPUT=$1
    test -s $PYNFS_OUTPUT
    # last line: determined return value of function
    ! grep -q FAILURE $PYNFS_OUTPUT
}

echo "nfs-ganesha PyNFS test script running as $(whoami) on $(hostname --fqdn)"
set +x
for delay in 60 60 60 60 ; do
    sudo zypper --non-interactive --gpg-auto-import-keys refresh && break
    sleep $delay
done
set -x
zypper --non-interactive install --no-recommends krb5-devel python3-devel
git clone --depth 1 https://github.com/supriti/Pynfs
cd Pynfs
./setup.py build
cd nfs4.0
sleep 90 # NFSv4 grace period
LOGFILE="PyNFS.out"
./testserver.py -v \
    --outfile RESULTS.out \
    --maketree GANESHANODE:/cephfs/ \
    --showomit \
    --secure \
    --rundeps \
    all \
    ganesha 2>&1 | tee $LOGFILE
#./showresults.py RESULTS.out
assert_success $LOGFILE
echo "Result: OK"
EOF
  sed -i 's/GANESHANODE/'$GANESHANODE'/' $TESTSCRIPT
  _run_test_script_on_node $TESTSCRIPT $CLIENTNODE
}
