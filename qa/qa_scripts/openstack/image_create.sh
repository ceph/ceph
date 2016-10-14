#/bin/bash -fv
#
# Set up a vm on packstack.  Use the iso in the files directory.
#
source ./copy_func.sh
source ./fix_conf_file.sh
openstack_node=${1}
ceph_node=${2}

copy_file files/rhel-server-7.2-x86_64-boot.iso $openstack_node .
copy_file execs/run_openstack.sh $openstack_node . 0755
filler=`date +%s`
ssh $openstack_node ./run_openstack.sh "${openstack_node}X${filler}" rhel-server-7.2-x86_64-boot.iso
ssh $ceph_node sudo ceph df
