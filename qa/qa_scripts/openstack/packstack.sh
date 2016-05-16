#/bin/bash -fv
#
# Install openstack by running packstack.
#
# Implements the operations in:
# https://docs.google.com/document/d/1us18KR3LuLyINgGk2rmI-SVj9UksCE7y4C2D_68Aa8o/edit?ts=56a78fcb
#
# The directory named files contains a template for the kilo.conf file used by packstack.
#
source ./copy_func.sh
source ./fix_conf_file.sh
openstack_node=${1}
ceph_node=${2}

copy_file execs/openstack-preinstall.sh $openstack_node . 0777 
fix_conf_file $openstack_node kilo .
ssh $openstack_node sudo ./openstack-preinstall.sh
sleep 240
ssh $openstack_node sudo packstack --answer-file kilo.conf
