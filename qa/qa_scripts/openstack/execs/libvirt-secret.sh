#!/bin/bash -f

#
# Generate a libvirt secret on the Openstack node. 
#
openstack_node=${1}
uuid=`uuidgen`
cat > secret.xml <<EOF
<secret ephemeral='no' private='no'>
  <uuid>${uuid}</uuid>
  <usage type='ceph'>
    <name>client.cinder secret</name>
  </usage>
</secret>
EOF
sudo virsh secret-define --file secret.xml
sudo virsh secret-set-value --secret ${uuid} --base64 $(cat client.cinder.key)
echo ${uuid}
