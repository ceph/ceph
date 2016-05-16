#!/bin/bash -fv
#
# Create a glance image, a corresponding cinder volume, a nova instance, attach, the cinder volume to the
# nova instance, and create a backup.
#
image_name=${1}X
file_name=${2-rhel-server-7.2-x86_64-boot.iso}
source ./keystonerc_admin 
glance image-create --name $image_name --disk-format iso --container-format bare --file $file_name 
glance_id=`glance image-list | grep ${image_name} | sed 's/^| //' | sed 's/ |.*//'`
cinder create --image-id ${glance_id} --display-name ${image_name}-volume 8
nova boot --image ${image_name} --flavor 1 ${image_name}-inst
cinder_id=`cinder list | grep ${image_name} | sed 's/^| //' | sed 's/ |.*//'`
chkr=`cinder list | grep ${image_name}-volume | grep available`
while [ -z "$chkr" ]; do
    sleep 30
    chkr=`cinder list | grep ${image_name}-volume | grep available`
done
nova volume-attach ${image_name}-inst ${cinder_id} auto
sleep 30
cinder backup-create --name ${image_name}-backup ${image_name}-volume --force
