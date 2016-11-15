#!/bin/bash -fv
#
# start the Openstack services
#
sudo cp /root/keystonerc_admin ./keystonerc_admin
sudo chmod 0644 ./keystonerc_admin
source ./keystonerc_admin
sudo service httpd stop
sudo service openstack-keystone restart
sudo service openstack-glance-api restart
sudo service openstack-nova-compute restart
sudo service openstack-cinder-volume restart
sudo service openstack-cinder-backup restart
