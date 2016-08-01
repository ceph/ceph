#!/bin/bash -f
#
# Remotely setup the stuff needed to run packstack.  This should do items 1-4 in
# https://docs.google.com/document/d/1us18KR3LuLyINgGk2rmI-SVj9UksCE7y4C2D_68Aa8o/edit?ts=56a78fcb
#
yum remove -y rhos-release
rpm -ivh http://rhos-release.virt.bos.redhat.com/repos/rhos-release/rhos-release-latest.noarch.rpm
rm -rf /etc/yum.repos.d/*
rm -rf /var/cache/yum/*
rhos-release 8
yum update -y
yum install -y nc puppet vim screen setroubleshoot crudini bpython openstack-packstack
systemctl disable ntpd
systemctl stop ntpd
reboot
