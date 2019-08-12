#! /usr/bin/env bash
if [ -f ~/secrets ]; then
    source ~/secrets
fi
subm=`which subscription-manager`
if [ ${#subm} -eq 0 ]; then
    sudo yum -y update
    exit
fi
subst=`sudo subscription-manager status | grep "^Overall" | awk '{print $NF}'`
if [ $subst == 'Unknown' ]; then
    mynameis=${subscrname:-'inigomontoya'}
    mypassis=${subscrpassword:-'youkeelmyfatherpreparetodie'}
    sudo subscription-manager register --username=$mynameis --password=$mypassis --force
    sudo subscription-manager refresh
    if [ $? -eq 1 ]; then exit 1; fi
    sudo subscription-manager attach --pool=8a85f9823e3d5e43013e3ddd4e2a0977
fi
sudo subscription-manager repos --enable=rhel-7-server-rpms
sudo yum -y update
