#! /bin/bash
if [ -f ~/secrets ]; then
    source ~/secrets
fi
mynameis=${subscrname:-'inigomontoya'}
mypassis=${subscrpassword:-'youkeelmyfatherpreparetodie'}
sudo subscription-manager register --username=$mynameis --password=$mypassis --force
sudo subscription-manager refresh
if [ $? -eq 1 ]; then exit 1; fi
sudo subscription-manager attach --pool=8a85f9823e3d5e43013e3ddd4e2a0977
sudo subscription-manager repos --enable=rhel-7-server-rpms
sudo yum -y update
