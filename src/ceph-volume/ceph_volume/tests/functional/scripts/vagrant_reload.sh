#!/bin/bash

# vagrant-libvirt has a common behavior where it times out when "reloading" vms. Instead
# of calling `vagrant reload` attempt to halt everything, and then start everything, which gives
# this script the ability to try the `vagrant up` again in case of failure
#

vagrant halt
# This should not really be needed, but in case of a possible race condition between halt
# and up, it might improve things
sleep 5


retries=0
until [ $retries -ge 5 ]
do
  echo "Attempting to start VMs. Attempts: $retries"
  timeout 10m vagrant up "$@" && break
  retries=$[$retries+1]
  sleep 5
done
