#!/bin/bash
# Generate a custom ssh config from Vagrant so that it can then be used by
# ansible.cfg 

path=$1

if [ $# -eq 0 ]
  then
    echo "A path to the scenario is required as an argument and it wasn't provided"
    exit 1
fi

cd "$path"
vagrant ssh-config > vagrant_ssh_config
