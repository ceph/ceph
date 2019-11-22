Development
-----------


There are multiple ways to set up a development environment for the SSH orchestrator.
In the following I'll use the `vstart` method.

1) Make sure remoto is installed (0.35 or newer)

2) Use vstart to spin up a cluster


::

   # ../src/vstart.sh -n --ssh

*Note that when you specify `--ssh` you have to have passwordless ssh access to localhost*

It will add your ~/.ssh/id_rsa and ~/.ssh/id_rsa.pub to `mgr/ssh/ssh_identity_{key, pub}`
and add your $HOSTNAME to the list of known hosts.

This will also enable the ssh mgr module and enable it as the orchestrator backend.

*Optional:*

While the above is sufficient for most operations, you may want to add a second host to the mix.
There is `Vagrantfile` for creating a minimal cluster in `src/pybind/mgr/ssh/`.

If you wish to extend the one-node-localhost cluster to i.e. test more sophisticated OSD deployments you can follow the next steps:

From within the `src/pybind/mgr/ssh` directory.


1) Spawn VMs

::

   # vagrant up

This will spawn three machines.
mon0, mgr0, osd0

NUM_DAEMONS can be used to increase the number of VMs created. (defaults to 1)

If will also come with the necessary packages preinstalled as well as your ~/.ssh/id_rsa.pub key
injected. (to users root and vagrant; the SSH-orchestrator currently connects as root)


2) Update the ssh-config

The SSH-orchestrator needs to understand how to connect to the new node. Most likely the VM isn't reachable with the default settings used:

```
Host *
User root
StrictHostKeyChecking no
```

You want to adjust this by retrieving an adapted ssh_config from Vagrant.

::

   # vagrant ssh-config > ssh-config


Now set the newly created config for Ceph.

::

   # ceph ssh set-ssh-config -i <path_to_ssh_conf>


3) Add the new host

Add the newly created host(s) to the inventory.

::


   # ceph orchestrator host add <host>


4) Verify the inventory

::

   # ceph orchestrator host ls


You should see the hostname in the list.
