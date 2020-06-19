
Troubleshooting
===============

Sometimes there is a need to investigate why a cephadm command failed or why
a specific service no longer runs properly.

As cephadm deploys daemons as containers, troubleshooting daemons is slightly
different. Here are a few tools and commands to help investigating issues.

Pausing or disabling cephadm
----------------------------

If something goes wrong and cephadm is doing behaving in a way you do
not like, you can pause most background activity with::

  ceph orch pause

This will stop any changes, but cephadm will still periodically check hosts to
refresh its inventory of daemons and devices.  You can disable cephadm
completely with::

  ceph orch set backend ''
  ceph mgr module disable cephadm

This will disable all of the ``ceph orch ...`` CLI commands but the previously
deployed daemon containers will still continue to exist and start as they
did before.

Checking cephadm logs
---------------------

You can monitor the cephadm log in real time with::

  ceph -W cephadm

You can see the last few messages with::

  ceph log last cephadm

If you have enabled logging to files, you can see a cephadm log file called
``ceph.cephadm.log`` on monitor hosts (see :ref:`cephadm-logs`).

Gathering log files
-------------------

Use journalctl to gather the log files of all daemons:

.. note:: By default cephadm now stores logs in journald. This means
   that you will no longer find daemon logs in ``/var/log/ceph/``.

To read the log file of one specific daemon, run::

    cephadm logs --name <name-of-daemon>

Note: this only works when run on the same host where the daemon is running. To
get logs of a daemon running on a different host, give the ``--fsid`` option::

    cephadm logs --fsid <fsid> --name <name-of-daemon>

where the ``<fsid>`` corresponds to the cluster ID printed by ``ceph status``.

To fetch all log files of all daemons on a given host, run::

    for name in $(cephadm ls | jq -r '.[].name') ; do
      cephadm logs --fsid <fsid> --name "$name" > $name;
    done

Collecting systemd status
-------------------------

To print the state of a systemd unit, run::

      systemctl status "ceph-$(cephadm shell ceph fsid)@<service name>.service";


To fetch all state of all daemons of a given host, run::

    fsid="$(cephadm shell ceph fsid)"
    for name in $(cephadm ls | jq -r '.[].name') ; do
      systemctl status "ceph-$fsid@$name.service" > $name;
    done


List all downloaded container images
------------------------------------

To list all container images that are downloaded on a host:

.. note:: ``Image`` might also be called `ImageID`

::

    podman ps -a --format json | jq '.[].Image'
    "docker.io/library/centos:8"
    "registry.opensuse.org/opensuse/leap:15.2"


Manually running containers
---------------------------

Cephadm writes small wrappers that run a containers. Refer to
``/var/lib/ceph/<cluster-fsid>/<service-name>/unit.run`` for the
container execution command.

.. _cephadm-ssh-errors:

ssh errors
----------

Error message::

  xxxxxx.gateway_bootstrap.HostNotFound: -F /tmp/cephadm-conf-kbqvkrkw root@10.10.1.2
  raise OrchestratorError('Failed to connect to %s (%s).  Check that the host is reachable and accepts  connections using the cephadm SSH key' % (host, addr)) from
  orchestrator._interface.OrchestratorError: Failed to connect to 10.10.1.2 (10.10.1.2).  Check that the host is reachable and accepts connections using the cephadm SSH key

Things users can do:

1. Ensure cephadm has an SSH identity key::
      
     [root@mon1~]# cephadm shell -- ceph config-key get mgr/cephadm/ssh_identity_key > key
     INFO:cephadm:Inferring fsid f8edc08a-7f17-11ea-8707-000c2915dd98
     INFO:cephadm:Using recent ceph image docker.io/ceph/ceph:v15 obtained 'mgr/cephadm/ssh_identity_key'
     [root@mon1 ~] # chmod 0600 key

 If this fails, cephadm doesn't have a key. Fix this by running the following command::
   
     [root@mon1 ~]# cephadm shell -- ceph cephadm generate-ssh-key

 or::
   
     [root@mon1 ~]# cat key | cephadm shell -- ceph cephadm set-ssk-key -i -

2. Ensure that the ssh config is correct::
   
     [root@mon1 ~]# cephadm shell -- ceph cephadm get-ssh-config > config

3. Verify that we can connect to the host::
    
     [root@mon1 ~]# ssh -F config -i key root@mon1

4. There is a limitation right now: the ssh user is always `root`.



Verifying that the Public Key is Listed in the authorized_keys file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To verify that the public key is in the authorized_keys file, run the following commands::

     [root@mon1 ~]# cephadm shell -- ceph config-key get mgr/cephadm/ssh_identity_pub > key.pub
     [root@mon1 ~]# grep "`cat key.pub`"  /root/.ssh/authorized_keys

Failed to infer CIDR network error
----------------------------------

If you see this error::

   ERROR: Failed to infer CIDR network for mon ip ***; pass --skip-mon-network to configure it later

Or this error::

   Must set public_network config option or specify a CIDR network, ceph addrvec, or plain IP

This means that you must run a command of this form::

  ceph config set mon public_network <mon_network>

For more detail on operations of this kind, see :ref:`deploy_additional_monitors`

Accessing the admin socket
--------------------------

Each Ceph daemon provides an admin socket that bypasses the
MONs (See :ref:`rados-monitoring-using-admin-socket`).

To access the admin socket, first enter the daemon container on the host::

    [root@mon1 ~]# cephadm enter --name <daemon-name>
    [ceph: root@mon1 /]# ceph --admin-daemon /var/run/ceph/ceph-<daemon-name>.asok config show
