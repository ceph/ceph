Troubleshooting
===============

Sometimes there is a need to investigate why a cephadm command failed or why
a specific service no longer runs properly.

As cephadm deploys daemons as containers, troubleshooting daemons is slightly
different. Here are a few tools and commands to help investigating issues.

.. _cephadm-pause:

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

Please refer to :ref:`cephadm-spec-unmanaged` for disabling individual
services.


Per-service and per-daemon events
---------------------------------

In order to aid debugging failed daemon deployments, cephadm stores 
events per service and per daemon. They often contain relevant information::

  ceph orch ls --service_name=<service-name> --format yaml

for example:

.. code-block:: yaml

  service_type: alertmanager
  service_name: alertmanager
  placement:
    hosts:
    - unknown_host
  status:
    ...
    running: 1
    size: 1
  events:
  - 2021-02-01T08:58:02.741162 service:alertmanager [INFO] "service was created"
  - '2021-02-01T12:09:25.264584 service:alertmanager [ERROR] "Failed to apply: Cannot
    place <AlertManagerSpec for service_name=alertmanager> on unknown_host: Unknown hosts"'

Or per daemon::

  ceph orch ceph --service-type mds --daemon-id=hostname.ppdhsz --format yaml

.. code-block:: yaml

  daemon_type: mds
  daemon_id: cephfs.hostname.ppdhsz
  hostname: hostname
  status_desc: running
  ...
  events:
  - 2021-02-01T08:59:43.845866 daemon:mds.cephfs.hostname.ppdhsz [INFO] "Reconfigured
    mds.cephfs.hostname.ppdhsz on host 'hostname'"


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

  execnet.gateway_bootstrap.HostNotFound: -F /tmp/cephadm-conf-73z09u6g -i /tmp/cephadm-identity-ky7ahp_5 root@10.10.1.2
  ...
  raise OrchestratorError(msg) from e
  orchestrator._interface.OrchestratorError: Failed to connect to 10.10.1.2 (10.10.1.2).
  Please make sure that the host is reachable and accepts connections using the cephadm SSH key
  ...

Things users can do:

1. Ensure cephadm has an SSH identity key::

     [root@mon1~]# cephadm shell -- ceph config-key get mgr/cephadm/ssh_identity_key > ~/cephadm_private_key
     INFO:cephadm:Inferring fsid f8edc08a-7f17-11ea-8707-000c2915dd98
     INFO:cephadm:Using recent ceph image docker.io/ceph/ceph:v15 obtained 'mgr/cephadm/ssh_identity_key'
     [root@mon1 ~] # chmod 0600 ~/cephadm_private_key

 If this fails, cephadm doesn't have a key. Fix this by running the following command::

     [root@mon1 ~]# cephadm shell -- ceph cephadm generate-ssh-key

 or::

     [root@mon1 ~]# cat ~/cephadm_private_key | cephadm shell -- ceph cephadm set-ssk-key -i -

2. Ensure that the ssh config is correct::

     [root@mon1 ~]# cephadm shell -- ceph cephadm get-ssh-config > config

3. Verify that we can connect to the host::

     [root@mon1 ~]# ssh -F config -i ~/cephadm_private_key root@mon1

Verifying that the Public Key is Listed in the authorized_keys file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To verify that the public key is in the authorized_keys file, run the following commands::

     [root@mon1 ~]# cephadm shell -- ceph cephadm get-pub-key > ~/ceph.pub
     [root@mon1 ~]# grep "`cat ~/ceph.pub`"  /root/.ssh/authorized_keys

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


Restoring the MON quorum
------------------------

In case the Ceph MONs cannot form a quorum, cephadm is not able
to manage the cluster, until the quorum is restored.

In order to restore the MON quorum, remove unhealthy MONs
form the monmap by following these steps:

1. Stop all MONs. For each MON host::

    ssh {mon-host}
    cephadm unit --name mon.`hostname` stop


2. Identify a surviving monitor and log in to that host::

    ssh {mon-host}
    cephadm enter --name mon.`hostname`

3. Follow the steps in :ref:`rados-mon-remove-from-unhealthy`


Manually deploying a MGR daemon
-------------------------------
cephadm requires a MGR daemon in order to manage the cluster. In case the cluster
the last MGR of a cluster was removed, follow these steps in order to deploy 
a MGR ``mgr.hostname.smfvfd`` on a random host of your cluster manually. 

Disable the cephadm scheduler, in order to prevent cephadm from removing the new 
MGR. See :ref:`cephadm-enable-cli`::

  ceph config-key set mgr/cephadm/pause true

Then get or create the auth entry for the new MGR::

  ceph auth get-or-create mgr.hostname.smfvfd mon "profile mgr" osd "allow *" mds "allow *"

Get the ceph.conf::

  ceph config generate-minimal-conf

Get the container image::

  ceph config get "mgr.hostname.smfvfd" container_image

Create a file ``config-json.json`` which contains the information neccessary to deploy
the daemon:

.. code-block:: json

  {
    "config": "# minimal ceph.conf for 8255263a-a97e-4934-822c-00bfe029b28f\n[global]\n\tfsid = 8255263a-a97e-4934-822c-00bfe029b28f\n\tmon_host = [v2:192.168.0.1:40483/0,v1:192.168.0.1:40484/0]\n",
    "keyring": "[mgr.hostname.smfvfd]\n\tkey = V2VyIGRhcyBsaWVzdCBpc3QgZG9vZi4=\n"
  }

Deploy the daemon::

  cephadm --image <container-image> deploy --fsid <fsid> --name mgr.hostname.smfvfd --config-json config-json.json

