Troubleshooting
===============

You may wish to investigate why a cephadm command failed
or why a certain service no longer runs properly.

Cephadm deploys daemons within containers. This means that
troubleshooting those containerized daemons will require
a different process than traditional package-install daemons.

Here are some tools and commands to help you troubleshoot
your Ceph environment.

.. _cephadm-pause:

Pausing or Disabling cephadm
----------------------------

If something goes wrong and cephadm is behaving badly, you can
pause most of the Ceph cluster's background activity by running
the following command: 

.. prompt:: bash #

  ceph orch pause

This stops all changes in the Ceph cluster, but cephadm will
still periodically check hosts to refresh its inventory of
daemons and devices.  You can disable cephadm completely by
running the following commands:

.. prompt:: bash #

  ceph orch set backend ''
  ceph mgr module disable cephadm

These commands disable all of the ``ceph orch ...`` CLI commands.
All previously deployed daemon containers continue to exist and
will start as they did before you ran these commands.

See :ref:`cephadm-spec-unmanaged` for information on disabling
individual services.


Per-service and Per-daemon Events
---------------------------------

In order to facilitate debugging failed daemons,
cephadm stores events per service and per daemon.
These events often contain information relevant to
troubleshooting your Ceph cluster. 

Listing Service Events
~~~~~~~~~~~~~~~~~~~~~~

To see the events associated with a certain service, run a
command of the and following form:

.. prompt:: bash #

  ceph orch ls --service_name=<service-name> --format yaml

This will return something in the following form:

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

Listing Daemon Events
~~~~~~~~~~~~~~~~~~~~~

To see the events associated with a certain daemon, run a
command of the and following form:

.. prompt:: bash #

  ceph orch ps --service-name <service-name> --daemon-id <daemon-id> --format yaml

This will return something in the following form:

.. code-block:: yaml

  daemon_type: mds
  daemon_id: cephfs.hostname.ppdhsz
  hostname: hostname
  status_desc: running
  ...
  events:
  - 2021-02-01T08:59:43.845866 daemon:mds.cephfs.hostname.ppdhsz [INFO] "Reconfigured
    mds.cephfs.hostname.ppdhsz on host 'hostname'"


Checking Cephadm Logs
---------------------

To learn how to monitor cephadm logs as they are generated, read :ref:`watching_cephadm_logs`.

If your Ceph cluster has been configured to log events to files, there will be a
``ceph.cephadm.log`` file on all monitor hosts (see
:ref:`cephadm-logs` for a more complete explanation).

Gathering Log Files
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

Collecting Systemd Status
-------------------------

To print the state of a systemd unit, run::

      systemctl status "ceph-$(cephadm shell ceph fsid)@<service name>.service";


To fetch all state of all daemons of a given host, run::

    fsid="$(cephadm shell ceph fsid)"
    for name in $(cephadm ls | jq -r '.[].name') ; do
      systemctl status "ceph-$fsid@$name.service" > $name;
    done


List all Downloaded Container Images
------------------------------------

To list all container images that are downloaded on a host:

.. note:: ``Image`` might also be called `ImageID`

::

    podman ps -a --format json | jq '.[].Image'
    "docker.io/library/centos:8"
    "registry.opensuse.org/opensuse/leap:15.2"


Manually Running Containers
---------------------------

Cephadm uses small wrappers when running containers. Refer to
``/var/lib/ceph/<cluster-fsid>/<service-name>/unit.run`` for the
container execution command.

.. _cephadm-ssh-errors:

SSH Errors
----------

Error message::

  execnet.gateway_bootstrap.HostNotFound: -F /tmp/cephadm-conf-73z09u6g -i /tmp/cephadm-identity-ky7ahp_5 root@10.10.1.2
  ...
  raise OrchestratorError(msg) from e
  orchestrator._interface.OrchestratorError: Failed to connect to 10.10.1.2 (10.10.1.2).
  Please make sure that the host is reachable and accepts connections using the cephadm SSH key
  ...

Things Ceph administrators can do:

1. Ensure cephadm has an SSH identity key::

     [root@mon1~]# cephadm shell -- ceph config-key get mgr/cephadm/ssh_identity_key > ~/cephadm_private_key
     INFO:cephadm:Inferring fsid f8edc08a-7f17-11ea-8707-000c2915dd98
     INFO:cephadm:Using recent ceph image docker.io/ceph/ceph:v15 obtained 'mgr/cephadm/ssh_identity_key'
     [root@mon1 ~] # chmod 0600 ~/cephadm_private_key

 If this fails, cephadm doesn't have a key. Fix this by running the following command::

     [root@mon1 ~]# cephadm shell -- ceph cephadm generate-ssh-key

 or::

     [root@mon1 ~]# cat ~/cephadm_private_key | cephadm shell -- ceph cephadm set-ssk-key -i -

2. Ensure that the SSH config is correct::

     [root@mon1 ~]# cephadm shell -- ceph cephadm get-ssh-config > config

3. Verify that we can connect to the host::

     [root@mon1 ~]# ssh -F config -i ~/cephadm_private_key root@mon1

Verifying that the Public Key is Listed in the authorized_keys file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To verify that the public key is in the authorized_keys file, run the following commands::

     [root@mon1 ~]# cephadm shell -- ceph cephadm get-pub-key > ~/ceph.pub
     [root@mon1 ~]# grep "`cat ~/ceph.pub`"  /root/.ssh/authorized_keys

Failed to Infer CIDR network error
----------------------------------

If you see this error::

   ERROR: Failed to infer CIDR network for mon ip ***; pass --skip-mon-network to configure it later

Or this error::

   Must set public_network config option or specify a CIDR network, ceph addrvec, or plain IP

This means that you must run a command of this form::

  ceph config set mon public_network <mon_network>

For more detail on operations of this kind, see :ref:`deploy_additional_monitors`

Accessing the Admin Socket
--------------------------

Each Ceph daemon provides an admin socket that bypasses the
MONs (See :ref:`rados-monitoring-using-admin-socket`).

To access the admin socket, first enter the daemon container on the host::

    [root@mon1 ~]# cephadm enter --name <daemon-name>
    [ceph: root@mon1 /]# ceph --admin-daemon /var/run/ceph/ceph-<daemon-name>.asok config show

Running Various Ceph Tools
--------------------------------

To run Ceph tools like ``ceph-objectstore-tool`` or 
``ceph-monstore-tool``, invoke the cephadm CLI with
``cephadm shell --name <daemon-name>``.  For example::

    root@myhostname # cephadm unit --name mon.myhostname stop
    root@myhostname # cephadm shell --name mon.myhostname
    [ceph: root@myhostname /]# ceph-monstore-tool /var/lib/ceph/mon/ceph-myhostname get monmap > monmap         
    [ceph: root@myhostname /]# monmaptool --print monmap
    monmaptool: monmap file monmap
    epoch 1
    fsid 28596f44-3b56-11ec-9034-482ae35a5fbb
    last_changed 2021-11-01T20:57:19.755111+0000
    created 2021-11-01T20:57:19.755111+0000
    min_mon_release 17 (quincy)
    election_strategy: 1
    0: [v2:127.0.0.1:3300/0,v1:127.0.0.1:6789/0] mon.myhostname

The cephadm shell sets up the environment in a way that is suitable
for extended daemon maintenance and running daemons interactively. 

.. _cephadm-restore-quorum:

Restoring the Monitor Quorum
----------------------------

If the Ceph monitor daemons (mons) cannot form a quorum, cephadm will not be
able to manage the cluster until quorum is restored.

In order to restore the quorum, remove unhealthy monitors
form the monmap by following these steps:

1. Stop all mons. For each mon host::

    ssh {mon-host}
    cephadm unit --name mon.`hostname` stop


2. Identify a surviving monitor and log in to that host::

    ssh {mon-host}
    cephadm enter --name mon.`hostname`

3. Follow the steps in :ref:`rados-mon-remove-from-unhealthy`

.. _cephadm-manually-deploy-mgr:

Manually Deploying a Manager Daemon
-----------------------------------
At least one manager (mgr) daemon is required by cephadm in order to manage the
cluster. If the last mgr in a cluster has been removed, follow these steps in
order to deploy a manager called (for example)
``mgr.hostname.smfvfd`` on a random host of your cluster manually. 

Disable the cephadm scheduler, in order to prevent cephadm from removing the new 
manager. See :ref:`cephadm-enable-cli`::

  ceph config-key set mgr/cephadm/pause true

Then get or create the auth entry for the new manager::

  ceph auth get-or-create mgr.hostname.smfvfd mon "profile mgr" osd "allow *" mds "allow *"

Get the ceph.conf::

  ceph config generate-minimal-conf

Get the container image::

  ceph config get "mgr.hostname.smfvfd" container_image

Create a file ``config-json.json`` which contains the information necessary to deploy
the daemon:

.. code-block:: json

  {
    "config": "# minimal ceph.conf for 8255263a-a97e-4934-822c-00bfe029b28f\n[global]\n\tfsid = 8255263a-a97e-4934-822c-00bfe029b28f\n\tmon_host = [v2:192.168.0.1:40483/0,v1:192.168.0.1:40484/0]\n",
    "keyring": "[mgr.hostname.smfvfd]\n\tkey = V2VyIGRhcyBsaWVzdCBpc3QgZG9vZi4=\n"
  }

Deploy the daemon::

  cephadm --image <container-image> deploy --fsid <fsid> --name mgr.hostname.smfvfd --config-json config-json.json

Capturing Core Dumps
---------------------

A Ceph cluster that uses cephadm can be configured to capture core dumps.
Initial capture and processing of the coredump is performed by
`systemd-coredump <https://www.man7.org/linux/man-pages/man8/systemd-coredump.8.html>`_.


To enable coredump handling, run:

.. prompt:: bash #

  ulimit -c unlimited

Core dumps will be written to ``/var/lib/systemd/coredump``.
This will persist until the system is rebooted.

.. note::

  Core dumps are not namespaced by the kernel, which means
  they will be written to ``/var/lib/systemd/coredump`` on
  the container host. 

Now, wait for the crash to happen again. To simulate the crash of a daemon, run
e.g. ``killall -3 ceph-mon``.


Running the Debugger with cephadm
----------------------------------

Running a single debugging session
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One can initiate a debugging session using the ``cephadm shell`` command.
From within the shell container we need to install the debugger and debuginfo
packages. To debug a core file captured by systemd, run the following:

.. prompt:: bash #

    # start the shell session
    cephadm shell --mount /var/lib/system/coredump
    # within the shell:
    dnf install ceph-debuginfo gdb zstd
    unzstd /var/lib/systemd/coredump/core.ceph-*.zst
    gdb /usr/bin/ceph-mon /mnt/coredump/core.ceph-*.zst

You can then run debugger commands at gdb's prompt.

.. prompt::

    (gdb) bt
    #0  0x00007fa9117383fc in pthread_cond_wait@@GLIBC_2.3.2 () from /lib64/libpthread.so.0
    #1  0x00007fa910d7f8f0 in std::condition_variable::wait(std::unique_lock<std::mutex>&) () from /lib64/libstdc++.so.6
    #2  0x00007fa913d3f48f in AsyncMessenger::wait() () from /usr/lib64/ceph/libceph-common.so.2
    #3  0x0000563085ca3d7e in main ()


Running repeated debugging sessions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When using ``cephadm shell``, like in the example above, the changes made to
the container the shell command spawned are ephemeral. Once the shell session
exits all of the files that were downloaded and installed are no longer
available. One can simply re-run the same commands every time ``cephadm shell``
is invoked, but in order to save time and resources one can create a new
container image and use it for repeated debugging sessions.

In the following example we create a simple file for constructing the
container image. The command below uses podman but it should work correctly
if ``podman`` is replaced with ``docker``.

.. prompt:: bash

  cat >Containerfile <<EOF
  ARG BASE_IMG=quay.io/ceph/ceph:v18
  FROM \${BASE_IMG}
  # install ceph debuginfo packages, gdb and other potentially useful packages
  RUN dnf install --enablerepo='*debug*' -y ceph-debuginfo gdb zstd strace python3-debuginfo
  EOF
  podman build -t ceph:debugging -f Containerfile .
  # pass --build-arg=BASE_IMG=<your image> to customize the base image

The result should be a new local image named ``ceph:debugging``. This image can
be used on the same machine that built it. Later, the image could be pushed to
a container repository, or saved and copied to a node runing other ceph
containers. Please consult the documentation for ``podman`` or ``docker`` for
more details on the general container workflow.

Once the image has been built it can be used to initiate repeat debugging
sessions without having to re-install the debug tools and debuginfo packages.
To debug a core file using this image, in the same way as previously described,
run:

.. prompt:: bash #

    cephadm --image ceph:debugging shell --mount /var/lib/system/coredump


Debugging live processes
~~~~~~~~~~~~~~~~~~~~~~~~

The gdb debugger has the ability to attach to running processes to debug them.
For a containerized process this can be accomplished by using the debug image
and attaching it to the same PID namespace as the process to be debugged.

This requires running a container command with some custom arguments. We can generate a script that can debug a process in a running container.

.. prompt:: bash #

   cephadm --image ceph:debugging shell --dry-run > /tmp/debug.sh

This creates a script with the container command cephadm would use to create a
shell. Now, modify the script by removing the ``--init`` argument and replace
that with the argument to join to the namespace used for a running running
container.  For example, let's assume we want to debug the MGR, and have
determnined that the MGR is running in a container named
``ceph-bc615290-685b-11ee-84a6-525400220000-mgr-ceph0-sluwsk``. The new
argument
``--pid=container:ceph-bc615290-685b-11ee-84a6-525400220000-mgr-ceph0-sluwsk``
should be used.

Now, we can run our debugging container with ``sh /tmp/debug.sh``. Within the shell
we can run commands such as ``ps`` to get the PID of the MGR process. In the following
example this will be ``2``. Running gdb, we can now attach to the running process:

.. prompt:: bash (gdb)

   attach 2
   info threads
   bt
