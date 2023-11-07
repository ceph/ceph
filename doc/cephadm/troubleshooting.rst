Troubleshooting
===============

This section explains how to investigate why a cephadm command failed or why a
certain service no longer runs properly.

Cephadm deploys daemons within containers. Troubleshooting containerized
daemons requires a different process than does troubleshooting traditional
daemons that were installed by means of packages.

Here are some tools and commands to help you troubleshoot your Ceph
environment.

.. _cephadm-pause:

Pausing or Disabling cephadm
----------------------------

If something goes wrong and cephadm is behaving badly, pause most of the Ceph
cluster's background activity by running the following command: 

.. prompt:: bash #

  ceph orch pause

This stops all changes in the Ceph cluster, but cephadm will still periodically
check hosts to refresh its inventory of daemons and devices. Disable cephadm
completely by running the following commands:

.. prompt:: bash #

  ceph orch set backend ''
  ceph mgr module disable cephadm

These commands disable all of the ``ceph orch ...`` CLI commands. All
previously deployed daemon containers continue to run and will start just as
they were before you ran these commands.

See :ref:`cephadm-spec-unmanaged` for more on disabling individual services.


Per-service and Per-daemon Events
---------------------------------

To make it easier to debug failed daemons, cephadm stores events per service
and per daemon. These events often contain information relevant to
the troubleshooting of your Ceph cluster. 

Listing Service Events
~~~~~~~~~~~~~~~~~~~~~~

To see the events associated with a certain service, run a command of the 
following form:

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

To see the events associated with a certain daemon, run a command of the
following form:

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

To learn how to monitor cephadm logs as they are generated, read
:ref:`watching_cephadm_logs`.

If your Ceph cluster has been configured to log events to files, there will be
a ``ceph.cephadm.log`` file on all monitor hosts. See :ref:`cephadm-logs` for a
more complete explanation.

Gathering Log Files
-------------------

Use ``journalctl`` to gather the log files of all daemons:

.. note:: By default cephadm now stores logs in journald. This means
   that you will no longer find daemon logs in ``/var/log/ceph/``.

To read the log file of one specific daemon, run a command of the following
form:

.. prompt:: bash

   cephadm logs --name <name-of-daemon>

.. Note:: This works only when run on the same host that is running the daemon.
   To get the logs of a daemon that is running on a different host, add the
   ``--fsid`` option to the command, as in the following example:

   .. prompt:: bash

      cephadm logs --fsid <fsid> --name <name-of-daemon>

   In this example, ``<fsid>`` corresponds to the cluster ID returned by the
   ``ceph status`` command.

To fetch all log files of all daemons on a given host, run the following
for-loop::

    for name in $(cephadm ls | jq -r '.[].name') ; do
      cephadm logs --fsid <fsid> --name "$name" > $name;
    done

Collecting Systemd Status
-------------------------

To print the state of a systemd unit, run a command of the following form: 

.. prompt:: bash

   systemctl status "ceph-$(cephadm shell ceph fsid)@<service name>.service";


To fetch the state of all daemons of a given host, run the following shell
script::

   fsid="$(cephadm shell ceph fsid)"
   for name in $(cephadm ls | jq -r '.[].name') ; do
     systemctl status "ceph-$fsid@$name.service" > $name;
   done


List all Downloaded Container Images
------------------------------------

To list all container images that are downloaded on a host, run the following
commands:

.. prompt:: bash #

   podman ps -a --format json | jq '.[].Image' "docker.io/library/centos:8" "registry.opensuse.org/opensuse/leap:15.2"

.. note:: ``Image`` might also be called ``ImageID``.


Manually Running Containers
---------------------------

Cephadm uses small wrappers when running containers. Refer to
``/var/lib/ceph/<cluster-fsid>/<service-name>/unit.run`` for the container
execution command.

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

If you receive the above error message, try the following things to
troubleshoot the SSH connection between ``cephadm`` and the monitor:

1. Ensure that ``cephadm`` has an SSH identity key::

     [root@mon1~]# cephadm shell -- ceph config-key get mgr/cephadm/ssh_identity_key > ~/cephadm_private_key
     INFO:cephadm:Inferring fsid f8edc08a-7f17-11ea-8707-000c2915dd98
     INFO:cephadm:Using recent ceph image docker.io/ceph/ceph:v15 obtained 'mgr/cephadm/ssh_identity_key'
     [root@mon1 ~] # chmod 0600 ~/cephadm_private_key

 If this fails, cephadm doesn't have a key. Fix this by running the following command::

     [root@mon1 ~]# cephadm shell -- ceph cephadm generate-ssh-key

 or::

     [root@mon1 ~]# cat ~/cephadm_private_key | cephadm shell -- ceph cephadm set-ssh-key -i -

2. Ensure that the SSH config is correct::

     [root@mon1 ~]# cephadm shell -- ceph cephadm get-ssh-config > config

3. Verify that it is possible to connect to the host::

     [root@mon1 ~]# ssh -F config -i ~/cephadm_private_key root@mon1

Verifying that the Public Key is Listed in the authorized_keys file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To verify that the public key is in the ``authorized_keys`` file, run the
following commands::

     [root@mon1 ~]# cephadm shell -- ceph cephadm get-pub-key > ~/ceph.pub
     [root@mon1 ~]# grep "`cat ~/ceph.pub`"  /root/.ssh/authorized_keys

Failed to Infer CIDR network error
----------------------------------

If you see this error::

   ERROR: Failed to infer CIDR network for mon ip ***; pass --skip-mon-network to configure it later

Or this error::

   Must set public_network config option or specify a CIDR network, ceph addrvec, or plain IP

This means that you must run a command of this form:

.. prompt:: bash

   ceph config set mon public_network <mon_network>

For more detail on operations of this kind, see
:ref:`deploy_additional_monitors`.

Accessing the Admin Socket
--------------------------

Each Ceph daemon provides an admin socket that bypasses the MONs (See
:ref:`rados-monitoring-using-admin-socket`).

#. To access the admin socket, enter the daemon container on the host::

   [root@mon1 ~]# cephadm enter --name <daemon-name>

#. Run a command of the following form to see the admin socket's configuration::
  
   [ceph: root@mon1 /]# ceph --admin-daemon /var/run/ceph/ceph-<daemon-name>.asok config show

Running Various Ceph Tools
--------------------------------

To run Ceph tools such as ``ceph-objectstore-tool`` or 
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

The cephadm shell sets up the environment in a way that is suitable for
extended daemon maintenance and for the interactive running of daemons. 

.. _cephadm-restore-quorum:

Restoring the Monitor Quorum
----------------------------

If the Ceph Monitor daemons (mons) cannot form a quorum, ``cephadm`` will not
be able to manage the cluster until quorum is restored.

In order to restore the quorum, remove unhealthy monitors
form the monmap by following these steps:

1. Stop all Monitors. Use ``ssh`` to connect to each Monitor's host, and then
   while connected to the Monitor's host use ``cephadm`` to stop the Monitor
   daemon:

   .. prompt:: bash

      ssh {mon-host}
      cephadm unit --name {mon.hostname} stop


2. Identify a surviving Monitor and log in to its host:

   .. prompt:: bash

      ssh {mon-host}
      cephadm enter --name {mon.hostname}

3. Follow the steps in :ref:`rados-mon-remove-from-unhealthy`.

.. _cephadm-manually-deploy-mgr:

Manually Deploying a Manager Daemon
-----------------------------------
At least one Manager (``mgr``) daemon is required by cephadm in order to manage
the cluster. If the last remaining Manager has been removed from the Ceph
cluster, follow these steps in order to deploy a fresh Manager on an arbitrary
host in your cluster. In this example, the freshly-deployed Manager daemon is
called ``mgr.hostname.smfvfd``.

#. Disable the cephadm scheduler, in order to prevent ``cephadm`` from removing
   the new Manager. See :ref:`cephadm-enable-cli`:

   .. prompt:: bash #

      ceph config-key set mgr/cephadm/pause true

#. Retrieve or create the "auth entry" for the new Manager:

   .. prompt:: bash #

      ceph auth get-or-create mgr.hostname.smfvfd mon "profile mgr" osd "allow *" mds "allow *"

#. Retrieve the Monitor's configuration:

   .. prompt:: bash #

      ceph config generate-minimal-conf

#. Retrieve the container image:

   .. prompt:: bash #

      ceph config get "mgr.hostname.smfvfd" container_image

#. Create a file called ``config-json.json``, which contains the information
   necessary to deploy the daemon:

   .. code-block:: json

     {
       "config": "# minimal ceph.conf for 8255263a-a97e-4934-822c-00bfe029b28f\n[global]\n\tfsid = 8255263a-a97e-4934-822c-00bfe029b28f\n\tmon_host = [v2:192.168.0.1:40483/0,v1:192.168.0.1:40484/0]\n",
       "keyring": "[mgr.hostname.smfvfd]\n\tkey = V2VyIGRhcyBsaWVzdCBpc3QgZG9vZi4=\n"
     }

#. Deploy the Manager daemon:

   .. prompt:: bash #

      cephadm --image <container-image> deploy --fsid <fsid> --name mgr.hostname.smfvfd --config-json config-json.json

Capturing Core Dumps
---------------------

A Ceph cluster that uses ``cephadm`` can be configured to capture core dumps.
The initial capture and processing of the coredump is performed by
`systemd-coredump
<https://www.man7.org/linux/man-pages/man8/systemd-coredump.8.html>`_.


To enable coredump handling, run the following command

.. prompt:: bash #

   ulimit -c unlimited


.. note::

  Core dumps are not namespaced by the kernel. This means that core dumps are
  written to ``/var/lib/systemd/coredump`` on the container host. The ``ulimit
  -c unlimited`` setting  will persist  only until the system is rebooted.

Wait for the crash to happen again. To simulate the crash of a daemon, run for
example ``killall -3 ceph-mon``.


Running the Debugger with cephadm
----------------------------------

Running a single debugging session
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Initiate a debugging session by using the ``cephadm shell`` command.
From within the shell container we need to install the debugger and debuginfo
packages. To debug a core file captured by systemd, run the following:


#. Start the shell session:

   .. prompt:: bash #

      cephadm shell --mount /var/lib/system/coredump

#. From within the shell session, run the following commands:

   .. prompt:: bash #

      dnf install ceph-debuginfo gdb zstd

   .. prompt:: bash #
      
    unzstd /var/lib/systemd/coredump/core.ceph-*.zst

   .. prompt:: bash #

    gdb /usr/bin/ceph-mon /mnt/coredump/core.ceph-*.zst

#. Run debugger commands at gdb's prompt:

   .. prompt:: bash (gdb)

      bt
      
   ::

      #0  0x00007fa9117383fc in pthread_cond_wait@@GLIBC_2.3.2 () from /lib64/libpthread.so.0
      #1  0x00007fa910d7f8f0 in std::condition_variable::wait(std::unique_lock<std::mutex>&) () from /lib64/libstdc++.so.6
      #2  0x00007fa913d3f48f in AsyncMessenger::wait() () from /usr/lib64/ceph/libceph-common.so.2
      #3  0x0000563085ca3d7e in main ()


Running repeated debugging sessions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When using ``cephadm shell``, as in the example above, any changes made to the
container that is spawned by the shell command are ephemeral. After the shell
session exits, the files that were downloaded and installed cease to be
available. You can simply re-run the same commands every time ``cephadm
shell`` is invoked, but in order to save time and resources one can create a
new container image and use it for repeated debugging sessions.

In the following example, we create a simple file that will construct the
container image. The command below uses podman but it is expected to work
correctly even if ``podman`` is replaced with ``docker``::

  cat >Containerfile <<EOF
  ARG BASE_IMG=quay.io/ceph/ceph:v18
  FROM \${BASE_IMG}
  # install ceph debuginfo packages, gdb and other potentially useful packages
  RUN dnf install --enablerepo='*debug*' -y ceph-debuginfo gdb zstd strace python3-debuginfo
  EOF
  podman build -t ceph:debugging -f Containerfile .
  # pass --build-arg=BASE_IMG=<your image> to customize the base image

The above file creates a new local image named ``ceph:debugging``. This image
can be used on the same machine that built it. The image can also be pushed to
a container repository or saved and copied to a node runing other Ceph
containers. Consult the ``podman`` or ``docker`` documentation for more
information about the container workflow.

After the image has been built, it can be used to initiate repeat debugging
sessions. By using an image in this way, you avoid the trouble of having to
re-install the debug tools and debuginfo packages every time you need to run a
debug session. To debug a core file using this image, in the same way as
previously described, run:

.. prompt:: bash #

    cephadm --image ceph:debugging shell --mount /var/lib/system/coredump


Debugging live processes
~~~~~~~~~~~~~~~~~~~~~~~~

The gdb debugger can attach to running processes to debug them. This can be
achieved with a containerized process by using the debug image and attaching it
to the same PID namespace in which the process to be debugged resides.

This requires running a container command with some custom arguments. We can
generate a script that can debug a process in a running container.

.. prompt:: bash #

   cephadm --image ceph:debugging shell --dry-run > /tmp/debug.sh

This creates a script that includes the container command that ``cephadm``
would use to create a shell. Modify the script by removing the ``--init``
argument and replace it with the argument that joins to the namespace used for
a running running container. For example, assume we want to debug the Manager
and have determnined that the Manager is running in a container named
``ceph-bc615290-685b-11ee-84a6-525400220000-mgr-ceph0-sluwsk``. In this case,
the argument
``--pid=container:ceph-bc615290-685b-11ee-84a6-525400220000-mgr-ceph0-sluwsk``
should be used.

We can run our debugging container with ``sh /tmp/debug.sh``. Within the shell,
we can run commands such as ``ps`` to get the PID of the Manager process. In
the following example this is ``2``. While running gdb, we can attach to the
running process:

.. prompt:: bash (gdb)

   attach 2
   info threads
   bt
