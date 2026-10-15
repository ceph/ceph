.. _cephadm_deploying_new_cluster:

============================================
 Using Cephadm to Deploy a New Ceph Cluster
============================================

.. meta::
   :description: Deploy a new Ceph cluster with cephadm: install cephadm, bootstrap the first host, enable the ceph command, add hosts and storage, and check the result.
   :ceph-page-type: procedure

:term:`Cephadm <cephadm>` creates a new Ceph cluster by bootstrapping a single
:term:`host <Host>`, expanding the cluster to encompass any additional hosts,
and then deploying the needed services. Check the `Requirements`_ on every
host, then follow the stages in order. To try Ceph on one machine, see
:ref:`quick-start-cephadm`.

.. _cephadm-host-requirements:

Requirements
============

Every host in the cluster needs the following. Install them on the bootstrap
host before you bootstrap, and on each other host before you add it:

- Python 3.9 or later. See `Cephadm Requires Python 3.9 or Later`_.
- Systemd
- Podman or Docker for running containers. Not every version of Podman is
  compatible with Ceph: see the table in :ref:`Compatibility With Podman
  Versions <cephadm-compatibility-with-podman>`.
- Time synchronization (such as Chrony or the legacy ``ntpd``)
- LVM2 for provisioning storage devices
- An SSH server, installed and running, that accepts logins with a key for
  ``root``, or for the user that you give to ``cephadm bootstrap`` with
  ``--ssh-user``. The :term:`Managers <Ceph Manager>` manage each host over
  SSH.

You also need root access on every host: run ``cephadm`` as ``root``.

If the bootstrap host has no container engine, LVM2, or time
synchronization, ``cephadm bootstrap`` tries to install Podman, LVM2, or
chrony with the package manager of the distribution, and stops with an error
where it cannot. On Ubuntu, it first adds a third-party repository for Podman
(see `Troubleshooting`_), and installs ``docker.io`` if Podman fails to
install. Pass ``--skip-prepare-host`` to keep it from checking the host and
from installing anything. Hosts that you add later are checked, not
prepared. For the distributions on which Ceph is tested as a container host,
see :ref:`start-platforms`.

The bootstrap host, the first host of the cluster, also needs the following:

- A short host name: the ``hostname`` command prints a name without dots, such
  as ``host1701``, not a fully qualified domain name. Otherwise, pass
  ``--allow-fqdn-hostname`` to ``cephadm bootstrap``.
- No ``ceph.conf``, ``ceph.client.admin.keyring``, or ``ceph.pub`` file in
  ``/etc/ceph``, for example from an earlier cluster. Otherwise, pass
  ``--allow-overwrite`` or ``--output-dir`` (see `Further Information about
  Cephadm Bootstrap`_).

With the default settings, the cluster places three object replicas on three
different hosts, so give at least three hosts storage devices for
:term:`OSDs <Ceph OSD>`. For a cluster on one host, see `Single Host`_.

.. _get-cephadm:

Install Cephadm
===============

To deploy a new cluster, install ``cephadm`` on the bootstrap host. The other
hosts do not need it to be managed: the Managers copy ``cephadm`` to each host
that they manage. To run ``cephadm shell`` on another host, install
``cephadm`` there too. To adopt an existing cluster (see
:ref:`cephadm-adoption`), install ``cephadm`` on every host. There are two
ways to get the initial copy of ``cephadm``:

- `Using Distribution Packages to Install Cephadm`_: the packages of your
  Linux distribution.
- `Using Curl to Install Cephadm`_: a download from ``download.ceph.com``.

Either way, the release of ``cephadm`` decides which Ceph release
``cephadm bootstrap`` deploys, so make sure that it is up to date.

.. important:: Use only one of these methods on a host: either the
   distribution-specific method or the curl-based method, not both.

.. note:: ``cephadm`` is distributed as an executable built from the Ceph
   source tree. Unlike with earlier versions of Ceph, copying a single script
   from Ceph's git tree and running it does not work. To run a development
   version of ``cephadm``, create your own build: see
   :ref:`compiling-cephadm`.

.. _cephadm_install_distros:

.. _distribution-specific-installations:

Using Distribution Packages to Install Cephadm
----------------------------------------------

Some Linux distributions ship Ceph packages, often for a release older than
the current stable one. Compare the version that your distribution offers
(``apt-cache policy cephadm``, ``dnf info cephadm``, or ``zypper info
cephadm``) with the `latest active releases`_ before you install it.

#. Install the ``cephadm`` package with the commands for your distribution.

   On Ubuntu:

   .. prompt:: bash #

      apt install -y cephadm

   On CentOS Stream, run these three commands in order:

   a. List the packages that add a Ceph repository:

      .. prompt:: bash #

         dnf search release-ceph

      The output lists ``centos-release-ceph-<release>`` packages.

   #. Install the package for the release that you want, for example the
      current :term:`stable release <Ceph Stable Release>`:

      .. prompt:: bash #
         :substitutions:

         dnf install --assumeyes centos-release-ceph-|stable-release|

   #. Install ``cephadm``:

      .. prompt:: bash #

         dnf install --assumeyes cephadm

   On Fedora:

   .. prompt:: bash #

      dnf -y install cephadm

   On SUSE:

   .. prompt:: bash #

      zypper install -y cephadm

   APT shows a ``Setting up cephadm`` line; DNF ends with ``Complete!``;
   zypper shows an ``Installing: cephadm`` line.

#. Confirm that ``cephadm`` runs:

   .. prompt:: bash #

      cephadm version

   The command prints one line, for example ``cephadm version 20.2.4
   (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)``.

   On CentOS Stream, if the command fails with ``No module named 'yaml'`` or
   ``No module named 'jinja2'``, install the two Python modules that the
   ``cephadm`` package of the CentOS Storage SIG leaves out, then run the
   command again:

   .. prompt:: bash #

      dnf install --assumeyes python3-pyyaml python3-jinja2

   A ``cephadm`` older than 18.2.1, such as the 17.2 package of Ubuntu
   22.04, runs ``ceph --version`` in the Ceph container image instead: it
   needs Podman or Docker, and prints a line that starts with ``ceph
   version``.

.. _cephadm_install_curl:

Using Curl to Install Cephadm
-----------------------------

#. Decide which release of Ceph to install. The current stable release is
   |stable-release|; the releases page lists the `latest active releases`_.

#. Use ``curl`` to fetch the build of cephadm for that release. For another
   release, replace the release name in the URL with its name or with a
   version in x.y.z form:

   .. prompt:: bash #
      :substitutions:

      curl --fail --silent --show-error --remote-name --location https://download.ceph.com/rpm-|stable-release|/el9/noarch/cephadm

   The command prints nothing on success. If the release does not exist, it
   prints ``curl: (22) The requested URL returned error: 404``. The ``el9``
   build also runs on other distributions. ``download.ceph.com`` publishes no
   checksum or signature for this file; the packages that `Installing Cephadm
   on the Host`_ installs are signed.

#. Make the ``cephadm`` file executable:

   .. prompt:: bash #

      chmod +x cephadm

   The command prints nothing on success.

#. Confirm that ``cephadm`` runs from the current directory:

   .. prompt:: bash #

      ./cephadm version

   The command prints one line, for example ``cephadm version 20.2.4
   (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)``. Run the
   downloaded file the same way for every command, as
   ``./cephadm <arguments...>``.

Installing Cephadm on the Host
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Although the standalone ``cephadm`` is sufficient to bootstrap a cluster, it is
best to have the ``cephadm`` command installed on the host. On a distribution
that Ceph builds packages for (see :ref:`start-platforms`), run the following
commands.

#. Add the repository of the release:

   .. warning:: ``cephadm add-repo`` replaces ``/etc/yum.repos.d/ceph.repo`` on
      Enterprise Linux (EL), or ``/etc/apt/sources.list.d/ceph.list`` and
      ``/etc/apt/trusted.gpg.d/ceph.release.gpg`` on Debian and Ubuntu, without
      keeping a copy. Copy these files first if you have changed them. On EL,
      the command also installs the ``epel-release`` package.

   .. prompt:: bash #
      :substitutions:

      ./cephadm add-repo --release |stable-release|

   ``Completed adding repo.`` is the last line of the output.

#. Install the ``cephadm`` package:

   .. prompt:: bash #

      ./cephadm install

   ``Installing packages ['cephadm']...`` is the last line of the output.

#. Confirm that ``cephadm`` is now in your ``PATH``:

   .. prompt:: bash #

      which cephadm

   The command prints ``/usr/sbin/cephadm``.

.. _cephadm_bootstrap_a_new_cluster:

Bootstrap a New Cluster
=======================

What to Know Before You Bootstrap
---------------------------------

The ``cephadm bootstrap`` command runs on the first host of the cluster and
creates the cluster's first :term:`Monitor <Ceph Monitor>` there. You must
pass the IP address of this host to the command. If the host has several
networks and interfaces, choose an address that every host that accesses the
Ceph cluster can reach.

.. warning:: ``cephadm bootstrap`` changes the host:

   - It installs missing packages, as described in `Requirements`_.
   - It adds the new SSH public key of the cluster to
     ``/root/.ssh/authorized_keys``. The Managers then log in to every host
     of the cluster as ``root`` with that key. The key stays in the file
     until you remove it.
   - If firewalld is running, it opens the ports of the daemons that it
     deploys.
   - It enables the :term:`Ceph Dashboard` on port 8443 with the user
     ``admin`` and a generated password, which it prints and also writes to
     ``/var/log/ceph/cephadm.log``.

   The options that change this behavior are in `Further Information about
   Cephadm Bootstrap`_.

Running the Bootstrap Command
-----------------------------

For a cluster on one host, in an isolated network, or with your own SSH keys,
use the bootstrap command in `Different Deployment Scenarios`_ instead of the
one in this step.

#. On the bootstrap host, run the ``cephadm bootstrap`` command, replacing
   ``<mon-ip>`` with the IP address of the host:

   .. prompt:: bash #

      cephadm bootstrap --mon-ip <mon-ip>

   For example, for a host with the address ``192.0.2.1``:

   .. prompt:: bash #

      cephadm bootstrap --mon-ip 192.0.2.1

   The command takes a few minutes and reports each stage. Near the end, it
   prints the address and the login of the Ceph Dashboard:

   .. code-block:: none

      Ceph Dashboard is now available at:

                   URL: https://host1701:8443/
                  User: admin
              Password: <generated password>

   A few lines later, the output includes ``Bootstrap complete.`` At the first
   login, the Dashboard asks for a new password.

The command has done the following:

- Pulled the Ceph container image, by default the image of the release of
  ``cephadm``.
- Created a Monitor and a Manager daemon for the new cluster on the local
  host, and deployed the crash service and the monitoring stack
  (``ceph-exporter``, Prometheus, Grafana, Node Exporter, and Alertmanager).
- Generated a new SSH key for the Ceph cluster, and written a copy of its
  public key to ``/etc/ceph/ceph.pub``.
- Written a minimal configuration file to ``/etc/ceph/ceph.conf``. This file
  is needed to communicate with Ceph daemons.
- Written a copy of the ``client.admin`` administrative (privileged) secret
  key to ``/etc/ceph/ceph.client.admin.keyring``.
- Added the ``_admin`` label to the bootstrap host. By default, cephadm
  maintains a copy of ``/etc/ceph/ceph.conf`` and
  ``/etc/ceph/ceph.client.admin.keyring`` on any host with this label. See
  :ref:`cephadm-special-host-labels`.

.. _cephadm-enable-cli:

Enable Ceph CLI
===============

Cephadm does not require any Ceph packages on the host, but the rest of this
procedure uses the ``ceph`` command. Make it available in one of these ways:

- The ``cephadm shell`` command launches a bash shell in a container with all
  of the Ceph packages installed:

  .. prompt:: bash #

     cephadm shell

  The prompt changes to ``[ceph: root@host1701 /]#``. By default, if
  configuration and :term:`keyring <Keyring>` files are found in ``/etc/ceph``
  on the host, they are passed into the container environment so that the
  shell is fully functional. When executed on a Monitor host, ``cephadm
  shell`` infers the configuration from the Monitor container instead of using
  the default configuration. If ``--mount <path>`` is given, then the host
  ``<path>`` (file or directory) appears under ``/mnt`` inside the container.

- To execute one ``ceph`` command at a time, pass it to ``cephadm shell``
  after ``--``:

  .. prompt:: bash #

     cephadm shell -- ceph -s

  The command prints the status of the cluster.

- Install the ``ceph-common`` package, which contains the main Ceph
  command-line tools, including ``ceph``, ``rados``, ``rbd``, and
  ``mount.ceph`` (for mounting :term:`CephFS` file systems). Follow :ref:`Using
  Cephadm <get-packages-cephadm>` in :ref:`packages`: it adds the repository,
  enables the CRB repository on Enterprise Linux, and installs the package.

Then check the ``ceph`` command, inside ``cephadm shell`` or after
``cephadm shell --`` if you did not install ``ceph-common``:

#. Confirm that the ``ceph`` command is accessible:

   .. prompt:: bash #

      ceph -v

   The command prints one line, for example ``ceph version 20.2.4
   (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)``.

#. Confirm that the ``ceph`` command can connect to the cluster, and check
   its status:

   .. prompt:: bash #

      ceph status

   The ``cluster`` section of the output shows the ``id`` of the cluster and
   its ``health``. Until you add storage, ``health`` shows ``HEALTH_WARN`` with
   ``OSD count 0 < osd_pool_default_size 3``. The ``services`` section shows
   one Monitor in ``quorum`` and an active Manager.

Adding Hosts
============

Add each of the other hosts to the cluster. Before you add a host, install the
software in `Requirements`_ on it. Run the ``ceph`` commands in the way that
you chose in `Enable Ceph CLI`_.

#. On the bootstrap host, outside ``cephadm shell``, install the SSH public
   key of the cluster in the ``authorized_keys`` file of ``root`` on the new
   host, for example ``host1702``. If you bootstrapped with your own SSH
   keys, follow `Deployment with Custom SSH Keys`_ or `Deployment with
   CA-Signed SSH Keys`_ for this step instead. Otherwise, run:

   .. prompt:: bash #

      ssh-copy-id -f -i /etc/ceph/ceph.pub root@host1702

   The output includes ``Number of key(s) added: 1``. ``ssh-copy-id`` must
   log in to the new host as ``root`` to add the key, for example with the
   password of ``root``. If it prints ``Permission denied``, see
   `Troubleshooting`_.

#. Tell Ceph that the new host is part of the cluster. Give the name that the
   ``hostname`` command prints on that host, and its IP address; without the
   address, cephadm resolves the name through DNS:

   .. prompt:: bash #

      ceph orch host add host1702 192.0.2.2

   The command prints ``Added host 'host1702' with addr '192.0.2.2'``. If the
   host lacks software from `Requirements`_, the command fails instead; see
   `Troubleshooting`_.

#. Repeat the last two steps for each host.

#. We recommend that you give the ``_admin`` label to one or more other hosts,
   so that the Ceph CLI (for example, through ``cephadm shell``) is available
   on more than one host:

   .. warning:: Each host with the ``_admin`` label gets a copy of the
      ``client.admin`` keyring, which gives full control of the cluster to
      anyone who can read it there. Label only hosts that you trust as much
      as the bootstrap host.

   .. prompt:: bash #

      ceph orch host label add host1702 _admin

   The command prints ``Added label _admin to host host1702``.

As you add hosts, cephadm deploys more Monitors and Managers on them by itself.
See `Adding Additional Monitors`_.

Adding Storage
==============

Create an OSD on each storage device that the cluster is to use. Ceph creates
OSDs only on devices that are *available*; the conditions are listed in
:ref:`cephadm-deploy-osds`. Create the OSDs one device at a time, or on every
available device at once.

Creating OSDs on Specific Devices
---------------------------------

#. List the storage devices on the hosts of the cluster:

   .. prompt:: bash #

      ceph orch device ls

   The output has one row for each device. Each device that Ceph can use shows
   ``Yes`` in the ``AVAILABLE`` column; for the others, the ``REJECT REASONS``
   column says why. Note the host name and the path of each device that you
   want to use.

#. Create an OSD on one device, replacing ``host1702:/dev/sdx`` with the host
   name and the path of the device:

   .. warning:: Ceph erases the device that you give it. Do not use a device
      that holds data you want to keep.

   .. prompt:: bash #

      ceph orch daemon add osd host1702:/dev/sdx

   The command prints the OSD that it created, for example ``Created osd(s) 0
   on host 'host1702'``.

#. Repeat the last step for each device.

Creating OSDs on Every Available Device
---------------------------------------

The ``all-available-devices`` service creates an OSD on every available
device, and also takes devices that become available later:

#. Preview the OSDs that the service would create:

   .. prompt:: bash #

      ceph orch apply osd --all-available-devices --dry-run

   The output starts with a warning that the preview is a snapshot of the
   current inventory, and an empty ``SERVICESPEC PREVIEWS`` table. It ends
   with a table titled ``OSDSPEC PREVIEWS``, with one row for each OSD and
   its host and device in the ``HOST`` and ``DATA`` columns. If that table
   is replaced by ``Preview data is being generated.. Please re-run this
   command in a bit.``, wait a minute and run the command again.

#. Create the OSDs:

   .. warning:: This command creates an OSD on every available device of every
      host, now and later. Cephadm keeps the service and also uses each device
      that becomes available afterward, including new drives and drives that
      you zap. See :ref:`cephadm-osd-declarative`.

   .. prompt:: bash #

      ceph orch apply osd --all-available-devices

   The command prints a line that starts with ``Scheduled
   osd.all-available-devices update``.

#. Optional: stop cephadm from creating OSDs on devices that become available
   later. The OSDs that exist stay:

   .. prompt:: bash #

      ceph orch apply osd --all-available-devices --unmanaged=true

   The output starts with ``Scheduled osd.all-available-devices update...``.

Verification
============

- Check the state of the cluster:

  .. prompt:: bash #

     ceph status

  The cluster is ready when the output shows ``health: HEALTH_OK``, the
  Monitors in ``quorum``, an active Manager, and every OSD ``up`` and ``in``,
  for example ``osd: 3 osds: 3 up (since 2m), 3 in (since 3m)``. It can take a
  few minutes after the last OSD is created for the cluster to reach
  ``HEALTH_OK``.

- List the hosts of the cluster:

  .. prompt:: bash #

     ceph orch host ls

  The output lists each host with its address and labels, and ends with a line
  such as ``3 hosts in cluster``.

- List the daemons that cephadm manages:

  .. prompt:: bash #

     ceph orch ps

  Each daemon shows ``running`` in the ``STATUS`` column.

Troubleshooting
===============

For errors from ``cephadm add-repo`` or ``cephadm install``, see the
Troubleshooting section of :ref:`packages`. For errors that are not listed
here, see :doc:`Cephadm Troubleshooting </cephadm/troubleshooting>`.

- **"ERROR: cephadm should be run as root".** Run ``cephadm`` as ``root``, for
  example with ``sudo``.
- **"curl: (22) The requested URL returned error: 404".** ``download.ceph.com``
  has no build for the release name or version in the URL. Check it against
  the `latest active releases`_, then run the ``curl`` command again.
- **"cephadm: command not found".** ``cephadm`` is not installed, or you
  downloaded the binary with ``curl``. Run the downloaded binary as
  ``./cephadm`` from its directory, or install the package as described in
  `Installing Cephadm on the Host`_.
- **"ERROR: failed to fetch GPG key" from "cephadm bootstrap".** On Ubuntu,
  the host has no container engine, and cephadm could not add the third-party
  repository that it installs Podman from. Install Podman or Docker with the
  package manager of your distribution, then run the bootstrap command again.
- **"No container engine binary found (podman or docker)", "lvcreate binary
  does not appear to be installed", or "No time synchronization is
  active".** The host lacks software from `Requirements`_: ``cephadm
  bootstrap`` could not install it, or you are adding a host, which cephadm
  checks but does not prepare. When you add a host, these messages follow
  ``Host host1702 (192.0.2.2) failed check(s)``. Install the missing package
  on that host, then run the command again.
- **"... already exists; delete or pass --allow-overwrite to overwrite".**
  ``/etc/ceph`` on the bootstrap host holds files from another cluster or from
  an earlier bootstrap. If you no longer need them, delete them. Otherwise,
  pass ``--allow-overwrite``, or ``--output-dir`` with another directory.
- **"hostname is a fully qualified domain name".** The host name contains
  dots. Set a short host name, as the message suggests, or pass
  ``--allow-fqdn-hostname``. See :ref:`cephadm-fqdn`.
- **"one of the arguments ... is required" from "cephadm bootstrap".** Pass
  ``--mon-ip`` with the IP address of the bootstrap host.
- **"Cannot assign requested address" from "cephadm bootstrap".** The
  address that you passed with ``--mon-ip``, such as the example
  ``192.0.2.1``, is not configured on any network interface of the bootstrap
  host. Pass an address of the host, then run the bootstrap command again.
- **"Failed to pull container image. Check that correct registry credentials
  are provided".** The registry of the image requires a login. Pass
  ``--registry-json``; see `Further Information about Cephadm Bootstrap`_.
- **"Container release ... != cephadm release ...".** The container image is
  of another Ceph release than ``cephadm``. Use the ``cephadm`` of the release
  of the image, or an image of the release of ``cephadm``.
- **"Invalid --cluster-network parameter".** The value of
  ``--cluster-network`` is not a subnet in CIDR notation, such as
  ``198.51.100.0/24``.
- **"ceph: command not found".** The ``ceph`` command is not installed on the
  host. Run it inside ``cephadm shell``, or install ``ceph-common``, as
  described in `Enable Ceph CLI`_.
- **"Error initializing cluster client" from "ceph".** The host has no
  ``/etc/ceph/ceph.conf``. Run the command on a host with the ``_admin``
  label, such as the bootstrap host (see `Adding Hosts`_). If you
  bootstrapped with ``--output-dir``, pass the ``ceph.conf`` and
  ``ceph.client.admin.keyring`` files of that directory to ``ceph`` with
  ``-c`` and ``-k``.
- **"Permission denied" from "ssh-copy-id".** The new host does not let
  ``root`` log in with a password (``PermitRootLogin prohibit-password``, the
  OpenSSH default and the default on many distributions). On the new host,
  add the line in ``/etc/ceph/ceph.pub`` of the bootstrap host to
  ``/root/.ssh/authorized_keys`` by other means, then continue with the next
  step of `Adding Hosts`_.
- **"Can't communicate with remote host" or "Failed to connect to" when you
  add a host.** Cephadm cannot reach the host over SSH, or cannot log in to
  it. Check that the host is reachable, that Python 3 is installed on it, and
  that the public key of the cluster is in ``/root/.ssh/authorized_keys``
  there (the first step of `Adding Hosts`_). See :ref:`cephadm-ssh-errors`.
- **"does not match expected hostname" when you add a host.** The name that
  you gave is not the name that the ``hostname`` command prints on that host.
  Add the host under that name. See :ref:`cephadm-fqdn`.
- **"ceph orch device ls" lists no devices, or a device shows "No" in the
  "AVAILABLE" column.** The list of devices can take a minute to fill after
  you add a host: run ``ceph orch device ls --refresh``, then check again. For
  a device that shows ``No``, the ``REJECT REASONS`` column says why.
- **"Created no osd(s) on host ...; already created?".** The device already
  holds an OSD, or Ceph cannot use it. Check the device in the output of
  ``ceph orch device ls``.
- **"OSD count ... < osd_pool_default_size 3" in "ceph status".** The cluster
  has fewer OSDs than the default number of object replicas. Add OSDs as
  described in `Adding Storage`_.
- **The health stays "HEALTH_WARN" with "Degraded data redundancy" and
  undersized placement groups.** The OSDs are on fewer than three hosts, and
  the default settings place each object replica on a different host. Add
  OSDs on more hosts. For a cluster on one host, see `Single Host`_.

.. _cephadm-requires-python-3-6-or-later:

Cephadm Requires Python 3.9 or Later
------------------------------------

- **"bad interpreter" or "cannot execute: required file not found" when you
  run cephadm.** The host has no ``/usr/bin/python3``. Install Python 3, then
  run the command again.
- **"No module named 'dataclasses'", "cannot import name 'TypedDict'", or
  "'type' object is not subscriptable".** The Python that ran ``cephadm`` is
  too old. Install Python 3.9 or later.

You can manually run cephadm with a particular version of Python by prefixing
the command with your installed Python version. For example:

.. prompt:: bash #

   python3.9 ./cephadm <arguments...>

Next Steps
==========

- Add more hosts, with labels if you want them. See
  :ref:`cephadm-adding-hosts`.
- Label and remove hosts, and put them into maintenance. See
  :ref:`orchestrator-cli-host-management`.
- Create OSDs in other ways, for example from a service specification. See
  :ref:`cephadm-deploy-osds`.
- On hosts that also run other workloads, reduce the memory that OSDs use.
  See `Enabling OSD Memory Autotuning`_.
- Deploy and manage services. See :doc:`/cephadm/services/index`.
- Log in to the :ref:`Ceph Dashboard <mgr-dashboard>` with the address and the
  password that ``cephadm bootstrap`` printed.
- Configure monitoring and alerts. See :ref:`mgr-cephadm-monitoring`.
- With Docker, see `Docker Live Restore
  <https://docs.docker.com/engine/daemon/live-restore/>`_ for an optional
  feature that allows restarting Docker Engine without restarting all running
  containers, and :ref:`cephadm-docker-live-restore` for how cephadm
  integrates with Docker Engine to facilitate it.

Adding Additional Monitors
--------------------------

A typical Ceph cluster has three or five Monitor daemons spread across
different hosts. We recommend deploying five Monitors if there are five or
more nodes in your cluster. Most clusters do not benefit from seven or more
Monitors. Cephadm adds Monitors on new hosts by itself, up to five by default.
To choose how many Monitors run and on which hosts, follow
:ref:`deploy_additional_monitors`. For how many Monitors a cluster needs, see
:ref:`adding-monitors`.

Enabling OSD Memory Autotuning
------------------------------

.. warning:: By default, cephadm enables ``osd_memory_target_autotune`` on
   bootstrap, with ``mgr/cephadm/autotune_memory_target_ratio`` set to ``0.7``
   of total host memory. See :ref:`osd_autotune`.

To deploy hyperconverged Ceph with TripleO, please refer to the TripleO documentation: `Scenario: Deploy Hyperconverged Ceph <https://docs.openstack.org/project-deploy-guide/tripleo-docs/latest/features/deployed_ceph.html#scenario-deploy-hyperconverged-ceph>`_

#. If the cluster hardware is not used exclusively by Ceph (converged
   infrastructure), and you do not deploy with TripleO, reduce the memory
   consumption of Ceph:

   .. prompt:: bash #

      ceph config set mgr mgr/cephadm/autotune_memory_target_ratio 0.2

   The command prints nothing on success.

#. If memory autotuning is off, for example because the file that you passed
   with ``cephadm bootstrap --config`` turned it off, enable it:

   .. prompt:: bash #

      ceph config set osd osd_memory_target_autotune true

   The command prints nothing on success.

Using Ceph
----------

- To use the *Ceph File System*, follow :ref:`orchestrator-cli-cephfs`.
- To use the *Ceph Object Gateway*, follow :ref:`cephadm-deploy-rgw`.
- To use *NFS*, follow :ref:`deploy-cephadm-nfs-ganesha`.
- To use *iSCSI* in an existing deployment, follow :ref:`cephadm-iscsi`. The
  iSCSI gateway is in maintenance; for new deployments, see
  :ref:`ceph-nvmeof`.
- To create and mount a block device, follow :ref:`quick-rbd`.

.. _cephadm-bootstrap-further-info:

Further Information about Cephadm Bootstrap
===========================================

The default bootstrap process works for most users. These options of
``cephadm bootstrap`` change it; ``cephadm bootstrap -h`` lists all of them.

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Option
     - What it does
   * - ``--mon-ip <mon-ip>``
     - The IP address of the bootstrap host, for the first Monitor. See `What
       to Know Before You Bootstrap`_.
   * - ``--config <config-file>``, ``-c``
     - Passes initial Ceph configuration options to the new cluster, from a
       standard ini-style configuration file. See the example below the
       table.
   * - ``--cluster-network <subnet>``
     - Carries replication, recovery, and heartbeat traffic between OSDs on a
       separate network: a subnet in CIDR notation, for example
       ``198.51.100.0/24``. Whether a separate network helps depends on your
       links; see :ref:`cluster network <cluster-network>`. An IPv6 subnet,
       such as ``2001:db8::/64``, makes ``cephadm bootstrap`` enable IPv6
       binding and disable IPv4 binding for the whole cluster.
   * - ``--log-to-file``
     - Makes Ceph daemons write traditional log files to
       ``/var/log/ceph/<fsid>``, instead of logging to journald (with Podman
       2.1.0 or later) or to standard error, which the container runtime
       collects (with Docker or an older Podman). See :ref:`cephadm-logs`.
   * - ``--output-dir <directory>``
     - Writes ``ceph.conf``, the ``client.admin`` keyring, and ``ceph.pub`` to
       this directory (for example, ``.``) instead of ``/etc/ceph``, where
       Ceph packages installed on the host find them. Daemon containers
       deployed with cephadm do not need ``/etc/ceph`` at all, so this may help
       avoid conflicts with an existing Ceph configuration (cephadm or
       otherwise) on the same host. With any directory other than
       ``/etc/ceph``, bootstrap does not add the ``_admin`` label and does not
       distribute the ``client.admin`` keyring.
   * - ``--allow-overwrite``
     - Replaces ``ceph.conf``, ``ceph.client.admin.keyring``, and
       ``ceph.pub`` if they exist in the output directory.
   * - ``--allow-fqdn-hostname``
     - Allows a bootstrap host whose name is a fully qualified domain name
       (contains dots).
   * - ``--skip-prepare-host``
     - Does not check the host, and does not install Podman, LVM2, or chrony.
   * - ``--ssh-user <user>``
     - Designates which SSH user cephadm uses to connect to hosts. The
       associated SSH key is added to ``~<user>/.ssh/authorized_keys``. The
       user must have passwordless ``sudo`` access. The default is ``root``.
   * - ``--ssh-private-key``, ``--ssh-public-key``
     - Uses your own SSH key pair. See `Deployment with Custom SSH Keys`_.
   * - ``--ssh-private-key``, ``--ssh-signed-cert``
     - Uses a CA-signed SSH key. See `Deployment with CA-Signed SSH Keys`_.
   * - ``--registry-json <path to JSON file>``
     - Logs in to a registry that requires a login, before cephadm pulls the
       container image. See the example below the table. The
       ``--registry-url``, ``--registry-username``, and
       ``--registry-password`` options give the same information on the
       command line.
   * - ``--single-host-defaults``
     - Adjusts configuration defaults to suit a single-host cluster. See
       `Single Host`_.
   * - ``--image <image>``, given before ``bootstrap``
     - Uses this container image instead of the default image of the
       ``cephadm`` release. See `Deployment in an Isolated Environment`_.
   * - ``--skip-monitoring-stack``
     - Does not deploy ``ceph-exporter``, Prometheus, Grafana, Node Exporter,
       or Alertmanager.
   * - ``--skip-dashboard``
     - Does not enable the Ceph Dashboard.
   * - ``--initial-dashboard-password <password>``
     - Sets the password of the Dashboard user ``admin`` instead of a
       generated one. Bootstrap still prints the password and writes it to
       ``/var/log/ceph/cephadm.log``.
   * - ``--dashboard-password-noupdate``
     - Does not make the Dashboard ask for a new password at the first login.
   * - ``--ssl-dashboard-port <port>``
     - The HTTPS port of the Dashboard. The default is 8443.
   * - ``--skip-firewalld``
     - Does not open ports in firewalld for the daemons that bootstrap deploys
       on this host or for the Dashboard. Daemons that cephadm deploys later
       still get their ports opened.

For example, to pass the ``osd_crush_chooseleaf_type`` option, create a file
named ``initial-ceph.conf`` with the following content. This value suits a
cluster on one host only: on a cluster with more hosts, it lets Ceph place
every object replica on the same host.

.. code-block:: ini

   [global]
   osd_crush_chooseleaf_type = 0

Then pass the file to bootstrap:

.. prompt:: bash #

   cephadm bootstrap --mon-ip <mon-ip> --config initial-ceph.conf

The output is the same as in `Running the Bootstrap Command`_.

Example contents of a JSON file with login information for
``--registry-json``:

.. code-block:: json

   {"url":"REGISTRY_URL", "username":"REGISTRY_USERNAME", "password":"REGISTRY_PASSWORD"}

Cephadm stores the login information in the config-key store of the cluster,
under ``mgr/cephadm/registry_credentials``, so that hosts added to the cluster
later can also use the authenticated container registry.

.. _cephadm-deployment-scenarios:

Different Deployment Scenarios
==============================

Each of these scenarios changes the command in `Running the Bootstrap
Command`_. Before you run it, read the warning in `What to Know Before You
Bootstrap`_. After it succeeds, continue with `Enable Ceph CLI`_.

Single Host
-----------

To deploy a Ceph cluster running on a single host, use the
``--single-host-defaults`` flag when bootstrapping. Such clusters are
generally not suitable for production. For use cases, see
:ref:`one-node-cluster`; for the whole procedure on one machine, see
:ref:`quick-start-cephadm`.

.. prompt:: bash #

   cephadm bootstrap --mon-ip <mon-ip> --single-host-defaults

The output is the same as in `Running the Bootstrap Command`_, with one more
line, ``Adjusting default settings to suit single-host cluster...``. The
``--single-host-defaults`` flag sets the following configuration options,
unless the file given with ``--config`` sets them:

.. list-table::
   :header-rows: 1
   :widths: 12 33 10 45

   * - Section
     - Option
     - Value
     - Effect
   * - ``global``
     - ``osd_crush_chooseleaf_type``
     - ``0``
     - Object replicas are placed on different OSDs, not on different hosts.
   * - ``global``
     - ``osd_pool_default_size``
     - ``2``
     - New pools keep two object replicas.
   * - ``mgr``
     - ``mgr_standby_modules``
     - ``false``
     - Standby Managers do not run modules, so that several Managers can run on
       one host.

For more information on these options, see :ref:`one-node-cluster` and
:confval:`mgr_standby_modules` in :ref:`mgr-administrator-guide`.

.. _cephadm-airgap:

Deployment in an Isolated Environment
-------------------------------------

To deploy on hosts that are not connected directly to the Internet (an
"isolated" or "airgapped" environment), use a custom container registry:
either a Podman-based or Docker-based insecure registry, or a secure registry.
Installing software on such hosts is called "airgapping", and a registry that
is not connected directly to the Internet is also called "airgapped".

Before you start:

- Make sure that you have access to all hosts that you plan to add to the
  cluster.
- Install Podman or Docker, LVM2, and a time synchronization service on every
  host. ``cephadm`` cannot install them without access to a package
  repository.
- Install ``cephadm`` on the bootstrap host. It must be of the same release as
  the Ceph container image that you use; otherwise, bootstrap stops with a
  ``Container release`` error (see `Troubleshooting`_).

The steps run the registry on ``host1701``, port 5000. Replace the name with
your own.

#. On the registry host, create the directory that holds the data of the
   registry:

   .. prompt:: bash #

      mkdir -p /var/lib/registry

   The command prints nothing on success.

#. Run a local container registry:

   .. warning:: This registry accepts any client that can reach port 5000,
      over plain HTTP and without a login, and its container runs with
      ``--privileged``. Run it only on a network that you trust, or use a
      secure registry.

   .. prompt:: bash #

      podman run --privileged -d --name registry -p 5000:5000 -v /var/lib/registry:/var/lib/registry --restart=always docker.io/library/registry:3

   The last line of the output is the ID of the new container. If the
   registry image is not on the host yet, Podman first pulls it from
   ``docker.io``, which needs access to that registry.

#. If you are using an insecure registry, configure Podman or Docker with the
   hostname and port where the registry is running. For Podman, add these
   lines to ``/etc/containers/registries.conf``:

   .. code-block:: toml

      [[registry]]
      location = "host1701:5000"
      insecure = true

   For Docker, add ``"host1701:5000"`` to the ``insecure-registries`` list in
   ``/etc/docker/daemon.json``, then restart Docker.

   ``podman info`` then lists ``host1701:5000`` under ``registries``, with
   ``Insecure: true``. ``docker info`` lists it under
   ``Insecure Registries``.

   .. note:: You must repeat this step for every host that accesses the local
      insecure registry.

#. On a host that can reach the public registries, list the container images
   that cephadm deploys by default:

   .. prompt:: bash #

      cephadm list-images

   The output is a ``[mgr]`` section with one line for each image, for example
   ``mgr/cephadm/container_image_prometheus =
   quay.io/prometheus/prometheus:v3.6.0``.

#. Copy your container images to your local registry, for example:

   - Ceph container image. See :ref:`containers`.
   - Prometheus container image
   - Node Exporter container image
   - Grafana container image
   - AlertManager container image

   Start with the Ceph image of the release of ``cephadm``, for example
   ``quay.io/ceph/ceph:v20`` for Tentacle. Pull it:

   .. prompt:: bash #

      podman pull quay.io/ceph/ceph:v20

   The last line of the output is the ID of the image.

#. Tag the image with the name of your registry:

   .. prompt:: bash #

      podman tag quay.io/ceph/ceph:v20 host1701:5000/ceph/ceph:v20

   The command prints nothing on success.

#. Push the image to your registry. ``--tls-verify=false`` is needed for an
   insecure registry only:

   .. prompt:: bash #

      podman push --tls-verify=false host1701:5000/ceph/ceph:v20

   The output ends with ``Writing manifest to image destination``.

#. Repeat the last three steps for the Prometheus, Node Exporter, Grafana, and
   Alertmanager images that ``cephadm list-images`` printed. Keep the path and
   the tag of each image, and replace its registry with ``host1701:5000``.
   With Docker, use ``docker`` instead of ``podman``, without
   ``--tls-verify=false``.

#. On the bootstrap host, create a temporary configuration file named
   ``initial-ceph.conf`` to store the names of the monitoring images (see
   :ref:`cephadm_monitoring-images`). Replace each ``<tag>`` with the tag that
   you pushed:

   .. code-block:: ini

      [mgr]
      mgr/cephadm/container_image_prometheus = host1701:5000/prometheus/prometheus:<tag>
      mgr/cephadm/container_image_node_exporter = host1701:5000/prometheus/node-exporter:<tag>
      mgr/cephadm/container_image_grafana = host1701:5000/ceph/grafana:<tag>
      mgr/cephadm/container_image_alertmanager = host1701:5000/prometheus/alertmanager:<tag>

#. Run bootstrap using the temporary configuration file, and pass the name of
   your container image as the argument of the ``--image`` flag:

   .. prompt:: bash #

      cephadm --image host1701:5000/ceph/ceph:v20 bootstrap --mon-ip <mon-ip> --config initial-ceph.conf

   The output is the same as in `Running the Bootstrap Command`_.

.. _cephadm-bootstrap-custom-ssh-keys:

Deployment with Custom SSH Keys
-------------------------------

Instead of letting cephadm generate an SSH key pair, you can create your own
private/public pair and pass it to bootstrap, for example a key that you have
already distributed to the hosts that you want in the cluster.

#. Make sure that the public key of the pair is set up as an authorized key
   for the SSH user, typically ``root``, on each host that you will add to the
   cluster. For a non-root SSH user, see ``--ssh-user`` in `Further
   Information about Cephadm Bootstrap`_.

   To check, run ``ssh -i <private-key-filepath> root@host1702`` on the
   bootstrap host: it opens a shell on ``host1702`` without asking for a
   password.

#. Pass the ``--ssh-private-key`` and ``--ssh-public-key`` options to
   bootstrap. Both require a path to the file where the key is stored:

   .. prompt:: bash #

      cephadm bootstrap --mon-ip <mon-ip> --ssh-private-key <private-key-filepath> --ssh-public-key <public-key-filepath>

   Instead of ``Generating ssh key...`` and ``Wrote public SSH key to
   /etc/ceph/ceph.pub``, the output shows ``Using provided ssh keys...``;
   the rest is the same as in `Running the Bootstrap Command`_.

Bootstrap still adds your public key to the ``authorized_keys`` file of the SSH
user on the bootstrap host, but it does not write ``/etc/ceph/ceph.pub``. In
`Adding Hosts`_, skip the ``ssh-copy-id`` step for hosts that already have the
key, or give ``ssh-copy-id`` your public key file instead of
``/etc/ceph/ceph.pub``.

.. _cephadm-bootstrap-ca-signed-keys:

Deployment with CA-Signed SSH Keys
----------------------------------

As an alternative to standard public key authentication, cephadm also supports
deployment using CA-signed keys: every host trusts the public key of a
certificate authority (CA), and cephadm logs in with a key that the CA has
signed. Before bootstrapping, set up the CA public key as a trusted CA key on
the hosts that you want in the cluster. In this example, the bootstrap host,
``host1701``, acts as its own CA, and ``host1702`` is another host.

.. warning:: Anyone who holds the CA private key can sign a certificate that
   logs in as ``root`` to every host that trusts the CA. This example creates
   the key on a cluster host, in the home directory of ``root`` and without a
   passphrase. Keep the CA private key off the cluster hosts, or protect it.

#. On ``host1701``, make a CA key:

   .. prompt:: bash #

      ssh-keygen -t rsa -f ca-key -N ""

   The output includes ``Your identification has been saved in ca-key`` and
   ``Your public key has been saved in ca-key.pub``.

#. Copy the CA public key to ``/etc/ssh``:

   .. prompt:: bash #

      cp ca-key.pub /etc/ssh

   The command prints nothing on success.

#. Mark the key as trusted: with your preferred editor, add this line to
   ``/etc/ssh/sshd_config``:

   .. code-block:: none

      TrustedUserCAKeys /etc/ssh/ca-key.pub

   ``grep ca-key /etc/ssh/sshd_config`` then prints the line.

#. Check the configuration of the SSH server:

   .. prompt:: bash #

      sshd -t

   The command prints nothing if the configuration is valid.

#. Restart the SSH server, so that it picks up the configuration change. On
   Debian and Ubuntu, the unit is named ``ssh``: run ``systemctl restart
   ssh`` there instead.

   .. warning:: Keep your current session open until you have checked that you
      can still log in to the host.

   .. prompt:: bash #

      systemctl restart sshd

   The command prints nothing on success.

#. Install the CA public key on every other host that you want in the
   cluster:

   .. prompt:: bash #

      scp /etc/ssh/ca-key.pub host1702:/etc/ssh/

   ``/etc/ssh/ca-key.pub`` now exists on ``host1702``.

#. Make the CA key trusted on each of those hosts:

   .. warning:: Keep a session open on each host until you have checked that
      you can still log in to it.

   On each host, add the same ``TrustedUserCAKeys`` line to
   ``/etc/ssh/sshd_config``, check the file with ``sshd -t``, and restart the
   SSH server, as on ``host1701``.

#. On ``host1701``, make a new key pair for host access:

   .. prompt:: bash #

      ssh-keygen -t rsa -f cephadm-ssh-key -N ""

   The output includes ``Your identification has been saved in
   cephadm-ssh-key``.

#. Sign the public key with the CA key. This creates
   ``cephadm-ssh-key-cert.pub``:

   .. prompt:: bash #

      ssh-keygen -s ca-key -I user_root -n root -V +52w cephadm-ssh-key.pub

   The command prints ``Signed user key cephadm-ssh-key-cert.pub: id
   "user_root" serial 0 for root``, followed by the period in which the
   certificate is valid. This example uses the user ``root``: for a non-root
   user, adjust the arguments of ``-I`` and ``-n``. The ``-V`` parameter sets
   how long the certificate is valid, here 52 weeks.

#. Check the files:

   .. prompt:: bash #

      ls

   The output lists ``ca-key``, ``ca-key.pub``, ``cephadm-ssh-key``,
   ``cephadm-ssh-key-cert.pub``, and ``cephadm-ssh-key.pub``.

#. Verify that the signed key works. Keep the private key
   (``cephadm-ssh-key``) and the signed certificate in the same directory,
   and log in with the private key:

   .. prompt:: bash #

      ssh -i cephadm-ssh-key host1702

   You get a shell on ``host1702``. ``ssh`` uses the certificate that is
   stored beside the private key.

#. Pass the private key and the signed certificate to bootstrap, so that
   cephadm uses them for SSH between cluster hosts:

   .. prompt:: bash #

      cephadm bootstrap --mon-ip <mon-ip> --ssh-private-key cephadm-ssh-key --ssh-signed-cert cephadm-ssh-key-cert.pub

   Instead of ``Generating ssh key...``, the output shows ``Using provided
   ssh private key and signed cert ...``, and it has no lines about writing
   or authorizing a public key; the rest is the same as in `Running the
   Bootstrap Command`_.

The other hosts do not need the public key of ``cephadm-ssh-key``, so skip
the ``ssh-copy-id`` step in `Adding Hosts`_. Do not pass ``--ssh-public-key``
with ``--ssh-signed-cert``: cephadm rejects the combination. The public key
would not break anything, but it is not needed, and leaving it out tells
bootstrap that you want the CA-signed setup and not standard public key
authentication.

To rotate the SSH key, get another key signed by the same CA, and give cephadm
the new private key and the new signed certificate. Once the hosts trust the
CA key, they need no further keys, however many new key and certificate pairs
you rotate in.

Additional Resources
====================

- :ref:`orchestrator-cli-host-management`
- :doc:`/cephadm/services/index`
- :doc:`/cephadm/operations`
- :doc:`Cephadm Troubleshooting </cephadm/troubleshooting>`
- :ref:`cephadm-adoption`
- :ref:`packages`
- :ref:`containers`
- :ref:`os-recommendations`

.. Needs to be an external link because doc/releases/index.rst is not in
   stable branches and we want to always use the main branch version
.. _latest active releases: https://docs.ceph.com/en/latest/releases/#active-releases
