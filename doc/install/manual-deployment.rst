.. _manual-deployment:

===================================
 Deploying a Ceph Cluster Manually
===================================

.. meta::
   :description: Deploy a Ceph cluster by hand without cephadm: bootstrap the first Monitor, then add a Manager, OSDs, a Metadata Server, and a RADOS Gateway.
   :ceph-page-type: procedure

Deploy a Ceph cluster by hand, without :term:`cephadm`, when you develop a
deployment tool or cannot use cephadm. For production clusters, use the
recommended method, :ref:`cephadm <cephadm_deploying_new_cluster>`.
:ref:`install-overview` lists all methods.

Prerequisites
=============

- At least one :term:`Monitor <Ceph Monitor>` host and, with the default
  settings, :term:`OSDs <Ceph OSD>` on at least three hosts (see `Adding
  OSDs`_).
- Ceph packages installed on every host. See :ref:`install_storage_cluster`.
  For the :ref:`short form <manual-deployment-short-form>` of adding OSDs,
  also install the ``ceph-volume`` package on each OSD host: the ``ceph``
  package does not require it.
- Root access on every host through ``sudo``.
- Root SSH logins from ``host1701`` to itself and to each OSD host, for the
  ``scp -3`` copy steps in `Adding OSDs`_. You can copy the files another
  way instead.
- Network access between the hosts on the ports that the daemons (background
  services) use. The steps open the Monitor ports. If the hosts run a
  firewall, also open ports 6800 to 7568 on hosts that run OSDs,
  :term:`Managers <Ceph Manager>`, or
  :term:`Metadata Servers <Ceph Metadata Server>` (the firewalld ``ceph``
  service), and port 7480 on gateway hosts. See :doc:`network configuration
  settings </rados/configuration/network-config-ref>`.

The examples use the following hosts. Replace the names and addresses with
your own.

.. list-table::
   :header-rows: 1
   :widths: 30 25 45

   * - Host
     - Address
     - Runs
   * - ``host1701``
     - ``192.0.2.1``
     - Monitor, Manager, Metadata Server, and :term:`RADOS Gateway <RGW>`.
       It also holds the administrator :term:`keyring <Keyring>`.
   * - ``host1702``, ``host1703``, ``host1704``
     - In ``192.0.2.0/24``
     - One OSD each.

.. _manual-deployment-monitor-bootstrapping:

Monitor Bootstrapping
=====================

The first Monitor needs the following:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Item
     - What it is
   * - ``fsid``
     - The cluster's unique ID, a UUID. The name is short for "file system ID".
   * - Cluster name
     - Use the default cluster name, ``ceph``. :ref:`Custom cluster names are
       deprecated <ceph-runtime-config>`, and the systemd units no longer pass
       ``--cluster``, so daemons started with ``systemctl`` always use
       ``ceph``. :ref:`Multisite <multisite>` zones are named with
       ``--rgw-zone``, not with the cluster name.
   * - Monitor name
     - A name that is unique in the cluster, usually the short host name
       (``hostname -s``). We recommend one Monitor per host, and no OSDs on
       Monitor hosts.
   * - Monitor map
     - The list of Monitors and their addresses. It requires the ``fsid`` and
       at least one host name and its IP address.
   * - Monitor keyring
     - The secret key, ``mon.``, that Monitors use to communicate with each
       other.
   * - Administrator keyring
     - The keyring of the ``client.admin`` user, which the ``ceph`` CLI tools
       need.

A :ref:`Ceph configuration file <configuring-ceph>` is not required, but as a
best practice, create one with the ``fsid``, ``mon_initial_members``, and
``mon_host`` settings.

#. Log in to the Monitor host:

   .. prompt:: bash $

      ssh {hostname}

   For example:

   .. prompt:: bash $

      ssh host1701

   You get a shell prompt on ``host1701``.

#. Check that the directory for the Ceph configuration file exists. By
   default, Ceph uses ``/etc/ceph``, which the packages create:

   .. prompt:: bash $

      ls /etc/ceph

   The output lists ``rbdmap``, which the ``ceph-common`` package installs.

#. Create a Ceph configuration file, and add a line that contains
   ``[global]``. By default, Ceph uses ``ceph.conf``, where ``ceph`` is the
   cluster name:

   .. prompt:: bash $

      sudo vim /etc/ceph/ceph.conf

   The command opens the file in the editor.

#. Generate a unique ID (the ``fsid``) for the cluster:

   .. prompt:: bash $

      uuidgen

   The command prints a UUID, for example
   ``a7f64266-0894-4f1e-a635-d0aeaca0e993``.

#. Add the unique ID to the configuration file:

   .. code-block:: ini

      fsid = {UUID}

   For example:

   .. code-block:: ini

      fsid = a7f64266-0894-4f1e-a635-d0aeaca0e993

#. Add the initial Monitors to the configuration file:

   .. code-block:: ini

      mon_initial_members = {hostname}[,{hostname}]

   For example:

   .. code-block:: ini

      mon_initial_members = host1701

#. Add the IP addresses of the initial Monitors to the configuration file,
   and save the file:

   .. code-block:: ini

      mon_host = {ip-address}[,{ip-address}]

   For example:

   .. code-block:: ini

      mon_host = 192.0.2.1

   To use IPv6 addresses instead of IPv4 addresses, also set
   :confval:`ms_bind_ipv6` to ``true``. See :doc:`network configuration
   settings </rados/configuration/network-config-ref>` for details.

   ``cat /etc/ceph/ceph.conf`` shows the ``[global]`` line and the ``fsid``,
   ``mon_initial_members``, and ``mon_host`` lines.

#. Create a keyring for the cluster and generate a Monitor secret key:

   .. warning:: ``--create-keyring`` replaces any existing file at the path
      that you give it. On a host of an existing cluster, the next two steps
      would replace ``/etc/ceph/ceph.client.admin.keyring`` and
      ``/var/lib/ceph/bootstrap-osd/ceph.keyring``.

   .. prompt:: bash $

      sudo ceph-authtool --create-keyring /tmp/ceph.mon.keyring --gen-key -n mon.

   The command prints ``creating /tmp/ceph.mon.keyring``. The ``mon.``
   credential does not require any capabilities. All Monitors share this
   single key.

#. Generate an administrator keyring, generate a ``client.admin`` user, and
   add the user to the keyring:

   .. prompt:: bash $

      sudo ceph-authtool --create-keyring /etc/ceph/ceph.client.admin.keyring --gen-key -n client.admin --cap mon 'allow *' --cap osd 'allow *' --cap mds 'allow *' --cap mgr 'allow *'

   The command prints ``creating /etc/ceph/ceph.client.admin.keyring``.

#. Generate a bootstrap-osd keyring, generate a ``client.bootstrap-osd`` user
   (the user that may create OSDs), and add the user to the keyring:

   .. prompt:: bash $

      sudo ceph-authtool --create-keyring /var/lib/ceph/bootstrap-osd/ceph.keyring --gen-key -n client.bootstrap-osd --cap mon 'profile bootstrap-osd' --cap mgr 'allow r'

   The command prints ``creating /var/lib/ceph/bootstrap-osd/ceph.keyring``.

#. Add the administrator key to ``ceph.mon.keyring``:

   .. prompt:: bash $

      sudo ceph-authtool /tmp/ceph.mon.keyring --import-keyring /etc/ceph/ceph.client.admin.keyring

   The command prints ``importing contents of
   /etc/ceph/ceph.client.admin.keyring into /tmp/ceph.mon.keyring``.

#. Add the bootstrap-osd key to ``ceph.mon.keyring``:

   .. prompt:: bash $

      sudo ceph-authtool /tmp/ceph.mon.keyring --import-keyring /var/lib/ceph/bootstrap-osd/ceph.keyring

   The command prints ``importing contents of
   /var/lib/ceph/bootstrap-osd/ceph.keyring into /tmp/ceph.mon.keyring``.

#. Change the owner of ``ceph.mon.keyring``, so that the Monitor, which runs
   as the ``ceph`` user, can read it:

   .. prompt:: bash $

      sudo chown ceph:ceph /tmp/ceph.mon.keyring

   The command prints nothing on success.

#. Optional: allow legacy clients that authenticate with the cluster through
   :term:`CephX` to use the older, insecure ``aes`` cipher. You can also do
   this after the cluster is created (see :ref:`cephx-upgrade`). The default
   key type is ``aes256k``.

   .. warning:: If you enable legacy cipher types, the Monitors raise the
      ``AUTH_INSECURE_KEYS_ALLOWED`` and ``AUTH_INSECURE_KEYS_CREATABLE``
      warnings, and an insecure ``--auth-service-cipher`` raises the
      ``AUTH_INSECURE_SERVICE_TICKETS`` error. You may mute these checks (see
      :ref:`rados-monitoring-muting-health-checks`). See :ref:`health-checks`
      for details.

   .. prompt:: bash $

      AUTH_SETTINGS="--auth-allowed-ciphers=aes,aes256k"

   The command prints nothing. If you skip the three optional steps,
   ``$AUTH_SETTINGS`` is empty in the ``monmaptool`` step.

#. Optional: also make new keys use the legacy (and insecure) ``aes`` cipher
   by default:

   .. prompt:: bash $

      AUTH_SETTINGS="$AUTH_SETTINGS --auth-preferred-cipher=aes"

   The command prints nothing.

#. Optional: set the service cipher type. This is generally needed only if an
   older version of a service daemon must run in the cluster. Clients do not
   and cannot decrypt the service cipher:

   .. prompt:: bash $

      AUTH_SETTINGS="$AUTH_SETTINGS --auth-service-cipher=aes"

   The command prints nothing.

#. Generate a monitor map from the host names, the host IP addresses, and the
   ``fsid``, and save it as ``/tmp/monmap``:

   .. prompt:: bash $

      monmaptool --create --enable-all-features $AUTH_SETTINGS --add {hostname} {ip-address} --fsid {uuid} /tmp/monmap

   For example:

   .. prompt:: bash $

      monmaptool --create --enable-all-features $AUTH_SETTINGS --add host1701 192.0.2.1 --fsid a7f64266-0894-4f1e-a635-d0aeaca0e993 /tmp/monmap

   The output ends with ``monmaptool: writing epoch 0 to /tmp/monmap (1
   monitors)``. ``--enable-all-features`` gives the Monitor a msgr2 address
   (the current wire protocol) as well as a legacy v1 address.

#. Create the default data directory on the Monitor host. If you listed more
   than one initial Monitor, repeat this step, the ``ceph-mon --mkfs`` step,
   and the start and firewall steps on each of those hosts, with copies of
   ``/etc/ceph/ceph.conf``, ``/tmp/monmap``, and ``/tmp/ceph.mon.keyring``
   (owned by ``ceph``):

   .. prompt:: bash $

      sudo -u ceph mkdir /var/lib/ceph/mon/ceph-{hostname}

   For example:

   .. prompt:: bash $

      sudo -u ceph mkdir /var/lib/ceph/mon/ceph-host1701

   The command prints nothing on success.

#. Populate the Monitor data directory with the monitor map and the keyring:

   .. prompt:: bash $

      sudo -u ceph ceph-mon --mkfs -i {hostname} --monmap /tmp/monmap --keyring /tmp/ceph.mon.keyring

   For example:

   .. prompt:: bash $

      sudo -u ceph ceph-mon --mkfs -i host1701 --monmap /tmp/monmap --keyring /tmp/ceph.mon.keyring

   The command prints nothing on success.

#. Add the network settings to the ``[global]`` section of the configuration
   file. Common settings include the following:

   .. code-block:: ini

      [global]
      fsid = {cluster-id}
      mon_initial_members = {hostname}[, {hostname}]
      mon_host = {ip-address}[, {ip-address}]
      public_network = {network}[, {network}]
      cluster_network = {network}[, {network}]

   In this example, the ``[global]`` section looks like this:

   .. code-block:: ini

      [global]
      fsid = a7f64266-0894-4f1e-a635-d0aeaca0e993
      mon_initial_members = host1701
      mon_host = 192.0.2.1
      public_network = 192.0.2.0/24

   See :confval:`public_network` and :confval:`cluster_network`. The file
   needs only the settings that override the defaults. Cluster-wide
   defaults, such as the replica count (:confval:`osd_pool_default_size`),
   the number of :term:`placement groups <Placement Groups (PGs)>` per OSD,
   the heartbeat intervals, and the authentication settings, are ordinary
   options: set them here before the first start, or later with ``ceph
   config set``. You can get and set all Monitor settings at runtime. Most
   have working defaults, but review them before you put the cluster into
   production. For other changes, prefer the ``ceph config`` API to the
   ``ceph.conf`` file. See :ref:`configuring-ceph-api`.

#. Start the Monitor with systemd (the text after ``@`` in the unit name is
   the daemon ID):

   .. prompt:: bash $

      sudo systemctl start ceph-mon@host1701

   The command prints nothing on success.

#. Start the Monitor at boot:

   .. prompt:: bash $

      sudo systemctl enable ceph-mon@host1701

   The command prints a ``Created symlink`` line.

#. Open the Monitor ports in the running firewall. The predefined
   ``ceph-mon`` service of firewalld opens the Monitor ports, 3300 and 6789:

   .. prompt:: bash $

      sudo firewall-cmd --zone=public --add-service=ceph-mon

   The command prints ``success``.

#. Open the same ports in the permanent firewall configuration:

   .. prompt:: bash $

      sudo firewall-cmd --zone=public --add-service=ceph-mon --permanent

   The command prints ``success``.

#. Check that the Monitor is running:

   .. prompt:: bash $

      sudo ceph -s

   You should see the Monitor in :term:`quorum <Quorum>`. Until you add a
   Manager and OSDs, the cluster has no :term:`pools <Pools>` or placement
   groups, and health is ``HEALTH_WARN``. The output looks something like
   this::

        cluster:
          id:     a7f64266-0894-4f1e-a635-d0aeaca0e993
          health: HEALTH_WARN
                  mon is allowing insecure global_id reclaim

        services:
          mon: 1 daemons, quorum host1701 (age 2m) [leader: host1701]
          mgr: no daemons active
          osd: 0 osds: 0 up, 0 in

        data:
          pools:   0 pools, 0 pgs
          objects: 0 objects, 0 B
          usage:   0 B used, 0 B / 0 B avail
          pgs:

   The ``mon is allowing insecure global_id reclaim`` warning is expected on
   a new cluster. `Troubleshooting`_ says how to clear it.

.. _manager-daemon-configuration:

Adding a Manager
================

On each host where you run a Monitor, also run a Manager. A production cluster
needs at least two Managers for high availability. Run these steps on
``host1701``.

#. Create the Manager's data directory:

   .. prompt:: bash $

      sudo -u ceph mkdir /var/lib/ceph/mgr/ceph-host1701

   The command prints nothing on success.

#. Create the Manager's key, and write it to a keyring file in that
   directory:

   .. prompt:: bash $

      sudo ceph auth get-or-create mgr.host1701 mon 'allow profile mgr' osd 'allow *' mds 'allow *' -o /var/lib/ceph/mgr/ceph-host1701/keyring

   The command prints nothing on success.

#. Change the owner of the keyring to the ``ceph`` user:

   .. prompt:: bash $

      sudo chown ceph:ceph /var/lib/ceph/mgr/ceph-host1701/keyring

   The command prints nothing on success.

#. Start the Manager:

   .. prompt:: bash $

      sudo systemctl start ceph-mgr@host1701

   The command prints nothing on success.

#. Start the Manager at boot:

   .. prompt:: bash $

      sudo systemctl enable ceph-mgr@host1701

   The command prints a ``Created symlink`` line.

#. Check that the Manager is active:

   .. prompt:: bash $

      sudo ceph -s

   The ``services`` section shows ``mgr: host1701(active, since ...)``. Until
   you add OSDs, health also shows ``OSD count 0 < osd_pool_default_size 3``.

.. _manual-deployment-adding-osds:

Adding OSDs
===========

Add OSDs once the Monitor is running. A cluster cannot reach ``active+clean``
(every placement group serves I/O and holds all of its object replicas or
shards) until it has at least as many OSDs as object replicas or shards. For
example, ``osd_pool_default_size = 2`` requires at least two OSDs. To reach
``active+clean`` with the default :confval:`osd_pool_default_size` of 3,
create OSDs on at least three hosts. For a two-host test cluster, run
``sudo ceph config set global osd_pool_default_size 2`` on ``host1701``
before you create the OSDs: the Monitor and the Manager are already running,
and they read ``ceph.conf`` only when they start. The Manager creates the
``.mgr`` pool once that many OSDs are ``up`` and ``in``.

After bootstrapping, the cluster has a default :term:`CRUSH` map, but no OSDs
in it. When a new OSD starts, it adds itself to the CRUSH map under its host
(``osd_crush_update_on_start``, default ``true``).

Create one OSD on each OSD host (``host1702``, ``host1703``, and
``host1704``) with either the short form or the long form. With the long
form, the ``ceph`` user's ownership of the device does not survive a reboot;
the short form (``ceph-volume``) sets it again each time it activates the
OSD. Both forms need the two copy steps below, run from ``host1701``, which
give the OSD host the cluster configuration and the ``client.bootstrap-osd``
keyring.

#. Copy the bootstrap-osd keyring from ``host1701`` to the OSD host, here
   ``host1702``:

   .. warning:: These copies overwrite any existing file at the target path
      on the OSD host.

   .. prompt:: bash $

      scp -3 root@host1701:/var/lib/ceph/bootstrap-osd/ceph.keyring root@host1702:/var/lib/ceph/bootstrap-osd/ceph.keyring

   On success, the command prints at most a progress line, such as
   ``ceph.keyring  100%``.

#. Copy the Ceph configuration file from ``host1701`` to the OSD host:

   .. prompt:: bash $

      scp -3 root@host1701:/etc/ceph/ceph.conf root@host1702:/etc/ceph/ceph.conf

   On success, the command prints at most a progress line, such as
   ``ceph.conf  100%``.

.. _short-form:

.. _manual-deployment-short-form:

Short Form: Creating an OSD with ceph-volume
--------------------------------------------

The ``ceph-volume`` utility prepares a logical volume (LVM, the Linux Logical
Volume Manager), disk, or partition for use with Ceph, and automates the
steps of the :ref:`long form <manual-deployment-long-form>`. It asks the
Monitors for a new :term:`OSD ID` (they assign the lowest unused ID). Run
``ceph-volume -h`` for CLI details.

.. warning:: ``ceph-volume lvm create`` and ``ceph-volume lvm prepare``
   destroy all data on the device. ``ceph-volume`` refuses a device that has
   partitions, a file system, GPT headers, or a :term:`BlueStore` label, but
   it does not detect other data.

#. Log in to the OSD host:

   .. prompt:: bash $

      ssh {osd-host}

   For example:

   .. prompt:: bash $

      ssh host1702

   You get a shell prompt on ``host1702``.

#. Create the OSD on its device:

   .. prompt:: bash $

      sudo ceph-volume lvm create --data {data-path}

   For example:

   .. prompt:: bash $

      sudo ceph-volume lvm create --data /dev/sdx

   The output ends with ``--> ceph-volume lvm create successful for:
   /dev/sdx``. The OSD starts at once.

Alternatively, split the creation into two phases, prepare and activate,
instead of the ``lvm create`` step:

#. Prepare the OSD:

   .. prompt:: bash $

      sudo ceph-volume lvm prepare --data {data-path}

   For example:

   .. prompt:: bash $

      sudo ceph-volume lvm prepare --data /dev/sdx

   The output ends with ``--> ceph-volume lvm prepare successful for:
   /dev/sdx``.

#. Find the ID and the :term:`FSID <OSD FSID>` of the prepared OSD, which
   activation requires, by listing the OSDs on this host:

   .. prompt:: bash $

      sudo ceph-volume lvm list

   Each OSD is listed under a header such as ``====== osd.0 =======``, with
   its ``osd id`` and ``osd fsid``.

#. Activate the OSD:

   .. prompt:: bash $

      sudo ceph-volume lvm activate {ID} {FSID}

   For example, if ``ceph-volume lvm list`` shows ``osd id 0`` and ``osd fsid
   7d6ee1c6-5d41-4a3b-8f2d-0c9a4e1b2f35``:

   .. prompt:: bash $

      sudo ceph-volume lvm activate 0 7d6ee1c6-5d41-4a3b-8f2d-0c9a4e1b2f35

   The output ends with ``--> ceph-volume lvm activate successful for osd
   ID: 0``, and the OSD starts.

.. _long-form:

.. _manual-deployment-long-form:

Long Form: Creating an OSD by Hand
----------------------------------

Run these steps once for each OSD.

.. note:: This procedure does not describe deployment on top of dm-crypt
   (Linux block-device encryption) that uses the dm-crypt "lockbox" (the
   CephX key, ``cephx_lockbox_secret``, that ``ceph-volume`` uses to fetch the
   dm-crypt key from the Monitors).

#. Log in to the OSD host:

   .. prompt:: bash $

      ssh {node-name}

   For example:

   .. prompt:: bash $

      ssh host1702

   You get a shell prompt on ``host1702``.

#. Become root. The following steps set shell variables that later steps
   use, so run them all in this shell:

   .. prompt:: bash $

      sudo bash

   The command opens a root shell.

#. Generate a UUID for the OSD:

   .. prompt:: bash #

      UUID=$(uuidgen)

   The command prints nothing.

#. Generate a CephX key for the OSD:

   .. prompt:: bash #

      OSD_SECRET=$(ceph-authtool --gen-print-key)

   The command prints nothing.

#. Create the OSD. This command assumes that the ``client.bootstrap-osd`` key
   is present on the host (see `Adding OSDs`_). You may alternatively run it
   as ``client.admin`` on a different host where that key is present. To
   reuse the ID of a previously destroyed OSD, give the ID as an additional
   argument to ``ceph osd new``.

   .. prompt:: bash #

      ID=$(echo "{\"cephx_secret\": \"$OSD_SECRET\"}" | \
         ceph osd new $UUID -i - \
         -n client.bootstrap-osd -k /var/lib/ceph/bootstrap-osd/ceph.keyring)

   The command prints nothing; the new OSD ID is in ``$ID``, and ``echo
   $ID`` shows it. To set an initial device class other than the default
   (``ssd`` or ``hdd``, based on the detected device type), include a
   ``crush_device_class`` property in the JSON. A device class is a label
   that CRUSH rules can select.

#. Create the default directory for the new OSD:

   .. prompt:: bash #

      mkdir /var/lib/ceph/osd/ceph-$ID

   The command prints nothing on success.

#. Link the OSD's device into the directory you just created, as ``block``.
   Use a drive other than the OS drive: an OSD on the OS drive is slow, and
   reimaging the OS takes the OSD data with it.

   .. prompt:: bash #

      ln -s /dev/{DEV} /var/lib/ceph/osd/ceph-$ID/block

   The command prints nothing on success. Without a ``block`` link,
   ``ceph-osd --mkfs`` stores the OSD in a 100 GiB file
   (``bluestore_block_size``) inside the directory.

#. Let the ``ceph`` user open the device:

   .. prompt:: bash #

      chown ceph:ceph /dev/{DEV}

   The command prints nothing on success.

#. Write the secret to the OSD keyring file:

   .. warning:: ``--create-keyring`` replaces any existing keyring at this
      path.

   .. prompt:: bash #

      ceph-authtool --create-keyring /var/lib/ceph/osd/ceph-$ID/keyring \
         --name osd.$ID --add-key $OSD_SECRET

   The command prints ``creating /var/lib/ceph/osd/ceph-{ID}/keyring`` and an
   ``added entity osd.{ID} auth(key=...)`` line, with your OSD ID.

#. Initialize the OSD data directory:

   .. warning:: This step writes a new BlueStore to the device that
      ``block`` links to, and destroys all data on it. ``ceph-osd --mkfs``
      refuses only a directory that already holds a different OSD.

   .. prompt:: bash #

      ceph-osd -i $ID --mkfs --osd-uuid $UUID

   The command can print ``_read_fsid unparsable uuid``, which is expected
   for a new OSD. A line that contains ``** ERROR`` means that it failed.

#. Fix the ownership of the OSD directory:

   .. prompt:: bash #

      chown -R ceph:ceph /var/lib/ceph/osd/ceph-$ID

   The command prints nothing on success.

#. Start the OSD at boot:

   .. prompt:: bash #

      systemctl enable ceph-osd@$ID

   For example:

   .. prompt:: bash #

      systemctl enable ceph-osd@0

   The command prints a ``Created symlink`` line.

#. Start the OSD. It is in your configuration, but until it runs, it cannot
   receive data:

   .. prompt:: bash #

      systemctl start ceph-osd@$ID

   For example:

   .. prompt:: bash #

      systemctl start ceph-osd@0

   The command prints nothing on success.

Checking the OSDs
-----------------

#. On ``host1701``, list the OSDs in the CRUSH map:

   .. prompt:: bash $

      sudo ceph osd tree

   Each new OSD is listed as ``up`` under its host. The output looks
   something like this::

       ID  CLASS  WEIGHT   TYPE NAME          STATUS  REWEIGHT  PRI-AFF
       -1         3.00000  root default
       -3         1.00000      host host1702
        0    hdd  1.00000          osd.0          up   1.00000  1.00000
       -5         1.00000      host host1703
        1    hdd  1.00000          osd.1          up   1.00000  1.00000
       -7         1.00000      host host1704
        2    hdd  1.00000          osd.2          up   1.00000  1.00000

   The ``WEIGHT`` column shows each device's size in TiB.

#. Check the OSD count:

   .. prompt:: bash $

      sudo ceph -s

   Once all three OSDs are running, the ``services`` section shows ``osd: 3
   osds: 3 up (since ...), 3 in (since ...)``.

.. _adding-mds:

Adding a Metadata Server (MDS)
==============================

A :term:`Metadata Server <Ceph Metadata Server>` (MDS) is needed only if you
use :term:`CephFS`. Run these steps on ``host1701``. See :ref:`manual-mds`
for more options, such as the file system that the MDS joins.

#. Create the MDS data directory:

   .. prompt:: bash $

      sudo -u ceph mkdir -p /var/lib/ceph/mds/ceph-host1701

   The command prints nothing on success.

#. Create the MDS key, and write it to a keyring file in that directory. The
   OSD capability ``allow rw tag cephfs *=*`` grants read and write access to
   the pools of any CephFS file system:

   .. prompt:: bash $

      sudo ceph auth get-or-create mds.host1701 mon 'allow profile mds' mgr 'allow profile mds' osd 'allow rw tag cephfs *=*' mds 'allow' -o /var/lib/ceph/mds/ceph-host1701/keyring

   The command prints nothing on success.

#. Change the owner of the keyring to the ``ceph`` user:

   .. prompt:: bash $

      sudo chown ceph:ceph /var/lib/ceph/mds/ceph-host1701/keyring

   The command prints nothing on success.

#. Start the MDS:

   .. prompt:: bash $

      sudo systemctl start ceph-mds@host1701

   The command prints nothing on success.

#. Start the MDS at boot:

   .. prompt:: bash $

      sudo systemctl enable ceph-mds@host1701

   The command prints a ``Created symlink`` line.

#. Check that the MDS is running:

   .. prompt:: bash $

      sudo ceph mds stat

   The output shows ``1 up:standby``. The MDS stays on standby until you
   :ref:`create a file system <create-fs>`.

.. _manually-installing-radosgw:

Adding a RADOS Gateway (RGW)
============================

A :term:`RADOS Gateway (RGW) <RGW>` provides S3-compatible and
Swift-compatible object storage. In this example, the gateway runs on
``host1701``. Run the commands as root.

#. Install the ``radosgw`` package (Debian, Ubuntu) or the ``ceph-radosgw``
   package (RPM-based distributions) on each host that will run a gateway.
   APT shows a ``Setting up radosgw`` line; DNF ends with ``Complete!``.

#. From a host that has the ``client.admin`` keyring, create a key for each
   RGW host. Replace ``{rgw-host}`` with the RGW host's short host name (the
   output of ``hostname -s`` on that host). Umbrella (21.2), Tentacle (20.2),
   and Squid (19.2) do not have ``profile rgw`` unless the release notes for
   your version list it. On a release without it, use
   ``mon 'allow rw' osd 'allow rwx'`` instead:

   .. prompt:: bash #

      ceph auth get-or-create client.{rgw-host} mon 'profile rgw' osd 'profile rgw'

   For example:

   .. prompt:: bash #

      ceph auth get-or-create client.host1701 mon 'profile rgw' osd 'profile rgw'

   The command prints the new keyring: a ``[client.host1701]`` line and a
   ``key =`` line.

#. On the RGW host, create a directory that the ``ceph`` user owns:

   .. prompt:: bash #

      install -d -o ceph -g ceph /var/lib/ceph/radosgw/ceph-$(hostname -s)

   The command prints nothing on success.

#. Create a ``keyring`` file in that directory:

   .. prompt:: bash #

      touch /var/lib/ceph/radosgw/ceph-$(hostname -s)/keyring

   The command prints nothing on success.

#. Put the key from the ``ceph auth get-or-create`` step in the ``keyring``
   file, with your preferred editor:

   .. prompt:: bash #

      $EDITOR /var/lib/ceph/radosgw/ceph-$(hostname -s)/keyring

   The file must contain the two lines that the ``ceph auth get-or-create``
   step printed.

#. Start the RADOS Gateway service:

   .. prompt:: bash #

      systemctl start ceph-radosgw@$(hostname -s).service

   The command prints nothing on success.

#. Start the RADOS Gateway at boot:

   .. prompt:: bash #

      systemctl enable ceph-radosgw@$(hostname -s).service

   The command prints a ``Created symlink`` line.

#. Check that the gateway is running:

   .. prompt:: bash #

      ceph -s

   The ``services`` section shows ``rgw: 1 daemon active (1 hosts, 1
   zones)``.

#. Check that the gateway answers HTTP requests. Its built-in web server,
   beast, listens on port 7480 by default:

   .. prompt:: bash $

      curl http://host1701:7480

   The output is an XML document that contains ``ListAllMyBucketsResult``.

#. Repeat these steps on every RGW host, including the step that creates a
   key for the host.

Verification
============

- Check the state of the cluster:

  .. prompt:: bash $

     sudo ceph -s

  The cluster is ready when the output shows the following:

  - ``health: HEALTH_OK``, after you clear the ``mon is allowing insecure
    global_id reclaim`` warning (see `Troubleshooting`_).
  - One Monitor in quorum.
  - An active Manager.
  - Three OSDs that are ``up`` and ``in``.
  - The gateway under ``rgw``.
  - Every placement group ``active+clean``.

  The MDS appears in this output only after you create a file system; check
  it with ``ceph mds stat``.

- Watch the placement groups peer:

  .. prompt:: bash $

     sudo ceph -w

  Press Ctrl-C to stop.

.. _manual-deployment-troubleshooting:

Troubleshooting
===============

- **"error opening mon data directory" or "unable to find a keyring", with
  "(13) Permission denied".** The ``ceph-mon --mkfs`` step runs as the
  ``ceph`` user, which cannot write the Monitor data directory or read
  ``/tmp/ceph.mon.keyring``. Change the owner of the directory or the keyring
  to ``ceph:ceph``, then run the step again. If the second run prints
  ``already exists and is not empty: monitor may already exist``, the first
  run left a partial store. On a Monitor that has never started, remove its
  data directory with ``sudo rm -r /var/lib/ceph/mon/ceph-host1701``, which
  deletes that Monitor's data, then repeat the ``mkdir`` and ``ceph-mon
  --mkfs`` steps.
- **"ceph -s" waits for up to five minutes, then prints "RADOS timed out
  (error connecting to the cluster)".** The ``ceph`` command cannot reach a
  Monitor. Check that ``sudo systemctl status ceph-mon@host1701`` shows the
  Monitor as ``active (running)``, that ``mon_host`` in
  ``/etc/ceph/ceph.conf`` holds its address, and that the firewall allows
  ports 3300 and 6789. ``sudo journalctl -u ceph-mon@host1701`` shows why
  the Monitor stopped.
- **"1 monitors have not enabled msgr2".** The monitor map was created
  without ``--enable-all-features``, so the Monitor listens only on the
  legacy v1 port (see :doc:`/rados/configuration/msgr2`). Run ``sudo ceph mon
  enable-msgr2``. See ``MON_MSGR2_NOT_ENABLED`` in :ref:`health-checks`.
- **"mon is allowing insecure global_id reclaim".** This warning is expected
  on a new cluster, because ``auth_allow_insecure_global_id_reclaim``
  defaults to ``true``. The warning clears once the option is ``false``. If
  ``AUTH_INSECURE_GLOBAL_ID_RECLAIM`` is not also raised, no connected client
  needs the option, and you can run ``sudo ceph config set mon
  auth_allow_insecure_global_id_reclaim false``. See
  ``AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED`` in :ref:`health-checks`.
- **"no active mgr".** No Manager is running. The Monitors raise this
  warning once the cluster has OSDs and is more than two minutes old
  (``mon_mgr_mkfs_grace``). Start a Manager as described in `Adding a
  Manager`_.
- **"OSD count 2 < osd_pool_default_size 3".** The cluster has fewer OSDs
  than the default number of object replicas. Add OSDs on more hosts, or
  lower ``osd_pool_default_size`` as described in `Adding OSDs`_. The
  warning clears once the cluster has enough OSDs, and the Manager then
  creates the ``.mgr`` pool.
- **"Monitors are configured to allow auth using insecure key types",
  "Monitors are configured to allow creation of insecure key types", or
  "Monitors are configured to issue insecure service tickets".** You enabled
  legacy ciphers in the optional CephX steps of `Monitor Bootstrapping`_.
  The warning in the first of those steps says how to mute these checks.
- **"Permission denied" from "scp -3".** Root cannot log in over SSH from
  ``host1701`` to itself or to the OSD host. Set up root SSH logins, as
  listed in `Prerequisites`_, or copy the two files another way.
- **"ceph-volume: command not found".** Install the ``ceph-volume`` package
  on the OSD host.
- **"Unable to create a new OSD id".** ``ceph-volume`` could not get an OSD
  ID from the Monitors. Check that the OSD host has
  ``/etc/ceph/ceph.conf`` and ``/var/lib/ceph/bootstrap-osd/ceph.keyring``
  (see `Adding OSDs`_), and that it can reach the Monitor address in
  ``mon_host``.
- **"Unable to proceed with non-existing device", "has partitions", "has a
  filesystem", "has bluestore signature", or "GPT headers found".**
  ``ceph-volume`` refused the device that you gave with ``--data``. Check
  the device name. To reuse a device whose data you no longer need, wipe it
  with ``sudo ceph-volume lvm zap --destroy /dev/sdx``, which destroys all
  data on the device.
- **Placement groups stay "unknown" or "peering" after the OSDs are "up".**
  A firewall blocks the ports of the OSDs, the Manager, or the MDS, 6800 to
  7568 by default. Open them on every host, for example with the ``ceph``
  service of firewalld, as you did for the Monitor ports.
- **The OSD does not start, and its journal shows "OSD data directory ...
  does not exist; bailing out." or "is not owned by 'ceph' or 'root'".**
  The long form directory is missing or has the wrong owner. Check that
  ``/var/lib/ceph/osd/ceph-$ID`` exists, and run the ``chown -R`` step
  again. ``journalctl -u ceph-osd@0`` shows the journal of OSD 0.
- **An OSD created with the long form does not start after a reboot, and its
  log shows "open got: (13) Permission denied".** The device is owned by
  ``root`` again. Run
  ``chown ceph:ceph /dev/{DEV}`` and start the OSD, or create OSDs with the
  short form, which sets the ownership each time it activates the OSD.
- **A Manager, MDS, or gateway does not start, and its journal shows "failed
  to fetch mon config".** The full message is ``failed to fetch mon config
  (--no-mon-config to skip)``: the daemon could not authenticate with the
  Monitors. Check that its keyring is in its data
  directory and readable by the ``ceph`` user, and that the key name matches
  the daemon: ``mgr.host1701``, ``mds.host1701``, or ``client.host1701`` for
  the gateway. A ``keyring`` setting in the ``[global]`` section of
  ``ceph.conf`` (or, for the gateway, in ``[client]``) overrides the keyring
  in the data directory; move it to the ``[client.admin]`` section. ``sudo
  ceph auth get mgr.host1701`` shows the key that the Monitors expect.
- **"Failed to connect to host1701 port 7480" from curl.** The gateway is not
  running, or a firewall blocks port 7480. ``sudo systemctl status
  ceph-radosgw@host1701.service`` shows whether it runs, and ``sudo journalctl
  -u ceph-radosgw@host1701.service`` shows why it stopped.

Next Steps
==========

- Add Monitors (three for redundancy), or remove them. See
  :ref:`adding-and-removing-monitors`.
- Add or remove OSDs. See :ref:`adding-and-removing-osds`.
- Create a file system for the MDS. See :ref:`create-fs`.
- Change settings with the ``ceph config`` API. See
  :ref:`configuring-ceph-api`.

Additional Resources
====================

- :doc:`Network configuration settings
  </rados/configuration/network-config-ref>`
- `Monitor data directory settings <Monitor Config Reference - Data_>`_
- :ref:`Setting up a Manager by hand <mgr-administrator-guide>`
- :ref:`Adding an MDS by hand <manual-mds>`
- `RadosGW manual deployment thread on the ceph-users mailing list
  <https://lists.ceph.io/hyperkitty/list/ceph-users@ceph.io/message/LB3YRIKAPOHXYCW7MKLVUJPYWYRQVARU/>`_
- :ref:`Users and capability profiles <user-management>`
- :ref:`CephX configuration settings <rados-cephx-config-ref>`
- :ref:`Health checks <health-checks>`
- :ref:`Deploying on FreeBSD <manual-freebsd-deployment>`

.. _Monitor Config Reference - Data: ../../rados/configuration/mon-config-ref#data
