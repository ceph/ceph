.. _cephadm-adoption:

===========================================
 Converting an Existing Cluster to Cephadm
===========================================

.. meta::
   :description: Convert a Ceph cluster that was deployed with ceph-ansible, DeepSea, or ceph-deploy so that cephadm manages it.
   :ceph-page-type: procedure

Convert a cluster that was deployed with ``ceph-deploy`` (a legacy deployment
tool), ``ceph-ansible``, or DeepSea so that :term:`cephadm` manages it.
Cephadm adopts the :term:`Monitors <Ceph Monitor>`,
:term:`Managers <Ceph Manager>`, and :term:`OSDs <Ceph OSD>` in place, and
deploys new Metadata Server (:term:`MDS`) and
:term:`RADOS Gateway (RGW) <RGW>` daemons to replace the legacy ones. Only
some clusters can be converted: check the `Prerequisites`_ first.

Prerequisites
=============

- Every OSD in the cluster uses :term:`BlueStore`. Cephadm works only with
  BlueStore OSDs.
- The data of each Monitor, Manager, and OSD is in its default directory,
  ``/var/lib/ceph/<type>/ceph-<id>``, for example
  ``/var/lib/ceph/mon/ceph-host1701``. ``cephadm adopt`` looks for it there.
- Every Monitor is in :term:`quorum <Quorum>`, and every
  :term:`placement group <Placement Groups (PGs)>` is ``active+clean``. The
  steps check for this state again after daemons restart.
- The container image of the Ceph :term:`release <Ceph Release>` that the
  cluster is to run, written ``<image>`` in the steps, which pass it to
  ``cephadm`` with ``--image``:

  - The release must be Octopus (15.2.z) or later. Each adopted daemon runs
    this image, so an image of a newer release than the cluster runs today
    also upgrades the daemon. In that case, also follow the upgrade
    instructions in the `release notes`_ of the new release.
  - Use an explicit version tag, in the form ``quay.io/ceph/ceph:v<x.y.z>``,
    so that every :term:`host <Host>` gets the same image (see
    :ref:`containers`). If you leave out ``--image``, ``cephadm`` uses the
    image of its own release, for example ``quay.io/ceph/ceph:v20`` for the
    Tentacle ``cephadm``. That tag moves to each new point release.

- The ``cephadm`` command-line tool is available on each host in the cluster,
  in the same release as ``<image>`` (see :ref:`get-cephadm`). ``cephadm``
  starts each adopted daemon with the command-line options of its own
  release, and a daemon of an older release can fail to start with them.
- Root access on each host in the cluster.
- An SSH server running on each host in the cluster, and a login on each host
  over SSH as ``root``, or as the user that cephadm is to connect as. Cephadm
  manages the hosts over SSH, with a key that you install in the steps.

.. _adoption-process:

Procedure
=========

Run the ``cephadm`` commands as root on the host that each step names. Run the
``ceph`` commands on any host that has the ``ceph`` command and the admin
:term:`keyring <Keyring>`.

Preparing the Hosts
-------------------

Run these steps on each host in the cluster.

#. Prepare the host for cephadm:

   .. prompt:: bash #

      cephadm prepare-host

   The command installs Podman, LVM2, and chrony if the host has no
   container engine, LVM2, or time synchronization. The last line of the
   output is ``Host looks OK``.

#. List the Ceph daemons on the host:

   .. prompt:: bash #

      cephadm ls

   The output is a JSON list. Each Monitor, Manager, OSD, and MDS that is not
   yet adopted has ``"style": "legacy"``. Note the ``name`` of each Monitor,
   Manager, and OSD, such as ``mon.host1701`` or ``osd.1701``: the
   ``cephadm adopt`` commands take these names. An adopted daemon has
   ``"style": "cephadm:v1"``. Legacy RGW daemons are not listed.

Centralizing the Configuration
------------------------------

#. Copy the settings in ``/etc/ceph/ceph.conf`` to the cluster's central
   configuration database (see :ref:`ceph-conf-database`). If the file is
   identical on all hosts, run this command on one host: it takes effect for
   all hosts. If the file differs between hosts, run the command on each
   host; where hosts disagree on a setting, the first value that was stored
   is kept:

   .. prompt:: bash #

      ceph config assimilate-conf -i /etc/ceph/ceph.conf

   The command prints the settings that it did not store: settings that are
   unrecognized or invalid, settings that the Monitors cannot manage (such as
   ``fsid`` and ``mon_host``), and settings that are already in the database
   with a different value. To write them to a file instead, add
   ``-o <file>``.

   Review these settings before you go on. Once cephadm manages an adopted
   daemon, it replaces the configuration file of the daemon with a minimal
   one, which holds ``fsid`` and ``mon_host``, and the daemon takes every
   other setting from the database. Store each printed setting that the
   cluster still needs with ``ceph config set``.

#. Check that the database holds the configuration of the cluster:

   .. prompt:: bash #

      ceph config dump

   The output lists the stored settings, one per line, with the daemons that
   each setting applies to in the ``WHO`` column.

Adopting the Monitors and Managers
----------------------------------

.. warning:: From here on, the conversion cannot be undone: no ``cephadm``
   command converts a daemon back. ``cephadm adopt`` stops and disables the
   legacy systemd unit of the daemon, moves the daemon's data to
   ``/var/lib/ceph/<fsid>/`` and its logs to ``/var/log/ceph/<fsid>/``, and
   removes the legacy data directory.

#. On a host that runs a Monitor, adopt the Monitor. Replace ``<image>`` with
   the image from `Prerequisites`_, and ``<hostname>`` so that the name
   matches the one that ``cephadm ls`` shows, for example ``mon.host1701``:

   .. prompt:: bash #

      cephadm --image <image> adopt --style legacy --name mon.<hostname>

   The command reports each stage, including ``Moving data...``,
   ``Moving logs...``, and ``Creating new units...``. The legacy Monitor
   stops, quickly restarts as a cephadm container, and rejoins the quorum.
   ``cephadm ls`` on that host now reports the Monitor with the style
   ``cephadm:v1``.

#. Check that the Monitor is back in quorum:

   .. prompt:: bash #

      ceph -s

   The ``mon`` line of the output lists the Monitor after ``quorum``, and
   has no ``out of quorum`` part. If the Monitor is listed after
   ``out of quorum``, wait and run the command again. Do not adopt the next
   Monitor until this Monitor is back in quorum.

#. Repeat the last two steps on each of the other Monitor hosts, one host at
   a time.

#. On each host that runs a Manager, adopt the Manager:

   .. prompt:: bash #

      cephadm --image <image> adopt --style legacy --name mgr.<hostname>

   The command reports the same stages as for a Monitor. ``cephadm ls`` on
   that host now reports the Manager with the style ``cephadm:v1``.

Enabling Cephadm and Adding the Hosts
-------------------------------------

#. Enable the ``cephadm`` Manager module:

   .. prompt:: bash #

      ceph mgr module enable cephadm

   The command prints nothing on success. The Managers restart to load the
   module: if the next command fails, wait a few seconds and run it again.

#. Select cephadm as the backend of the orchestrator (the Manager module
   behind the ``ceph orch`` commands):

   .. prompt:: bash #

      ceph orch set backend cephadm

   The command prints nothing on success.

#. Set the container image that cephadm deploys new daemons with, such as
   the MDS and RGW daemons later in this procedure. Replace ``<image>`` with
   the image that you passed to ``cephadm adopt``:

   .. prompt:: bash #

      ceph config set global container_image <image>

   The command prints nothing on success. ``cephadm adopt`` does not set
   this option. Without it, cephadm deploys new daemons with the default
   image, ``docker.io/ceph/daemon-base:latest-master-devel``, and not with
   ``<image>``.

#. Generate an SSH key for cephadm:

   .. prompt:: bash #

      ceph cephadm generate-key

   The command prints nothing on success. ``ceph orch status`` now prints
   ``Backend: cephadm`` and ``Available: Yes``.

   To use an existing SSH key pair instead, run
   ``ceph cephadm set-priv-key -i <private-key-file>`` and
   ``ceph cephadm set-pub-key -i <public-key-file>`` in place of this
   command.

#. Write the public key to a file:

   .. prompt:: bash #

      ceph cephadm get-pub-key > ~/ceph.pub

   The command prints nothing. ``~/ceph.pub`` now holds the public key, on
   one line.

#. Install the public key on each host in the cluster. Run this command once
   for each host, from the host that holds ``~/ceph.pub``:

   .. prompt:: bash #

      ssh-copy-id -f -i ~/ceph.pub root@<host>

   The output includes ``Number of key(s) added: 1``.

   Cephadm can connect as a user other than ``root``. That user needs
   passwordless ``sudo``. Run ``ceph cephadm set-user <user>``, then install
   the public key for that user on each host with
   ``ssh-copy-id -f -i ~/ceph.pub <user>@<host>``. See
   :ref:`cephadm-ssh-user`.

#. Tell cephadm which hosts to manage. Run this command for each host in the
   cluster, with the name that the ``hostname`` command prints on that host:

   .. prompt:: bash #

      ceph orch host add <hostname> <ip-address>

   The command prints ``Added host '<hostname>' with addr '<ip-address>'``.
   Before it adds the host, cephadm runs ``cephadm check-host`` on it to make
   sure that the host is ready. The IP address is optional but recommended:
   without it, cephadm resolves the host name through DNS.

#. Check that cephadm sees the adopted Monitors and Managers:

   .. prompt:: bash #

      ceph orch ps

   The output lists each adopted daemon with its host, and with ``running``
   in the ``STATUS`` column.

Adopting the OSDs
-----------------

#. On a host that runs OSDs, adopt every OSD on the host. Run this command
   once for each OSD, and replace ``<id>`` with the ID of the OSD, so that
   the name reads, for example, ``osd.1701``:

   .. prompt:: bash #

      cephadm --image <image> adopt --style legacy --name osd.<id>

   The command reports the same stages as for a Monitor. The OSD stops and
   restarts in a container. ``cephadm ls`` on that host now reports the OSD
   with the style ``cephadm:v1``.

#. Check that the cluster has recovered:

   .. prompt:: bash #

      ceph -s

   The ``osd`` line of the output shows as many OSDs ``up`` as there are
   OSDs, and the ``pgs`` line shows every placement group as
   ``active+clean``. If it does not, wait and run the command again. Do not
   go on to the next host until it does.

#. Repeat the last two steps on each of the other hosts that run OSDs, one
   host at a time.

Redeploying the MDS Daemons
---------------------------

Skip this section if the cluster has no MDS daemons for :term:`CephFS`. The
legacy MDS daemons are not adopted: cephadm deploys new ones, and you then
remove the legacy ones.

#. List the file systems:

   .. prompt:: bash #

      ceph fs ls

   The command prints one line for each file system, for example:

   .. code-block:: console

      name: fs1701, metadata pool: fs1701_metadata, data pools: [fs1701_data ]

#. For each file system, tell cephadm how many MDS daemons to run. Run at
   least as many as the file system has now: the new daemons take over every
   active rank when you stop the legacy ones. Replace ``<fs-name>`` with the
   ``name`` that the previous step printed, and ``<placement>`` with a number
   of daemons or a list of hosts (see
   :ref:`orchestrator-cli-placement-spec`):

   .. prompt:: bash #

      ceph orch apply mds <fs-name> [--placement=<placement>]

   For example, ``ceph orch apply mds <fs-name> 2`` runs two MDS daemons for
   the file system. The command prints a line that starts with
   ``Scheduled mds.<fs-name> update``.

#. Check that the new MDS daemons have started:

   .. prompt:: bash #

      ceph orch ps --daemon-type mds

   The output lists the new MDS daemons with ``running`` in the ``STATUS``
   column, and with the version of ``<image>`` in the ``VERSION`` column. If
   the ``VERSION`` column shows another version, do not go on to the next
   step.

#. Check that the new MDS daemons have joined the cluster:

   .. prompt:: bash #

      ceph fs status

   The output lists the new MDS daemons under ``STANDBY MDS``, or with the
   state ``standby-replay`` in the table of the file system.

#. On each host that runs a legacy MDS, stop the legacy MDS daemons.

   .. warning:: This step and the next one stop the legacy MDS daemons and
      delete their data directories, which hold their keyrings. Run them only
      after the previous two steps show the new MDS daemons as ``running``,
      at the version of ``<image>``, and as standby. Without a running MDS,
      the file system is unavailable.

   .. prompt:: bash #

      systemctl stop ceph-mds.target

   The command prints nothing on success.

#. On the same hosts, remove the data directories of the legacy MDS daemons:

   .. prompt:: bash #

      rm -rf /var/lib/ceph/mds/ceph-*

   The command prints nothing on success.

Redeploying the RGW Daemons
---------------------------

Skip this section if the cluster has no RGW daemons. The legacy RGW daemons
are not adopted: cephadm deploys new ones, and you then remove the legacy
ones.

#. For each zone, deploy new RGW daemons. Cephadm manages RGW daemons by
   zone. Replace ``<svc_id>`` with a name that you choose for the service,
   and ``<placement>`` with a number of daemons or a list of hosts (see
   :ref:`orchestrator-cli-placement-spec`). The ``--realm`` and ``--zone``
   arguments are needed only for a multisite setup (see :ref:`multisite`),
   and must be given together:

   .. prompt:: bash #

      ceph orch apply rgw <svc_id> [--realm=<realm>] [--zone=<zone>] \
        [--port=<port>] [--ssl] [--placement=<placement>]

   The command prints a line that starts with
   ``Scheduled rgw.<svc_id> update``.

#. Check that the new RGW daemons have started:

   .. prompt:: bash #

      ceph orch ps --daemon-type rgw

   The output lists the new RGW daemons with ``running`` in the ``STATUS``
   column, and with the version of ``<image>`` in the ``VERSION`` column. If
   the ``VERSION`` column shows another version, do not go on to the next
   step.

#. Check that the new RGW daemons answer client requests. Run this command
   for each new daemon, with the host and the port that the previous step
   printed for it in the ``HOST`` and ``PORTS`` columns (the port is the
   number after the colon, as in ``*:80``):

   .. prompt:: bash #

      curl http://<host>:<port>

   The output is an XML document that contains ``ListAllMyBucketsResult``.
   If you deployed the service with ``--ssl``, run
   ``curl --insecure https://<host>:<port>`` instead. Beginning with the
   Umbrella release, when a service is given no certificate, cephadm
   generates one and signs it with its own certificate authority, which
   ``curl`` does not trust.

#. On each host that runs a legacy RGW, stop the legacy RGW daemons.

   .. warning:: This step and the next one stop the legacy RGW daemons and
      delete their data directories, which hold their keyrings. Run them only
      after the new daemons have started at the version of ``<image>`` and
      you have confirmed that they are functioning.

   .. prompt:: bash #

      systemctl stop ceph-radosgw.target

   The command prints nothing on success.

#. On the same hosts, remove the data directories of the legacy RGW daemons:

   .. prompt:: bash #

      rm -rf /var/lib/ceph/radosgw/ceph-*

   The command prints nothing on success.

Verification
============

- On each host, check that no legacy daemon is left:

  .. prompt:: bash #

     cephadm ls

  No entry in the output has ``"style": "legacy"``.

- Check that cephadm manages every daemon:

  .. prompt:: bash #

     ceph orch ps

  The output lists every Monitor, Manager, OSD, MDS, and RGW daemon of the
  cluster with ``running`` in the ``STATUS`` column.

- Check the health of the cluster:

  .. prompt:: bash #

     ceph health detail

  The output shows no cephadm warning about stray daemons
  (``CEPHADM_STRAY_DAEMON``) or about hosts that cephadm does not yet manage
  (``CEPHADM_STRAY_HOST``). Cephadm repeats this check every 30 minutes by
  default, so these warnings can remain for up to 30 minutes after the last
  step.

- Check that every daemon runs the same release:

  .. prompt:: bash #

     ceph versions

  The ``overall`` section of the output shows one version.

Troubleshooting
===============

- **"cephadm: command not found".** ``cephadm`` is not installed, or you
  downloaded the binary with ``curl``. Install it as described in
  :ref:`get-cephadm`, or run the binary as ``./cephadm`` from its directory.
- **"Failed command: ... pull" or "Failed to pull container image" from
  "cephadm adopt".** The host cannot pull ``<image>``. The command stops
  before it changes the daemon. Check the name of the image and that the host
  can reach the registry. If the registry needs a login, run
  ``cephadm registry-login`` on the host, then run the command again.
- **"could not detect legacy fsid; set fsid in ceph.conf".** The
  ``cephadm adopt`` command reads the ID of the cluster from the ``fsid``
  setting in the ``[global]`` section of ``/etc/ceph/ceph.conf`` on the host.
  For an OSD, it first reads the ID from
  ``/var/lib/ceph/osd/ceph-<id>/ceph_fsid``, and uses the setting only if it
  cannot read an ID from that file. Add the setting, then run the command
  again. ``ceph fsid`` prints the value.
- **"data directory ... does not exist. Incorrect ID specified, or daemon
  already adopted?"** No legacy daemon with that name has its data on this
  host. Run ``cephadm ls`` on the host, and use a ``name`` that has the style
  ``legacy``. If the name of the cluster is not ``ceph``, add
  ``--cluster <cluster-name>`` after ``adopt``, and use that name in place of
  ``ceph`` in the ``/etc/ceph/ceph.conf`` path and in the two ``rm`` paths of
  this procedure.
- **"FileStore is not supported by cephadm".** The OSD uses FileStore, and
  the command leaves it as it was. Migrate the OSD to BlueStore, then adopt
  it. See :ref:`rados_operations_bluestore_migration`.
- **"Unable to find OSD ...".** The directory
  ``/var/lib/ceph/osd/ceph-<id>`` exists, but ``cephadm adopt`` found no OSD
  that belongs to it, and the command changed no daemon. ``cephadm ls`` lists
  every ``ceph-<id>`` directory in ``/var/lib/ceph/osd`` as a legacy OSD,
  including an empty one that a removed OSD left behind. If ``ceph osd ls``
  does not print the ID, the directory is a leftover: remove it with
  ``rmdir``.
- **An adopted daemon is not running.** ``cephadm adopt`` starts the new
  systemd unit only if the legacy daemon was running. Start the unit that
  ``cephadm ls`` shows as the ``systemd_unit`` of the daemon with
  ``systemctl start``. To make ``cephadm adopt`` start a daemon that was
  stopped, add ``--force-start`` after ``adopt``. If the unit starts and
  stops again, read its log with ``cephadm logs --name <name>``.
- **"all mgr daemons do not support module 'cephadm'" when you enable the
  module.** A Manager, active or standby, does not have the ``cephadm``
  module. Adopt every Manager, then run ``ceph mgr module enable cephadm``
  again.
- **"Failed to connect to" or "Can't communicate with remote host" when you
  add a host.** Cephadm cannot log in to the host over SSH. Check that the
  host is reachable, that Python 3 is installed on it, and that the public
  key is installed for the SSH user on that host, then add the host again.
  See :ref:`cephadm-ssh-errors`.
- **"does not match expected hostname" when you add a host.** The name that
  you gave is not the name that the ``hostname`` command prints on that
  host. Add the host under that name. See :ref:`cephadm-fqdn`.
- **"No daemons reported" from "ceph orch ps", or an adopted daemon is
  missing from its output.** Cephadm lists only the daemons on hosts that it
  manages, and it scans a host shortly after you add it. Check that
  ``ceph orch host ls`` lists the host, then run ``ceph orch ps --refresh``.
  That command starts a new scan and prints the list from before the scan:
  wait a few seconds, then run ``ceph orch ps`` again.
- **A new MDS or RGW daemon shows a different version from the adopted
  daemons in "ceph orch ps" or "ceph versions".** Cephadm deployed the daemon
  from another container image than ``<image>``. Do not stop the legacy
  daemons, and do not remove their data directories, until the new daemons
  run the version of ``<image>``.
- **"TCP Port(s) ... already in use" in "ceph health detail" after you
  deploy the RGW daemons.** A legacy RGW daemon on that host listens on the
  port that you gave to the new service, so cephadm cannot start the new
  daemon there. Deploy the new daemons with a port that is free on those
  hosts, or place them on other hosts.
- **"stray daemon(s) not managed by cephadm" or "stray host(s) with ...
  daemon(s) not managed by cephadm" in "ceph health detail".** These
  warnings are expected during the conversion. They name each legacy daemon
  that is not yet adopted or replaced, and each host that runs Ceph daemons
  but is not yet added to cephadm. Cephadm repeats this check every 30
  minutes by default, so a warning can still name a daemon or a host for up
  to 30 minutes after you adopted, replaced, or added it. If a warning
  remains longer than that after the last step, adopt or replace the daemons
  that it names, or add the host. See
  :ref:`the health checks for stray hosts and daemons <cephadm-stray-host>`.

Next Steps
==========

- Give the adopted daemons a service specification. Until then,
  ``ceph orch ls`` shows the Monitors, Managers, and OSDs as ``<unmanaged>``,
  and cephadm does not deploy or remove their daemons. See
  :doc:`/cephadm/services/mon`, :doc:`/cephadm/services/mgr`, and
  :doc:`/cephadm/services/osd`.
- :ref:`Deploy the monitoring services <mgr-cephadm-monitoring>` with
  cephadm.
- :doc:`Upgrade the cluster with cephadm </cephadm/upgrade>` when a new
  release is available.

Additional Resources
====================

- :ref:`cephadm_deploying_new_cluster`
- :ref:`orchestrator-cli-host-management`
- :ref:`cephadm-ssh-configuration`
- :doc:`/cephadm/services/index`
- :doc:`/cephadm/operations`
- :doc:`Cephadm Troubleshooting </cephadm/troubleshooting>`

.. Needs to be an external link because doc/releases/index.rst is not in
   stable branches and we want to always use the main branch version
.. _release notes: https://docs.ceph.com/en/latest/releases/
