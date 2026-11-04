.. _cephfs-mds-daemon-commands:

===================
MDS Daemon Commands
===================

Each MDS daemon accepts commands that report its state and change its
behavior. Most of these commands have no equivalent among the ``ceph fs`` and
``ceph mds`` commands. To send one to a running MDS, run a command of the
following form:

.. prompt:: bash #

   ceph tell mds.<name> <command>

``<name>`` is the name of an MDS daemon, ``<fs_name>:<rank>`` for the MDS that
holds a rank of a file system, or ``*`` for every MDS. For example,
``ceph tell mds.cephfs:0 scrub status`` reaches rank 0 of the file system
``cephfs``.

The same commands can be sent through the admin socket of the MDS on the host
where it runs, with ``ceph daemon mds.<name> <command>``. This works even when
the MDS or the Monitors can't be reached over the network. See
:ref:`rados-monitoring-using-admin-socket`, which also describes the commands
that every Ceph daemon accepts, such as ``config show`` and ``perf dump``.

To list the commands that a running MDS accepts, run a command of the
following form:

.. prompt:: bash #

   ceph tell mds.<name> help

A standby MDS holds no rank, so of the commands on this page it accepts only
``status``, ``respawn``, ``exit``, ``heap``, ``cpu_profiler``, and ``lockup``.
The others return the error ``mds_not_active``.

.. warning::

   Some of these commands change the state of the MDS or of client sessions,
   and some hold the MDS lock while they run, which stalls every metadata
   operation on that MDS. The descriptions below say which. The commands in
   :ref:`cephfs-mds-daemon-commands-internal` are for developers and for
   testing.

Status and Cache Contents
=========================

``status``
    Reports the rank of the MDS, its state and wanted state, its MDS map and
    OSD map epochs, its OSD map epoch barrier, its uptime, and information
    about its host.

``cache status``
    Reports how much memory the MDS cache uses.

``get subtrees``
    Lists the subtrees in the MDS cache, with the rank that is authoritative
    for each one and any export pin. See :ref:`cephfs-multimds`.

``dump loads [<depth>]``
    Reports the load that the MDS balancer has measured for each directory
    fragment in the cache, down to ``<depth>`` levels if ``<depth>`` is given.

``dump_export_states``
    Lists the exports of subtrees to other ranks that are in progress, with the
    state of each.

``dump snaps [--server]``
    Lists the snapshots that the MDS knows about. With ``--server``, sent to
    the rank that serves the snapshot table (rank 0), it dumps the snapshot
    table itself.

``openfiles ls``
    Lists the files that clients have open on this rank, with the path and
    the client capabilities of each.

``dump stray``
    Lists the stray entries of this rank: files that have been unlinked but
    not yet purged. See :ref:`mds-scrub`.

``dump inode <number>``
    Dumps an inode from the cache. Give the inode number in decimal or, with a
    ``0x`` prefix, in hexadecimal. The command fails if the inode isn't in the
    cache.

``dump dir <path> [--dentry-dump]``
    Dumps each fragment of a directory that is in the cache, and with
    ``--dentry-dump`` the dentries of each fragment as well. Fragments that
    aren't in the cache are listed as such.

``dump cache [<path>] [<timeout>]``
    Dumps the whole MDS cache. Without ``<path>``, the dump is returned to the
    caller, and the MDS refuses if its cache is larger than
    ``mds_dump_cache_threshold_formatter`` (1 GiB by default). With
    ``<path>``, the MDS writes the dump to that file on its host, and refuses
    if the file already exists. In a cluster that cephadm manages, the path is
    inside the MDS container. The MDS holds its lock during the dump and stops
    dumping after ``<timeout>`` seconds. The default timeout is half of
    ``mds_beacon_interval``, and a longer timeout is reduced to
    ``mds_beacon_interval``.

``dump tree <root> [<depth>] [<path>]``
    Dumps the inodes in the cache under ``<root>``, down to ``<depth>`` levels
    if ``<depth>`` is given. With ``<path>``, the MDS writes the dump as JSON
    to that file on its host. Unlike ``dump cache``, ``dump tree`` has no size
    threshold and no timeout, and it holds the MDS lock until it finishes. On a
    large cache, it can stall the MDS long enough for the Monitors to replace
    the MDS with a standby. Use a small ``<depth>``.

Requests
========

The request commands need :confval:`mds_enable_op_tracker`, which is enabled
by default. A request ID has the form ``client.<id>:<tid>``, for example
``client.4567:89``, and appears in the output of ``dump_ops_in_flight``.

``dump_ops_in_flight``
    Lists the requests that the MDS is processing, with the events recorded
    for each one so far.

``ops [locks] [<path>]``
    Lists the requests that the MDS is processing, like
    ``dump_ops_in_flight``. With ``locks``, it also reports the locks that each
    request holds or waits for, and it holds the MDS lock to do so. With
    ``<path>``, the MDS writes the list as JSON to that file on its host.

``op get [locks] <id>``
    Reports one request that the MDS is processing.

``op kill <id>``
    Kills a request that the MDS is processing. A request that has already
    started on other ranks might not be killed.

``dump_blocked_ops``
    Lists the requests that have been in progress for longer than
    :confval:`mds_op_complaint_time` (30 seconds by default).

``dump_blocked_ops_count``
    Reports only the number of such requests.

``dump_historic_ops``
    Lists recently completed requests: at most
    :confval:`mds_op_history_size` (20 by default) from the last
    :confval:`mds_op_history_duration` seconds (600 by default).

``dump_historic_ops_by_duration``
    Lists the same requests, longest first.

Client Sessions
===============

``session ls [--cap-dump] [<filter>...]``
    Lists client sessions, with metadata such as the host name, mount point,
    and Ceph version of each client. A filter has one of the forms
    ``id=<client_id>``, ``auth_name=<name>``, ``state=<state>``,
    ``reconnecting=<true|false>``, or ``client_metadata.<key>=<value>``. With
    ``--cap-dump``, the list includes the capabilities that each session
    holds. ``client ls`` is an alias of this command.

``session evict <filter>...``
    Evicts the sessions that match the filters. Unless
    :confval:`mds_session_blocklist_on_evict` is disabled, the MDS also
    blocklists the evicted clients. ``client evict`` is an alias of this
    command. See :doc:`/cephfs/eviction`.

``session kill <client_id>``
    Evicts the session with this client ID. Unlike ``session evict
    id=<client_id>``, it returns an error when no session has that ID.

``session config <client_id> <option> [<value>]``
    Sets an option of one session, or removes it if ``<value>`` is not given.
    The only option is ``timeout``: the number of seconds that the client can
    go without renewing its capabilities before the MDS marks its session
    stale, in place of the file system's ``session_timeout``. A value of ``0``
    means that the session never goes stale. Clients that reclaim their
    sessions after a restart, such as NFS-Ganesha, set a timeout themselves
    when they open their sessions. After a failover, a session with a ``timeout`` keeps the rank
    from going active until the client reclaims the session or is evicted.
    ``client config`` is an alias of this command.

Scrub and Damage
================

Send the commands in this section to rank 0, except ``scrub status``,
``damage ls``, and ``damage rm``, which any rank accepts. See
:ref:`mds-scrub`.

``scrub start <path> [<scrubops>...] [<tag>]``
    Starts a scrub of ``<path>``. ``<scrubops>`` is any of ``force``,
    ``recursive``, ``repair``, and, when ``<path>`` is ``/``,
    ``scrub_mdsdir``.

``scrub status``
    Reports the state of scrubs.

``scrub pause``, ``scrub resume``, ``scrub abort``
    Pause, resume, or abort the scrubs in progress.

``scrub purge_status <tag>``
    Removes the recorded results of completed scrubs that have ``<tag>``, or
    of all scrubs if ``<tag>`` is ``all``.

``tag path <path> <tag>``
    Scrubs ``<path>`` recursively and records ``<tag>`` in an attribute of the
    object that holds the backtrace of each inode that it visits. A scrub that
    is started with a tag records it in the same way. The ``--filter-tag <tag>`` option of
    ``cephfs-data-scan`` then skips the objects that have the tag.

``damage ls``
    Lists the metadata damage that the MDS has detected. See ``MDS_DAMAGE`` in
    :ref:`cephfs-health-messages`.

``damage rm <damage_id>``
    Removes an entry from the damage table.

Journal and Cache
=================

``flush journal``
    Writes the metadata updates in the journal to the metadata pool and trims
    the journal. See :ref:`cephfs-disaster-recovery`.

``flush_path <path>``
    Writes the metadata of ``<path>`` to the metadata pool, together with the
    metadata of its fragments if ``<path>`` is a directory.

``cache drop [<timeout>]``
    Asks clients to release their capabilities, flushes the journal, and trims
    the cache, and then reports how many capabilities were recalled, how many
    dentries were trimmed, and the cache status. With ``<timeout>``, the MDS
    stops asking clients after that many seconds. Clients then have to fetch
    metadata from the MDS again, so metadata operations are slower for a
    while afterward.

``osdmap barrier <target_epoch>``
    Sets the OSD map epoch barrier of the MDS to ``<target_epoch>``, and
    waits until the MDS has that OSD map. See
    :ref:`background_blocklisting_and_osd_epoch_barrier`.

.. _cephfs-mds-daemon-commands-dirfrags:

Subtrees and Directory Fragments
================================

``export dir <path> <rank>``
    Migrates the subtree at ``<path>`` from this rank to ``<rank>``. This
    rank must be authoritative for the directory, and ``<rank>`` must be up.
    Export pins and the balancer can move the subtree again. To keep a
    subtree on a rank, use an export pin. See :ref:`cephfs-pinning`.

``dirfrag ls <path>``
    Lists the fragments of a directory that is in the cache, in the form
    ``<value>/<bits>``, where ``<value>`` is hexadecimal.

``dirfrag split <path> <frag> <bits>``
    Splits fragment ``<frag>`` of a directory into 2\ :sup:`<bits>`
    fragments. The fragment must be in the cache.

``dirfrag merge <path> <frag>``
    Merges the fragments under ``<frag>`` into one fragment.

The MDS normally splits and merges fragments itself. See
:doc:`/cephfs/dirfrags`.

Daemon Control
==============

``force_readonly``
    Puts the MDS into read-only mode. The MDS returns ``EROFS`` to every client
    operation that would change metadata, and raises the
    ``MDS_HEALTH_READ_ONLY`` health warning. The MDS stays read-only until it
    restarts, for example after ``ceph mds fail <name>``.

``respawn``
    Restarts the MDS process. A standby takes over the rank of the MDS, and
    that standby can be the restarted MDS itself.

``exit``
    Stops the MDS process. A standby takes over the rank of the MDS, if there
    is a standby.

Profiling
=========

``heap <heapcmd> [<value>]``
    Reports and controls the memory use of an MDS built with tcmalloc. See
    :doc:`/rados/troubleshooting/memory-profiling`.

``cpu_profiler status|flush``
    Reports the state of the CPU profiler, or flushes its data. See
    :doc:`/rados/troubleshooting/cpu-profiling`.

.. _cephfs-mds-daemon-commands-internal:

Internal and Developer Commands
===============================

``lockup <millisecs>``
    Holds the MDS lock for ``<millisecs>`` milliseconds, which stalls the MDS.
    Use it only for testing.

``lock path <path> ...``
    Takes locks on ``<path>``. Use it only for testing.

``quiesce path <path> [--await]``, ``quiesce db ...``
    Quiesce a subtree, and query or update the quiesce database. The
    ``ceph fs quiesce`` command uses ``quiesce db``. Run ``ceph fs quiesce``
    instead. See :ref:`fs-volumes-and-subvolumes` and
    :doc:`/dev/mds_internals/quiesce`.

``scrub_path``
    The MDS still lists this older name, but it no longer runs it. Use
    ``scrub start``.

Beginning with the Vampire release, the MDS also accepts ``dump phase times``,
``dump qos``, ``qos get``, ``qos set``, ``qos rm``, ``quarantine enable``, and
``quarantine disable``.

CephFS Client Commands
======================

``ceph-fuse`` and other clients that use ``libcephfs`` have admin socket
commands as well. The kernel client has none. ``ceph-fuse`` creates its admin
socket by default, at a path of the form
``/var/run/ceph/<cluster>-client.<id>.<pid>.<number>.asok``. Other
applications that use ``libcephfs`` create one only if the client's
``admin_socket`` option is set. To send a command to a client, run a command of
the following form on the client host:

.. prompt:: bash #

   ceph daemon <path_to_socket> <command>

``status``
    Reports the client's ID and address, the file system it has mounted, its
    MDS map and OSD map epochs, the number of inodes and dentries in its
    cache, and whether it has been blocklisted.

``mds_sessions [--cap-dump]``
    Lists the client's sessions with MDSes, and with ``--cap-dump`` the
    capabilities that each session holds.

``mds_requests``
    Lists the client's requests to MDSes that are in progress.

``dump_cache``
    Dumps the client's metadata cache.
