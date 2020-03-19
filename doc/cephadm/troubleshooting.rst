
Troubleshooting
===============

Sometimes there is a need to investigate why a cephadm command failed or why
a specific service no longer runs properly.

As cephadm deploys daemons as containers, troubleshooting daemons is slightly
different. Here are a few tools and commands to help investigating issues.

Pausing or disabling cephadm
----------------------------

If thigns go wrong and cephadm is doing something you don't like, you can
pause most background activity with::

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

You can monitor the cephadm log in realtime with::

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
