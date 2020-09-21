==============
Upgrading Ceph
==============

Cephadm is capable of safely upgrading Ceph from one bugfix release to
another.  For example, you can upgrade from v15.2.0 (the first Octopus
release) to the next point release v15.2.1.

The automated upgrade process follows Ceph best practices.  For example:

* The upgrade order starts with managers, monitors, then other daemons.
* Each daemon is restarted only after Ceph indicates that the cluster
  will remain available.

Keep in mind that the Ceph cluster health status is likely to switch to
`HEALTH_WARNING` during the upgrade.


Starting the upgrade
====================

Before you start, you should verify that all hosts are currently online
and your cluster is healthy.

::

  # ceph -s

To upgrade (or downgrade) to a specific release::

  # ceph orch upgrade start --ceph-version <version>

For example, to upgrade to v15.2.1::

  # ceph orch upgrade start --ceph-version 15.2.1


Monitoring the upgrade
======================

Determine whether an upgrade is in process and what version the cluster is
upgrading to with::

  # ceph orch upgrade status

While the upgrade is underway, you will see a progress bar in the ceph
status output.  For example::

  # ceph -s
  [...]
    progress:
      Upgrade to docker.io/ceph/ceph:v15.2.1 (00h 20m 12s)
        [=======.....................] (time remaining: 01h 43m 31s)

You can also watch the cephadm log with::

  # ceph -W cephadm


Canceling an upgrade
====================

You can stop the upgrade process at any time with::

  # ceph orch upgrade stop


Potential problems
==================

There are a few health alerts that can arise during the upgrade process.

UPGRADE_NO_STANDBY_MGR
----------------------

Ceph requires an active and standby manager daemon in order to proceed, but
there is currently no standby.

You can ensure that Cephadm is configured to run 2 (or more) managers with::

  # ceph orch apply mgr 2  # or more

You can check the status of existing mgr daemons with::

  # ceph orch ps --daemon-type mgr

If an existing mgr daemon has stopped, you can try restarting it with::

  # ceph orch daemon restart <name>

UPGRADE_FAILED_PULL
-------------------

Ceph was unable to pull the container image for the target version.
This can happen if you specify an version or container image that does
not exist (e.g., 1.2.3), or if the container registry is not reachable from
one or more hosts in the cluster.

You can cancel the existing upgrade and specify a different target version with::

  # ceph orch upgrade stop
  # ceph orch upgrade start --ceph-version <version>


Using customized container images
=================================

For most users, simplify specifying the Ceph version is sufficient.
Cephadm will locate the specific Ceph container image to use by
combining the ``container_image_base`` configuration option (default:
``docker.io/ceph/ceph``) with a tag of ``vX.Y.Z``.

You can also upgrade to an arbitrary container image.  For example, to
upgrade to a development build::

  # ceph orch upgrade start --image quay.io/ceph-ci/ceph:recent-git-branch-name

For more information about available container images, see :ref:`containers`.
