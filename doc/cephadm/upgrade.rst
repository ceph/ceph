==============
Upgrading Ceph
==============

Cephadm can safely upgrade Ceph from one bugfix release to the next.  For
example, you can upgrade from v15.2.0 (the first Octopus release) to the next
point release, v15.2.1.

The automated upgrade process follows Ceph best practices.  For example:

* The upgrade order starts with managers, monitors, then other daemons.
* Each daemon is restarted only after Ceph indicates that the cluster
  will remain available.

.. note::

   The Ceph cluster health status is likely to switch to
   ``HEALTH_WARNING`` during the upgrade.

.. note:: 

   In case a host of the cluster is offline, the upgrade is paused.


Starting the upgrade
====================

Before you use cephadm to upgrade Ceph, verify that all hosts are currently online and that your cluster is healthy by running the following command:

.. prompt:: bash #

   ceph -s

To upgrade (or downgrade) to a specific release, run the following command:

.. prompt:: bash #

  ceph orch upgrade start --ceph-version <version>

For example, to upgrade to v16.2.6, run the following command:

.. prompt:: bash #

  ceph orch upgrade start --ceph-version 16.2.6

.. note::

    From version v16.2.6 the Docker Hub registry is no longer used, so if you use Docker you have to point it to the image in the quay.io registry:

.. prompt:: bash #

  ceph orch upgrade start --image quay.io/ceph/ceph:v16.2.6


Monitoring the upgrade
======================

Determine (1) whether an upgrade is in progress and (2) which version the
cluster is upgrading to by running the following command:

.. prompt:: bash #

  ceph orch upgrade status

Watching the progress bar during a Ceph upgrade
-----------------------------------------------

During the upgrade, a progress bar is visible in the ceph status output. It
looks like this:

.. code-block:: console

  # ceph -s

  [...]
    progress:
      Upgrade to docker.io/ceph/ceph:v15.2.1 (00h 20m 12s)
        [=======.....................] (time remaining: 01h 43m 31s)

Watching the cephadm log during an upgrade
------------------------------------------

Watch the cephadm log by running the following command:

.. prompt:: bash #

  ceph -W cephadm


Canceling an upgrade
====================

You can stop the upgrade process at any time by running the following command:

.. prompt:: bash #

  ceph orch upgrade stop


Potential problems
==================

There are a few health alerts that can arise during the upgrade process.

UPGRADE_NO_STANDBY_MGR
----------------------

This alert (``UPGRADE_NO_STANDBY_MGR``) means that Ceph does not detect an
active standby manager daemon. In order to proceed with the upgrade, Ceph
requires an active standby manager daemon (which you can think of in this
context as "a second manager").

You can ensure that Cephadm is configured to run 2 (or more) managers by
running the following command:

.. prompt:: bash #

  ceph orch apply mgr 2  # or more

You can check the status of existing mgr daemons by running the following
command:

.. prompt:: bash #

  ceph orch ps --daemon-type mgr

If an existing mgr daemon has stopped, you can try to restart it by running the
following command: 

.. prompt:: bash #

  ceph orch daemon restart <name>

UPGRADE_FAILED_PULL
-------------------

This alert (``UPGRADE_FAILED_PULL``) means that Ceph was unable to pull the
container image for the target version. This can happen if you specify a
version or container image that does not exist (e.g. "1.2.3"), or if the
container registry can not be reached by one or more hosts in the cluster.

To cancel the existing upgrade and to specify a different target version, run
the following commands: 

.. prompt:: bash #

  ceph orch upgrade stop
  ceph orch upgrade start --ceph-version <version>


Using customized container images
=================================

For most users, upgrading requires nothing more complicated than specifying the
Ceph version number to upgrade to.  In such cases, cephadm locates the specific
Ceph container image to use by combining the ``container_image_base``
configuration option (default: ``docker.io/ceph/ceph``) with a tag of
``vX.Y.Z``.

But it is possible to upgrade to an arbitrary container image, if that's what
you need. For example, the following command upgrades to a development build:

.. prompt:: bash #

  ceph orch upgrade start --image quay.io/ceph-ci/ceph:recent-git-branch-name

For more information about available container images, see :ref:`containers`.
