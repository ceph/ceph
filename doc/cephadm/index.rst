.. _cephadm:

=======
Cephadm
=======

Cephadm deploys and manages a Ceph cluster by connection to hosts from the
manager daemon via SSH to add, remove, or update Ceph daemon containers.  It
does not rely on external configuration or orchestration tools like Ansible,
Rook, or Salt.

Cephadm manages the full lifecycle of a Ceph cluster.  It starts
by bootstrapping a tiny Ceph cluster on a single node (one monitor and
one manager) and then uses the orchestration interface ("day 2"
commands) to expand the cluster to include all hosts and to provision
all Ceph daemons and services.  This can be performed via the Ceph
command-line interface (CLI) or dashboard (GUI).

Cephadm is new in the Octopus v15.2.0 release and does not support older
versions of Ceph.

.. note::

   Cephadm is new.  Please read about :ref:`cephadm-stability` before
   using cephadm to deploy a production system.

.. toctree::
    :maxdepth: 1

    stability
    install
    adoption
    upgrade
    Cephadm operations <operations>
    Cephadm monitoring <monitoring>
    Cephadm CLI <../mgr/orchestrator>
    Client Setup <client-setup>
    DriveGroups <drivegroups>
    troubleshooting
    concepts