.. _cephadm:

=======
Cephadm
=======

Cephadm deploys and manages a Ceph cluster by connection to hosts from the
manager daemon via SSH to add, remove, or update Ceph daemon containers.  It
does not rely on external configuration or orchestration tools like Ansible,
Rook, or Salt.

Cephadm starts by bootstrapping a tiny Ceph cluster on a single node
(one monitor and one manager) and then using the orchestration
interface (so-called "day 2" commands) to expand the cluster to include
all hosts and to provision all Ceph daemons and services, either via the Ceph
command-line interface (CLI) or dashboard (GUI).

Cephadm is new in the Octopus v15.2.0 release and does not support older
versions of Ceph.

.. toctree::
    :maxdepth: 2

    install
    adoption
    upgrade
    Cephadm operations <operations>
    Cephadm monitoring <monitoring>
    Cephadm CLI <../mgr/orchestrator>
    DriveGroups <drivegroups>
    troubleshooting
