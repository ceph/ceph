.. _cephadm:

=======
Cephadm
=======

``cephadm`` deploys and manages a Ceph cluster. It does this by connecting the
manager daemon to hosts via SSH. The manager daemon is able to add, remove, and
update Ceph containers. ``cephadm`` does not rely on external configuration
tools such as Ansible, Rook, and Salt.

``cephadm`` manages the full lifecycle of a Ceph cluster. This lifecycle
starts with the bootstrapping process, when ``cephadm`` creates a tiny
Ceph cluster on a single node. This cluster consists of one monitor and
one manager. ``cephadm`` then uses the orchestration interface ("day 2"
commands) to expand the cluster, adding all hosts and provisioning all
Ceph daemons and services. Management of this lifecycle can be performed
either via the Ceph command-line interface (CLI) or via the dashboard (GUI).

``cephadm`` is new in Ceph release v15.2.0 (Octopus) and does not support older
versions of Ceph.

.. note::

   Cephadm is new.  Please read about :ref:`cephadm-stability` before
   using cephadm to deploy a production system.

.. toctree::
    :maxdepth: 1

    compatibility
    install
    adoption
    host-management
    mon
    osd
    rgw
    mds
    nfs
    iscsi
    custom-container
    monitoring
    service-management
    upgrade
    Cephadm operations <operations>
    Client Setup <client-setup>
    troubleshooting
    Cephadm Feature Planning <../dev/cephadm/index>
