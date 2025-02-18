.. _cephadm:

=======
Cephadm
=======

``cephadm`` is a utility that is used to manage a Ceph cluster. 

Here is a list of some of the things that ``cephadm`` can do:

- ``cephadm`` can add a Ceph container to the cluster.
- ``cephadm`` can remove a Ceph container from the cluster.
- ``cephadm`` can update Ceph containers.

``cephadm`` does not rely on external configuration tools like Ansible, Rook,
or Salt. However, those external configuration tools can be used to automate
operations not performed by cephadm itself. To learn more about these external
configuration tools, visit their pages:

 * https://github.com/ceph/cephadm-ansible
 * https://rook.io/docs/rook/v1.10/Getting-Started/intro/
 * https://github.com/ceph/ceph-salt

``cephadm`` manages the full lifecycle of a Ceph cluster. This lifecycle starts
with the bootstrapping process, when ``cephadm`` creates a tiny Ceph cluster on
a single node. This cluster consists of one monitor and one manager.
``cephadm`` then uses the orchestration interface to expand the cluster, adding
hosts and provisioning Ceph daemons and services. Management of this lifecycle
can be performed either via the Ceph command-line interface (CLI) or via the
dashboard (GUI).

To use ``cephadm`` to get started with Ceph, follow the instructions in
:ref:`cephadm_deploying_new_cluster`.

``cephadm`` was introduced in Ceph release v15.2.0 (Octopus) and does not
support older versions of Ceph.

.. toctree::
    :maxdepth: 2

    compatibility
    install
    adoption
    host-management
    Service Management <services/index>
    certmgr
    upgrade
    Cephadm operations <operations>
    Client Setup <client-setup>
    troubleshooting
    Cephadm Feature Planning <../dev/cephadm/index>
