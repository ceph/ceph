===========================
Getting Started with CephFS
===========================

For most deployments of Ceph, setting up a CephFS file system is as simple as:

.. code:: bash

    ceph fs volume create <fs name>

The Ceph `Orchestrator`_  will automatically create and configure MDS for
your file system if the back-end deployment technology supports it (see
`Orchestrator deployment table`_). Otherwise, please `deploy MDS manually
as needed`_.

Finally, to mount CephFS on your client nodes, see `Mount CephFS:
Prerequisites`_ page. Additionally, a command-line shell utility is available
for interactive access or scripting via the `cephfs-shell`_.

.. _cephfs-administration: administration
.. _Orchestrator: ../mgr/orchestrator
.. _deploy MDS manually as needed: add-remove-mds
.. _Orchestrator deployment table: ../mgr/orchestrator/#current-implementation-status
.. _Mount CephFS\: Prerequisites: mount-prerequisites
.. _cephfs-shell: cephfs-shell

