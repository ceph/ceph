===========
MDS Service
===========


.. _orchestrator-cli-cephfs:

Deploy CephFS
=============

One or more MDS daemons is required to use the :term:`CephFS` file system.
These are created automatically if the newer ``ceph fs volume``
interface is used to create a new file system. For more information,
see :ref:`fs-volumes-and-subvolumes`.

For example:

.. prompt:: bash #

  ceph fs volume create <fs_name> --placement="<placement spec>"

where ``fs_name`` is the name of the CephFS and ``placement`` is a
:ref:`orchestrator-cli-placement-spec`. For example, to place
MDS daemons for the new ``foo`` volume on hosts labeled with ``mds``:

.. prompt:: bash #

  ceph fs volume create foo --placement="label:mds"

You can also update the placement after-the-fact via:

.. prompt:: bash #

  ceph orch apply mds foo 'mds-[012]'

For manually deploying MDS daemons, use this specification:

.. code-block:: yaml

    service_type: mds
    service_id: fs_name
    placement:
      count: 3
      label: mds


The specification can then be applied using:

.. prompt:: bash #

   ceph orch apply -i mds.yaml

See :ref:`orchestrator-cli-stateless-services` for manually deploying
MDS daemons on the CLI.

Further Reading
===============

* :ref:`ceph-file-system`


