.. _deploy-cephadm-nvmeof:

===============
NVMe-oF Service
===============

The NVMe-oF gateway exports RBD images to clients over NVMe/TCP. Cephadm
deploys and manages the gateway daemons as the ``nvmeof`` service. To deploy
them, run a command of the following form:

.. prompt:: bash #

   ceph orch apply nvmeof --group <group_name> --placement="<host1> <host2>"

The ``--group`` option is required. The ``--pool`` option defaults to
``.nvmeof``, a pool that the ``nvmeof`` Manager module creates. See
:ref:`mgr-nvmeof`.

For requirements, gateway configuration, and initiator setup, see
:ref:`ceph-nvmeof`.
