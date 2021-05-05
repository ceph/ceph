===========
NFS Service
===========

.. note:: Only the NFSv4 protocol is supported.

.. _deploy-cephadm-nfs-ganesha:

Deploying NFS ganesha
=====================

Cephadm deploys NFS Ganesha using a pre-defined RADOS *pool*
and optional *namespace*.

To deploy a NFS Ganesha gateway, run the following command:

.. prompt:: bash #

    ceph orch apply nfs *<svc_id>* [--port *<port>*] [--placement ...]

For example, to deploy NFS with a service id of *foo* on the default
port 2049 with the default placement of a single daemon:

.. prompt:: bash #

   ceph orch apply nfs foo

See :ref:`orchestrator-cli-placement-spec` for the details of the placement
specification.

Service Specification
=====================

Alternatively, an NFS service can be applied using a YAML specification. 

.. code-block:: yaml

    service_type: nfs
    service_id: mynfs
    placement:
      hosts:
        - host1
        - host2
    spec:
      port: 12345

In this example, we run the server on the non-default ``port`` of
12345 (instead of the default 2049) on ``host1`` and ``host2``.

The specification can then be applied by running the following command:

.. prompt:: bash #

   ceph orch apply -i nfs.yaml
