===========
NFS Service
===========

.. note:: Only the NFSv4 protocol is supported.

.. _deploy-cephadm-nfs-ganesha:

Deploying NFS ganesha
=====================

Cephadm deploys NFS Ganesha using a pre-defined RADOS *pool*
and optional *namespace*

To deploy a NFS Ganesha gateway, run the following command:

.. prompt:: bash #

    ceph orch apply nfs *<svc_id>* *<pool>* *<namespace>* --placement="*<num-daemons>* [*<host1>* ...]"

For example, to deploy NFS with a service id of *foo*, that will use the RADOS
pool *nfs-ganesha* and namespace *nfs-ns*:

.. prompt:: bash #

   ceph orch apply nfs foo nfs-ganesha nfs-ns

.. note::
   Create the *nfs-ganesha* pool first if it doesn't exist.

See :ref:`orchestrator-cli-placement-spec` for details of the placement specification.

Service Specification
=====================

Alternatively, an NFS service can also be applied using a YAML specification. 

A service of type ``nfs`` requires a pool name and may contain
an optional namespace:

.. code-block:: yaml

    service_type: nfs
    service_id: mynfs
    placement:
      hosts:
        - host1
        - host2
    spec:
      pool: mypool
      namespace: mynamespace

where ``pool`` is a RADOS pool where NFS client recovery data is stored
and ``namespace`` is a RADOS namespace where NFS client recovery
data is stored in the pool.

The specification can then be applied using:

.. prompt:: bash #

   ceph orch apply -i nfs.yaml
