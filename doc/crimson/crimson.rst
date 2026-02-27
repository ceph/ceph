.. _crimson_doc:

======================
Crimson (Tech Preview)
======================

Crimson is the code name of ``crimson-osd``, which is the next generation ``ceph-osd``.
It is designed to deliver enhanced performance on fast network and storage devices by leveraging modern technologies including DPDK and SPDK.

Crimson is intended to be a drop-in replacement for the classic Object Storage Daemon (OSD),
aiming to allow seamless migration from existing ``ceph-osd`` deployments.

The second phase of the project introduces :ref:`seastore`, a complete redesign of the object storage backend built around Crimson's native architecture.
Seastore is optimized for high-performance storage devices like NVMe and may not be suitable for traditional HDDs.
Crimson will continue to support BlueStore ensuring compatibility with HDDs and slower SSDs.

See `ceph.io/en/news/crimson <https://ceph.io/en/news/crimson/>`_

Crimson is in tech-preview stage.
See :ref:`Crimson's Developer Guide <crimson_dev_doc>` for developer information.

.. highlight:: console

.. note::
   Cephadm SeaStore support is in `early stages <https://tracker.ceph.com/issues/71946>`_.

Deploying Crimson with cephadm
==============================

.. note::
   Crimson is in a tech preview stage and is **not suitable for production use**.

The Ceph CI/CD pipeline builds containers with both ``ceph-osd-crimson`` and the standard ``ceph-osd``.

Once a branch at commit <sha1> has been built and is available in
Shaman / Quay, you can deploy it using the cephadm instructions outlined
in :ref:`cephadm` with the following adaptations.

The latest `main` branch is built `daily <https://shaman.ceph.com/builds/ceph/main>`_
and the images are available in `quay <https://quay.ceph.io/repository/ceph-ci/ceph?tab=tags>`_.
We recommend using one of the latest available builds, as Crimson evolves rapidly.

Before deploying a crimson OSD ensure the :ref:`required flags <crimson-required-flags>` are set.

The cephadm :ref:`bootstrap command <cephadm_bootstrap_a_new_cluster>` can be used as is, no further 
changes are needed for crimson OSDs. You'll likely need to include the ``--allow-mismatched-release`` flag 
to use a non-release branch.

.. prompt:: bash #

   cephadm --image quay.ceph.io/ceph-ci/ceph:<sha1> --allow-mismatched-release bootstrap ...

When :ref:`deploying OSDs <cephadm-deploy-osds>`, use ``--osd-type`` 
flag to specify crimson OSDs. By default this value is set to ``classic``. To deploy a crimson OSD, 
set this flag to ``crimson``.

.. prompt:: bash #

   ceph orch apply osd --osd-type crimson ...

Alternatively, you can also set the `osd_type 
<https://docs.ceph.com/en/latest/cephadm/services/osd/#ceph.deployment.drive_group.DriveGroupSpec.osd_type>`_ 
to ``crimson`` in the :ref:`OSD Service Specification <drivegroups>` file
like so: 

.. code-block:: yaml 

   service_type: osd
   service_id: default_drive_group  
   placement:
     host_pattern: '*'              
   spec:
     data_devices:                 
       all: true  
     osd_type: crimson   # osd_type should be set to crimson                  

If the above file is named ``osd-spec.yaml``, it can be used to deploy OSDs like so: 

.. prompt:: bash #

   ceph orch apply -i /path/to/osd_spec.yml

.. _crimson-cpu-allocation:

Enabling io_uring
=================

Seastar backend can beneift significantly from Linux's io_uring interface, providing lower latency and higher throughput.
io_uring is the defualt reactor backend (see `crimson_reactor_backend` option).
On some conservative distributions, io_uring may be disabled, preventing Crimson from using it.
If this configuration change is acceptable in your environment, you may enable io_uring support by running:

.. prompt:: bash #
    sudo sysctl -w kernel.io_uring_disabled=0


Crimson CPU allocation
======================

.. note::

   #. Allocation options **cannot** be changed after deployment.
   #. :ref:`vstart.sh <dev_crimson_vstart>` sets these options using the ``--crimson-smp`` flag.

The ``crimson_cpu_num`` parameter defines the number of CPUs used to serve Seastar reactors.
Each reactor is expected to run on a dedicated CPU core.

This parameter **does not have a default value**.
Admins must configure it at the OSD level based on system resources and cluster requirements **before** deploying the OSDs.

We recommend setting a value for ``crimson_cpu_num`` that is less than the host's
number of CPU cores (``nproc``) divided by the **number of OSDs on that host**.

For example, for deploying a node with eight CPU cores per OSD:

.. prompt:: bash #

   ceph config set osd crimson_cpu_num 8

Note that ``crimson_cpu_num`` does **not** pin threads to specific CPU cores.
To explicitly assign CPU cores to Crimson OSDs, use the ``crimson_cpu_set`` parameter.
This enables CPU pinning, which *may* improve performance.
However, using this option requires manually setting the CPU set for each OSD,
and is generally less recommended due to its complexity.

.. _crimson-required-flags:

Crimson Required Flags
======================

After starting your cluster, prior to deploying OSDs (in cephadm terms, 
after bootstrap is done and hosts are added), enable Crimson by setting the following flags: 

.. prompt:: bash #

   ceph config set global 'enable_experimental_unrecoverable_data_corrupting_features' crimson
   ceph osd set-allow-crimson --yes-i-really-mean-it
   ceph config set mon osd_pool_default_crimson true
   ceph config set osd crimson_cpu_num <SUITABLE_INT>

The first command enables the ``crimson`` experimental feature.  

The second enables the ``allow_crimson`` OSDMap flag.  The monitor will
not allow ``crimson-osd`` to boot without that flag.

The third causes pools to be created by default with the ``crimson`` flag.
Crimson pools are restricted to operations supported by Crimson.
``crimson-osd`` won't instantiate PGs from non-Crimson pools.

Lastly, ensure that `Crimson CPU allocation`_ flags were set appropriately.

.. _crimson-bakends:

Object Store Backends
=====================

``crimson-osd`` supports two categories of object store backends: **native** and **non-native**.

Native Backends
---------------

Native backends perform I/O operations using the **Seastar reactor**. These are tightly integrated with the Seastar framework and follow its design principles:

.. describe:: seastore

   SeaStore is the primary native object store for Crimson OSD, though it is not the default as the support is in early stages. 
   It is built with the Seastar framework and adheres to its asynchronous, shard-based architecture.

.. note::
   The Orchastrator's ``apply osd --method`` command does not currently support deploying
   Crimson OSDs with SeaStore directly on the physical device with ``--method raw``.
   Use the default ``lvm`` method instead.

   When :ref:`deploying OSDs <cephadm-deploy-osds>`, use the ``--objectstore`` flag to specify the object store type. 
   The default value is ``bluestore``. To deploy a Crimson OSD with SeaStore, set this flag to ``seastore``.

   .. prompt:: bash #

      ceph orch apply osd --osd-type crimson --objectstore seastore ...

   Alternatively, you can also set the ``objectstore`` to ``seastore`` in the :ref:`OSD Service Specification <drivegroups>` file
   like so: 

   .. code-block:: yaml 

      service_type: osd
      service_id: default_drive_group  
      placement:
        host_pattern: '*'              
      spec:
        data_devices:                 
          all: true  
        osd_type: crimson   
        objectstore: seastore # objectstore should be set to seastore      

.. describe:: cyanstore

   CyanStore is inspired by ``memstore`` from the classic OSD, offering a lightweight, in-memory object store model.
   CyanStore **does not store data** and should be used only for measuring OSD overhead, without the cost of actually storing data.

Non-Native Backends
-------------------

Non-native backends operate through a **thread pool proxy**, which interfaces with object stores running in **alien threads**—worker threads not managed by Seastar.
These backends allow Crimson to interact with legacy or external object store implementations:

.. describe:: bluestore

   The default object store. It provides robust, production-grade storage capabilities.

   The ``crimson_bluestore_num_threads`` option needs to be set according to the CPU set available.
   This defines the number of threads dedicated to serving the BlueStore ObjectStore on each OSD.

   If ``crimson_cpu_num`` is used from `Crimson CPU allocation`_,
   The counterpart ``crimson_bluestore_cpu_set`` should also be used accordingly to
   allow the two sets to be mutually exclusive.

.. describe:: memstore

   An in-memory object store backend, primarily used for testing and development purposes.

Metrics and Tracing
===================

Crimson offers three ways to report stats and metrics.

PG stats reported to the Manager
--------------------------------

Crimson collects the per-PG, per-pool, and per-OSD stats in a `MPGStats`
message which is sent to the Ceph Managers. Manager modules can query
them using the ``MgrModule.get()`` method.

Asock command
-------------

An admin socket command is offered for dumping metrics::

.. prompt:: bash #

  $ ceph tell osd.0 dump_metrics
  $ ceph tell osd.0 dump_metrics reactor_utilization

Here `reactor_utilization` is an optional string allowing us to filter
the dumped metrics by prefix.

Prometheus text protocol
------------------------

The listening port and address can be configured using the command line options of
``--prometheus_port``
see `Prometheus`_ for more details.

.. _Prometheus: https://github.com/scylladb/seastar/blob/master/doc/prometheus.md
