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

Deploying Crimson with cephadm
==============================

.. note::
   Cephadm SeaStore support is in `early stages <https://tracker.ceph.com/issues/71946>`_.

The Ceph CI/CD pipeline builds containers with ``crimson-osd`` replacing the standard ``ceph-osd``.

Once a branch at commit <sha1> has been built and is available in
Shaman / Quay, you can deploy it using the cephadm instructions outlined
in :ref:`cephadm` with the following adaptations.

The latest `main` branch is built `daily <https://shaman.ceph.com/builds/ceph/main>`_
and the images are available in `quay <https://quay.ceph.io/repository/ceph-ci/ceph?tab=tags>`_
(filter ``crimson-release``).
We recommend using one of the latest available builds, as Crimson evolves rapidly.

Use the ``--image`` flag to specify a Crimson build:

.. prompt:: bash #

   cephadm --image quay.ceph.io/ceph-ci/ceph:<sha1>-crimson-release --allow-mismatched-release bootstrap ...


.. note::
   Crimson builds are available in two variants: ``crimson-debug`` and ``crimson-release``.
   For testing purposes the `release` variant should be used.
   The `debug` variant is intended primarily for development.

You'll likely need to include the ``--allow-mismatched-release`` flag to use a non-release branch.

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

Crimson Requried Flags
======================

.. note::
   Crimson is in a tech preview stage and is **not suitable for production use**.

After starting your cluster, prior to deploying OSDs, you'll need to configure the
`Crimson CPU allocation`_ and enable Crimson to
direct the default pools to be created as Crimson pools.  You can proceed by running the following after you have a running cluster:

.. prompt:: bash #

   ceph config set global 'enable_experimental_unrecoverable_data_corrupting_features' crimson
   ceph osd set-allow-crimson --yes-i-really-mean-it
   ceph config set mon osd_pool_default_crimson true

The first command enables the ``crimson`` experimental feature.  

The second enables the ``allow_crimson`` OSDMap flag.  The monitor will
not allow ``crimson-osd`` to boot without that flag.

The last causes pools to be created by default with the ``crimson`` flag.
Crimson pools are restricted to operations supported by Crimson.
``crimson-osd`` won't instantiate PGs from non-Crimson pools.

.. _crimson-bakends:

Object Store Backends
=====================

``crimson-osd`` supports two categories of object store backends: **native** and **non-native**.

Native Backends
---------------

Native backends perform I/O operations using the **Seastar reactor**. These are tightly integrated with the Seastar framework and follow its design principles:

.. describe:: seastore

   SeaStore is the primary native object store for Crimson OSD. It is built with the Seastar framework and adheres to its asynchronous, shard-based architecture.

.. describe:: cyanstore

   CyanStore is inspired by ``memstore`` from the classic OSD, offering a lightweight, in-memory object store model.
   CyanStore **does not store data** and should be used only for measuring OSD overhead, without the cost of actually storing data.

Non-Native Backends
-------------------

Non-native backends operate through a **thread pool proxy**, which interfaces with object stores running in **alien threads**—worker threads not managed by Seastar.
These backends allow Crimson to interact with legacy or external object store implementations:

.. describe:: bluestore

   The default object store used by the classic ``ceph-osd``. It provides robust, production-grade storage capabilities.

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
