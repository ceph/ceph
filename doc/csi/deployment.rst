.. _csi-deployment:

====================
 Deploying Ceph-CSI
====================

There are several ways to deploy the Ceph-CSI drivers into a Kubernetes
cluster. The `Ceph-CSI-Operator`_ is the recommended method for new
deployments. Helm charts and raw manifests remain available, and Rook
users get Ceph-CSI automatically.

.. note::

   If you deploy Ceph with `Rook`_, Rook installs and manages Ceph-CSI
   for you. Do not deploy Ceph-CSI by hand in a Rook cluster.


Gather Ceph Cluster Information
===============================

Whichever method you choose, the drivers need to know how to reach the
Ceph cluster. Collect the cluster ``fsid`` and the monitor addresses:

.. prompt:: bash $

   ceph mon dump

The monitor addresses are listed in the driver's cluster connection
configuration. The ``clusterID`` parameter that StorageClasses use to
select a cluster must match the name of the corresponding
``ClientProfile`` resource when deploying with the operator; Helm and
raw manifest deployments conventionally use the cluster ``fsid`` as
the ``clusterID``. Each backend page in this chapter describes the
cephx user that the drivers authenticate with.


Ceph-CSI-Operator (Recommended)
===============================

The `Ceph-CSI-Operator`_ manages the deployment, configuration, and
lifecycle of Ceph-CSI drivers through Kubernetes custom resources.
Install the operator:

.. prompt:: bash $

   kubectl create -f https://raw.githubusercontent.com/ceph/ceph-csi-operator/main/deploy/all-in-one/install.yaml

Then deploy a driver by creating a ``Driver`` resource. For example, to
deploy the RBD driver:

.. code-block:: yaml

   apiVersion: csi.ceph.io/v1
   kind: Driver
   metadata:
     name: rbd.csi.ceph.com
     namespace: ceph-csi-operator-system

The CephFS and NFS drivers are deployed the same way, using the names
``cephfs.csi.ceph.com`` and ``nfs.csi.ceph.com``. Connection details
for the Ceph cluster are supplied through the operator's
``CephConnection`` and ``ClientProfile`` resources; cephx credentials
are stored in Kubernetes Secrets referenced from the StorageClass. See
the
`operator quick start`_ and `operator installation guide`_ for complete
walkthroughs, supported Kubernetes versions, and Helm-based installation
of the operator itself.


Helm Charts
===========

The Ceph-CSI project publishes per-driver Helm charts:

.. prompt:: bash $

   helm repo add ceph-csi https://ceph.github.io/csi-charts
   helm install --namespace ceph-csi-rbd ceph-csi-rbd ceph-csi/ceph-csi-rbd \
       --create-namespace

A ``ceph-csi-cephfs`` chart is available for the CephFS driver. Chart
values are documented in the `Ceph-CSI charts`_ directory.


Raw Manifests
=============

Per-driver Kubernetes manifests are maintained in the ``deploy/``
directory of the Ceph-CSI repository, together with deployment
instructions in the `Ceph-CSI documentation`_. This method offers the
most control but requires you to track and apply manifest changes on
every upgrade.

.. _Ceph-CSI-Operator: https://github.com/ceph/ceph-csi-operator
.. _Rook: https://rook.io
.. _operator quick start: https://github.com/ceph/ceph-csi-operator/blob/main/docs/quick-start.md
.. _operator installation guide: https://github.com/ceph/ceph-csi-operator/blob/main/docs/installation.md
.. _Ceph-CSI charts: https://github.com/ceph/ceph-csi/tree/devel/charts
.. _Ceph-CSI documentation: https://ceph.github.io/ceph-csi/
