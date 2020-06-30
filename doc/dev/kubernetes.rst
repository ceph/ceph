
.. _kubernetes-dev:

=======================================
Hacking on Ceph in Kubernetes with Rook
=======================================

.. warning::

    This is *not* official user documentation for setting up production
    Ceph clusters with Kubernetes.  It is aimed at developers who want
    to hack on Ceph in Kubernetes.

This guide is aimed at Ceph developers getting started with running
in a Kubernetes environment.  It assumes that you may be hacking on Rook,
Ceph or both, so everything is built from source.

TL;DR for hacking on MGR modules
================================

Make your changes to the Python code base and then from Ceph's
``build`` directory, run::

    ../src/script/kubejacker/kubejacker.sh '192.168.122.1:5000'

where ``'192.168.122.1:5000'`` is a local docker registry and
Rook's ``CephCluster`` CR uses ``image: 192.168.122.1:5000/ceph/ceph:latest``.

1. Build a kubernetes cluster
=============================

Before installing Ceph/Rook, make sure you've got a working kubernetes
cluster with some nodes added (i.e. ``kubectl get nodes`` shows you something).
The rest of this guide assumes that your development workstation has network
access to your kubernetes cluster, such that ``kubectl`` works from your
workstation.

`There are many ways <https://kubernetes.io/docs/setup/>`_
to build a kubernetes cluster: here we include some tips/pointers on where
to get started.

`kubic-terraform-kvm <https://github.com/kubic-project/kubic-terraform-kvm>`_
might also be an option.

Or `Host your own <https://kubernetes.io/docs/setup/independent/create-cluster-kubeadm/>`_ with
``kubeadm``.

Some Tips
---------

Here are some tips for a smoother ride with ``kubeadm``:

- If you have previously added any yum/deb repos for kubernetes packages,
  disable them before trying to use the packages.cloud.google.com repository.
  If you don't, you'll get quite confusing conflicts.
- Even if your distro already has docker, make sure you're installing it
  a version from docker.com that is within the range mentioned in the
  kubeadm install instructions.  Especially, note that the docker in CentOS 7
  will *not* work.

Hosted elsewhere
----------------

If you do not have any servers to hand, you might try a pure
container provider such as Google Compute Engine.  Your mileage may
vary when it comes to what kinds of storage devices are visible
to your kubernetes cluster.

Make sure you check how much it's costing you before you spin up a big cluster!


2. Run a docker registry
========================

Run this somewhere accessible from both your workstation and your
kubernetes cluster (i.e. so that ``docker push/pull`` just works everywhere).
This is likely to be the same host you're using as your kubernetes master.

1. Install the ``docker-distribution`` package.
2. If you want to configure the port, edit ``/etc/docker-distribution/registry/config.yml``
3. Enable the registry service:

::

    systemctl enable docker-distribution
    systemctl start docker-distribution

You may need to mark the registry as **insecure**.

3. Build Rook
=============

.. note::

    Building Rook is **not required** to make changes to Ceph.

Install Go if you don't already have it.

Download the Rook source code:

::

    go get github.com/rook/rook

    # Ignore this warning, as Rook is not a conventional go package
    can't load package: package github.com/rook/rook: no Go files in /home/jspray/go/src/github.com/rook/rook

You will now have a Rook source tree in ~/go/src/github.com/rook/rook -- you may
be tempted to clone it elsewhere, but your life will be easier if you
leave it in your GOPATH.

Run ``make`` in the root of your Rook tree to build its binaries and containers:

::

    make
    ...
    === saving image build-9204c79b/ceph-amd64
    === docker build build-9204c79b/ceph-toolbox-base-amd64
    sha256:653bb4f8d26d6178570f146fe637278957e9371014ea9fce79d8935d108f1eaa
    === docker build build-9204c79b/ceph-toolbox-amd64
    sha256:445d97b71e6f8de68ca1c40793058db0b7dd1ebb5d05789694307fd567e13863
    === caching image build-9204c79b/ceph-toolbox-base-amd64

You can use ``docker image ls`` to see the resulting built images.  The
images you care about are the ones with tags ending "ceph-amd64" (used
for the Rook operator and Ceph daemons) and "ceph-toolbox-amd64" (used
for the "toolbox" container where the CLI is run).

4. Build Ceph
=============

.. note::

    Building Ceph is **not required** to make changes to MGR modules
    written in Python.


The Rook containers and the Ceph containers are independent now. Note that
Rook's Ceph client libraries need to communicate with the Ceph cluster,
therefore a compatible major version is required.

You can run a CentOS docker container with access to your Ceph source
tree using a command like:

::

    docker run -i -v /my/ceph/src:/my/ceph/src -t centos:7 /bin/bash

Once you have built Ceph, you can inject the resulting binaries into
the Rook container image using the ``kubejacker.sh`` script (run from
your build directory but from *outside* your build container).

5. Run Kubejacker
=================

``kubejacker`` needs access to your docker registry. Execute the script
to build a docker image containing your latest Ceph binaries:

::

    build$ ../src/script/kubejacker/kubejacker.sh "<host>:<port>"


Now you've got your freshly built Rook and freshly built Ceph into
a single container image, ready to run.  Next time you change something
in Ceph, you can re-run this to update your image and restart your
kubernetes containers.  If you change something in Rook, then re-run the Rook
build, and the Ceph build too.

5. Run a Rook cluster
=====================

Please refer to `Rook's documentation <https://rook.io/docs/rook/master/ceph-quickstart.html>`_
for setting up a Rook operator, a Ceph cluster and the toolbox.

The Rook source tree includes example .yaml files in
``cluster/examples/kubernetes/ceph/``. Copy these into
a working directory, and edit as necessary to configure
the setup you want:

- Ensure that ``spec.cephVersion.image`` points to your docker registry::

    spec:
      cephVersion:
        allowUnsupported: true
        image: 192.168.122.1:5000/ceph/ceph:latest

Then, load the configuration into the kubernetes API using ``kubectl``:

::

    kubectl apply -f ./cluster-test.yaml

Use ``kubectl -n rook-ceph get pods`` to check the operator
pod the Ceph daemons and toolbox are is coming up.

Once everything is up and running,
you should be able to open a shell in the toolbox container and
run ``ceph status``.

If your mon services start but the rest don't, it could be that they're
unable to form a quorum due to a Kubernetes networking issue: check that
containers in your Kubernetes cluster can ping containers on other nodes.

Cheat sheet
===========

Open a shell in your toolbox container::

    kubectl -n rook-ceph exec -it $(kubectl -n rook-ceph get pod -l "app=rook-ceph-tools" -o jsonpath="{.items[0].metadata.name}") -- bash

Inspect the Rook operator container's logs::

    kubectl -n rook-ceph logs -l app=rook-ceph-operator

Inspect the ceph-mgr container's logs::

    kubectl -n rook-ceph logs -l app=rook-ceph-mgr

