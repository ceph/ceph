
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

1. Build a kubernetes cluster
=============================

Before installing Ceph/Rook, make sure you've got a working kubernetes
cluster with some nodes added (i.e. ``kubectl get nodes`` shows you something).
The rest of this guide assumes that your development workstation has network
access to your kubernetes cluster, such that ``kubectl`` works from your
workstation.

There are many ways (https://kubernetes.io/docs/setup/pick-right-solution/)
to build a kubernetes cluster: here we include some tips/pointers on where
to get started.

Host your own
-------------

If you already have some linux servers (bare metal or VMs), you can set up
your own kubernetes cluster using the ``kubeadm`` tool.

https://kubernetes.io/docs/setup/independent/create-cluster-kubeadm/

Here are some tips for a smoother ride with ``kubeadm``:

- Don't worry if your servers aren't powerful: at time of writing, @jcsp is
  running his home kubernetes cluster on 3 nodes Turion N54L nodes with 8GB RAM.
- If you have installed any kubernetes/etcd/flannel packages before, make sure
  they (and their configuration) are erased before you start.  kubeadm
  installs containerised daemons that will be oblivious to any non-containerised
  services you might already have running.
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


2. Run a docker repository
===========================

Ideally, run this somewhere accessible from both your workstation and your
kubernetes cluster (i.e. so that ``docker push/pull`` just works everywhere).
This is likely to be the same host you're using as your kubernetes master.

1. Install the ``docker-distribution`` package.
2. If you want to configure the port, edit ``/etc/docker-distribution/registry/config.yml``
3. Enable the registry service:

::

    systemctl enable docker-distribution
    systemctl start docker-distribution


3. Build Rook
=============

.. note::

    Work within your $GOPATH -- here we assume it's ~/go

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

The rest of this guide assumes that you will want to load your own binaries,
and then push the container directly into your docker repository.  


4. Build Ceph
=============

It is important that you build Ceph in an environment compatible with
the base OS used in the Rook containers.  By default, the Rook containers
are built with a CentOS base OS.  The simplest way to approach this
is to build Ceph inside a docker container on your workstation.

You can run a centos docker container with access to your Ceph source
tree using a command like:

::

    docker run -i -v /my/ceph/src:/my/ceph/src -t centos:7 /bin/bash

Once you have built Ceph, you can inject the resulting binaries into
the Rook container image using the ``kubejacker.sh`` script (run from
your build directory but from *outside* your build container).

Setting the ``$REPO`` environment variable to your docker repository,
execute the script to build a docker image containing your latest Ceph
binaries:

::

    build$ REPO=<host>:<port> sh ../src/script/kubejacker/kubejacker.sh

.. note::

    You can also set ``BASEIMAGE`` to control that Rook image used
    as the base -- by default this is set to any "ceph-amd64" image.
    

Now you've got your freshly built Rook and freshly built Ceph into
a single container image, ready to run.  Next time you change something
in Ceph, you can re-run this to update your image and restart your
kubernetes containers.  If you change something in Rook, then re-run the Rook
build, and the Ceph build too.

5. Run a Rook cluster
=====================

.. note::

    This is just some basic instructions: the Rook documentation
    is much more expansive, at https://github.com/rook/rook/tree/master/Documentation

The Rook source tree includes example .yaml files in
``cluster/examples/kubernetes/ceph/``.  The important ones are:

- ``operator.yaml`` -- runs the Rook operator, which will execute any other
  rook objects we create.
- ``cluster.yaml`` -- defines a Ceph cluster
- ``toolbox.yaml`` -- runs the toolbox container, which contains the Ceph
  CLI client.

Copy these into a working directory, and edit as necessary to configure
the setup you want:

- Ensure that the ``image`` field in the operator matches the built Ceph image
  you have uploaded to your Docker repository.
- Edit the ``storage`` section of the cluster: set ``useAllNodes`` and
  ``useAllDevices`` to false if you want to create OSDs explicitly
  using ceph-mgr.
    
Then, load the configuration into the kubernetes API using ``kubectl``:

::

    kubectl apply -f ./operator.yaml 
    kubectl apply -f ./cluster.yaml 
    kubectl apply -f ./toolbox.yaml 

Use ``kubectl -n rook-ceph-system get pods`` to check the operator
pod is coming up, then ``kubectl -n rook-ceph get pods`` to check on
the Ceph daemons and toolbox.  Once everything is up and running,
you should be able to open a shell in the toolbox container and
run ``ceph status``.

If your mon services start but the rest don't, it could be that they're
unable to form a quorum due to a Kubernetes networking issue: check that
containers in your Kubernetes cluster can ping containers on other nodes.

Cheat sheet
===========

Open a shell in your toolbox container::

    kubectl -n rook-ceph exec -it rook-ceph-tools bash

Inspect the Rook operator container's logs::

    kubectl -n rook-ceph-system logs -l app=rook-ceph-operator

Inspect the ceph-mgr container's logs::

    kubectl -n rook-ceph logs -l app=rook-ceph-mgr

