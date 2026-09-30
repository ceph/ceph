.. _containers:

=======================
 Ceph Container Images
=======================

.. meta::
   :description: Where Ceph publishes its container images, what each image tag points to, and the status of development and legacy images.
   :ceph-page-type: reference

Ceph publishes each :term:`release <Ceph Release>` as the general-purpose
container image ``quay.io/ceph/ceph:<tag>``
(https://quay.io/repository/ceph/ceph), with every Ceph daemon and its
dependencies installed. :term:`cephadm` uses it by default.

.. _ceph-ceph:

.. _official-releases:

Release Images and Tags
=======================

.. list-table::
   :header-rows: 1
   :widths: 28 52 20

   * - Tag
     - Points to
     - Example
   * - ``vRELNUM``
     - The newest image in that major series, including
       :term:`release candidates <Ceph Release Candidate>` until the first
       :term:`stable release <Ceph Stable Release>` (``v20`` is Tentacle)
     - ``v20``
   * - ``vRELNUM.1``
     - The newest release candidate in the series
     - ``v21.1``
   * - ``vRELNUM.2``
     - The newest stable release in the series
     - ``v20.2``
   * - ``vRELNUM.Y.Z``
     - A specific release
     - ``v20.2.4``
   * - ``vRELNUM.Y.Z-YYYYMMDD``
     - A specific build of that release
     - ``v20.2.4-20260818``

Pull an explicit version tag or an image digest, for example:

.. prompt:: bash #

   podman pull quay.io/ceph/ceph:v20.2.4

``quay.io/ceph/ceph`` has no ``:latest`` tag, and tags such as ``v20`` and
``v20.2`` move when a new :term:`point release <Ceph Point Release>` ships.
With a moving tag, the :term:`hosts <Host>` of a cluster can end up with
different images, and upgrades might not work properly. cephadm pins its own
daemons to a digest: it converts a tag to the image digest when
``mgr/cephadm/use_repo_digest`` is ``true``, the default.

.. _ceph-ci-ceph:

.. _development-builds:

Development Images
==================

Development images are built like ``quay.io/ceph/ceph``, from unreleased and
untested code. Do not use them in production. They are pushed to
``quay.ceph.io/ceph-ci/ceph`` (https://quay.ceph.io/organization/ceph-ci) for
the branches pushed to ceph-ci, the copy of the Ceph Git repository that the
CI system builds from, including ``main`` and the release branches.

.. list-table::
   :header-rows: 1
   :widths: 28 52 20

   * - Tag
     - Points to
     - Example
   * - ``BRANCH``
     - The latest build of a branch
     - ``main``, ``wip-foo``
   * - ``BRANCH-BASEOS-ARCH-devel``
     - The latest build of a branch for one base image and CPU architecture
     - ``main-rockylinux-10-x86_64-devel``
   * - ``SHA1``
     - The build of one commit, named by its full commit ID
     -
   * - ``BRANCH-BASEOS``, ``SHA1-BASEOS``
     - A build on a base image that is not the default for the branch (the
       default is ``centos-stream9`` for reef, squid and tentacle, and
       ``rockylinux-10`` from umbrella onwards)
     - ``main-centos-stream9``
   * - ``BRANCH-debug``, ``SHA1-debug``
     - A debug build
     - ``main-debug``
   * - ``BRANCH-arm64``, ``SHA1-arm64``
     - An arm64 build
     - ``main-arm64``

Suffixes combine in the order ``-BASEOS``, ``-debug``, ``-arm64``. The
exception is tentacle and branches based on it: there, a debug build on a
base image that is not the default gets the same ``-BASEOS`` tag as the
non-debug build, without ``-debug``, so that tag points to whichever build
finished last. The ``-devel`` tag of a debug build always ends in ``-debug``.

.. _ceph-daemon-base:

.. _ceph-daemon:

.. _legacy-container-images:

Legacy Images
=============

Use ``quay.io/ceph/ceph``, which is built from ``container/Containerfile`` in
the Ceph repository, instead of these legacy images:

- The ``ceph/daemon-base`` image, and the ``ceph/daemon`` image
  (``ceph/daemon-base`` plus the Bash scripts that ceph-ansible and ceph-nano
  used), are no longer built. Their source repository, ceph/ceph-container,
  was archived in December 2024, and their last tags on quay.io are from 2024.
- The Ceph images on Docker Hub (https://hub.docker.com/u/ceph) have not been
  updated since 2021; ``ceph/ceph`` there stops at ``v16.2.5``.

Additional Resources
====================

- :ref:`cephadm-airgap`
- :doc:`/cephadm/upgrade`
- :ref:`start-platforms`
- `Containerfile of quay.io/ceph/ceph`_

.. _Containerfile of quay.io/ceph/ceph: https://github.com/ceph/ceph/blob/main/container/Containerfile
