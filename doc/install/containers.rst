.. _containers:

Ceph Container Images
=====================

.. important::

   Using the ``:latest`` tag is discouraged. If you use the ``:latest`` 
   tag, there is no guarantee that the same image will be on each of 
   your hosts.  Under these conditions, upgrades might not work 
   properly. Remember that ``:latest`` is a relative tag, and a moving
   target.

   Instead of the ``:latest`` tag, use explicit tags or image IDs. For
   example:

   ``podman pull ceph/ceph:v15.2.0``

Official Releases
-----------------

Ceph Container images are available from Docker Hub at::

  https://hub.docker.com/r/ceph


ceph/ceph
^^^^^^^^^

- General purpose Ceph container with all necessary daemons and
  dependencies installed.

+----------------------+--------------------------------------------------------------+
| Tag                  | Meaning                                                      |
+----------------------+--------------------------------------------------------------+
| vRELNUM              | Latest release in this series (e.g., *v14* = Nautilus)       |
+----------------------+--------------------------------------------------------------+
| vRELNUM.2            | Latest *stable* release in this stable series (e.g., *v14.2*)|
+----------------------+--------------------------------------------------------------+
| vRELNUM.Y.Z          | A specific release (e.g., *v14.2.4*)                         |
+----------------------+--------------------------------------------------------------+
| vRELNUM.Y.Z-YYYYMMDD | A specific build (e.g., *v14.2.4-20191203*)                  |
+----------------------+--------------------------------------------------------------+

ceph/daemon-base
^^^^^^^^^^^^^^^^

- General purpose Ceph container with all necessary daemons and
  dependencies installed.
- Basically the same as *ceph/ceph*, but with different tags.
- Note that all of the *-devel* tags (and the *latest-master* tag) are based on
  unreleased and generally untested packages from https://shaman.ceph.com.

:note: This image will soon become an alias to *ceph/ceph*.

+------------------------+---------------------------------------------------------+
| Tag                    | Meaning                                                 |
+------------------------+---------------------------------------------------------+
| latest-master          | Build of master branch a last ceph-container.git update |
+------------------------+---------------------------------------------------------+
| latest-master-devel    | Daily build of the master branch                        |
+------------------------+---------------------------------------------------------+
| latest-RELEASE-devel   | Daily build of the *RELEASE* (e.g., nautilus) branch    |
+------------------------+---------------------------------------------------------+


ceph/daemon
^^^^^^^^^^^

- *ceph/daemon-base* plus a collection of BASH scripts that are used
  by ceph-nano and ceph-ansible to manage a Ceph cluster.

+------------------------+---------------------------------------------------------+
| Tag                    | Meaning                                                 |
+------------------------+---------------------------------------------------------+
| latest-master          | Build of master branch a last ceph-container.git update |
+------------------------+---------------------------------------------------------+
| latest-master-devel    | Daily build of the master branch                        |
+------------------------+---------------------------------------------------------+
| latest-RELEASE-devel   | Daily build of the *RELEASE* (e.g., nautilus) branch    |
+------------------------+---------------------------------------------------------+


Development builds
------------------

We automatically build container images for development ``wip-*``
branches in the ceph-ci.git repositories and push them to Quay at::

  https://quay.io/organization/ceph-ci

ceph-ci/ceph
^^^^^^^^^^^^

- This is analogous to the ceph/ceph image above
- TODO: remove the ``wip-*`` limitation and also build ceph.git branches.

+------------------------------------+------------------------------------------------------+
| Tag                                | Meaning                                              |
+------------------------------------+------------------------------------------------------+
| BRANCH                             | Latest build of a given GIT branch (e.g., *wip-foo*) |
+------------------------------------+------------------------------------------------------+
| BRANCH-SHORTSHA1-BASEOS-ARCH-devel | A specific build of a branch                         |
+------------------------------------+------------------------------------------------------+
| SHA1                               | A specific build                                     |
+------------------------------------+------------------------------------------------------+
