.. _get-tarballs:

====================================
 Downloading a Ceph Release Tarball
====================================

.. meta::
   :description: Download and unpack the source code tarball of a Ceph release from download.ceph.com or a mirror.
   :ceph-page-type: procedure

Download the source code of a Ceph :term:`release <Ceph Release>` as a
tarball. To work on the code, clone the Git repository instead; see
:ref:`install-clone-source`.

Prerequisites
=============

- The version that you want. `Releases`_ lists every release, and
  `Ceph Release Tarballs`_ lists every tarball, named
  ``ceph-{version}.tar.gz``.
- The ``wget`` command.
- About 333 MB of free space for the tarball of 20.2.4, and more to unpack
  it.

Procedure
=========

#. Download the tarball, replacing ``20.2.4`` with the version that you want,
   and ``download.ceph.com`` with a host from :ref:`install-mirrors` to use a
   mirror near you:

   .. prompt:: bash $

      wget https://download.ceph.com/tarballs/ceph-20.2.4.tar.gz

   ``wget`` shows its progress, and its last line reports the file as saved
   (``HTTP response 200`` from Wget2).

#. Unpack the tarball:

   .. prompt:: bash $

      tar xzf ceph-20.2.4.tar.gz

   The command prints nothing on success and creates the directory
   ``ceph-20.2.4``.

Verification
============

- List the top of the source tree:

  .. prompt:: bash $

     ls ceph-20.2.4

  The output includes ``CMakeLists.txt``, ``doc``, and ``src``.

Troubleshooting
===============

- **"ERROR 404: Not Found" or "HTTP ERROR response 404" from wget.** No
  tarball has that name. Check the version against `Ceph Release Tarballs`_.
- **"gzip: stdin: unexpected end of file" from tar.** The download is
  incomplete. Delete the file and download it again.

Next Steps
==========

- :ref:`build-ceph`: build Ceph from the source tree.
- :ref:`install-storage-cluster-build`: install the build.

Additional Resources
====================

- `Ceph Release Tarballs`_
- :ref:`install-mirrors`
- :ref:`install-clone-source`

.. _Ceph Release Tarballs: https://download.ceph.com/tarballs/

.. Needs to be an external link because doc/releases/index.rst is not in
   stable branches and we want to always use the main branch version
.. _Releases: https://docs.ceph.com/en/latest/releases/
