.. _install-mirrors:

=======================
 Ceph Download Mirrors
=======================

.. meta::
   :description: Mirrors of download.ceph.com for Ceph packages and tarballs, how to download from them, and how to run or register a mirror.
   :ceph-page-type: reference

Mirrors keep a synchronized copy of ``download.ceph.com``. Companies and
universities that support the Ceph project run them. Use the mirror nearest
to you.

.. _locations:

Mirror Locations
================

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Location
     - URL
   * - EU: Netherlands
     - https://eu.ceph.com/
   * - AU: Australia
     - https://au.ceph.com/
   * - UK: United Kingdom
     - https://uk.ceph.com/
   * - US-Mid-West: Chicago
     - https://mirrors.gigenet.com/ceph/
   * - CN: China
     - https://mirrors.ustc.edu.cn/ceph/
   * - CA: Canada
     - https://ca.ceph.com/

Downloading from a Mirror
=========================

Replace ``download.ceph.com`` with the mirror in any URL. For example, these
URLs:

.. code-block:: none
   :substitutions:

   https://download.ceph.com/tarballs/
   https://download.ceph.com/debian-|stable-release|/
   https://download.ceph.com/rpm-|stable-release|/

become these on the EU mirror:

.. code-block:: none
   :substitutions:

   https://eu.ceph.com/tarballs/
   https://eu.ceph.com/debian-|stable-release|/
   https://eu.ceph.com/rpm-|stable-release|/

With :term:`cephadm`, give the mirror to ``cephadm add-repo`` with
``--repo-url``:

.. prompt:: bash #
   :substitutions:

   cephadm add-repo --release |stable-release| --repo-url https://eu.ceph.com

The release key still comes from ``download.ceph.com`` unless you also give
``--gpg-url``. On Debian and Ubuntu, cephadm saves the key in a ``.gpg`` file,
which APT reads only if the key is binary, so give the ``release.gpg`` URL,
not ``release.asc``, for example
``--gpg-url https://eu.ceph.com/keys/release.gpg``.

.. _mirroring:

Running a Mirror
================

To keep your own copy, for example for :term:`hosts <Host>` without internet
access, use ``mirror-ceph.sh``, a Bash script in the ``mirroring`` directory
of the Ceph repository on `GitHub`_. It copies a mirror with rsync in two
stages, the packages first and the repository metadata last, so that the
metadata never points to files that are not there yet.

.. warning:: The second stage runs ``rsync --delete-after``, which deletes
   every file in the target directory that is not on the source mirror. Use a
   directory that holds nothing else.

For example, this command copies ``eu.ceph.com`` into ``/srv/mirrors/ceph``:

.. prompt:: bash $

   ./mirror-ceph.sh -q -s eu -t /srv/mirrors/ceph

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Option
     - Meaning
   * - ``-s``
     - The source mirror, from the list in the script. Sync from a mirror
       near you.
   * - ``-t``
     - The target directory, which must exist.
   * - ``-q``
     - No progress output; the script prints nothing unless it fails.

When you schedule the sync:

- Do not sync more often than every 3 hours.
- Start the sync at a minute between 1 and 59 past the hour, not at minute 0.

For example, this cron entry syncs every four hours, at 13 minutes past the
hour:

.. code-block:: none

   13 1,5,9,13,17,21 * * * /home/ceph/mirror-ceph.sh -q -s eu -t /srv/mirrors/ceph

.. _becoming-a-mirror:

Becoming an Official Mirror
===========================

A public mirror can become an official mirror of the Ceph project, with an
``xx.ceph.com`` name (a DNS CNAME record) on request. The mirroring README on
`GitHub`_ sets these requirements:

- A network connection of 1 Gbit/s or more.
- Native IPv4 and IPv6.
- HTTP and rsync access.
- 2 TB of storage or more.
- Monitoring of the mirror.
- HTTP access logs kept for at least 6 months, for download statistics.

To apply, write to the ceph-users mailing list. Mirror maintainers should also
join the ceph-mirrors mailing list:
https://lists.ceph.io/postorius/lists/ceph-mirrors.ceph.io/.

Additional Resources
====================

- :ref:`packages`
- :doc:`get-tarballs`
- `Mirror script and README on GitHub <GitHub_>`_

.. _GitHub: https://github.com/ceph/ceph/tree/main/mirroring
