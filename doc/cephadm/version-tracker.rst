.. _cli-version-tracker:

=========================
Version Tracking for Ceph
=========================

Cephadm tracks all cluster version history and allows accessing 
of this information for debugging purposes. Information is managed
with two main CLI commands. This feature only tracks version history
starting from when it was introduced. Version history before the 
introduction of this feature is not tracked.

Viewing Version history
=======================

.. prompt:: bash #

    ceph cephadm get-cluster-version-history

This command displays stored version history stored in 
chronological order.

::

    {
        "2025-09-09 19:42:29.391284+00:00": "ceph version 17.2.8 (f817ceb7f187defb1d021d6328fa833eb8e943b3) quincy (stable)",
        "2025-09-09 20:27:52.786591+00:00": "ceph version 18.2.7 (6b0e988052ec84cf2d4a54ff9bbbc5e720b621ad) reef (stable)",
        "2025-09-09 21:14:20.407522+00:00": "ceph version 19.2.3 (c92aebb279828e9c3c1f5d24613efca272649e62) squid (stable)"
    }
     
Removing Version history (Not Recommended)
==========================================

.. prompt:: bash #

    ceph cephadm remove-cluster-version-history <datetime>

This command will allow users to delete version history with an optional
<datetime> parameter, if no parameter is passed, all version history is
deleted, if a valid <datetime> formatted parameter is passed, only version
history that came before that <datetime> is deleted. The format of this
<datetime> parameter is "YYYY-MM-DD HH:MM:SS".

Before:

::

    {
        "2025-09-09 19:42:29.391284+00:00": "ceph version 17.2.8 (f817ceb7f187defb1d021d6328fa833eb8e943b3) quincy (stable)",
        "2025-09-09 20:27:52.786591+00:00": "ceph version 18.2.7 (6b0e988052ec84cf2d4a54ff9bbbc5e720b621ad) reef (stable)",
        "2025-09-09 21:14:20.407522+00:00": "ceph version 19.2.3 (c92aebb279828e9c3c1f5d24613efca272649e62) squid (stable)"
    }

.. prompt:: bash #

    ceph cephadm remove-cluster-version-history "2025-09-09 20:30:00"

After:
::

    {
        "2025-09-09 21:14:20.407522+00:00": "ceph version 19.2.3 (c92aebb279828e9c3c1f5d24613efca272649e62) squid (stable)"
    }

.. note::

    If all version history is deleted the bootstrap or current version of the
    Ceph cluster is automatically re-added. This is to ensure that there is
    still some base information available.
    




