.. _cli-version-tracker:

=========================
Version Tracking for Ceph
=========================

Cephadm tracks all cluster version history and allows accessing 
of this information for debugging purposes. Information is managed
with two main CLI commands. This feature only tracks version history
starting from when it was introduced. Version history before the 
introduction of this feature is not tracked.

Viewing Version History
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
     
Removing Version History (Not Recommended)
==========================================

.. prompt:: bash #

    ceph cephadm remove-cluster-version-history --all --before <datetime> --after <datetime>

This command will allow users to delete version history and requires
at least one of the three provided options to be passed. If ``--all`` is 
passed all version history is deleted, this option is incompatible 
with ``--before`` and ``--after`` and returns an error if either are passed 
with it. Option ``--before`` can be used to specify deletion of version 
history before the <datetime> specified. Option ``--after`` can be used 
to specify deletion of version history after the <datetime> 
specified. Options ``--after`` and ``--before`` can be used together to 
specify a range for version history deletion. The format of 
<datetime> should be "YYYY-MM-DD HH:MM:SS".

Option ``--all``:

::

    {
        "2025-09-09 19:42:29.391284+00:00": "ceph version 17.2.8 (f817ceb7f187defb1d021d6328fa833eb8e943b3) quincy (stable)",
        "2025-09-09 20:27:52.786591+00:00": "ceph version 18.2.7 (6b0e988052ec84cf2d4a54ff9bbbc5e720b621ad) reef (stable)",
        "2025-09-09 21:14:20.407522+00:00": "ceph version 19.2.3 (c92aebb279828e9c3c1f5d24613efca272649e62) squid (stable)"
    }

.. prompt:: bash #

    ceph cephadm remove-cluster-version-history --all

::

    {
        No Cluster Version History
    }

Option ``--before``:

::

    {
        "2025-09-09 19:42:29.391284+00:00": "ceph version 17.2.8 (f817ceb7f187defb1d021d6328fa833eb8e943b3) quincy (stable)",
        "2025-09-09 20:27:52.786591+00:00": "ceph version 18.2.7 (6b0e988052ec84cf2d4a54ff9bbbc5e720b621ad) reef (stable)",
        "2025-09-09 21:14:20.407522+00:00": "ceph version 19.2.3 (c92aebb279828e9c3c1f5d24613efca272649e62) squid (stable)"
    }

.. prompt:: bash #

    ceph cephadm remove-cluster-version-history --before "2025-09-09 21:00:00"

::

    {
        "2025-09-09 21:14:20.407522+00:00": "ceph version 19.2.3 (c92aebb279828e9c3c1f5d24613efca272649e62) squid (stable)"
    }

Option ``--after``:

::

    {
        "2025-09-09 19:42:29.391284+00:00": "ceph version 17.2.8 (f817ceb7f187defb1d021d6328fa833eb8e943b3) quincy (stable)",
        "2025-09-09 20:27:52.786591+00:00": "ceph version 18.2.7 (6b0e988052ec84cf2d4a54ff9bbbc5e720b621ad) reef (stable)",
        "2025-09-09 21:14:20.407522+00:00": "ceph version 19.2.3 (c92aebb279828e9c3c1f5d24613efca272649e62) squid (stable)"
    }

.. prompt:: bash #

    ceph cephadm remove-cluster-version-history --after "2025-09-09 21:00:00"

::

    {
        "2025-09-09 19:42:29.391284+00:00": "ceph version 17.2.8 (f817ceb7f187defb1d021d6328fa833eb8e943b3) quincy (stable)",
        "2025-09-09 20:27:52.786591+00:00": "ceph version 18.2.7 (6b0e988052ec84cf2d4a54ff9bbbc5e720b621ad) reef (stable)"
    }

Options ``--before`` and ``--after``:

::

    {
        "2025-09-09 19:42:29.391284+00:00": "ceph version 17.2.8 (f817ceb7f187defb1d021d6328fa833eb8e943b3) quincy (stable)",
        "2025-09-09 20:27:52.786591+00:00": "ceph version 18.2.7 (6b0e988052ec84cf2d4a54ff9bbbc5e720b621ad) reef (stable)",
        "2025-09-09 21:14:20.407522+00:00": "ceph version 19.2.3 (c92aebb279828e9c3c1f5d24613efca272649e62) squid (stable)"
    }

.. prompt:: bash #

    ceph cephadm remove-cluster-version-history --after "2025-09-09 20:00:00" --before "2025-09-09 21:00:00"

::

    {
        "2025-09-09 19:42:29.391284+00:00": "ceph version 17.2.8 (f817ceb7f187defb1d021d6328fa833eb8e943b3) quincy (stable)",
        "2025-09-09 21:14:20.407522+00:00": "ceph version 19.2.3 (c92aebb279828e9c3c1f5d24613efca272649e62) squid (stable)"
    }
    




