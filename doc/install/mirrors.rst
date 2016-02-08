=============
 Ceph Mirrors
=============

For improved user experience multiple mirrors for Ceph are available around the
world.

These mirrors are kindly sponsored by various companies who want to support the
Ceph project.


Locations
=========

These mirrors are available on the following locations:

- **EU: Netherlands**: http://eu.ceph.com/
- **AU: Australia**: http://au.ceph.com/
- **CZ: Czech Republic**: http://cz.ceph.com/
- **SE: Sweden**: http://se.ceph.com/
- **DE: Germany**: http://de.ceph.com/
- **HK: Hong Kong**: http://hk.ceph.com/
- **US-East: US East Coast**: http://us-east.ceph.com/
- **US-West: US West Coast**: http://us-west.ceph.com/

You can replace all download.ceph.com URLs with any of the mirrors, for example:

  http://download.ceph.com/tarballs/
  http://download.ceph.com/debian-hammer/
  http://download.ceph.com/rpm-hammer/

Change this to:

  http://eu.ceph.com/tarballs/
  http://eu.ceph.com/debian-hammer/
  http://eu.ceph.com/rpm-hammer/


Mirroring
=========

You can easily mirror Ceph yourself using a Bash script and rsync. A easy to use
script can be found at `Github`_.

When mirroring Ceph, please keep the following guidelines in mind:

- Choose a mirror close to you
- Do not sync in a interval shorter than 3 hours
- Avoid syncing at minute 0 of the hour, use something between 0 and 59


Becoming a mirror
=================

If you want to provide a public mirror for other users of Ceph you can opt to
become a official mirror.

To make sure all mirrors meet the same standards some requirements have been
set for all mirrors. These can be found on `Github`_.

If you want to apply for an official mirror, please contact the ceph-users mailinglist.


.. _Github: https://github.com/ceph/ceph/tree/master/mirroring
