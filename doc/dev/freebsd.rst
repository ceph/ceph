==============================
FreeBSD Implementation details
==============================


Disk layout 
-----------

Current implementation works on ZFS pools

* created in /var/lib/ceph
* One ZFS pool per OSD, like::

    gpart create -s GPT ada1
    gpart add -t freebsd-zfs -l osd1 ada1
    zpool create -o mountpoint=/var/lib/ceph/osd/osd.1 osd

* Maybe add some cache and log (ZIL)? Assuming that ada2 is an SSD::
 
    gpart create -s GPT ada2
    gpart add -t freebsd-zfs -l osd1-log -s 1G ada2
    zpool add osd1 log gpt/osd1-log
    gpart add -t freebsd-zfs -l osd1-cache -s 10G ada2
    zpool add osd1 log gpt/osd1-cache

* Note: *UFS2 does not allow large xattribs*


Configuration
-------------

As per FreeBSD default parts of extra software go into ``/usr/local/``. Which
means that for ``/etc/ceph.conf`` the default location is 
``/usr/local/etc/ceph/ceph.conf``. Smartest thing to do is to create a softlink
from ``/etc/ceph`` to ``/usr/local/etc/ceph``::

  ln -s /usr/local/etc/ceph /etc/ceph
  
A sample file is provided in ``/usr/local/share/doc/ceph/sample.ceph.conf``


MON creation
------------

Monitors are created by following the manual creation steps on::

  http://docs.ceph.com/docs/master/install/manual-deployment/


OSD creation
------------

OSDs can be create with ``ceph-disk``::

  ceph-disk prepare /var/lib/ceph/osd/osd1
  ceph-disk activate /var/lib/ceph/osd/osd1
  
And things should automagically work out.
