
Upgrading pre-Firefly filesystems past Jewel
============================================

.. tip::

    This advice only applies to users with filesystems
    created using versions of Ceph older than *Firefly* (0.80).
    Users creating new filesystems may disregard this advice.

Pre-firefly versions of Ceph used a now-deprecated format
for storing CephFS directory objects, called TMAPs.  Support
for reading these in RADOS will be removed after the Jewel
release of Ceph, so for upgrading CephFS users it is important
to ensure that any old directory objects have been converted.

After installing Jewel on all your MDS and OSD servers, and restarting
the services, run the following command:

::
    
    cephfs-data-scan tmap_upgrade <metadata pool name>

This only needs to be run once, and it is not necessary to
stop any other services while it runs.  The command may take some
time to execute, as it iterates overall objects in your metadata
pool.  It is safe to continue using your filesystem as normal while
it executes.  If the command aborts for any reason, it is safe
to simply run it again.

If you are upgrading a pre-Firefly CephFS filesystem to a newer Ceph version
than Jewel, you must first upgrade to Jewel and run the ``tmap_upgrade``
command before completing your upgrade to the latest version.

