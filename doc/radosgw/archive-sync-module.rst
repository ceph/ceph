===================
Archive Sync Module
===================

.. versionadded:: Nautilus

This sync module leverages the versioning feature of the S3 objects in RGW to
have an archive zone that captures the different versions of the S3 objects
as they occur over time in the other zones.

An archive zone allows to have a history of versions of S3 objects that can
only be eliminated through the gateways associated with the archive zone.

This functionality is useful to have a configuration where several
non-versioned zones replicate their data and metadata through their zone
gateways (mirror configuration) providing high availability to the end users,
while the archive zone captures all the data updates and metadata for
consolidate them as versions of S3 objects.

Including an archive zone in a multizone configuration allows you to have the
flexibility of an S3 object history in one only zone while saving the space
that the replicas of the versioned S3 objects would consume in the rest of the
zones.



Archive Sync Tier Type Configuration
------------------------------------

How to Configure
~~~~~~~~~~~~~~~~

See `Multisite Configuration`_ for how to multisite config instructions. The
archive sync module requires a creation of a new zone. The zone tier type needs
to be defined as ``archive``:

::

    # radosgw-admin zone create --rgw-zonegroup={zone-group-name} \
                                --rgw-zone={zone-name} \
                                --endpoints={http://fqdn}[,{http://fqdn}]
                                --tier-type=archive

.. _Multisite Configuration: ./multisite
