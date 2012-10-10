========================
 Purging Temporary Data
========================

.. deprecated:: 0.52

When you delete objects (and buckets/containers), the Gateway marks the  data
for removal, but it is still available to users until it is purged. Since data
still resides in storage until it is purged, it may take up available storage
space. To ensure that data marked for deletion isn't taking up a significant
amount of storage space, you should run the following command periodically:: 

	radosgw-admin temp remove

.. important:: Data marked for deletion may still be read. So consider
   executing the foregoing command a reasonable interval after data
   was marked for deletion.
.. tip:: Consider setting up a ``cron`` job to purge data.