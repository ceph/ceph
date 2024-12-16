==================
Rados Bucket Index
==================

Buckets in RGW store their list of objects in a bucket index. Each index entry stores just enough metadata (size, etag, mtime, etc.) to serve API requests to list objects. These APIs are `ListObjectsV2`_ and `ListObjectVersions`_ in S3, and `GET Container`_ in Swift.

.. note:: Buckets can be created as 'indexless'. Such buckets have no index, and cannot be listed.

---------------------
Consistency Guarantee
---------------------

RGW guarantees read-after-write consistency on object operations. This means that once a client receives a successful response to a write request, then the effects of that write must be visible to subsequent read requests.

For example: if an S3 client sends a PutObject request to overwrite an existing object, followed by a GetObject request to read it back, RGW must not return the previous object's contents. It must either respond with the new object's contents, or with the result of a later object write or delete.

This consistency guarantee applies to all object write requests (PutObject, DeleteObject, PutObjectAcl, etc) and all object read requests (HeadObject, GetObject, ListObjectsV2, etc).

------------------
Rados Object Model
------------------

S3/Swift objects, or 'API objects', are stored as rados objects in the rgw.buckets.data pool. Each API object is comprised of a head object and zero or more tail objects. Bucket index objects are stored in the rgw.buckets.index pool.

When writing an object, its head object is written last. This acts as an atomic 'commit' to make it visible to read requests.

-----------------------
Sharding and Resharding
-----------------------

For a given bucket, the index may be split into several rados objects, called bucket index shards. In RADOS, multiple writes to the same object cannot run in parallel. By spreading the index over more rados objects, we increase its write parallelism. For a given object upload, the corresponding bucket index shard is selected based on a hash of the object's name.

The default shard count for new buckets is 11, but can be overridden in the zonegroup's ``bucket_index_max_shards`` or ceph.conf's ``rgw_override_bucket_index_max_shards``. As the number of objects in a bucket grows, its index shard count will also increase as a result of dynamic resharding.

Information about the bucket's index object layout is stored in ``RGWBucketInfo`` as ``struct rgw::BucketLayout`` from ``src/rgw/rgw_bucket_layout.h``. The resharding logic is in ``src/rgw/driver/rados/rgw_reshard.cc``.

-----------------
Index Transaction
-----------------

To keep the bucket index consistent, all object writes or deletes must also update the index accordingly. Because the head objects are stored in different rados objects than the bucket indices, we can't update both atomically with a single rados operation. In order to satisfy the `Consistency Guarantee`_ for listing operations, we have to coordinate these two object writes using a three-step bucket index transaction:

#. Prepare a transaction on its bucket index object.
#. Write or delete the head object.
#. Commit the transaction on the bucket index object (or cancel the transaction if step 2 fails).

Object writes and deletes may race with each other, so a given object may have more than one prepared transaction at a time. RGW considers an object entry to be 'pending' if there are any outstanding transactions, or 'completed' otherwise.

This transaction is implemented in ``src/rgw/driver/rados/rgw_rados.cc`` as ``RGWRados::Object::Write::write_meta()`` for object writes, and ``RGWRados::Object::Delete::delete_obj()`` for object deletes. The bucket index operations are implemented in ``src/cls/rgw/cls_rgw.cc`` as ``rgw_bucket_prepare_op()`` and ``rgw_bucket_complete_op()``.

-------
Listing
-------

When listing objects, RGW will read all entries (pending and completed) from the bucket index. For any pending entries, it must check whether the head object exists before including that entry in the final listing.

If an RGW crashes in the middle of an `Index Transaction`_, an index entry may get stuck in this 'pending' state. When bucket listing encounters these pending entries, it also sends information from the head object back to the bucket index so it can update the entry and resolve its stale transactions. This message is called 'dir suggest', because the bucket index treats it as a hint or suggestion.

Bucket listing is implemented in ``src/rgw/driver/rados/rgw_rados.cc`` as ``RGWRados::Bucket::List::list_objects_ordered()`` and ``RGWRados::Bucket::List::list_objects_unordered()``. ``RGWRados::check_disk_state()`` is the part that reads the head object and encodes suggested changes. The corresponding bucket index operations are implemented in ``src/cls/rgw/cls_rgw.cc`` as ``rgw_bucket_list()`` and ``rgw_dir_suggest_changes()``.

--------------------
S3 Object Versioning
--------------------

For versioned buckets, the bucket index contains an entry for each object version and delete marker. In addition to sorting index entries by object name, it also has to sort object versions of the same name from newest to oldest.

RGW stores a head object in the rgw.buckets.data pool for each object version. This rados object's oid is a combination of the object name and its version id.

In S3, a GET/HEAD request for an object name will give you that object's "current" version. To support this, RGW stores an extra 'object logical head' (olh) object whose oid includes the object name only, that acts as an indirection to the head object of its current version. This indirection logic is implemented in ``src/rgw/driver/rados/rgw_rados.cc`` as ``RGWRados::follow_olh()``.

To maintain the consistency between this olh object and the bucket index, the index keeps a separate 'olh' entry for each object name. This entry stores a log of all writes/deletes to its versions. In ``src/rgw/driver/rados/rgw_rados.cc``, ``RGWRados::apply_olh_log()`` replays this log to guarantee that this olh object converges on the same "current" version as the bucket index.

.. _ListObjectsV2: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
.. _ListObjectVersions: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
.. _GET Container: https://docs.openstack.org/api-ref/object-store/?expanded=show-container-details-and-list-objects-detail#show-container-details-and-list-objects
