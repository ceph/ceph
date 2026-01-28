==================
Rados Bucket Index
==================

Buckets in RGW store their list of objects in a bucket index. Each
index entry stores just enough metadata (size, etag, mtime, etc.) to
serve API requests to list objects. These APIs are `ListObjectsV2`_
and `ListObjectVersions`_ in S3, and `GET Container`_ in Swift.

The entries are stored in the index object's (or index objects', see
sharding below) RADOS omap entries.

Buckets can also be created as 'indexless'. Such buckets have no
index and cannot be listed.

For non-versioned buckets there is one entry in the bucket index for
every object. For S3 versioned buckets there are two entries for each
version of an object, plus two additional fixed entries (see
:ref:`versioned-buckets` for details).

.. _versioned-buckets:

--------------------
S3 Object Versioning
--------------------

For versioned buckets the bucket index contains an entry for each
object version and delete marker. In addition to sorting index entries
by object name, it also has entries that sort object versions of the
same name from newest to oldest, which are used for versioned listings.

RGW stores a head object in the rgw.buckets.data pool for each object
version. This rados object's oid is a combination of the object name
and its version id.

In S3 a GET/HEAD request for an object name will give you that
object's "current" version. To support this RGW stores an extra
'object logical head' (olh) object whose oid includes the object name
only, that acts as an indirection to the head object of its current
version. This indirection logic is implemented in
``src/rgw/driver/rados/rgw_rados.cc`` as ``RGWRados::follow_olh()``.

To maintain the consistency between this olh object and the bucket
index, the index keeps a separate 'olh' entry for each object
name. This entry stores a log of all writes/deletes to its
versions. In ``src/rgw/driver/rados/rgw_rados.cc``,
``RGWRados::apply_olh_log()`` replays this log to guarantee that this
olh object converges on the same "current" version as the bucket
index.

.. _ListObjectsV2: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
.. _ListObjectVersions: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
.. _GET Container: https://docs.openstack.org/api-ref/object-store/?expanded=show-container-details-and-list-objects-detail#show-container-details-and-list-objects

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

A bucket's index is generally split into several RADOS objects that
are called bucket index shards. In RADOS multiple writes to the same
object cannot run in parallel. By spreading the index over more RADOS
objects, we increase its write parallelism. For a given object upload,
the corresponding bucket index shard is selected based on a hash of
the object's name. This guarantees that all entries for a given object
(and its versions, if it's an S3 versioned bucket) are located on the
same shard.

The default shard count for new buckets is 11, but can be overridden
in the zonegroup's ``bucket_index_max_shards`` or ceph.conf's
``rgw_override_bucket_index_max_shards``.

Because there's a practical limit of 100,000 entries for any RADOS
object's omap, we need to make sure that we use enough shards such
that we do not exceed this limit. As the number of objects in a bucket
grows, we may have to *reshard* a bucket index, so there are a
sufficient number of shards. The ``radosgw-admin`` administration
command allows a bucket to be resharded manually, but *dynamic
resharding* allows resharding to occur automatically without
administrative intervention.

More recently dynamic resharding can also reduce the number of shards
for a bucket. Because some buckets may experience rapid increases and
decreases in the number of objects and because we don't want to chase
those changes with multiple reshards, in order to reduce the number of
shards there's a delay between when the need to reduce the number of
shards is noted and when it will occur. At the time the resharding
would occur, the bucket is checked again and only proceeds if the
bucket still needs its number of bucket index shards reduced.  This
delay is by default 5 days but can be configured with ceph.conf's
``rgw_dynamic_resharding_reduction_wait``.

Information about the bucket's index object layout is stored in
``RGWBucketInfo`` as ``struct rgw::BucketLayout`` from
``src/rgw/rgw_bucket_layout.h``. The resharding logic is in
``src/rgw/driver/rados/rgw_reshard.cc``.

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
