.. _RGW-s3vectors:

==========
S3 Vectors
==========

.. versionadded:: Vampire

.. contents::

S3 Vectors provide a mechanism for storing, indexing, and querying high-dimension
vectors (embeddings) inside the RGW. Vectors are grouped into
*indexes*, and indexes are grouped into *vector buckets*. Each vector has a
key, the vector data itself, and an optional JSON metadata document that can be
used to filter query results.

The API implemented by the RGW follows the `AWS S3 Vectors`_ API. Applications
that already use the AWS SDK for S3 Vectors should be able to work in the same way
by pointing the SDK at the RGW. A small number of extensions to the AWS API
are provided, and are marked as such throughout this document.

Requests are sent as ``POST`` requests with a JSON body, and are signed using
AWS Signature Version 4 with the ``s3vectors`` service name.

.. note:: To enable the S3 Vectors API, the ``rgw_enable_apis`` configuration
   parameter should contain: "s3vectors".

Storage Backends
----------------

The vector data and the indexes built on top of it are stored by an embedded
`LanceDB`_ engine. The location in which LanceDB stores its files is selected
with:

.. confval:: rgw_s3vector_backend

Three backends are supported:

- ``rgw`` (the default): LanceDB files are stored in the same Ceph cluster,
  through the RGW storage abstraction layer. No external service and no extra
  configuration are needed.
- ``s3``: LanceDB files are stored in an S3-compatible object store.
  The endpoint is configured centrally, while the credentials used to access it
  are the credentials of the user that issued the S3 Vectors request.
  The endpoint must be reachable from the RGW process, and the user must have
  access to it with its credentials. Using this option may give better visibility
  to the data being stored, allow for quota management, rate-limiting etc.

  .. tip:: using ``http://localhost:<port>`` (where ``<port>`` is the port
  configured for the RGW's frontend) lets the RGW send the S3 requests to itself.
  make sure to set: ``rgw_s3vector_s3_allow_http = true`` in this case.

  .. tip:: setting the endpoint to be the RGW's load balancer (if one exists),
  may achieve better load distribution.

- ``local``: LanceDB files are stored on the local filesystem of the RGW
  process. This backend is intended for testing and single RGW setups only:
  vector buckets created on one RGW will not be usable from another one.

The ``local`` backend is configured with:

.. confval:: rgw_s3vector_local_path

The ``s3`` backend is configured with:

.. confval:: rgw_s3vector_s3_endpoint
.. confval:: rgw_s3vector_s3_region
.. confval:: rgw_s3vector_s3_allow_http

.. warning:: ``rgw_s3vector_s3_allow_http`` should only be enabled when the
   configured endpoint is reachable over a trusted network, since the user
   credentials would otherwise be sent in cleartext.


Backing Buckets
~~~~~~~~~~~~~~~

With the ``rgw`` and the ``s3`` backends, LanceDB stores its files inside a
regular S3 bucket. The name of that bucket is identical to the name of the
vector bucket. **This bucket is not created automatically**: it must be created
before ``CreateVectorBucket`` is called.

For example, to create a vector bucket named ``my-vectors``, first create a
regular S3 bucket named ``my-vectors`` (at the RGW endpoint for the ``rgw``
backend, or at the endpoint configured in ``rgw_s3vector_s3_endpoint`` for the
``s3`` backend), and only then call ``CreateVectorBucket``.

.. note:: S3 buckets are kept in separate namespaces, and a name may be used by both
   at the same time. Listing S3 buckets does not show vector buckets,
   and listing vector buckets does not show S3 buckets.

Ownership
`````````

Since the credentials used for the S3 bucket are the same as the credentials
used in the S3 vectors request, it is advised that the same user will be owner of both.
If not, proper policy must be set to allow the user to access the S3 bucket.

Because the backing bucket holds the raw vector data, anyone who is granted
plain S3 access to it can read, modify or delete vectors and indexes without
passing through the S3 Vectors API and its permission checks. Write access to the
backing bucket should therefore be restricted to its owner, or to the users
performing the vector bucket operations.

Recommended Bucket Configuration
````````````````````````````````

The following is recommended for any bucket that backs a vector bucket:

- **Disable sync on the bucket.** In a multisite configuration, each zone
  maintains its own copy of the vector data (see `Multisite`_ below).

  .. prompt:: bash #

     radosgw-admin bucket sync disable --bucket={bucket-name}

  Alternatively, use a bucket-level sync policy that does not enable any pipe
  for this bucket. See: :ref:`radosgw-multisite-sync-policy`.

- **Do not enable object locking.** LanceDB continuously rewrites and removes
  files as vectors are added, deleted and compacted, which does not go well
  with a retention period.

- **Do not enable versioning.** LanceDB does not support versioned objects, and
  object deletions would leave behind stale files that are not cleaned up.


Multisite
---------

In a multisite configuration, S3 Vectors entities are not synced between the
zones of a zonegroup. Neither the metadata of the vector buckets and their
indexes, nor the vector data itself, is synced.

Each zone has its own vector buckets, and a vector bucket has to be created in
every zone in which it is needed. A vector bucket of the same name may exist in
more than one zone, holding different indexes and vectors.

The backing bucket of a vector bucket is a regular S3 bucket, and its own sync
configuration applies to it. It should not be synced. See:
`Recommended Bucket Configuration`_.

 .. note:: When an ``s3`` backend is used, all endnpoits must belong to the same zone/site.

Metadata Filtering
------------------

Every vector may carry a JSON metadata document. ``QueryVectors`` accepts a
``filter`` expression over that metadata, so that only the vectors matching the
filter participate in the result. See: `AWS metadata filtering`_.

Filter Syntax
~~~~~~~~~~~~~

The filter is a JSON object following the syntax used by `AWS metadata
filtering`_. It is evaluated against the metadata document of each vector, and
only the vectors for which it evaluates to true take part in the result. An
empty filter
(``{}``) matches every vector.

A filter that cannot be evaluated is rejected before the query runs, with a
``ValidationException`` whose ``fieldList`` points at ``filter``. See:
`S3 Vectors REST API`_.

The examples below assume vectors carrying metadata of this shape::

   {"genre": "rock", "year": 2021, "active": true, "tags": ["live", "remaster"]}


Field Conditions
````````````````

A filter object maps metadata field names to conditions. A condition is either
a bare value, which means equality, or an object of operators::

   {"genre": "rock"}                       // genre == "rock"
   {"genre": {"$eq": "rock"}}              // the same condition, written explicitly
   {"year": {"$gte": 2000, "$lt": 2026}}   // 2000 <= year < 2026

Multiple operators on the same field are combined with a logical AND, and so
are multiple fields at the top level of the filter::

   {"genre": "rock", "year": {"$gte": 2000}}   // genre == "rock" AND year >= 2000

Field names always refer to top-level keys of the metadata document. Nested
documents cannot be addressed, and a name that contains a ``.`` is rejected with
a validation error, since a metadata key may not contain a ``.`` either.

A field that was declared in ``nonFilterableMetadataKeys`` must not appear in a filter at all.
A field that was declared in ``filterableMetadataKeys`` would be used in pre-filtering.
A field which appeared in neither of these list would be used in post-filtering.
See: `Pre-Filtering and Post-Filtering`_.

Comparison Operators
````````````````````

- ``$eq``, ``$ne``: equal, not equal.
- ``$gt``, ``$gte``, ``$lt``, ``$lte``: greater than, greater or equal, less
  than, less or equal. Numbers are compared numerically, strings
  lexicographically.

Each of them takes a single string, number or boolean value::

   {"genre": {"$ne": "blues"}}
   {"active": {"$eq": true}}
   {"year": {"$gt": 2019, "$lt": 2026}}

The value may not be ``null``, an array, or an object. These are all rejected::

   {"genre": null}                     // null is not a value
   {"genre": {"$ne": null}}            // also as an operand
   {"tags": ["live"]}                  // implicit equality with an array
   {"tags": {"$eq": ["live"]}}         // arrays are not comparable
   {"genre": {"$eq": {"a": "b"}}}      // objects are not comparable
   {"genre": {"$regex": "ro.*"}}       // unknown operator

``$in`` and ``$nin``
````````````````````

``$in`` matches when the field is equal to one of the values in the list, and
``$nin`` when it is equal to none of them. Both take a non-empty JSON array of
scalar values::

   {"genre": {"$in": ["rock", "jazz"]}}
   {"year": {"$nin": [2020, 2021]}}
   {"genre": {"$in": ["rock"]}}          // a single-element list is fine

The array must hold scalars only, and it must not be empty. For a field that is
not filterable, all the elements must also be of the same JSON type,
since the type of the elements determines how the metadata field is read. These
are rejected::

   {"genre": {"$in": []}}                // empty list
   {"genre": {"$in": "rock"}}            // not a list
   {"genre": {"$in": {"a": "b"}}}        // not a list
   {"genre": {"$in": ["rock", 42]}}      // mixed types
   {"genre": {"$in": [null, "rock"]}}    // null element
   {"genre": {"$in": [{"a": "b"}]}}      // object element

For a filterable field, each element is converted to the declared type of the
field instead, so a list such as ``["rock", 42]`` is accepted on a ``String``
field and read as ``["rock", "42"]``.

Note that ``$in`` tests the *value of the field*, not membership in a stored
list. See `Lists in the Metadata`_ below.

``$exists``
```````````

``$exists`` tests whether the field is present, and takes an unquoted ``true``
or ``false``::

   {"tags": {"$exists": true}}    // the vector has a "tags" field
   {"tags": {"$exists": false}}   // the vector has no "tags" field

Anything else is rejected, including a quoted boolean::

   {"tags": {"$exists": "true"}}
   {"tags": {"$exists": 1}}

For a field stored in the metadata document, ``$exists`` only checks that the
key is there; its value may be of any type, including an array or a nested
object. A vector that was stored without any metadata has no keys at all, so
``$exists: false`` matches it. For a filterable field, that was declared with
``mustExist`` the key is always there, so ``$exists: true`` matches every
vector of the index and ``$exists: false`` matches none.

Logical Operators
`````````````````

``$and`` and ``$or`` take a non-empty array of filter objects, and may be
nested to any depth::

   {"$and": [{"genre": "rock"}, {"year": {"$gte": 2000}}]}
   {"$or": [{"genre": "rock"}, {"genre": "jazz"}]}

   {
       "$and": [
           {"$or": [{"genre": "rock"}, {"genre": "jazz"}]},
           {"year": {"$gte": 2000, "$lte": 2025}},
           {"active": true}
       ]
   }

The array must be non-empty, and each of its elements must be a filter object::

   {"$and": []}                    // empty array
   {"$or": {"genre": "rock"}}      // not an array
   {"$or": [1, 2, 3]}              // elements are not filter objects

There is no ``$not`` operator. A negation is expressed with ``$ne``, ``$nin``
or ``$exists: false``.

``$or`` has an additional restriction when some of the fields are filterable.
See: `Post-Filtering Limitations`_.

Value Types
```````````

For a field that is stored in the metadata document, the JSON type of the value
in the filter selects how the field is read: a quoted value is read as a
string, ``true``/``false`` as a boolean, and anything else as a number.
A condition therefore only matches when the types agree::

   {"year": 2021}               // matches metadata {"year": 2021}
   {"year": "2021"}             // does not match it: "year" is read as a string
   {"genre": {"$eq": 42}}       // does not match it: "genre" is read as a number
   {"active": {"$eq": "true"}}  // does not match it: "active" is read as a string

For a field that is declared as a filterable field, the declared type of the
field wins, and the value from the filter is converted to it. Quoting does not
matter, and a value that cannot be converted is rejected::

   {"year": {"$eq": "2021"}}    // Number field: accepted, converted to 2021
   {"genre": {"$eq": 42}}       // String field: accepted, converted to "42"
   {"active": {"$eq": "true"}}  // Boolean field: accepted, converted to true
   {"year": {"$eq": "recent"}}  // Number field: rejected
   {"active": {"$eq": "yes"}}   // Boolean field: rejected

Lists in the Metadata
`````````````````````

A metadata field whose value is a list, such as ``"tags"`` above, can only be
tested with ``$exists``. There is no operator for testing the members of a
stored list: the comparison operators, ``$in`` and ``$nin`` all compare the
*value of the field* against the value in the filter, and a list value never
compares equal to a scalar. So with the metadata above::

   {"tags": {"$exists": true}}      // matches
   {"tags": "live"}                 // does not match
   {"tags": {"$in": ["live"]}}      // does not match

This holds for filterable fields as well, and there it is an error rather than
a non-match: a key declared with one of the list types (``StringList``,
``NumberList``, ``BooleanList``) supports ``$exists`` only, and any other
operator on it is rejected with a validation error.

.. note:: This is a divergence from the AWS S3 Vectors API. Array
   membership is not supported here, and such a filter does not match. See:
   `AWS metadata filtering`_.

Missing Fields
``````````````

A condition on a field that the vector does not have never matches. This
includes the negative operators: a vector without a ``genre`` field is not
matched by ``{"genre": {"$ne": "rock"}}`` or by
``{"genre": {"$nin": ["rock"]}}``. To include those vectors, combine the
condition with ``$exists``::

   {"$or": [{"genre": {"$ne": "rock"}}, {"genre": {"$exists": false}}]}

The same applies to a filterable field that was not declared with
``mustExist``.

Pre-Filtering and Post-Filtering
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, metadata is stored as a JSON document next to the vector, and is
not part of the index schema. A filter over such a field can only be evaluated
after the vector search has returned its candidates. This is called
*post-filtering*: the index is asked for ``topK`` nearest vectors, and the
filter is then applied to that result set. Vectors that do not match are
dropped, so the query may return fewer than ``topK`` results even when enough
matching vectors exist in the index.

To avoid that, an index may declare some metadata fields as *filterable*. A
filterable field is stored as a real field of the index, and a filter over it
is pushed down into the vector search itself. This is called *pre-filtering*:
only matching vectors are considered by the search, so a query returns ``topK``
results whenever the index holds at least ``topK`` matching vectors.

Filterable fields are declared at index creation time, using the
``filterableMetadataKeys`` field of ``metadataConfiguration``. This is an
extension to the AWS S3 Vectors API. See: `Create Index`_.

.. note:: The AWS API has a ``nonFilterableMetadataKeys`` list, which is also
   supported. It marks fields that may never be used in a filter: a filter that
   references such a field is rejected with a validation error.

A single filter may mix filterable fields and plain JSON metadata fields. In
that case the conditions on the filterable fields are pushed down into the
search, and the conditions on the JSON fields are applied to the search result.

Post-Filtering Limitations
~~~~~~~~~~~~~~~~~~~~~~~~~~

Since post-filtering discards vectors that were already returned by the search,
some oversampling is needed to have a reasonable chance of returning ``topK``
results. Whenever any part of the filter must be post-filtered, the number of
candidates fetched from the index is ``topK`` multiplied by:

.. confval:: rgw_s3vector_topk_post_filter_factor

With the default value of ``1`` no oversampling is done. Raising it increases
the chance of returning a full result set at the cost of reading and filtering
more candidates. Note that even with a high factor, returning ``topK`` results
is never guaranteed, since the matching vectors may all be far away from the
query vector.

The ``$or`` operator has an additional limitation: its branches must be either
all filterable fields or all JSON metadata fields. A ``$or`` that mixes the
two cannot be evaluated correctly, because one half of the expression would be
applied before the search and the other half after it, and the request is
rejected with a validation error. To run such a query, set ``postFiltering`` to
``true`` in the ``QueryVectors`` request. This is an extension to the AWS S3
Vectors API, which forces the whole filter to be evaluated as post-filtering:
declared filterable fields are ignored, and every condition is evaluated
against the JSON metadata document.

S3 Vectors REST API
-------------------

.. note::

    In all S3 Vectors actions, the operation name is the path of the request,
    the parameters are sent in the message body using this content type:
    ``application/json``, and the response body is JSON as well::

       POST /<ActionName> HTTP/1.1
       Content-Type: application/json

       {<request parameters>}

Vector buckets and indexes may be identified either by name or by `ARN`_. Both
may not be given in the same request. The ARN formats are::

   arn:aws:s3vectors:<zone-group>:<account>:bucket/<vector-bucket>
   arn:aws:s3vectors:<zone-group>:<account>:bucket/<vector-bucket>/index/<index>

In case that ``CreateIndex``, ``PutVectors`` and ``QueryVectors``,
encounter errors that are the result of an invalid request, they return  a ``400``
status, the ``ValidationException`` code, and a ``fieldList`` array that points
at the offending fields::

    {
        "code": "ValidationException",
        "message": "The requested action isn't valid.",
        "fieldList": [
            {
                "path": "vectors[0].data",
                "message": "expected dimension 4 but got 3"
            }
        ]
    }

Other requests that encounter errors that are the result of an invalid request return a ``400`` status and
the ``InvalidArgument`` code, without a ``fieldList``.

Vector Buckets
~~~~~~~~~~~~~~

Create Vector Bucket
````````````````````

Creates a new vector bucket. The bucket name must be between 3 and 63
characters long. With the ``rgw`` and ``s3`` backends, a regular S3 bucket with
the same name must already exist. See: `Backing Buckets`_.

::

   POST /CreateVectorBucket

   {
       "vectorBucketName": "<vector-bucket>"
   }

The response has the following format:

::

    {
        "vectorBucketArn": "arn:aws:s3vectors:::bucket/<vector-bucket>"
    }

Get Vector Bucket
`````````````````

Returns information about a specific vector bucket.

::

   POST /GetVectorBucket

   {
       "vectorBucketName": "<vector-bucket>" | "vectorBucketArn": "<vector-bucket-arn>"
   }

The response has the following format:

::

    {
        "vectorBucket": {
            "creationTime": "<iso-8601-timestamp>",
            "vectorBucketArn": "<vector-bucket-arn>",
            "vectorBucketName": "<vector-bucket>"
        }
    }

List Vector Buckets
```````````````````

Lists the vector buckets owned by the requesting user.

::

   POST /ListVectorBuckets

   {
       ["maxResults": <number>,]
       ["nextToken": "<token>",]
       ["prefix": "<prefix>"]
   }

Request parameters:

- ``maxResults``: The maximum number of buckets to return. Must be between 1
  and 500. (This is 500 by default.)
- ``nextToken``: The pagination token returned by a previous call.
- ``prefix``: Return only buckets whose name starts with this prefix. Must be
  between 1 and 63 characters long.

The response has the following format:

::

    {
        "nextToken": "<token>",
        "vectorBuckets": [
            {
                "creationTime": "<iso-8601-timestamp>",
                "vectorBucketArn": "<vector-bucket-arn>",
                "vectorBucketName": "<vector-bucket>"
            }
        ]
    }

``nextToken`` is present only if more buckets exist beyond the returned page.

Delete Vector Bucket
````````````````````

Deletes a vector bucket. All of the indexes of the vector bucket must be
deleted before the vector bucket itself can be deleted.

::

   POST /DeleteVectorBucket

   {
       "vectorBucketName": "<vector-bucket>" | "vectorBucketArn": "<vector-bucket-arn>"
   }

.. note:: A vector bucket that still holds indexes is not deleted. The request
   fails with a ``409`` status and the ``BucketNotEmpty`` code, and neither the
   bucket nor its indexes are modified. Use `Delete Index`_ on each of the
   indexes of the bucket, and then delete the bucket.

.. note:: The backing S3 bucket itself is not deleted, and should be removed
   separately if it is no longer needed.

Vector Bucket Policy
````````````````````

The ``PutVectorBucketPolicy``, ``GetVectorBucketPolicy`` and
``DeleteVectorBucketPolicy`` actions exist, and validate that the vector bucket
exists, but the policy itself is not yet stored or enforced.

::

   POST /PutVectorBucketPolicy

   {
       "policy": "<policy-JSON-string>",
       "vectorBucketName": "<vector-bucket>" | "vectorBucketArn": "<vector-bucket-arn>"
   }

::

   POST /GetVectorBucketPolicy

   {
       "vectorBucketName": "<vector-bucket>" | "vectorBucketArn": "<vector-bucket-arn>"
   }

::

   POST /DeleteVectorBucketPolicy

   {
       "vectorBucketName": "<vector-bucket>" | "vectorBucketArn": "<vector-bucket-arn>"
   }

Indexes
~~~~~~~

Create Index
````````````

Creates a new index inside a vector bucket. The index name must be between 3
and 63 characters long.

::

   POST /CreateIndex

   {
       "dataType": "float32",
       "dimension": <number>,
       "distanceMetric": "cosine" | "euclidean",
       "indexName": "<index>",
       ["metadataConfiguration": {
           ["nonFilterableMetadataKeys": ["<key>"],]
           ["filterableMetadataKeys": [
               {
                   "name": "<key>",
                   ["type": "String" | "Number" | "Boolean" | "StringList" | "NumberList" | "BooleanList",]
                   ["mustExist": true|false]
               }
           ]]
       },]
       "vectorBucketName": "<vector-bucket>" | "vectorBucketArn": "<vector-bucket-arn>"
   }

Request parameters:

- ``dataType``: The type of the vector elements. Only ``float32`` is supported.
- ``dimension``: The number of elements in each vector. Must be between 1 and
  4096. All vectors written to the index must have exactly this dimension.
- ``distanceMetric``: The metric used to compare vectors, either ``cosine`` or
  ``euclidean``.
- ``metadataConfiguration.nonFilterableMetadataKeys``: Up to 10 metadata keys
  that may not be used in a query filter. A filter that references one of them
  is rejected. Key names must not contain a ``.``.
- ``metadataConfiguration.filterableMetadataKeys``: Up to 10 metadata keys that
  are stored as fields of the index, so that filters over them are evaluated
  before the vector search. (This is an extension to the S3 Vectors API. See:
  `Metadata Filtering`_.) Each entry has:

  - ``name``: The metadata key. Must not start with ``_`` and must not contain
    a ``.``. A key may not appear both in ``filterableMetadataKeys`` and in
    ``nonFilterableMetadataKeys``.
  - ``type``: The type of the value. (This is ``String`` by default.) The list
    types may be stored and returned but cannot be used in a filter.
  - ``mustExist``: If "true", every vector written to the index must have this
    key in its metadata, and a ``PutVectors`` request that omits it fails with
    a validation error. (This is "false" by default.)

The response has the following format:

::

    {
        "indexArn": "<index-arn>"
    }

Get Index
`````````

Returns the configuration of a specific index.

::

   POST /GetIndex

   {
       "indexName": "<index>",
       "vectorBucketName": "<vector-bucket>" | "indexArn": "<index-arn>"
   }

The response has the following format:

::

    {
        "index": {
            "creationTime": <number>,
            "dataType": "float32",
            "dimension": <number>,
            "distanceMetric": "cosine" | "euclidean",
            "indexArn": "<index-arn>",
            "indexName": "<index>",
            "metadataConfiguration": {
                "nonFilterableMetadataKeys": ["<key>"],
                "filterableMetadataKeys": [
                    {"name": "<key>", "type": "<type>", "mustExist": true|false}
                ]
            },
            "vectorBucketName": "<vector-bucket>"
        }
    }

List Indexes
````````````

Lists the indexes of a vector bucket.

::

   POST /ListIndexes

   {
       ["maxResults": <number>,]
       ["nextToken": "<token>",]
       ["prefix": "<prefix>",]
       "vectorBucketName": "<vector-bucket>" | "vectorBucketArn": "<vector-bucket-arn>"
   }

Request parameters:

- ``maxResults``: The maximum number of indexes to return. Must be between 1
  and 500. (This is 500 by default.)
- ``nextToken``: The pagination token returned by a previous call.
- ``prefix``: Return only indexes whose name starts with this prefix. Must be
  between 1 and 63 characters long.

The response has the following format:

::

    {
        "indexes": [
            {
                "creationTime": <number>,
                "indexArn": "<index-arn>",
                "indexName": "<index>",
                "vectorBucketName": "<vector-bucket>"
            }
        ],
        "nextToken": "<token>"
    }

Delete Index
````````````

Deletes an index and all of the vectors it holds.

::

   POST /DeleteIndex

   {
       "indexName": "<index>",
       "vectorBucketName": "<vector-bucket>" | "indexArn": "<index-arn>"
   }

.. note:: An index that still holds vectors may be deleted. Its vectors are
   removed together with it, and there is no need to delete them first.

.. note:: Deleting an index that does not exist is not considered an error, and
   returns a ``200`` status.

Vectors
~~~~~~~

Put Vectors
```````````

Adds vectors to an index, or overwrites vectors that already exist under the
same keys.

::

   POST /PutVectors

   {
       "indexName": "<index>",
       "vectorBucketName": "<vector-bucket>" | "indexArn": "<index-arn>",
       "vectors": [
           {
               "key": "<key>",
               "data": {"float32": [<float>]},
               ["metadata": {<JSON object>}]
           }
       ]
   }

Request parameters:

- ``vectors``: Between 1 and 500 vectors. Each has:

  - ``key``: The identifier of the vector inside the index. Must be between 1
    and 1024 characters long.
  - ``data.float32``: The vector itself. Its length must match the ``dimension``
    of the index.
  - ``metadata``: An optional JSON object attached to the vector. It may hold up
    to 50 top-level fields, and must not exceed 40KB. Field names must not
    contain a ``.``, and ``null`` values are not supported. The field names and
    the ``null`` values are validated for the top-level fields of the document
    only: a nested document may hold names with a ``.`` and ``null`` values, but
    its fields cannot be used in a filter. (See: `Filter Syntax`_.) Any field
    that was declared in ``filterableMetadataKeys`` must hold a value of the
    declared type, and must be present if the key was declared with
    ``mustExist``.

An empty response body is returned on success.

Get Vectors
```````````

Returns specific vectors by key.

::

   POST /GetVectors

   {
       "indexName": "<index>",
       "vectorBucketName": "<vector-bucket>" | "indexArn": "<index-arn>",
       "keys": ["<key>"],
       ["returnData": true|false,]
       ["returnMetadata": true|false]
   }

Request parameters:

- ``keys``: Between 1 and 100 keys. Each key must be between 1 and 1024
  characters long.
- ``returnData``: If "true", the vector data is returned. (This is "false" by
  default.)
- ``returnMetadata``: If "true", the vector metadata is returned. (This is
  "false" by default.)

The response has the following format:

::

    {
        "vectors": [
            {
                "key": "<key>",
                "data": {"float32": [<float>]},
                "metadata": {<JSON object>}
            }
        ]
    }

.. note:: Keys that do not exist in the index are silently omitted from the
   response.

List Vectors
````````````

Lists the vectors of an index.

::

   POST /ListVectors

   {
       "indexName": "<index>",
       "vectorBucketName": "<vector-bucket>" | "indexArn": "<index-arn>",
       ["maxResults": <number>,]
       ["nextToken": "<token>",]
       ["returnData": true|false,]
       ["returnMetadata": true|false,]
       ["segmentCount": <number>,]
       ["segmentIndex": <number>]
   }

Request parameters:

- ``maxResults``: The maximum number of vectors to return. Must be between 1
  and 1000. (This is 500 by default.)
- ``nextToken``: The pagination token returned by a previous call. Note that
  in our implementation the token is a numeric offset and not an opaque string.
- ``returnData``: If "true", the vector data is returned. (This is "false" by
  default.)
- ``returnMetadata``: If "true", the vector metadata is returned. (This is
  "false" by default.)

.. note:: Segmented listing is not implemented. Only a single segment may be
   requested: ``segmentCount`` must be 1 and ``segmentIndex`` must be 0, which
   lists the entire index.

The response has the following format:

::

    {
        "nextToken": "<token>",
        "vectors": [
            {
                "key": "<key>",
                "data": {"float32": [<float>]},
                "metadata": {<JSON object>}
            }
        ]
    }

.. note:: The order in which vectors are listed is not defined, and vectors
   that are added or removed while a listing is in progress may or may not be
   returned.

Query Vectors
`````````````

Returns the vectors of an index that are nearest to a given query vector,
according to the ``distanceMetric`` of the index.

::

   POST /QueryVectors

   {
       "indexName": "<index>",
       "vectorBucketName": "<vector-bucket>" | "indexArn": "<index-arn>",
       "queryVector": {"float32": [<float>]},
       "topK": <number>,
       ["filter": {<filter expression>},]
       ["returnDistance": true|false,]
       ["returnMetadata": true|false,]
       ["postFiltering": true|false]
   }

Request parameters:

- ``queryVector.float32``: The vector to search for. Its length must match the
  ``dimension`` of the index.
- ``topK``: The maximum number of vectors to return. Must be between 1 and
  10000.
- ``filter``: A filter over the vector metadata. See: `Metadata Filtering`_.
- ``returnDistance``: If "true", the distance from the query vector is returned
  for each result. (This is "false" by default.)
- ``returnMetadata``: If "true", the vector metadata is returned. (This is
  "false" by default.)
- ``postFiltering``: If "true", the whole filter is evaluated against the JSON
  metadata after the search, and filterable fields are ignored. (This is
  "false" by default, and is an extension to the S3 Vectors API. See:
  `Post-Filtering Limitations`_.)

The response has the following format:

::

    {
        "distanceMetric": "cosine" | "euclidean",
        "vectors": [
            {
                "distance": <float>,
                "key": "<key>",
                "metadata": {<JSON object>}
            }
        ]
    }

Results are ordered by their distance from the query vector, nearest first.

Delete Vectors
``````````````

Deletes specific vectors from an index by key.

::

   POST /DeleteVectors

   {
       "indexName": "<index>",
       "vectorBucketName": "<vector-bucket>" | "indexArn": "<index-arn>",
       "keys": ["<key>"]
   }

Request parameters:

- ``keys``: Between 1 and 500 keys. Each key must be between 1 and 1024
  characters long.

.. note:: Deleting a key that does not exist in the index is not considered an
   error.

Permissions
-----------

Access to the S3 Vectors actions is controlled by identity-based policies,
using the following actions:

- ``s3vectors:CreateVectorBucket``
- ``s3vectors:GetVectorBucket``
- ``s3vectors:ListVectorBuckets``
- ``s3vectors:DeleteVectorBucket``
- ``s3vectors:PutVectorBucketPolicy``
- ``s3vectors:GetVectorBucketPolicy``
- ``s3vectors:DeleteVectorBucketPolicy``
- ``s3vectors:CreateIndex``
- ``s3vectors:GetIndex``
- ``s3vectors:ListIndexes``
- ``s3vectors:DeleteIndex``
- ``s3vectors:PutVectors``
- ``s3vectors:GetVectors``
- ``s3vectors:ListVectors``
- ``s3vectors:QueryVectors``
- ``s3vectors:DeleteVectors``

The resource of a policy statement is the ARN of the vector bucket or of the
index. For example:

::

    {
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": ["arn:aws:iam::usfolks:user/fred"]},
        "Action": ["s3vectors:QueryVectors", "s3vectors:GetVectors"],
        "Resource": ["arn:aws:s3vectors:::bucket/my-vectors/index/my-index"]
      }]
    }

.. note:: Anonymous access to the S3 Vectors API is not allowed.

.. _AWS S3 Vectors: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors.html
.. _AWS metadata filtering: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-metadata-filtering.html
.. _LanceDB: https://lancedb.com/
.. _ARN: https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html
