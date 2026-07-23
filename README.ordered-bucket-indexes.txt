July 8, 2026

RGW Ordered Bucket Indexes


== Quick Start ==

This is an EXPERIMENTAL FEATURE at this point in time. Do NOT use this
branch for/on critical data. Even if you never try ordered bucket
indexing, this branch is not considered safe for critical data. This
branch is STRICTLY intended for early testing.

Newly created buckets start with hashed bucket indexing.

You can reshard a bucket from hashed to ordered bucket indexing with a
command along the lines of:

    radosgw-admin bucket reshard --bucket-index-type=ordered \
        --bucket=<bucket> --num-shards=<shard-count> \
        [--yes-i-really-mean-it] [--debug-rgw=20]

And you can return to hashed indexing with a command along the lines of:

    radosgw-admin bucket reshard --bucket-index-type=hashed \
        --bucket=<bucket> --num-shards=<shard-count> \
        [--yes-i-really-mean-it] [--debug-rgw=20]

IMPORTANT: At this time you cannot reshard a bucket from ordered to
ordered. That is currently being worked on. You can reshard a bucket
from hashed to hashed, though.

IMPORTANT: Do not use this branch with multisite.

IMPORTANT: Some features have been disabled until they can be fully
integratred with ordered bucket indexes.


== To Do List ==

* Allow ordered bucket index shards to be split manually.

* Allow ordered bucket index shards to be operated on by dynamic
  resharding.

* Test/fix all functionality that's temporarily turned off.

* Allow ordered bucket index shards to work with multisite.

* Develop new scheme for versioned buckets that would allow us to
  exceed 49,999 versions of a given object, which is the current
  limitation with ordered bucket indexes.


== Background ==

The bucket indexes contains metadata about the RGW objects in a
bucket. The data is stored in the OMAP of the index object(s). Since
RADOS imposes a soft limit of 100,000 OMAP entries per RADOS object, the
index for a bucket is generally spread across a set of RADOS objects
referred, and each is referred to as a bucket index shard.

In order to map RGW objects to a specific shard RGW has used a hashing
mechanism where the name of the RGW object is run through a hashing
function and we then do a modulo op with the number of shards to come
up with a shard index. That means that RGW objects are not lexically
ordered across shards.

That creates challenges when listing a bucket with the entries
lexically ordered as we must read batches of entries from all shards
and sort within those batches, and move forward in each shard and so
forth.

With ordered bucket indexes, each shard contains a lexical slice of
entry names. In other words, they contain a lexical range. This means
that to provide a lexically ordered list of RGW objects, we simply
access the shards sequentially.


== Implementation Details ==

In order to know which shard a given RGW object is mapped to, we use
the OMAP of the bucket instance object. The keys are the "split
points" that divide the lexical space into the shards.

More specifically, an upper_bound operation is used to find the
correct OMAP entry. So if the split point for shard N-1 is "pointer"
and the split point for shard N is "retriever" then entries that are
greater-than-or-equal to "pointer" and less-than "retriever" are
stored on shard N.

The final shard has a split point of the empty string (""), so we do
not need to track the lexically highest entry.

One important added detail -- the split points have a fixed prefix
that acts as a namespace. In case any other future feature were to
want to store OMAP entries in the bucket instance object, we need to
keep the two uses separate, and we'll do that by giving each a unique
prefix.

For ordered bucket indexes the prefix is "obi1.". The first letters
"obi" stand for "ordered bucket indexes". The "1" is there in case we
need to update the scheme and acts as a version number. And the "."
is used to separate the prefix (namespace) from the actual key.

So returning to the example above, shard N-1 would have the key
"obi1.pointer", shard N would have "obi1.retriever", and the final
shard "obi1.".

The values associated with those keys are:

  struct rgw_ordered_bi_omap_value {
    std::string split;
    rgw::NestedIndex shard_ident;
  };

The split is repeated and the NestedIndex is used to generate the oid
for the shard. With hashed bucket index shards, resharding was an
operation that replaced all of the shards. But with ordered bucket
index shards we have the option of splitting a shard into consecutive
shards or joining consecutive shards into one shard. Because we need
to potentially insert new shards between existing shards, we need a
more flexible naming scheme. And that's what NestedIndex does. It's
declared as:

    using NestedIndex = std::vector<int32_t>;

Each entry in the vector is a level. So if the vector contains {
64009, 3, 2 } then the oid would be
".dir.ed9ead10-97bb-46b8-9e40-526eba1e5faf.4179.1.ordered.1.64009.3.2".

Let's focus on "ordered.2.64009.3.2". We "tag" ordered index shards
with "ordered" (any reason not to? should it be shorter?). Then "2"
represents a generation number. And "64009.3.2" is the index. If the
next index is "64009.4" and we need two indexes in between we could
have "64009.3.3" and "64009.3.4".

Additionally each bucket index object contains the following in its
data (not its OMAP).

  struct rgw_ordered_bi_shard_data {
    std::string split;
    rgw::NestedIndex shard_ident;
    rgw::NestedIndex prev_shard_ident;
    rgw::NestedIndex next_shard_ident;
  };

This allows us to easily figure out what the neighboring shards are.
