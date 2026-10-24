## use case

in a replicated zonegroup, create a non-replicated bucket for data that doesn't require redundancy. if requirements change, reenable redundancy on the bucket

## design

extend the [CreateBucketConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucketConfiguration.html) xml document so it can specify a non-replicated bucket. when requested by [CreateBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html), the bucket is "pinned" to the zone that creates it

when requests on that bucket are sent to other zones, they're redirected to the pinned zone. these redirects prevent object uploads on other zones, and DeleteBucket requests are redirected and processed without ambiguity

if the pinned zone gets removed from the zonegroup, other zones no longer redirect but instead return an error like [410 Gone](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/410). if the pinned zone is never restored, the bucket must be removed by an admin before the bucket name can be reused elsewhere

the contents of pinned buckets are not replicated unless requested by bucket replication policy (to a different destination bucket)

to transition a non-replicated bucket to a replicated one:
* clear the pinned zone id in the bucket metadata so other zones stop redirecting, and
* write a datalog entry for every index shard so other zones start full syncing

without an S3 API for this, the transition would require a radosgw-admin command

transitioning in the other direction may require deleting data on multiple zones, and is probably not worth implementing. the user can instead create a new non-replicated bucket then server-side copy their objects into it

the same method can transition from non-replicated to replicated, avoiding the need for radosgw-admin access so S3 users can do it themselves

as an alternative to a custom rgw-only extension, there's a [DataRedundancy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_BucketInfo.html#AmazonS3-Type-BucketInfo-DataRedundancy) member of CreateBucketConfiguration:

> The number of Zone (Availability Zone or Local Zone) that's used for redundancy for the bucket.
> 
> Type: String
> 
> Valid Values: SingleAvailabilityZone | SingleLocalZone
> 
> Required: No

it's related to directory buckets so not a perfect fit, but we might still consider using `<DataRedundancy> SingleAvailabilityZone` for this

## tasks
* CreateBucket parses xml extension and stores pinned zone id with bucket instance metadata
* requests on pinned buckets redirect or 410
* data sync skips processing of pinned buckets
* optional: radosgw-admin command to reenable replication
