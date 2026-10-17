# s3express CreateSession

a session-based scheme for streamlined authentication/authorization of s3 object operations. in aws, this is specific to directory buckets. we'd like to explore its use in rgw for normal buckets

references:

https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-create-session.html
https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateSession.html

## design

s3express CreateSession api
- authorize based on iam policy for `s3express:CreateSession` action and optional `s3express:SessionMode` condition key for as ReadOnly vs ReadWrite
- encrypt a session token that includes:
-- expiration datetime
-- bucket name
-- any information necessary to authorize subsequent requests
-- encryption attributes if specified

for subsequent requests with `x-amz-s3session-token`:
- authenticate with sigv4 as normal, then
- decrypt token
- check expiration
- verify that the token's bucket matches the requested bucket
- for ReadOnly sessions, deny actions other than GetObject, HeadObject, ListObjectsV2, GetObjectAttributes, ListParts, and ListMultipartUploads
- for ReadWrite sessions, only deny CopyObject/UploadPartCopy? normal credentials are required to authorize access to source bucket/object
- teach rgw_s3_prepare_encrypt() to use encryption attrs from session token, not from x-amz-server-side-encryption-* request headers

rgw buckets support bucket/object acls, but these session tokens would bypass acl enforcement. aws directory buckets never have acls enabled (see https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-overview.html#directory-buckets-access-management):
> S3 Object Ownership is set to bucket owner enforced and access control lists (ACLs) are disabled. These settings can't be modified.

should we reject CreateSession requests for buckets that aren't configured for BucketOwnerEnforced?

## evaluation

are there potential performance improvements here? i think the idea is that, by doing authorization up front during CreateSession, subsequent requests could avoid the overhead of reading/evaluating iam policies. this _might_ mean the requests could avoid reading/decoding the user/account metadata at all. however, we do expect the metadata cache to hide a lot of this overhead
