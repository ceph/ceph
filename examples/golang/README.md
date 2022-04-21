# Upload Object to Ceph Cluster using AWS sdk

This golang example shows us how to use a ceph cluster object gateway in place of an aws s3. A ceph cluster gateway is directly compatible with aws sdk or cli. So, you don't need to worry about rewriting your codebase. In more examples to come, you would be shown how to create and get notifications on a particular bucket and every other thing you can do with your aws cli or sdk

To understand it further, build the app: 
    
    $go build add-object.go

Then run the built binary with the value for the flags.

Make sure you have created an object bucket on your ceph cluster

You need to use this flags while running the built binary:

    objectPath: Provide an object file path
    bucketName: Provide the upload bucket path `example`
    endpoint: Provide Ceph rgw endpoint for cluster
    accessId: Provide your Ceph cluster rgw access id
    accessKey: Provide your Ceph cluster rgw access key

**Example:**

    $./add-object --endpoint https://localhost:8888 --accessId "jlsdjfladfjs" --accessKey "lkasdljsdfjdf2345ajkhfahdfad3423lksdjf" \
    --objectName ./ceph.png --bucketName s3://foo

**NOTE:** 

If you encounter errors with your id and key, wrap the access key in a string quote `""`, or remove any delimiter on your access key. An example of a delimiter is `/n`.

This error is most times caused by sdk or cli not knowing how to process delimiters. 

You can also, alternatively set your access key and file using the default aws config filepath `~./aws/config`.