while doing documentation do not forget to add how to use command and output for
success
# Upload Object to Ceph Cluster using AWS sdk

You need to use this flags while running the built binary:

    object Name: provide an object file path
    bucket name: provide the upload bucket path `example`
    endpoint: Provide ceph rgw endpoint for cluster
    accessId: Provide your ceph cluster rgw access id
    accessKey: provide your ceph cluster rgw access key

To understand it further run this example : 
    
    go build add-object.go

Then run the built binary with the value for the flags.

If you encounter errors with your id wrap the access key in a string quote or remove any delimeter on your access key. This problem occurs most times because our sdk might not know how to process delimeters in files.

You can alternatively set your config using aws config path `e.g` instead of the flags.