# Upload Object to Ceph Cluster using AWS sdk

clone the repo and create a test cluster using these commands:

    $ ./install-deps.sh
    $ ./do_cmake.sh
    $ cd build
    $ ninja vstart
    $ RGW=1 ../src/vstart.sh -d -n -x

create a bucket in your ceph dashboard.

Build the app: 
    
    $ go build -o add-object

Then run the built binary with the values for the flags:

    - objectPath: Provide an object file path
    - bucketName: Provide the upload bucket path 
    - endpoint: Provide Ceph rgw endpoint for cluster
    - accessId: Provide your Ceph cluster rgw access id
    - accessKey: Provide your Ceph cluster rgw access key

**Example:**

    $ ./add-object --endpoint http://localhost:8000 --access_key "jlsdjfladfjs" --secret_key "lkasdljsdfjdf2345ajkhfahdfad3423lksdjf" \
    --objectpath ./ceph.png --bucketName foo

**Note:**  Make sure you have created an object bucket on your ceph cluster.


If you encounter errors with your id and key, wrap the access key in a string quote `""`, or remove any delimiter on your access key. An example of a delimiter is `/n`.

This error is most times caused by sdk or cli not knowing how to process delimiters. 

You can also, alternatively set your access key and file using the default aws config filepath `~./aws/config`.