
# Golang Example to Upload Objects

---



### Description:

This example uploads objects in the bucket specified. The bucket is made by running the s3cmd tool after creating the vstart cluster. The access key and secret access key received after running ../src/vstart.sh that is creating the vstart cluster are very essential and need to be passed as arguments for this example.

The example also lists the objects present in the given bucket after uploading the object in the bucket.

Uploads a file to the Ceph bucket and object key is given by user. Also takes a duration value to terminate the update if it doesn't complete within that time.


### Usage:
   
Upload myfile.txt to myBucket/myKey. Must complete within given time or will fail
 Credentials required are the access key and the secret access key received on running ../src/vstart.sh
 
The session created will be for these given credentials.


### To run:

```
go run UploadObject.go -ak 0555b35654ad1656d804 -sk   h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q== -b mybucket(created using s3cmd) -k key-name -d 10m(duration)<filename(along with filepath)`
