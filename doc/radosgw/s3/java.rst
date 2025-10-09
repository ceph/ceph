.. _java:

Java S3 Examples
================

Pre-requisites
--------------

All examples are written against AWS Java SDK 2.17.42. You may need
to change some code when using another client.

Setup
-----

The following examples may require some or all of the following java
classes to be imported:

.. code-block:: java

	import java.net.URI;
	import java.net.URISyntaxException;
	import java.nio.ByteBuffer;
	import java.nio.file.Paths;
	import java.util.List;
	import java.util.ListIterator;
	import java.time.Duration;

	import software.amazon.awssdk.auth.credentials.AwsCredentials;
	import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
	import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
	import software.amazon.awssdk.core.sync.RequestBody;
	import software.amazon.awssdk.regions.Region;
	import software.amazon.awssdk.services.s3.S3Client;
	import software.amazon.awssdk.services.s3.model.Bucket;
	import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
	import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
	import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
	import software.amazon.awssdk.services.s3.model.S3Exception;
	import software.amazon.awssdk.services.s3.model.S3Object;
	import software.amazon.awssdk.services.s3.presigner.S3Presigner;
	import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;


If you are just testing the Ceph Object Storage services, consider
using HTTP protocol instead of HTTPS protocol. 

First, import the ``AwsBasicCredentials`` and ``S3Client`` classes.

.. code-block:: java

	import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
	import software.amazon.awssdk.services.s3.S3Client;

Then, use the client builder to create an S3 client:

.. code-block:: java

	AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);

	S3Client client = S3Client.builder()
		.endpointOverride(new URI("https://endpoint.com"))
		.credentialsProvider(StaticCredentialsProvider.create(credentials))
		.serviceConfiguration(srvcConf -> {
			srvcConf.pathStyleAccessEnabled();
		})
		.region(Region.US_EAST_1) // this is not used, but the AWS SDK requires it
		.build();


Listing Owned Buckets
---------------------

This gets a list of Buckets that you own.
This also prints out the bucket name and creation date of each bucket.

.. code-block:: java

	ListBucketsResponse lbResponse = client.listBuckets();
	for (Bucket bucket : lbResponse.buckets()) {
		System.out.println(bucket.name() + "\t" + bucket.creationDate());
	}

The output will look something like this::

   mahbuckat1    2021-09-20T14:12:57.231Z
   mahbuckat2    2021-09-20T14:12:59.402Z
   mahbuckat3    2021-09-20T14:13:02.288Z


Creating a Bucket
-----------------

This creates a new bucket called ``my-new-bucket``

.. code-block:: java

	client.createBucket(req -> {
		req.bucket("my-new-bucket");
	});


Listing a Bucket's Content
--------------------------
This gets a list of objects in the bucket.
This also prints out each object's name, the file size, and last
modified date.

.. code-block:: java

	ListObjectsResponse loResponse = client.listObjects(req -> {
		req.bucket("my-bucket");
	});

	for (S3Object object : loResponse.contents()) {
		System.out.println(
			object.key() + "\t" +
			object.size() + "\t" +
			object.lastModified()
		);
	}

The output will look something like this::

   myphoto1.jpg	251262	2021-09-20T17:47:07.317Z
   myphoto2.jpg	262518	2021-09-20T17:49:46.872Z


Deleting a Bucket
-----------------

.. note::
   The Bucket must be empty! Otherwise it won't work!

.. code-block:: java

	client.deleteBucket(req -> {
		req.bucket("my-new-bucket");
	});


Forced Delete for Non-empty Buckets
-----------------------------------
.. attention::
   not available


Creating an Object
------------------

This creates a file ``hello.txt`` with the string ``"Hello World!"``

.. code-block:: java

	ByteBuffer input = ByteBuffer.wrap("Hello World!".getBytes());
	client.putObject(
		req -> {
			req.bucket("my-bucket").key("hello.txt");
		},
		RequestBody.fromByteBuffer(input)
	);


Change an Object's ACL
----------------------

This makes the object ``hello.txt`` to be publicly readable, and
``secret_plans.txt`` to be private.

.. code-block:: java

	client.putObjectAcl(req -> {
		req.bucket("my-bucket").key("hello.txt").acl(ObjectCannedACL.PUBLIC_READ);
	});
	client.putObjectAcl(req -> {
		req.bucket("my-bucket").key("secret_plans.txt").acl(ObjectCannedACL.PRIVATE);
	});


Download an Object (to a file)
------------------------------

This downloads the object ``perl_poetry.pdf`` and saves it in
``/home/larry/documents``

.. code-block:: java

	client.getObject(
		req -> {
			req.bucket("my-bucket").key("perl_poetry.pdf");
		},
		Paths.get("/home/larry/documents/perl_poetry.pdf")
	);


Delete an Object
----------------

This deletes the object ``goodbye.txt``

.. code-block:: java

	client.deleteObject(req -> {
		req.bucket("my-bucket").key("goodbye.txt");
	});


Generate Object Download URLs (signed and unsigned)
---------------------------------------------------

This generates an unsigned download URL for ``hello.txt``. This works
because we made ``hello.txt`` public by setting the ACL above.
This then generates a signed download URL for ``secret_plans.txt`` that
will work for 1 hour. Signed download URLs will work for the time
period even if the object is private (when the time period is up, the
URL will stop working).

.. note::
   The java library does not have a method for generating unsigned
   URLs, so the example below just generates a signed URL.

.. code-block:: java

	S3Presigner presigner = S3Presigner.builder()
		.endpointOverride(new URI("https://endpoint.com"))
		.credentialsProvider(StaticCredentialsProvider.create(credentials))
		.region(Region.US_EAST_1) // this is not used, but the AWS SDK requires it
		.build();

	PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(preReq -> {
		preReq.getObjectRequest(req -> {
			req.bucket("my-bucket").key("secret_plans.txt");
		}).signatureDuration(
			Duration.ofMinutes(20)
		);
	});
	System.out.println(presignedRequest.url());

The output will look something like this::

   https://endpoint.com/my-bucket/secret_plans.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210921T151408Z&X-Amz-SignedHeaders=host&X-Amz-Expires=1200&X-Amz-Credential=XXXXXXXXXXXX%2F20210921%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=yyyyyyyyyyyyyyyyyyyyyy

