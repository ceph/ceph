.. _java:

Java S3 Examples
================

Setup
-----

The following examples may require some or all of the following java
classes to be imported:

.. code-block:: java

	import java.io.ByteArrayInputStream;
	import java.io.File;
	import java.util.List;
	import com.amazonaws.auth.AWSCredentials;
	import com.amazonaws.auth.BasicAWSCredentials;
	import com.amazonaws.util.StringUtils;
	import com.amazonaws.services.s3.AmazonS3;
	import com.amazonaws.services.s3.AmazonS3Client;
	import com.amazonaws.services.s3.model.Bucket;
	import com.amazonaws.services.s3.model.CannedAccessControlList;
	import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
	import com.amazonaws.services.s3.model.GetObjectRequest;
	import com.amazonaws.services.s3.model.ObjectListing;
	import com.amazonaws.services.s3.model.ObjectMetadata;
	import com.amazonaws.services.s3.model.S3ObjectSummary;


If you are just testing the Ceph Object Storage services, consider
using HTTP protocol instead of HTTPS protocol. 

First, import the ``ClientConfiguration`` and ``Protocol`` classes. 

.. code-block:: java

	import com.amazonaws.ClientConfiguration;
	import com.amazonaws.Protocol;


Then, define the client configuration, and add the client configuration
as an argument for the S3 client.

.. code-block:: java

	AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
			 	
	ClientConfiguration clientConfig = new ClientConfiguration();
	clientConfig.setProtocol(Protocol.HTTP);
			
	AmazonS3 conn = new AmazonS3Client(credentials, clientConfig);
	conn.setEndpoint("endpoint.com");


Creating a Connection
---------------------

This creates a connection so that you can interact with the server.

.. code-block:: java

	String accessKey = "insert your access key here!";
	String secretKey = "insert your secret key here!";

	AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
	AmazonS3 conn = new AmazonS3Client(credentials);
	conn.setEndpoint("objects.dreamhost.com");


Listing Owned Buckets
---------------------

This gets a list of Buckets that you own.
This also prints out the bucket name and creation date of each bucket.

.. code-block:: java

	List<Bucket> buckets = conn.listBuckets();
	for (Bucket bucket : buckets) {
		System.out.println(bucket.getName() + "\t" +
			StringUtils.fromDate(bucket.getCreationDate()));
	}

The output will look something like this::

   mahbuckat1	2011-04-21T18:05:39.000Z
   mahbuckat2	2011-04-21T18:05:48.000Z
   mahbuckat3	2011-04-21T18:07:18.000Z


Creating a Bucket
-----------------

This creates a new bucket called ``my-new-bucket``

.. code-block:: java

	Bucket bucket = conn.createBucket("my-new-bucket");


Listing a Bucket's Content
--------------------------
This gets a list of objects in the bucket.
This also prints out each object's name, the file size, and last
modified date.

.. code-block:: java

	ObjectListing objects = conn.listObjects(bucket.getName());
	do {
		for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
			System.out.println(objectSummary.getKey() + "\t" +
				ObjectSummary.getSize() + "\t" +
				StringUtils.fromDate(objectSummary.getLastModified()));
		}
		objects = conn.listNextBatchOfObjects(objects);
	} while (objects.isTruncated());

The output will look something like this::

   myphoto1.jpg	251262	2011-08-08T21:35:48.000Z
   myphoto2.jpg	262518	2011-08-08T21:38:01.000Z


Deleting a Bucket
-----------------

.. note::
   The Bucket must be empty! Otherwise it won't work!

.. code-block:: java

	conn.deleteBucket(bucket.getName());


Forced Delete for Non-empty Buckets
-----------------------------------
.. attention::
   not available


Creating an Object
------------------

This creates a file ``hello.txt`` with the string ``"Hello World!"``

.. code-block:: java

	ByteArrayInputStream input = new ByteArrayInputStream("Hello World!".getBytes());
	conn.putObject(bucket.getName(), "hello.txt", input, new ObjectMetadata());


Change an Object's ACL
----------------------

This makes the object ``hello.txt`` to be publicly readable, and
``secret_plans.txt`` to be private.

.. code-block:: java

	conn.setObjectAcl(bucket.getName(), "hello.txt", CannedAccessControlList.PublicRead);
	conn.setObjectAcl(bucket.getName(), "secret_plans.txt", CannedAccessControlList.Private);


Download an Object (to a file)
------------------------------

This downloads the object ``perl_poetry.pdf`` and saves it in
``/home/larry/documents``

.. code-block:: java

	conn.getObject(
		new GetObjectRequest(bucket.getName(), "perl_poetry.pdf"),
		new File("/home/larry/documents/perl_poetry.pdf")
	);


Delete an Object
----------------

This deletes the object ``goodbye.txt``

.. code-block:: java

	conn.deleteObject(bucket.getName(), "goodbye.txt");


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

	GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucket.getName(), "secret_plans.txt");
	System.out.println(conn.generatePresignedUrl(request));

The output will look something like this::

   https://my-bucket-name.objects.dreamhost.com/secret_plans.txt?Signature=XXXXXXXXXXXXXXXXXXXXXXXXXXX&Expires=1316027075&AWSAccessKeyId=XXXXXXXXXXXXXXXXXXX

