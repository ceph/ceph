.. _php:

PHP S3 Examples
===============

Installing AWS PHP SDK
----------------------

This installs AWS PHP SDK using composer (see here_ how to install composer).

.. _here: https://getcomposer.org/download/
  
.. code-block:: bash

	$ composer install aws/aws-sdk-php

Creating a Connection
---------------------

This creates a connection so that you can interact with the server.

.. note::

   The client initialization requires a region so we use ``''``.

.. code-block:: php

	<?php
	
	use Aws\S3\S3Client;
	
	define('AWS_KEY', 'place access key here');
	define('AWS_SECRET_KEY', 'place secret key here');
	$ENDPOINT = 'http://objects.dreamhost.com';

	// require the amazon sdk from your composer vendor dir
	require __DIR__.'/vendor/autoload.php';

	// Instantiate the S3 class and point it at the desired host
	$client = new S3Client([
	    'region' => '',
	    'version' => '2006-03-01',
	    'endpoint' => $ENDPOINT,
	    'credentials' => [
                'key' => AWS_KEY,
		'secret' => AWS_SECRET_KEY
	    ],
	    // Set the S3 class to use objects.dreamhost.com/bucket
	    // instead of bucket.objects.dreamhost.com
	    'use_path_style_endpoint' => true
	]);

Listing Owned Buckets
---------------------
This gets a ``AWS\Result`` instance that is more convenient to visit using array access way.
This also prints out the bucket name and creation date of each bucket.

.. code-block:: php

	<?php
	$listResponse = $client->listBuckets();
	$buckets = $listResponse['Buckets'];
	foreach ($buckets as $bucket) {
    	    echo $bucket['Name'] . "\t" . $bucket['CreationDate'] . "\n";
	}

The output will look something like this::

   mahbuckat1	2011-04-21T18:05:39.000Z
   mahbuckat2	2011-04-21T18:05:48.000Z
   mahbuckat3	2011-04-21T18:07:18.000Z


Creating a Bucket
-----------------

This creates a new bucket called ``my-new-bucket`` and returns a
``AWS\Result`` object.

.. code-block:: php

	<?php
	$client->createBucket(['Bucket' => 'my-new-bucket']);


List a Bucket's Content
-----------------------

This gets a ``AWS\Result`` instance that is more convenient to visit using array access way.
This then prints out each object's name, the file size, and last modified date.

.. code-block:: php

	<?php
	$objectsListResponse = $client->listObjects(['Bucket' => $bucketname]);
	$objects = $objectsListResponse['Contents'] ?? [];
	foreach ($objects as $object) {
    	    echo $object['Key'] . "\t" . $object['Size'] . "\t" . $object['LastModified'] . "\n";
	}

.. note::

   If there are more than 1000 objects in this bucket,
   you need to check $objectsListResponse['isTruncated']
   and run again with the name of the last key listed.
   Keep doing this until isTruncated is not true.

The output will look something like this if the bucket has some files::

   myphoto1.jpg	251262	2011-08-08T21:35:48.000Z
   myphoto2.jpg	262518	2011-08-08T21:38:01.000Z


Deleting a Bucket
-----------------

This deletes the bucket called ``my-old-bucket`` and returns a
``AWS\Result`` object

.. note::

   The Bucket must be empty! Otherwise it won't work!

.. code-block:: php

	<?php
	$client->deleteBucket(['Bucket' => 'my-old-bucket']);


Creating an Object
------------------

This creates an object ``hello.txt`` with the string ``"Hello World!"``

.. code-block:: php

	<?php
	$client->putObject([
    	    'Bucket' => 'my-bucket-name',
    	    'Key' => 'hello.txt',
    	    'Body' => "Hello World!"
	]);


Change an Object's ACL
----------------------

This makes the object ``hello.txt`` to be publicly readable and
``secret_plans.txt`` to be private.

.. code-block:: php

	<?php
	$client->putObjectAcl([
    	    'Bucket' => 'my-bucket-name',
    	    'Key' => 'hello.txt',
    	    'ACL' => 'public-read'
	]);
	$client->putObjectAcl([
    	    'Bucket' => 'my-bucket-name',
    	    'Key' => 'secret_plans.txt',
    	    'ACL' => 'private'
	]);


Delete an Object
----------------

This deletes the object ``goodbye.txt``

.. code-block:: php

	<?php
	$client->deleteObject(['Bucket' => 'my-bucket-name', 'Key' => 'goodbye.txt']);


Download an Object (to a file)
------------------------------

This downloads the object ``poetry.pdf`` and saves it in
``/home/larry/documents/``

.. code-block:: php

	<?php
	$object = $client->getObject(['Bucket' => 'my-bucket-name', 'Key' => 'poetry.pdf']);
	file_put_contents('/home/larry/documents/poetry.pdf', $object['Body']->getContents());

Generate Object Download URLs (signed and unsigned)
---------------------------------------------------

This generates an unsigned download URL for ``hello.txt``.
This works because we made ``hello.txt`` public by setting
the ACL above. This then generates a signed download URL
for ``secret_plans.txt`` that will work for 1 hour.
Signed download URLs will work for the time period even
if the object is private (when the time period is up,
the URL will stop working).

.. code-block:: php

	<?php
	$hello_url = $client->getObjectUrl('my-bucket-name', 'hello.txt');
	echo $hello_url."\n";
	
	$secret_plans_cmd = $client->getCommand('GetObject', ['Bucket' => 'my-bucket-name', 'Key' => 'secret_plans.txt']);
	$request = $client->createPresignedRequest($secret_plans_cmd, '+1 hour');
	echo $request->getUri()."\n";

The output of this will look something like::

   http://objects.dreamhost.com/my-bucket-name/hello.txt
   http://objects.dreamhost.com/my-bucket-name/secret_plans.txt?X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=sandboxAccessKey%2F20190116%2F%2Fs3%2Faws4_request&X-Amz-Date=20190116T125520Z&X-Amz-SignedHeaders=host&X-Amz-Expires=3600&X-Amz-Signature=61921f07c73d7695e47a2192cf55ae030f34c44c512b2160bb5a936b2b48d923

