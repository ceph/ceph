.. _ruby_swift:

=====================
 Ruby Swift Examples
=====================

Create a Connection
===================

This creates a connection so that you can interact with the server:

.. code-block:: ruby

	require 'cloudfiles'
	username = 'account_name:user_name'
	api_key  = 'your_secret_key'

	conn = CloudFiles::Connection.new(
		:username => username,
		:api_key  => api_key,
		:auth_url => 'http://objects.dreamhost.com/auth'
	)


Create a Container
==================

This creates a new container called ``my-new-container``

.. code-block:: ruby

	container = conn.create_container('my-new-container')


Create an Object
================

This creates a file ``hello.txt`` from the file named ``my_hello.txt``

.. code-block:: ruby

	obj = container.create_object('hello.txt')
	obj.load_from_filename('./my_hello.txt')
	obj.content_type = 'text/plain'



List Owned Containers
=====================

This gets a list of Containers that you own, and also prints out 
the container name:

.. code-block:: ruby

	conn.containers.each do |container|
		puts container
	end

The output will look something like this::

   mahbuckat1
   mahbuckat2
   mahbuckat3


List a Container's Contents
===========================

This gets a list of objects in the container, and prints out each 
object's name, the file size, and last modified date:

.. code-block:: ruby

	require 'date'  # not necessary in the next version

	container.objects_detail.each do |name, data|
		puts "#{name}\t#{data[:bytes]}\t#{data[:last_modified]}"
	end

The output will look something like this::

   myphoto1.jpg	251262	2011-08-08T21:35:48.000Z
   myphoto2.jpg	262518	2011-08-08T21:38:01.000Z



Retrieve an Object
==================

This downloads the object ``hello.txt`` and saves it in
``./my_hello.txt``:

.. code-block:: ruby

	obj = container.object('hello.txt')
	obj.save_to_filename('./my_hello.txt')


Delete an Object
================

This deletes the object ``goodbye.txt``:

.. code-block:: ruby

	container.delete_object('goodbye.txt')
	

Delete a Container
==================

.. note::

   The container must be empty! Otherwise the request won't work!

.. code-block:: ruby

	container.delete_container('my-new-container')
