.. _python_swift:

=====================
Python Swift Examples
=====================

Create a Connection
===================

This creates a connection so that you can interact with the server:

.. code-block:: python

	import cloudfiles
	username = 'account_name:username'
	api_key = 'your_api_key'

	conn = cloudfiles.get_connection(
		username=username,
		api_key=api_key,
		authurl='https://objects.dreamhost.com/auth',
	)


Create a Container
==================

This creates a new container called ``my-new-container``:

.. code-block:: python

	container = conn.create_container('my-new-container')
	

Create an Object
================

This creates a file ``hello.txt`` from the file named ``my_hello.txt``:

.. code-block:: python

	obj = container.create_object('hello.txt')
	obj.content_type = 'text/plain'
	obj.load_from_filename('./my_hello.txt')
	

List Owned Containers
=====================

This gets a list of containers that you own, and prints out the container name:

.. code-block:: python

	for container in conn.get_all_containers():
		print container.name

The output will look something like this::

   mahbuckat1
   mahbuckat2
   mahbuckat3

List a Container's Content
==========================

This gets a list of objects in the container, and prints out each 
object's name, the file size, and last modified date:

.. code-block:: python

	for obj in container.get_objects():
		print "{0}\t{1}\t{2}".format(obj.name, obj.size, obj.last_modified)

The output will look something like this::

   myphoto1.jpg	251262	2011-08-08T21:35:48.000Z
   myphoto2.jpg	262518	2011-08-08T21:38:01.000Z


Retrieve an Object
==================

This downloads the object ``hello.txt`` and saves it in
``./my_hello.txt``:

.. code-block:: python

	obj = container.get_object('hello.txt')
	obj.save_to_filename('./my_hello.txt')


Delete an Object
================

This deletes the object ``goodbye.txt``:

.. code-block:: python

	container.delete_object('goodbye.txt')
	
Delete a Container
==================

.. note::

   The container must be empty! Otherwise the request won't work!

.. code-block:: python

	conn.delete_container(container.name)

