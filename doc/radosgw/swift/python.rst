.. _python_swift:

=====================
Python Swift Examples
=====================

Create a Connection
===================

This creates a connection so that you can interact with the server:

.. code-block:: python

	import swiftclient
	user = 'account_name:username'
	key = 'your_api_key'

	conn = swiftclient.Connection(
		user=user,
		key=key,
		authurl='https://objects.dreamhost.com/auth',
	)


Create a Container
==================

This creates a new container called ``my-new-container``:

.. code-block:: python

	container_name = 'my-new-container'
	conn.put_container(container_name)


Create an Object
================

This creates a file ``hello.txt`` from the file named ``my_hello.txt``:

.. code-block:: python

	with open('hello.txt', 'r') as hello_file:
		conn.put_object(container_name, 'hello.txt',
						contents= hello_file.read(),
						content_type='text/plain')


List Owned Containers
=====================

This gets a list of containers that you own, and prints out the container name:

.. code-block:: python

	for container in conn.get_account()[1]:
		print container['name']

The output will look something like this::

   mahbuckat1
   mahbuckat2
   mahbuckat3

List a Container's Content
==========================

This gets a list of objects in the container, and prints out each
object's name, the file size, and last modified date:

.. code-block:: python

	for data in conn.get_container(container_name)[1]:
		print '{0}\t{1}\t{2}'.format(data['name'], data['bytes'], data['last_modified'])

The output will look something like this::

   myphoto1.jpg	251262	2011-08-08T21:35:48.000Z
   myphoto2.jpg	262518	2011-08-08T21:38:01.000Z


Retrieve an Object
==================

This downloads the object ``hello.txt`` and saves it in
``./my_hello.txt``:

.. code-block:: python

	obj_tuple = conn.get_object(container_name, 'hello.txt')
	with open('my_hello.txt', 'w') as my_hello:
		my_hello.write(obj_tuple[1])


Delete an Object
================

This deletes the object ``goodbye.txt``:

.. code-block:: python

	conn.delete_object(container_name, 'hello.txt')

Delete a Container
==================

.. note::

   The container must be empty! Otherwise the request won't work!

.. code-block:: python

	conn.delete_container(container_name)

