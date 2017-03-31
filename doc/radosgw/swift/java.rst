.. _java_swift:

=====================
 Java Swift Examples
=====================

Setup
=====

The following examples may require some or all of the following Java
classes to be imported:

.. code-block:: java

       import org.javaswift.joss.client.factory.AccountConfig;
       import org.javaswift.joss.client.factory.AccountFactory;
       import org.javaswift.joss.client.factory.AuthenticationMethod;
       import org.javaswift.joss.model.Account;
       import org.javaswift.joss.model.Container;
       import org.javaswift.joss.model.StoredObject;
       import java.io.File;
       import java.io.IOException;
       import java.util.*;


Create a Connection
===================

This creates a connection so that you can interact with the server:

.. code-block:: java

       String username = "USERNAME";
       String password = "PASSWORD";
       String authUrl  = "https://radosgw.endpoint/auth/1.0";

       AccountConfig config = new AccountConfig();
       config.setUsername(username);
       config.setPassword(password);
       config.setAuthUrl(authUrl);
       config.setAuthenticationMethod(AuthenticationMethod.BASIC);
       Account account = new AccountFactory(config).createAccount();


Create a Container
==================

This creates a new container called ``my-new-container``:

.. code-block:: java

       Container container = account.getContainer("my-new-container");
       container.create();


Create an Object
================

This creates an object ``foo.txt`` from the file named ``foo.txt`` in 
the container ``my-new-container``:

.. code-block:: java

       Container container = account.getContainer("my-new-container");
       StoredObject object = container.getObject("foo.txt");
       object.uploadObject(new File("foo.txt"));


Add/Update Object Metadata
==========================

This adds the metadata key-value pair ``key``:``value`` to the object named
``foo.txt`` in the container ``my-new-container``:

.. code-block:: java

       Container container = account.getContainer("my-new-container");
       StoredObject object = container.getObject("foo.txt");
       Map<String, Object> metadata = new TreeMap<String, Object>();
       metadata.put("key", "value");
       object.setMetadata(metadata);


List Owned Containers
=====================

This gets a list of Containers that you own.
This also prints out the container name.

.. code-block:: java

       Collection<Container> containers = account.list();
       for (Container currentContainer : containers) {
           System.out.println(currentContainer.getName());
       }

The output will look something like this::

	mahbuckat1
	mahbuckat2
	mahbuckat3


List a Container's Content
==========================

This gets a list of objects in the container ``my-new-container``; and, it also 
prints out each object's name, the file size, and last modified date:

.. code-block:: java

       Container container = account.getContainer("my-new-container");
       Collection<StoredObject> objects = container.list();
       for (StoredObject currentObject : objects) {
           System.out.println(currentObject.getName());
       }

The output will look something like this::

   myphoto1.jpg
   myphoto2.jpg


Retrieve an Object's Metadata
=============================

This retrieves metadata and gets the MIME type for an object named ``foo.txt``
in a container named ``my-new-container``:

.. code-block:: java

       Container container = account.getContainer("my-new-container");
       StoredObject object = container.getObject("foo.txt");
       Map<String, Object> returnedMetadata = object.getMetadata();
       for (String name : returnedMetadata.keySet()) {
           System.out.println("META / "+name+": "+returnedMetadata.get(name));
       }


Retrieve an Object
==================

This downloads the object ``foo.txt`` in the container ``my-new-container`` 
and saves it in ``./outfile.txt``:

.. code-block:: java

       Container container = account.getContainer("my-new-container");
       StoredObject object = container.getObject("foo.txt");
       object.downloadObject(new File("outfile.txt"));


Delete an Object
================

This deletes the object ``goodbye.txt`` in the container "my-new-container":

.. code-block:: java

       Container container = account.getContainer("my-new-container");
       StoredObject object = container.getObject("foo.txt");
       object.delete();


Delete a Container
==================

This deletes a container named "my-new-container": 

.. code-block:: java

       Container container = account.getContainer("my-new-container");
       container.delete();
	
.. note:: The container must be empty! Otherwise it won't work!
