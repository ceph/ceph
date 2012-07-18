==========
 Tutorial
==========

The Swift-compatible API tutorials follow a simple container-based object 
lifecycle. The first step requires you to setup a connection between your 
client and the RADOS Gateway server. Then, you may follow a natural 
container and object lifecycle, including adding and retrieving object 
metadata. See example code for the following languages:

- `Java`_
- `Python`_
- `Ruby`_


.. ditaa:: +----------------------------+        +-----------------------------+
           |                            |        |                             |
           |    Create a Connection     |------->|      Create a Container     |
           |                            |        |                             |
           +----------------------------+        +-----------------------------+
                                                                |
                         +--------------------------------------+ 
                         |
                         v
           +----------------------------+        +-----------------------------+
           |                            |        |                             |
           |     Create an Object       |------->| Add/Update Object Metadata  |
           |                            |        |                             |
           +----------------------------+        +-----------------------------+
                                                                |
                         +--------------------------------------+ 
                         |
                         v
           +----------------------------+        +-----------------------------+
           |                            |        |                             |
           |   List Owned Containers    |------->| List a Container's Contents |
           |                            |        |                             |
           +----------------------------+        +-----------------------------+
                                                                |
                         +--------------------------------------+ 
                         |
                         v
           +----------------------------+        +-----------------------------+
           |                            |        |                             |
           | Get an Object's Metadata   |------->|     Retrieve an Object      |
           |                            |        |                             |
           +----------------------------+        +-----------------------------+
                                                                |
                         +--------------------------------------+ 
                         |
                         v
           +----------------------------+        +-----------------------------+
           |                            |        |                             |
           |      Delete an Object      |------->|      Delete a Container     |
           |                            |        |                             |
           +----------------------------+        +-----------------------------+

.. _Java: ../java
.. _Python: ../python
.. _Ruby: ../ruby
