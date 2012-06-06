================
 Authentication
================

Default users and pools are suitable for initial testing purposes. For test bed 
and production environments, you should create users and assign pool access to 
the users. For user management, see the `ceph-authtool`_ command for details.

Enabling Authentication
-----------------------
In the ``[global]`` settings of your ``ceph.conf`` file, you must enable 
authentication for your cluster. ::

	[global]
		auth supported = cephx

The valid values are ``cephx`` or ``none``. If you specify ``cephx``, you should
also specify the keyring's path. We recommend using the ``/etc/ceph`` directory.
Provide a ``keyring`` setting in ``ceph.conf`` like this::

	[global]
		auth supported = cephx
		keyring = /etc/ceph/keyring.bin	

If there is no keyring in the path, generate one.  

Generating a Keyring
--------------------
To generate a keyring in the default location, use the ``ceph-authtool`` and 
specify the same path you specified in the ``[global]`` section of your 
``ceph.conf`` file. For example::

	sudo ceph-authtool --create-keyring /etc/ceph/keyring.bin
	sudo chmod +r /etc/ceph/keyring.bin	

Specify Keyrings for each Daemon
--------------------------------
In your ``ceph.conf`` file under the daemon settings, you must also specify the
keyring directory and keyring name. The metavariable ``$name`` resolves 
automatically. ::

	[mon]
		keyring = /etc/ceph/keyring.$name
		
	[osd]
		keyring = /etc/ceph/keyring.$name

	[mds]
		keyring = /etc/ceph/keyring.$name		

The ``client.admin`` Key
------------------------
Each Ceph command you execute on the command line assumes that you are
the ``client.admin`` default user. When running Ceph with ``cephx`` enabled,
you need to have a ``client.admin`` key to run ``ceph`` commands.

.. important: To continue to run Ceph commands on the command line with
   ``cephx`` enabled, you need to create a key for the ``client.admin`` 
   user, and create a secret file under ``/etc/ceph``. 
   
::
   
	sudo ceph-authtool /etc/ceph/keyring.bin -n client.admin --gen-key
	sudo ceph-authtool -n client.admin --cap mds 'allow' --cap osd 'allow *' --cap mon 'allow *' /etc/ceph/keyring.bin
	sudo ceph auth add client.admin -i /etc/ceph/keyring.bin

Generate a Key
--------------
Keys enable a specific user to access the monitor, metadata server and cluster
according to capabilities assigned to the key. To generate a key for a user,
you must specify specify a path to the keyring and a username. Replace 
the ``{keyring/path}`` and ``{username}`` below. ::

	sudo ceph-authtool {keyring/path} -n client.{username} --gen-key

For example:: 

	sudo ceph-authtool /etc/ceph/keyring.bin -n client.whirlpool --gen-key
	
.. note: User names are associated to user types, which include ``client``
   ``admin``, ``osd``, ``mon``, and ``mds``. In most cases, you will be 
   creating keys for ``client`` users.

List Keys
---------
To see a list of keys in a keyring, execute the following::

	sudo ceph-authtool /etc/ceph/keyring.bin --list
	
A keyring will display the user, the user's key, and the capabilities
associated to the user's key.

Add Capabilities to a Key
-------------------------
To add capabilities to a key, you must specify the username, and a capability 
for at least one of the monitor, metadata server and OSD. You may add more than
one capability when executing the ``ceph-authtool`` command. Replace the 
``{usertype.username}``, ``{daemontype}`` and ``{capability}`` below::  

	sudo ceph-authtool -n {usertype.username} --cap {daemontype} {capability}

For example:: 

	ceph-authtool -n client.whirlpool --cap mds 'allow' --cap osd 'allow rw pool=swimmingpool' --cap mon 'allow r' /etc/ceph/keyring.bin

Add the Keys to your Cluster
----------------------------
Once you have generated keys and added capabilities to the keys, add each of the
keys to your cluster. Replace the ``{usertype.username}`` below. ::

	sudo ceph auth add {usertype.username} -i /etc/ceph/keyring.bin

For example:: 

	sudo ceph auth add client.whirlpool -i /etc/ceph/keyring.bin
	

List Keys in your Cluster
-------------------------
To list the keys in your cluster, execute the following:: 

	sudo ceph auth list


.. _ceph-authtool: http://ceph.com/docs/master/man/8/ceph-authtool/
		