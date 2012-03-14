=======================================
Cloning the Ceph Source Code Repository
=======================================
To check out the Ceph source code, you must have ``git`` installed
on your local host. To install ``git``, execute::

	$ sudo apt-get install git

You must also have a ``github`` account. If you do not have a
``github`` account, go to `github.com <http://github.com>`_ and register. 
Follow the directions for setting up git at `Set Up Git <http://help.github.com/linux-set-up-git/>`_.

Generate SSH Keys
-----------------
You must generate SSH keys for github to clone the Ceph
repository. If you do not have SSH keys for ``github``, execute::

	$ ssh-keygen -d
	
Get the key to add to your ``github`` account::

	$ cat .ssh/id_dsa.pub
	
Copy the public key. 

Add the Key
-----------
Go to your your ``github`` account,
click on "Account Settings" (i.e., the 'tools' icon); then,
click "SSH Keys" on the left side navbar. 

Click "Add SSH key" in the "SSH Keys" list, enter a name for
the key, paste the key you generated, and press the "Add key"
button.

Clone the Source
----------------
To clone the Ceph source code repository, execute::

	$ git clone git@github.com:ceph/ceph.git
	 
Once ``git clone`` executes, you should have a full copy of the Ceph repository.

Clone the Submodules
--------------------
Before you can build Ceph, you must get the ``init`` submodule and the ``update`` submodule:: 

	$ git submodule init 
	$ git submodule update 

.. tip:: Make sure you maintain the latest copies of these submodules. Running ``git status`` will tell you if the submodules are out of date:: 

	$ git status
	
