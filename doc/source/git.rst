============
 Set Up Git
============

To clone the Ceph git repository, you must have ``git`` installed
on your local host. 

Install Git
===========

To install ``git``, execute::

	sudo apt-get install git

You must also have a ``github`` account. If you do not have a
``github`` account, go to `github.com`_ and register.
Follow the directions for setting up git at 
`Set Up Git`_.

.. _github.com: http://github.com
.. _Set Up Git: http://help.github.com/linux-set-up-git

Generate SSH Keys
=================

If you intend to commit code to Ceph or to clone using SSH (``git@github.com:ceph/ceph.git``), you must generate SSH keys for github. 

.. tip:: If you only intend to clone the repository, you may 
   use ``git clone --recursive https://github.com/ceph/ceph.git`` 
   without generating SSH keys.

To generate SSH keys for ``github``, execute::

	ssh-keygen

Get the key to add to your ``github`` account (the following example
assumes you used the default file path)::

	cat .ssh/id_rsa.pub

Copy the public key.

Add the Key
===========

Go to your your ``github`` account, click on "Account Settings" (i.e., the
'tools' icon); then, click "SSH Keys" on the left side navbar.

Click "Add SSH key" in the "SSH Keys" list, enter a name for the key, paste the
key you generated, and press the "Add key" button.