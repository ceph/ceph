=========================================
 Cloning the Ceph Source Code Repository
=========================================

You may clone a Ceph branch of the Ceph source code by going to `github Ceph
Repository`_,  selecting a branch (``main`` by default), and clicking the
**Download ZIP** button.

.. _github Ceph Repository: https://github.com/ceph/ceph


To clone the entire git repository, install and configure ``git``.


Install Git
===========

To install ``git`` on Debian/Ubuntu, execute::

	sudo apt-get install git


To install ``git`` on CentOS/RHEL, execute::

	sudo yum install git


You must also have a ``github`` account. If you do not have a
``github`` account, go to `github.com`_ and register.
Follow the directions for setting up git at 
`Set Up Git`_.

.. _github.com: https://github.com
.. _Set Up Git: https://help.github.com/linux-set-up-git


Add SSH Keys (Optional)
=======================

If you intend to commit code to Ceph or to clone using SSH
(``git@github.com:ceph/ceph.git``), you must generate SSH keys for github. 

.. tip:: If you only intend to clone the repository, you may 
   use ``git clone --recursive https://github.com/ceph/ceph.git`` 
   without generating SSH keys.

To generate SSH keys for ``github``, execute::

	ssh-keygen

Get the key to add to your ``github`` account (the following example
assumes you used the default file path)::

	cat .ssh/id_rsa.pub

Copy the public key.

Go to your ``github`` account, click on "Account Settings" (i.e., the
'tools' icon); then, click "SSH Keys" on the left side navbar.

Click "Add SSH key" in the "SSH Keys" list, enter a name for the key, paste the
key you generated, and press the "Add key" button.


Clone the Source
================

To clone the Ceph source code repository, execute::

	git clone --recursive https://github.com/ceph/ceph.git

Once ``git clone`` executes, you should have a full copy of the Ceph 
repository.

.. tip:: Make sure you maintain the latest copies of the submodules
   included in the repository. Running ``git status`` will tell you if
   the submodules are out of date.

::

	cd ceph
	git status

.. _update-submodules:

Updating Submodules
-------------------

#. Determine whether your submodules are out of date:

   .. prompt:: bash $

      git status

   #. If your submodules are up to date 
         If your submodules are up to date, the following console output will
         appear: 

         ::
   
           On branch main
           Your branch is up to date with 'origin/main'.
           
           nothing to commit, working tree clean
   
         If you see this console output, then your submodules are up to date.
         You do not need this procedure.


   #. If your submodules are not up to date 
         If your submodules are not up to date, you will see a message that
         includes a list of "untracked files". The example here shows such a
         list, which was generated from a real situation in which the
         submodules were no longer current. Your list of files will not be the
         same as this list of files, but this list is provided as an example.
         If in your case any untracked files are listed, then you should
         continue to the next step of this procedure.

         ::

            On branch main
            Your branch is up to date with 'origin/main'.
            
            Untracked files:
              (use "git add <file>..." to include in what will be committed)
            src/pybind/cephfs/build/
            src/pybind/cephfs/cephfs.c
            src/pybind/cephfs/cephfs.egg-info/
            src/pybind/rados/build/
            src/pybind/rados/rados.c
            src/pybind/rados/rados.egg-info/
            src/pybind/rbd/build/
            src/pybind/rbd/rbd.c
            src/pybind/rbd/rbd.egg-info/
            src/pybind/rgw/build/
            src/pybind/rgw/rgw.c
            src/pybind/rgw/rgw.egg-info/
            
            nothing added to commit but untracked files present (use "git add" to track)

#. If your submodules are out of date, run the following commands:

   .. prompt:: bash $

      git submodule update --force --init --recursive
      git clean -fdx
      git submodule foreach clean -fdx

#. Run ``git status`` again:

   .. prompt:: bash $

      git status
   
   Your submodules are up to date if you see the following message:

   ::

     On branch main
     Your branch is up to date with 'origin/main'.
     
     nothing to commit, working tree clean

Choose a Branch
===============

Once you clone the source code and submodules, your Ceph repository 
will be on the ``main`` branch by default, which is the unstable 
development branch. You may choose other branches too.

- ``main``: The unstable development branch.
- ``stable-release-name``: The name of the stable, :ref:`active release <ceph-releases-index>`. e.g. ``Pacific``
- ``next``: The release candidate branch.

::

	git checkout main
