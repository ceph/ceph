=========================================
 Cloning the Ceph Source Code Repository
=========================================

To clone the source, you must install Git. See `Set Up Git`_ for details.

.. _Set Up Git: ../git


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

If your submodules are out of date, run::

	git submodule update

Choose a Branch
===============

Once you clone the source code and submodules, your Ceph repository 
will be on the ``master`` branch by default, which is the unstable 
development branch. You may choose other branches too.

- ``master``: The unstable development branch.
- ``stable``: The bugfix branch.
- ``next``: The release candidate branch.

::


	git checkout master
