Building Ceph Documentation
===========================

Ceph utilizes Python's Sphinx documentation tool. For details on
the Sphinx documentation tool, refer to `The Sphinx Documentation Tool <http://sphinx.pocoo.org/>`_.

To build the Ceph documentation set, you must:

1. Clone the Ceph repository
2. Install the required tools
3. Build the documents

Clone the Ceph Repository
-------------------------

To clone the Ceph repository, you must have ``git`` installed
on your local host. To install ``git``, execute:

	``$ sudo apt-get install git``

You must also have a github account. If you do not have a
github account, go to `github <http://github.com>`_ and register.

You must set up SSH keys with github to clone the Ceph
repository. If you do not have SSH keys for github, execute:

	``$ ssh-keygen -d``

Get the key to add to your github account:

	``$ cat .ssh/id_dsa.pub``

Copy the public key. Then, go to your your github account,
click on **Account Settings** (*i.e.*, the tools icon); then,
click **SSH Keys** on the left side navbar.

Click **Add SSH key** in the **SSH Keys** list, enter a name for
the key, paste the key you generated, and press the **Add key**
button.

To clone the Ceph repository, execute:

	``$ git clone git@github:ceph/ceph.git``

You should have a full copy of the Ceph repository.


Install the Required Tools
--------------------------
If you do not have Sphinx and its dependencies installed,
a list of dependencies will appear in the output. Install
the dependencies on your system, and then execute the build.

To run Sphinx, at least the following are required:

- ``python-dev``
- ``python-pip``
- ``python-virtualenv``
- ``libxml2-dev``
- ``libxslt-dev``
- ``doxygen``
- ``ditaa``
- ``graphviz``

Execute ``apt-get install`` for each dependency that isn't
installed on your host.

	``$ apt-get install python-dev python-pip python-virtualenv libxml2-dev libxslt-dev doxygen ditaa graphviz``



Build the Documents
-------------------

Once you have installed all the dependencies, execute the build:

	``$ cd ceph``
	``$ admin/build-doc``

Once you build the documentation set, you may navigate to the source directory to view it:

	``$ cd build-doc/output``

There should be an ``html`` directory and a ``man`` directory containing documentation
in HTML and manpage formats respectively.
