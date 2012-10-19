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
on your local host. To install ``git``, execute::

	sudo apt-get install git

To clone the Ceph repository, execute::

	git clone git://ceph/ceph.git

You should have a full copy of the Ceph repository.


Install the Required Tools
--------------------------

If you do not have Sphinx and its dependencies installed, a list of
dependencies will appear in the output. Install the dependencies on
your system, and then execute the build.

To run Sphinx, at least the following are required:

- ``python-dev``
- ``python-pip``
- ``python-virtualenv``
- ``libxml2-dev``
- ``libxslt-dev``
- ``doxygen``
- ``ditaa``
- ``graphviz``

Execute ``apt-get install`` for each dependency that isn't installed
on your host.::

	sudo apt-get install python-dev python-pip python-virtualenv libxml2-dev libxslt-dev doxygen ditaa graphviz



Build the Documents
-------------------

Once you have installed all the dependencies, execute the build::

	cd ceph
	admin/build-doc

Once you build the documentation set, you may navigate to the source directory to view it::

	cd build-doc/output

There should be an ``html`` directory and a ``man`` directory containing documentation
in HTML and manpage formats respectively.
