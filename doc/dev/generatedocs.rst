Building Ceph Documentation
===========================

Ceph utilizes Python's Sphinx documentation tool. For details on
the Sphinx documentation tool, refer to `The Sphinx Documentation Tool <https://www.sphinx-doc.org/en/master/>`_.

To build the Ceph documentation set, you must:

1. Clone the Ceph repository
2. Install the required tools
3. Build the documents
4. Demo the documents (Optional)

.. highlight:: bash

Clone the Ceph Repository
-------------------------

To clone the Ceph repository, you must have ``git`` installed
on your local host. To install ``git``, execute::

	sudo apt-get install git

To clone the Ceph repository, execute::

	git clone git://github.com/ceph/ceph

You should have a full copy of the Ceph repository.


Install the Required Tools
--------------------------

To build the Ceph documentation, some dependencies are required.
To know what packages are needed, you can launch this command::

    cd ceph
    admin/build-doc

If dependencies are missing, the command above will fail
with a message that suggests you a command to install all
missing dependencies.


Build the Documents
-------------------

Once you have installed all the dependencies, execute the build (the
same command as above)::

	cd ceph
	admin/build-doc

Once you build the documentation set, you may navigate to the source directory to view it::

	cd build-doc/output

There should be an ``html`` directory and a ``man`` directory containing documentation
in HTML and manpage formats respectively.

``admin/build-doc`` takes a long time to prepare the environment and build the document.
But you can just rebuild the document on changes using::

  admin/build-doc livehtml

This feature uses ``sphinx-autobuild`` under the hood. You can also pass options to it. For
instance, to open the browser after building the documentation::

  admin/build-doc livehtml -- --open-browser

Please see `sphinx-autobuild <https://pypi.org/project/sphinx-autobuild/>`_ for more details.

Demo the Documents
-------------------

Once you build the documentation, as described above, you can demo the rendered documents
by running ``serve-doc``::

	cd ceph
	admin/serve-doc

This will serve the ``build-doc/output/html`` directory over port 8080 via 
Python's ``SimpleHTTPServer`` module.
