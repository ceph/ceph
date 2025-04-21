Hello World Module
==================

This is a simple module skeleton for documentation purposes.

Enabling
--------

The *hello* module is enabled with:

.. prompt:: bash #

   ceph mgr module enable hello

To check that it is enabled, run:

.. prompt:: bash #

   ceph mgr module ls

After editing the module file (found in ``src/pybind/mgr/hello/module.py``), you can see changes by running:

.. prompt:: bash #

   ceph mgr module disable hello
   ceph mgr module enable hello

or:

.. prompt:: bash #

   init-ceph restart mgr

To execute the module, run:

.. prompt:: bash #

   ceph hello

The log is found at::

  build/out/mgr.x.log


Documenting
-----------

After adding a new mgr module, be sure to add its documentation to ``doc/mgr/module_name.rst``.
Also, add a link to your new module into ``doc/mgr/index.rst``.
