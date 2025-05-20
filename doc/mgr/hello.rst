Hello World Module
==================

This is a simple module skeleton for documentation purposes.

Enabling
--------

Run the following command to enable the ``hello`` module: 

.. prompt:: bash #

   ceph mgr module enable hello

Run the following command to ensure that the ``hello`` module is enabled: 

.. prompt:: bash #

   ceph mgr module ls

After editing the module file (found in ``src/pybind/mgr/hello/module.py``),
reload the module in order to pick up the changes by running the following
commands:

.. prompt:: bash #

   ceph mgr module disable hello
   ceph mgr module enable hello

Alternatively, run the following command to reload the ``hello`` module and pick up changes to its settings: 

.. prompt:: bash #

   init-ceph restart mgr

Run the following command to run the module:

.. prompt:: bash #

   ceph hello

The log is found in the following file::

   build/out/mgr.x.log


Documenting
-----------

After adding a new mgr module, be sure to add its documentation to
``doc/mgr/module_name.rst``.  Also, add a link to your new module into
``doc/mgr/index.rst``.
