=================
 Getting Started
=================

.. todo:: write about vstart, somewhere

The Ceph Storage System consists of multiple components, and can be
used in multiple ways. To guide you through it, please pick an aspect
of Ceph that is most interesting to you:

- :doc:`Object storage <object>`: read and write objects,
  flexible-sized data containers that have both data (a sequence of
  bytes) and metadata (mapping from keys to values), either via a
  :doc:`library interface </api/index>`, or via an :ref:`HTTP API
  <radosgw>`.

  *Example*: an asset management system

- :doc:`Block storage <block>`: use a remote data store as if it was a
  local hard disk, for example for virtual machine disk storage, for
  high-availability, etc.

  *Example*: virtual machine system with live migration

- :doc:`Distributed filesystem <filesystem>`: access the data store as
  a network filesystem, with strict POSIX semantics such as locking.

  *Example*: organization with hundreds of Linux servers with active
  users accessing them remotely

.. todo:: should the api mention above link to librados or libradospp
   directly? which one?

.. todo:: fs example could be thin clients, HPC, typical university
   setting, what to pick?


.. toctree::
   :hidden:

   object
   block
   filesystem
