=========================
 RADOS Object Store APIs
=========================

Ceph's RADOS Object Store has a messaging layer protocol that enables clients
to interact with Ceph monitors and OSDs. ``librados`` provides this
functionality to object store clients in the form of a library.  All Ceph
clients either use ``librados`` or the same functionality  encapsulated in
``librados`` to interact with the object store.  For example, ``librbd`` and
``libcephfs`` leverage this functionality. You may use ``librados`` to interact
with Ceph directly (e.g., an application that talks to Ceph, your own interface
to Ceph, etc.). 

For an overview of where ``librados`` appears in the technology stack, 
see :doc:`../../architecture`.

.. toctree:: 

   librados (C) <librados>
   librados (C++) <libradospp>