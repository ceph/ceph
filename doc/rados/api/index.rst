===========================
 Ceph Storage Cluster APIs
===========================

The :term:`Ceph Storage Cluster` has a messaging layer protocol that enables
clients to interact with a :term:`Ceph Monitor` and a :term:`Ceph OSD Daemon`.
``librados`` provides this functionality to :term:`Ceph Clients` in the form of
a library.  All Ceph Clients either use ``librados`` or the same functionality
encapsulated in ``librados`` to interact with the object store.  For example,
``librbd`` and ``libcephfs`` leverage this functionality. You may use
``librados`` to interact with Ceph directly (e.g., an application that talks to
Ceph, your own interface to Ceph, etc.).


.. toctree::
   :maxdepth: 2 

   Introduction to librados <librados-intro>
   librados (C) <librados>
   librados (C++) <libradospp>
   librados (Python) <python>