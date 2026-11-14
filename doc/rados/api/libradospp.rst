.. _rados api cpp:

==================
 LibradosPP (C++)
==================

.. note:: The librados C++ API is not guaranteed to be API+ABI stable
   between major releases. All applications using the librados C++ API must
   be recompiled and relinked against a specific Ceph release.

Cluster handle, I/O context and operations
==========================================

.. autodoxygenfile:: librados.hpp

Types
=====

.. autodoxygenfile:: rados_types.hpp

Object class method traits
==========================

.. autodoxygenfile:: cls_traits.hpp
