===========================
SDK for Ceph Object Classes
===========================

`Ceph` can be extended by creating shared object classes called `Ceph Object 
Classes`. The existing framework to build these object classes has dependencies 
on the internal functionality of `Ceph`, which restricts users to build object 
classes within the tree. The aim of this project is to create an independent 
object class interface, which can be used to build object classes outside the 
`Ceph` tree. This allows us to have two types of object classes, 1) those that 
have in-tree dependencies and reside in the tree and 2) those that can make use 
of the `Ceph Object Class SDK framework` and can be built outside of the `Ceph` 
tree because they do not depend on any internal implementation of `Ceph`. This 
project decouples object class development from Ceph and encourages creation 
and distribution of object classes as packages.

In order to demonstrate the use of this framework, we have provided an example 
called ``cls_sdk``, which is a very simple object class that makes use of the 
SDK framework. This object class resides in the ``src/cls`` directory. 

Installing objclass.h
---------------------

The object class interface that enables out-of-tree development of object 
classes resides in ``src/include/rados/`` and gets installed with `Ceph` 
installation. After running ``make install``, you should be able to see it 
in ``<prefix>/include/rados``. ::

        ls /usr/local/include/rados

Using the SDK example
---------------------

The ``cls_sdk`` object class resides in ``src/cls/sdk/``. This gets built and 
loaded into Ceph, with the Ceph build process. You can run the 
``ceph_test_cls_sdk`` unittest, which resides in ``src/test/cls_sdk/``, 
to test this class.
