===========================
 Installing a Ceph cluster
===========================

For development and really early stage testing, see :doc:`/dev/index`.

For installing the latest development builds, see
:doc:`/ops/autobuilt`.

Installing any complex distributed software can be a lot of work. We
support two automated ways of installing Ceph:

- using Chef_: see :doc:`chef`
- with the ``mkcephfs`` shell script: :doc:`mkcephfs`

.. _Chef: http://wiki.opscode.com/display/chef

.. topic:: Status as of 2011-09

  This section hides a lot of the tedious underlying details. If you
  need to, or wish to, roll your own deployment automation, or are
  doing it manually, you'll have to dig into a lot more intricate
  details.  We are working on simplifying the installation, as that
  also simplifies our Chef cookbooks.


.. toctree::
   :hidden:

   chef
   mkcephfs
