============================
 Frequently Asked Questions
============================

These questions have been frequently asked on the ceph-devel mailing
list, the IRC channel, and on the `Ceph.com`_ blog.

.. _Ceph.com: http://ceph.com

Is Ceph Production-Quality?
===========================

Ceph's object store is production ready. Large-scale storage systems (i.e.,
petabytes of data) use Ceph's block devices and Ceph's RESTful object store
supporting APIs compatible with Amazon's S3 and OpenStack's Swift. `Inktank`_
provides commercial support for the Ceph object store, block devices, and 
RESTful interfaces.

The CephFS POSIX-compliant filesystem is functionally-complete and has
been evaluated by a large community of users, but is still undergoing
methodical QA testing. Once Ceph's filesystem passes QA muster, `Inktank`_ 
will provide commercial support for CephFS in production systems.

.. _Inktank: http://inktank.com

How can I add a question to this list?
======================================

If you'd like to add a question to this list (hopefully with an
accompanying answer!), you can find it in the doc/ directory of our
main git repository:

	`https://github.com/ceph/ceph/blob/master/doc/faq.rst`_

.. _https://github.com/ceph/ceph/blob/master/doc/faq.rst: https://github.com/ceph/ceph/blob/master/doc/faq.rst

We use Sphinx to manage our documentation, and this page is generated
from reStructuredText source.  See the section on Building Ceph
Documentation for the build procedure.

