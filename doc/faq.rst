============================
 Frequently Asked Questions
============================

These questions have been frequently asked on the ceph-devel mailing
list, the IRC channel, and on the Ceph.com blog.

Is Ceph Production-Quality?
===========================

The definition of "production quality" varies depending on who you ask.
Because it can mean a lot of different things depending on how you want to
use Ceph, we prefer not to think of it as a binary term.

At this point we support the RADOS object store, radosgw, and rbd because
we think they are sufficiently stable that we can handle the support
workload.  There are several organizations running those parts of the
system in production.  Others wouldn't dream of doing so at this stage.

The CephFS POSIX-compliant filesystem is functionally-complete and has
been evaluated by a large community of users, but has not yet been
subjected to extensive, methodical testing.

We can tell you how we test, and what we support, but in the end it's
your judgement that matters most!

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

