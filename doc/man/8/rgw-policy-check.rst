:orphan:

===================================================
rgw-policy-check -- verify syntax of bucket policy
===================================================

.. program:: rgw-policy-check

Synopsis
========

| **rgw-policy-check**
   -t *tenant* [ *filename* ... ]


Description
===========

This program reads one or more files containing bucket policy
and determines if it is syntactically correct.
It does not check to see if the policy makes sense;
it only checks to see if the file would be accepted
by the policy parsing logic inside
:program:`radsogw`.

More than one filename may be specified.  If no files are
given, the program will read from stdin.

On success, the program will say nothing.  On failure,
the program will emit a error message indicating the
problem.  The program will terminate with non-zero exit
status if one or more policies could not be read or parsed.

Options
=======

.. option: -t *tenant*

   Specify *tenant* as the tenant.  This is required by the
   policy parsing logic and is used to construct the internal
   state representation of the policy.

Availability
============

**rgw-policy-check** is part of Ceph, a massively scalable, open-source,
distributed storage system.  Please refer to the Ceph documentation at
http://ceph.com/docs for more information.

See also
========

:doc:`radosgw <radosgw>`\(8)

.. _Bucket Policies: ../../radosgw/bucketpolicy.rst
