:orphan:

===================================================
rgw-policy-test -- test evaluation of bucket policy
===================================================

.. program:: rgw-policy-test

Synopsis
========

| **rgw-policy-test**
   [-e *key*=*value*]... [-t *tenant*] [-R resource] [-I identity] *action* *filename* 


Description
===========

This utility reads a policy from a file or stdin and evaluates it
against the supplied identity, resource, action, and request context
(the key/value pairs evaluated by conditions.) The utility will print
a trace of evaluating the policy and the effect of its evaluation.

On error it will print a message and return non-zero.

Options
=======

.. option:: -t *tenant*, --tenant *tenant*

   Specify *tenant* as the tenant. If not provided, the default
   (empty) tenant is used.

.. option:: -e *key*=*value*, --environment *key*=*value*

   Add (*key*, *value*) to the environment for evaluation. May be
   specified multiple times.

.. option:: -I *SCHEMA:STRING*, --identity *SCHEMA:STRING*

   Specify *identity* as the identity whose access is to be checked.

.. option:: -R *ARN*, --resource *ARN*

   Specify *ARN* as the resource for which to check access.

.. option:: *action*

   Use *action* as the action to check.

.. option:: *filename*

   Read the policy from *filename*, or - for standard input.

Availability
============

**rgw-policy-test** is part of Ceph, a massively scalable, open-source,
distributed storage system.  Please refer to the Ceph documentation at
https://docs.ceph.com/ for more information.

See also
========

:doc:`radosgw <radosgw>`\(8)
