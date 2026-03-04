:orphan:

===================================================
rgw-policy-test -- test evaluation of bucket policy
===================================================

.. program:: rgw-policy-test

Synopsis
========

| **rgw-policy-test**
   [-e *key*=*value*]... [-t *tenant*] [-r resource] [-i identity] *action* *filename* 


Description
===========

This program reads a policy from a file or stdin and evaluates it
against the supplied identity, resource, action, and environment. The
program will print a trace of evaluating the policy and the effect of
its evaluation.

On error it will print a message and return non-zero.

Options
=======

.. option:: -t *tenant*, --tenant *tenant*

   Specify *tenant* as the tenant.  This is required by the
   policy parsing logic and is used to construct the internal
   state representation of the policy.

.. option:: -e *key*=*value*, --environment *key*=*value*

   Add (*key*, *value*) to the environment for evaluation. May be
   specified multiple times.

.. option:: -I *SCHEMA:STRING*, --identity *SDCHEMA:STRING*

   Specify *identity* as the identity whose access is to be checked.

.. option:: -R *ARN*, --resource *ARN*

   Specify *ARN* as the resource to check access for.

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
