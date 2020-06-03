.. _cephadm-termnilogy:

============
Termninology
============


`Cephadm` introduces quite a few new terms that need clear demarcations. This article is supposed to go over them and
explain what they are all about.


cephadm (The manager module)
============================

This is the thing we're talking about when someone is referring to `cephadm`. It is implemented
as a `mgr module` and is currently the most elaborate orchestrator for ceph.
Cephadm contains most of the orchestration logic including the `Scheduling` algorithms.
It's often referred to `mgr/cephadm` to distinguish it from `cephadm` (The tool).


cephadm (The daemon tool)
=========================

This is the tool that operates on a host level and contains logic on how to deploy individual daemons like monitors, managers
etc. This tool is usually not being used by the end-user but is rather used by the orchestrator module `cephadm`.
To separate it from it's counterpart, the manager module `cephadm`, it's often referred to `bin/cephadm`.

Services
========

The term `Services` refers to a group of `Daemons` (ref) that make up some sort of complete entitiy(rewrite).
On the example of a Rados Gateway that is configured to have three instances running at all times, we'd speak
of the the `Rados Gateway Service`

Service Specifications
======================

A Service Specification declaratively describes `Services` (ref). These mostly contain meta information about
a Service, like name, service_type, placement and more. The only way to alter a `Service` is via it's specification.
More information about the Specification can be found here. TODO: Link to Service Specification page.


Daemons
=======

Daemons are the building blocks of Services. A Daemon is a single instance of a `Service` like a `ceph-mgr`, `ceph-mon`
or a non-ceph related tool like a `prometheus node-exporter`. A daemon always runs in a container.


