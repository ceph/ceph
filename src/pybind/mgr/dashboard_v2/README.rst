openATTIC Module for Ceph Manager
=================================

This module is an ongoing project to convert the `openATTIC openATTIC Ceph
management and monitoring tool <https://openattic.org/>`_ (both the backend and
WebUI) into a Ceph Manager module.

The current openATTIC backend implementation is based on Django and the Django
REST framework. The plan is to convert the backend code to use the CherryPy
framework and a custom REST API implementation instead.

The current WebUI implementation is written using AngularJS 1.x JavaScript
framework. The upstream openATTIC project already has plans to move to
Angular/TypeScript; this migration will be performed at a later stage, once the
openATTIC mgr module has been accepted and is providing basic functionality.

The porting of the existing openATTIC functionality will be done in stages. The
work is done by the openATTIC team and is currently tracked in the `openATTIC
JIRA <https://tracker.openattic.org/browse/OP-3039>`_.

Unit Testing
____________

To run the unit tests that reside in :emphasis:`tests/*` do::

  UNITTEST=true py.test --cov=. tests/


These tests depend on the following python libraries:
 * pytest
 * pytest-cov
 * mock

