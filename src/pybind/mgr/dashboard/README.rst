Dashboard and Administration Module for Ceph Manager
====================================================

Overview
--------

The original Ceph manager dashboard that was shipped with Ceph "Luminous"
started out as a simple read-only view into various run-time information and
performance data of a Ceph cluster. It used a very simple architecture to
achieve the original goal.

However, there was a growing demand for adding more web-based management
capabilities, to make it easier to administer Ceph for users that prefer a WebUI
over using the command line.

This new dashboard module is a replacement of the previous one and an
ongoing project to add a native web based monitoring and administration
application to Ceph Manager.

The architecture and functionality of this module are derived from and inspired
by the `openATTIC Ceph management and monitoring tool
<https://openattic.org/>`_. The development is actively driven by the team
behind openATTIC at SUSE.

The intention is to reuse as much of the existing openATTIC functionality as
possible, while adapting it to the different environment. The Dashboard module's
backend code uses the CherryPy framework and a custom REST API implementation
instead of Django and the Django REST Framework.

The WebUI implementation is based on Angular/TypeScript, merging both
functionality from the original dashboard as well as adding new functionality
originally developed for the standalone version of openATTIC.

Enabling and Starting the Dashboard
-----------------------------------

If you have installed Ceph from distribution packages, the package management
system should have taken care of installing all the required dependencies.

If you want to start the dashboard from within a development environment, you
need to have built Ceph (see the toplevel ``README.md`` file and the `developer
documentation <http://docs.ceph.com/docs/master/dev/>`_ for details on how to
accomplish this.

Finally, you need to build the dashboard frontend code. See the file
``HACKING.rst`` in this directory for instructions on setting up the necessary
development environment.

If you use the ``vstart.sh`` script to start up your development cluster, it
will configure and enable the dashboard automatically. The URL and login
credentials are displayed when the script finishes.

See the `Ceph Dashboard plugin documentation
<http://docs.ceph.com/docs/master/mgr/dashboard/>`_ for details on how to enable
and configure the dashboard manually and how to configure other settings, e.g.
access to the Ceph object gateway.

Working on the Dashboard Code
-----------------------------

If you're interested in helping with the development of the dashboard, please
see the file ``HACKING.rst`` for details on how to set up a development
environment and some other development-related topics.
