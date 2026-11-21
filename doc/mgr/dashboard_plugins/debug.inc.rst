.. _dashboard-debug:

Debug
^^^^^

This plug-in changes the behavior of the dashboard in debug mode. It can be
enabled, disabled or checked with the following command:

.. prompt:: bash #

   ceph dashboard debug status

::

   Debug: 'disabled'

.. prompt:: bash #

   ceph dashboard debug enable

.. prompt:: bash #

   ceph dashboard debug disable

The ``enable`` and ``disable`` commands print nothing. Run
``ceph dashboard debug status`` to check the result. While debug mode is
enabled, the cluster reports the ``DASHBOARD_DEBUG`` health warning
(``Dashboard debug mode is enabled``).

By default, it's disabled. This is the recommended setting for production
deployments. If required, debug mode can be enabled without need of restarting.
When debug mode is disabled, the dashboard uses the CherryPy ``production``
environment. When debug mode is enabled, it uses the CherryPy ``test_suite``
environment. See `CherryPy Environments
<https://docs.cherrypy.dev/en/latest/config.html#environments>`_.

Error responses always include a ``request_id``. With debug mode enabled,
they also include the Python traceback and the CherryPy version.
