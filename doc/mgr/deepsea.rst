
================================
DeepSea orchestrator integration
================================

DeepSea (https://github.com/SUSE/DeepSea) is a collection of `Salt
<https://github.com/saltstack/salt>`_ state files, runners and modules for
deploying and managing Ceph.

The ``deepsea`` module provides integration between Ceph's orchestrator
framework (used by modules such as ``dashboard`` to control cluster services)
and DeepSea.

Orchestrator modules only provide services to other modules, which in turn
provide user interfaces.  To try out the deepsea module, you might like
to use the :ref:`Orchestrator CLI <orchestrator-cli-module>` module.

Requirements
------------

- A salt-master node with DeepSea 0.9.9 or later installed, and the salt-api
  service running.
- Ideally, several salt-minion nodes against which at least DeepSea's stages 0
  through 2 have been run (this is the minimum required for the orchestrator's
  inventory and status functions to return interesting information).

Configuration
-------------

Four configuration keys must be set in order for the module to talk to
salt-api:

- salt_api_url
- salt_api_username
- salt_api_password
- salt_api_eauth (default is "sharedsecret")

These all need to match the salt-api configuration on the salt master (see
eauth.conf, salt-api.conf and sharedsecret.conf in /etc/salt/master.d/ on the
salt-master node).

Configuration keys
^^^^^^^^^^^^^^^^^^^

Configuration keys can be set on any machine with the proper cephx credentials,
these are usually Monitors where the *client.admin* key is present.

::

    ceph deepsea config-set <key> <value>

For example:

::

    ceph deepsea config-set salt_api_url http://admin.example.com:8000/
    ceph deepsea config-set salt_api_username admin
    ceph deepsea config-set salt_api_password 12345

The current configuration of the module can also be shown:

::

   ceph deepsea config-show

Debugging
---------

Should you want to debug the deepsea module, increase the logging level for
ceph-mgr and check the logs.

::

    [mgr]
        debug mgr = 20

With the log level set to 20, the module will print out all the data received
from the salt event bus.  All log messages will be prefixed with *mgr[deepsea]*
for easy filtering.
