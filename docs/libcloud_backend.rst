.. _libcloud-backend:

LibCloud backend
================
This is an *experimental* provisioning backend that eventually intends to support several libcloud drivers. At this time only the OpenStack driver is supported.

Prerequisites
-------------
* An account with an OpenStack provider that supports Nova and Cinder
* A DNS server supporting `RFC 2136 <https://tools.ietf.org/html/rfc2136>`_. We use `bind <https://www.isc.org/downloads/bind/>`_ and `this ansible role <https://github.com/ceph/ceph-cm-ansible/blob/master/roles/nameserver/README.rst>`_ to help configure ours.
* An `nsupdate-web <https://github.com/zmc/nsupdate-web>`_ instance configured to update DNS records. We use `an ansible role <https://github.com/ceph/ceph-cm-ansible/blob/master/roles/nsupdate_web/README.rst>`_ for this as well. 
* Configuration in `teuthology.yaml` for this backend itself (see :ref:`libcloud_config`) and `nsupdate-web`
* You will also need to choose a maximum number of nodes to be running at once, and create records in your paddles database for each one - making sure to set `is_vm` to `True` for each.

.. _libcloud_config:

Configuration
-------------
An example configuration using OVH as an OpenStack provider::

    libcloud:
      providers:
        ovh:  # This string is the 'machine type' value you will use when locking these nodes
          driver: openstack
          driver_args:  # driver args are passed directly to the libcloud driver
            username: 'my_ovh_username'
            password: 'my_ovh_password'
            ex_force_auth_url: 'https://auth.cloud.ovh.net/v2.0/tokens'
            ex_force_auth_version: '2.0_password'
            ex_tenant_name: 'my_tenant_name'
            ex_force_service_region: 'my_region'

Why nsupdate-web?
-----------------
While we could have supported directly calling `nsupdate <https://en.wikipedia.org/wiki/Nsupdate>`_, we chose not to. There are a few reasons for this:

* To avoid piling on yet another feature of teuthology that could be left up to a separate service
* To avoid teuthology users having to request, obtain and safeguard the private key that nsupdate requires to function
* Because we use one subdomain for all of Sepia's test nodes, we had to enable dynamic DNS for that whole zone (this is a limitation of bind). However, we do not want users to be able to push DNS updates for the entire zone. Instead, we gave nsupdate-web the ability to accept or reject requests based on whether the hostname matches a configurable regular expression. The private key itself is not shared with non-admin users.

Bugs
----
At this time, only OVH has been tested as a provider. PRs are welcome to support more!
