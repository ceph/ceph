===============================
Ceph Object Gateway Quick Start
===============================

As of `firefly` (v0.80), Ceph Storage dramatically simplifies installing and
configuring a Ceph Object Gateway. The Gateway daemon embeds Civetweb, so you
do not have to install a web server or configure FastCGI. Additionally,
``ceph-deploy`` can install the gateway package, generate a key, configure a
data directory and create a gateway instance for you.

.. tip:: Civetweb uses port ``7480`` by default. You must either open port
   ``7480``, or set the port to a preferred port (e.g., port ``80``) in your Ceph
   configuration file.

To start a Ceph Object Gateway, follow the steps below:

Installing Ceph Object Gateway
==============================

#. Execute the pre-installation steps on your ``client-node``. If you intend to
   use Civetweb's default port ``7480``, you must open it using either
   ``firewall-cmd`` or ``iptables``. See `Preflight Checklist`_ for more
   information.

#. From the working directory of your administration server, install the Ceph
   Object Gateway package on the ``client-node`` node. For example::

    ceph-deploy install --rgw <client-node> [<client-node> ...]

Creating the Ceph Object Gateway Instance
=========================================

From the working directory of your administration server, create an instance of
the Ceph Object Gateway on the ``client-node``. For example::

    ceph-deploy rgw create

Once the gateway is running, you should be able to access it on port ``7480``.
(e.g., ``http://client-node:7480``).

Configuring the Ceph Object Gateway Instance
============================================

#. To change the default port (e.g,. to port ``80``), modify your Ceph
   configuration file. Add a section entitled ``[client.rgw.<client-node>]``,
   replacing ``<client-node>`` with the short node name of your Ceph client
   node (i.e., ``hostname -s``). For example, if your node name is
   ``client-node``, add a section like this after the ``[global]`` section::

    [client.rgw.client-node]
    rgw_frontends = "civetweb port=80"

   .. note:: Ensure that you leave no whitespace between ``port=<port-number>``
      in the ``rgw_frontends`` key/value pair.

   .. important:: If you intend to use port 80, make sure that the Apache
      server is not running otherwise it will conflict with Civetweb. We recommend
      to remove Apache in this case.

#. To make the new port setting take effect, restart the Ceph Object Gateway.
   On Red Hat Enterprise Linux 7 and Fedora, run the following command::

    sudo systemctl restart ceph-radosgw.service

   On Red Hat Enterprise Linux 6 and Ubuntu, run the following command::

    sudo service radosgw restart id=rgw.<short-hostname>

#. Finally, check to ensure that the port you selected is open on the node's
   firewall (e.g., port ``80``). If it is not open, add the port and reload the
   firewall configuration. For example::

    sudo firewall-cmd --list-all sudo firewall-cmd --zone=public --add-port
    80/tcp --permanent
    sudo firewall-cmd --reload

   See `Preflight Checklist`_ for more information on configuring firewall with
   ``firewall-cmd`` or ``iptables``.

   You should be able to make an unauthenticated request, and receive a
   response. For example, a request with no parameters like this::

    http://<client-node>:80

   Should result in a response like this::

    <?xml version="1.0" encoding="UTF-8"?>
    <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Owner>
        <ID>anonymous</ID>
        <DisplayName></DisplayName>
      </Owner>
    	<Buckets>
      </Buckets>
    </ListAllMyBucketsResult>

See the `Configuring Ceph Object Gateway`_ guide for additional administration
and API details.

.. _Configuring Ceph Object Gateway: ../../radosgw/config
.. _Preflight Checklist: ../quick-start-preflight
