.. _mgr-cephadm-mgr:

===========
MGR Service
===========

The cephadm MGR service hosts multiple modules. These include the
:ref:`mgr-dashboard` and the cephadm manager module.

.. _cephadm-mgr-networks:

Specifying Networks
-------------------

The MGR service supports binding only to a specific IP within a network.

example spec file (leveraging a default placement):

.. code-block:: yaml

    service_type: mgr
    networks:
    - 192.169.142.0/24

.. _cephadm_mgr_co_location:

Allow co-location of MGR daemons
================================

In deployment scenarios with just a single host, cephadm still needs
to deploy at least two MGR daemons in order to allow an automated
upgrade of the cluster. See ``mgr_standby_modules`` in
the :ref:`mgr-administrator-guide` for further details.

See also: :ref:`cephadm_co_location`.


Further Reading
===============

* :ref:`ceph-manager-daemon`
* :ref:`cephadm-manually-deploy-mgr`

