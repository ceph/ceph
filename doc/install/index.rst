.. _install-overview:

===============
Installing Ceph
===============

There are various options for installing Ceph. Review the documention for each method before choosing the one that best serves your needs.

We recommend the following installation methods:

   * cephadm
   * Rook


We offer these other methods of installation in addition to the ones we recommend:

   * ceph-ansible
   * ceph-deploy (no longer actively maintained)
   * Deepsea (Salt)
   * Juju
   * Manual installation (using packages)
   * Puppet


Recommended Methods of Ceph Installation
========================================

cephadm
-------

Installs Ceph using containers and systemd.

* :ref:`cephadm-bootstrap`

     * cephadm is supported only on Octopus and newer releases.
     * cephadm is fully integrated with the new orcehstration API and fully supports the new CLI and dashboard features to manage cluster deployment.
     * cephadm requires container support (podman or docker) and Python 3.

Rook
----

Installs Ceph in Kubernetes.

* `rook.io <https://rook.io/>`_ 

   * Rook supports only Nautilus and newer releases of Ceph.
   * Rook is the preferred deployment method for Ceph with Kubernetes.
   * Rook fully suports the new orchestrator API. New management features in the CLI and dashboard are fully supported.

Other Methods of Ceph Installation
==================================

ceph-ansible
------------

Installs Ceph using Ansible.

* `docs.ceph.com/ceph-ansible <https://docs.ceph.com/ceph-ansible/>`_ 

ceph-deploy
-----------

Install ceph using ceph-deploy

* :ref:`ceph-deploy-index`

  .. IMPORTANT::


   ceph-deploy is no longer actively maintained. It is not tested on versions of Ceph newer than Nautilus. It does not support RHEL8, CentOS 8, or newer operating systems.


DeepSea
-------

Install Ceph using Salt

* `github.com/SUSE/DeepSea <https://github.com/SUSE/DeepSea>`_

Juju
----

Installs Ceph using Juju.

* `jaas.ai/ceph-mon <https://jaas.ai/ceph-mon>`_


Manual
------

Manually install Ceph using packages.

* :ref:`install-manual`

Puppet
------

Installs Ceph using Puppet

* `github.com/openstack/puppet-ceph <https://github.com/openstack/puppet-ceph>`_




.. toctree::
   :hidden:
  
   index_manual
