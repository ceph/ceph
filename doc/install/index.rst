.. _install-overview:

===============
Installing Ceph
===============

There are multiple ways to install Ceph.

Recommended methods
~~~~~~~~~~~~~~~~~~~

:ref:`Cephadm <cephadm_deploying_new_cluster>` is a tool that can be used to
install and manage a Ceph cluster.

* cephadm supports only Octopus and newer releases.
* cephadm is fully integrated with the orchestration API and fully supports the
  CLI and dashboard features that are used to manage cluster deployment.
* cephadm requires container support (in the form of Podman or Docker) and
  Python 3.
* cephadm requires systemd.

`Rook <https://rook.io/>`_ deploys and manages Ceph clusters running
in Kubernetes, while also enabling management of storage resources and
provisioning via Kubernetes APIs. We recommend Rook as the way to run Ceph in
Kubernetes or to connect an existing Ceph storage cluster to Kubernetes.

* Rook supports only Nautilus and newer releases of Ceph.
* Rook is the preferred method for running Ceph on Kubernetes, or for
  connecting a Kubernetes cluster to an existing (external) Ceph
  cluster.
* Rook supports the orchestrator API. Management features in the CLI and
  dashboard are fully supported.

Other methods
~~~~~~~~~~~~~

`ceph-ansible <https://docs.ceph.com/ceph-ansible/>`_ deploys and manages
Ceph clusters using Ansible.

* ceph-ansible is widely deployed.
* ceph-ansible is not integrated with the orchestrator APIs that were
  introduced in Nautilus and Octopus, which means that the management features
  and dashboard integration introduced in Nautilus and Octopus are not
  available in Ceph clusters deployed by means of ceph-ansible.


`ceph-deploy <https://docs.ceph.com/projects/ceph-deploy/en/latest/>`_ is a
tool that can be used to quickly deploy clusters. It is deprecated.

  .. IMPORTANT::

   ceph-deploy is not actively maintained. It is not tested on versions of Ceph
   newer than Nautilus. It does not support RHEL8, CentOS 8, or newer operating
   systems.

`ceph-salt <https://github.com/ceph/ceph-salt>`_ installs Ceph using Salt and cephadm.

`jaas.ai/ceph-mon <https://jaas.ai/ceph-mon>`_ installs Ceph using Juju.

`github.com/openstack/puppet-ceph <https://github.com/openstack/puppet-ceph>`_  installs Ceph via Puppet.

`OpenNebula HCI clusters <https://docs.opennebula.io/stable/provision_clusters/hci_clusters/overview.html>`_ deploys Ceph on various cloud platforms.

Ceph can also be :ref:`installed manually <install-manual>`.


.. toctree::
   :hidden:

   index_manual

Windows
~~~~~~~

For Windows installations, consult this document:
`Windows installation guide`_.

.. _Windows installation guide: ./windows-install
