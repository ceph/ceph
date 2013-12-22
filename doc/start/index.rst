======================
 Installation (Quick)
======================

.. raw:: html

	<style type="text/css">div.body h3{margin:5px 0px 0px 0px;}</style>
	<table cellpadding="10"><colgroup><col width="33%"><col width="33%"><col width="33%"></colgroup><tbody valign="top"><tr><td><h3>Step 1: Preflight</h3>

A :term:`Ceph Client` and a :term:`Ceph Node` may require some basic
configuration  work prior to deploying a Ceph Storage Cluster. You can also
avail yourself of help by getting involved in the Ceph community.

.. toctree::

   Preflight <quick-start-preflight>

.. raw:: html 

	</td><td><h3>Step 2: Storage Cluster</h3>
	
Once you've completed your preflight checklist, you should be able to begin
deploying a Ceph Storage Cluster.

.. toctree::

	Storage Cluster Quick Start <quick-ceph-deploy>


.. raw:: html 

	</td><td><h3>Step 3: Ceph Client(s)</h3>
	
Most Ceph users don't store objects directly in the Ceph Storage Cluster. They typically use at least one of
Ceph Block Devices, the Ceph Filesystem, and Ceph Object Storage.

.. toctree::
	
   Block Device Quick Start <quick-rbd>
   Filesystem Quick Start <quick-cephfs>
   Object Storage Quick Start <quick-rgw>

.. raw:: html

	</td></tr></tbody></table>


