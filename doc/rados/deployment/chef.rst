=====================
 Deploying with Chef
=====================

We use Chef cookbooks to deploy Ceph. See `Managing Cookbooks with Knife`_ for details
on using ``knife``.  For Chef installation instructions, see `Installing Chef`_.

.. _clonecbs:

Clone the Required Cookbooks
============================

To get the cookbooks for Ceph, clone them from git.::

	cd ~/chef-cookbooks
	git clone https://github.com/opscode-cookbooks/apache2.git
	git clone https://github.com/ceph/ceph-cookbooks.git ceph

.. _addcbpaths:

Add the Required Cookbook Paths
===============================

If you added a default cookbook path when you installed Chef, ``knife``
may be able to upload the cookbook you've cloned to your cookbook path
directory without further configuration. If you used a different path, 
or if the cookbook repository you cloned has a different tree structure, 
add the required cookbook path to your ``knife.rb`` file. The 
``cookbook_path`` setting takes a string or an array of strings. 
For example, you can replace a string path with an array of string paths::

	cookbook_path '/home/{user-name}/chef-cookbooks/'

Becomes::
	
	cookbook_path [
		'/home/{user-name}/chef-cookbooks/', 
		'/home/{user-name}/chef-cookbooks/{another-directory}/',
		'/some/other/path/to/cookbooks/'
	]

.. _installcbs:

Install the Cookbooks
=====================

To install Ceph, you must upload the Ceph cookbooks and the Apache cookbooks
(for use with RADOSGW) to your Chef server. :: 

	knife cookbook upload apache2 ceph

.. _configcephenv:

Configure your Ceph Environment
===============================

The Chef server can support installation of software for multiple environments.
The environment you create for Ceph requires an ``fsid``, the secret for
your monitor(s) if you are running Ceph with ``cephx`` authentication, and
the host name (i.e., short name) for your monitor hosts.

.. tip: Open an empty text file to hold the following values until you create
   your Ceph environment.

For the filesystem ID, use ``uuidgen`` from the ``uuid-runtime`` package to 
generate a unique identifier. :: 

	uuidgen -r

For the monitor(s) secret(s), use ``ceph-authtool`` to generate the secret(s)::

	sudo apt-get update	
	sudo apt-get install ceph-common
	ceph-authtool /dev/stdout --name=mon. --gen-key  
 
The secret is the value to the right of ``"key ="``, and should look something 
like this:: 

	AQBAMuJPINJgFhAAziXIrLvTvAz4PRo5IK/Log==

To create an environment for Ceph, set a command line editor. For example:: 

	export EDITOR=vim

Then, use ``knife`` to create an environment. :: 

	knife environment create {env-name}
	
For example:: 

	knife environment create Ceph

A JSON file will appear. Perform the following steps: 

#. Enter a description for the environment. 
#. In ``"default_attributes": {}``, add ``"ceph" : {}``.
#. Within ``"ceph" : {}``, add ``"monitor-secret":``.
#. Immediately following ``"monitor-secret":`` add the key you generated within quotes, followed by a comma.
#. Within ``"ceph":{}`` and following the ``monitor-secret`` key-value pair, add ``"config": {}``
#. Within ``"config": {}`` add ``"fsid":``.
#. Immediately following ``"fsid":``, add the unique identifier you generated within quotes, followed by a comma.
#. Within ``"config": {}`` and following the ``fsid`` key-value pair, add ``"mon_initial_members":``
#. Immediately following ``"mon_initial_members":``, enter the initial monitor host names.

For example:: 

	"default_attributes" : {
		"ceph": {
			"monitor-secret": "{replace-with-generated-secret}",
			"config": {
				"fsid": "{replace-with-generated-uuid}",
				"mon_initial_members": "{replace-with-monitor-hostname(s)}"
			}
		}
	}
	
Advanced users (i.e., developers and QA) may also add ``"ceph_branch": "{branch}"``
to ``default-attributes``, replacing ``{branch}`` with the name of the branch you
wish to use (e.g., ``master``). 

.. configroles:

Configure the Roles
===================

Navigate to the Ceph cookbooks directory. :: 

	cd ~/chef-cookbooks/ceph
	
Create roles for OSDs, monitors, metadata servers, and RADOS Gateways from
their respective role files. ::

	knife role from file roles/ceph-osd.rb
	knife role from file roles/ceph-mon.rb
	knife role from file roles/ceph-mds.rb
	knife role from file roles/ceph-radosgw.rb

.. _confignodes:

Configure Nodes
===============

You must configure each node you intend to include in your Ceph cluster. 
Identify nodes for your Ceph cluster. ::

	knife node list
	
.. note: for each host where you installed Chef and executed ``chef-client``, 
   the Chef server should have a minimal node configuration. You can create
   additional nodes with ``knife node create {node-name}``.

For each node you intend to use in your Ceph cluster, configure the node 
as follows:: 

	knife node edit {node-name}

The node configuration should appear in your text editor. Change the 
``chef_environment`` value to ``Ceph`` (or whatever name you set for your
Ceph environment). 

In the ``run_list``, add ``"recipe[ceph::apt]",`` to all nodes as 
the first setting, so that Chef can install or update the necessary packages. 
Then, add at least one of:: 

	"role[ceph-mon]"
	"role[ceph-osd]"
	"role[ceph-mds]"
	"role[ceph-radosgw]"

If you add more than one role, separate them with a comma. Run ``hostname``
on your command line, and replace the ``{hostname}`` setting of the ``name`` 
key to the host name for the node. ::

	{
  		"chef_environment": "Ceph",
  		"name": "{hostname}",
  		"normal": {
    		"tags": [

    		]
  		},
 		 "run_list": [
			"recipe[ceph::apt]",
			"role[ceph-mon]",
			"role[ceph-mds]"
  		]
	}

.. _prepdisks:

Prepare OSD Disks
=================

Configuring a node with an OSD role tells Chef that the node will run at
least one OSD. However, you may run many OSDs on one host. For example, 
you may run one ``ceph-osd`` daemon for each data disk on the system. 
This step prepares the OSD disk(s) and tells Chef how many OSDs the 
node will be running.


For the Ceph 0.48 Argonaut release, install ``gdisk``:: 

	sudo apt-get install gdisk

For the Ceph 0.48 Argonaut release, on each hard disk that will store data for
an OSD daemon, configure the  hard disk for use with Ceph. Replace ``{fsid}``
with the UUID you generated  while using ``uuidgen -r``. 

.. important: This procedure will erase all information in ``/dev/{disk}``.

:: 
	
	sudo sgdisk /dev/{disk} --zap-all --clear --mbrtogpt --largest-new=1 --change-name=1:'ceph data' --typecode=1:{fsid}

Create a file system and allocate the disk to your cluster. Specify a 
filesystem (e.g., ``ext4``, ``xfs``, ``btrfs``). When you execute 
``ceph-disk-prepare``, remember to replace ``{fsid}`` with the UUID you 
generated while using ``uuidgen -r``::

	sudo mkfs -t ext4 /dev/{disk}
	sudo mount -o user_xattr /dev/{disk} /mnt
	sudo ceph-disk-prepare --cluster-uuid={fsid} /mnt
	sudo umount /mnt

Finally, simulate a hotplug event. :: 

	sudo udevadm trigger --subsystem-match=block --action=add
	

.. _runchefclient:

Run ``chef-client`` on each Node
================================

Once you have completed the preceding steps, you must run ``chef-client`` 
on each node. For example::

	sudo chef-client

.. _proceedtoops:

Proceed to Operating the Cluster
================================

Once you complete the deployment, you may begin operating your cluster.
See `Operating a Cluster`_ for details.


.. _Managing Cookbooks with Knife: http://wiki.opscode.com/display/chef/Managing+Cookbooks+With+Knife
.. _Installing Chef: ../../deployment/chef
.. _Operating a Cluster: ../../operations/
