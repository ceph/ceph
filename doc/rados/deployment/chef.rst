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

You may also set Ceph settings within ``"config": {}``.

For example::

    "default_attributes" : {
        "ceph": {
            "monitor-secret": "{replace-with-generated-secret}",
            "config": {
                "fsid": "{replace-with-generated-uuid}",
                "mon_initial_members": "{replace-with-monitor-hostname(s)}",
                "global": {
                    "public network": "xxx.xxx.xxx.xxx/yy",
                    "cluster network": "xxx.xxx.xxx.xxx/yy"
                },
                "osd": {
                    "osd journal size": "1000"
                }
            }
        }
    }

Will generate the following ceph.conf::

    [global]
        fsid = <fsid>
        mon initial members = X,Y,Z
        mon host = ipX:port, ipY:port, ipZ:port ;mon host is auto generated
        public network = xxx.xxx.xxx.xxx/yy
        cluster network = xxx.xxx.xxx.xxx/yy

    [osd]
        osd journal size = 1000

Advanced users (i.e., developers and QA) may also add ``"branch": "{branch}"``
to ``"ceph": {}``. Valid values are ``stable``, ``testing``, ``dev``.
You can specify which stable release (e.g. argonaut, bobtail) or which dev
branch to use with ``"version": "{version}"`` within ``"ceph": {}``.
If ``version`` is not specified for ``stable``, the latest stable release
will be used. ``testing`` does not require ``version``.

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
	
.. note:: for each host where you installed Chef and executed ``chef-client``, 
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

.. _deployosds:

Deploy OSDs
=================

Configuring a node with an OSD role tells Chef that the node will run at
least one OSD. However, you may run many OSDs on one host. For example, 
you may run one ``ceph-osd`` daemon for each data disk on the system. 
To tell Chef to deploy OSDs, edit the node and add the following
within ``"normal": {}``::

    "ceph": {
        "osd_devices": [
            {
                "device": "/dev/...",
                "journal": "/dev/..."
            },
            {
                "device": "/dev/...",
                "dmcrypt": true
            }
        ]
    }
	
Supported values are ``device``, ``journal``, ``dmcrypt`` (deactivated by default).

.. note:: dmcrypt is only supported starting with Cuttlefish

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
