=====================
 Deploying with Chef
=====================

We use Chef cookbooks to deploy Ceph. See `Managing Cookbooks with Knife`_ for details
on using ``knife``.  For Chef installation instructions, see
`Installing Chef <../../install/chef>`_.

Add a Cookbook Path
-------------------
Add the ``cookbook_path`` to your ``~/.ceph/knife.rb`` configuration file. For example:: 

	cookbook_path '/home/userId/.chef/ceph-cookbooks'

Install Ceph Cookbooks
----------------------
To get the cookbooks for Ceph, clone them from git.::

	cd ~/.chef	
	git clone https://github.com/ceph/ceph-cookbooks.git
	knife cookbook site upload parted btrfs parted

Install Apache Cookbooks
------------------------
RADOS Gateway uses Apache 2. So you must install the Apache 2 cookbooks. 
To retrieve the Apache 2 cookbooks, execute the following::  

	cd ~/.chef/ceph-cookbooks
	knife cookbook site download apache2

The `apache2-{version}.tar.gz`` archive will appear in your ``~/.ceph`` directory.
In the following example, replace ``{version}`` with the version of the Apache 2
cookbook archive knife retrieved. Then, expand the archive and upload it to the 
Chef server.:: 

	tar xvf apache2-{version}.tar.gz
	knife cookbook upload apache2

Configure Chef
--------------
To configure Chef, you must specify an environment and a series of roles. You 
may use the Web UI or ``knife`` to perform these tasks.

The following instructions demonstrate how to perform these tasks with ``knife``.


Create a role file for the Ceph monitor. :: 

	cat >ceph-mon.rb <<EOF
	name "ceph-mon"
	description "Ceph monitor server"
	run_list(
		'recipe[ceph::single_mon]'
	)
	EOF

Create a role file for the OSDs. ::

	cat >ceph-osd.rb <<EOF
	name "ceph-osd"
	description "Ceph object store"
	run_list(
		'recipe[ceph::bootstrap_osd]'
	)
	EOF

Add the roles to Chef using ``knife``. :: 

	knife role from file ceph-mon.rb ceph-osd.rb

You may also perform the same tasks with the command line and a ``vim`` editor.
Set an ``EDITOR`` environment variable. :: 

	export EDITOR=vi

Then exectute:: 

	knife create role {rolename}

The ``vim`` editor opens with a JSON object, and you may edit the settings and
save the JSON file.

Finally configure the nodes. ::

	knife node edit {nodename}




.. _Managing Cookbooks with Knife: http://wiki.opscode.com/display/chef/Managing+Cookbooks+With+Knife
