=================
 Installing Chef
=================

Chef defines three types of entities:
 
#. **Chef Nodes:** Run ``chef-client``, which installs and manages software.
#. **Chef Server:** Interacts with ``chef-client`` on Chef nodes.
#. **Chef Workstation:** Manages the Chef server.

See `Chef Architecture Introduction`_ for details.

.. _createuser:

Create a ``chef`` User
======================

The ``chef-client`` command requires the proper privileges to install and manage
installations. On each Chef node, we recommend creating a ``chef`` user with 
full ``root`` privileges. For example:: 

	ssh user@chef-node
	sudo useradd -d /home/chef -m chef
	sudo passwd chef
	
To provide full privileges, add the following to ``/etc/sudoers.d/chef``. ::

	echo "chef ALL = (root) NOPASSWD:ALL" | sudo tee /etc/sudoers.d/chef
	sudo chmod 0440 /etc/sudoers.d/chef
 
If you are using a version of ``sudo`` that doesn't support includes, you will
need to add the following to the ``/etc/sudoers`` file::

	chef ALL = (root) NOPASSWD:ALL

.. important:: Do not change the file permissions on ``/etc/sudoers``. Use a
   suitable tool such as ``visudo``.
	
.. _genkeys:

Generate SSH Keys for Chef Clients
==================================

Chef's ``knife`` tool can run ``ssh``. To streamline deployments, we 
recommend generating an SSH key pair without a passphrase for your 
Chef nodes and copying the public key(s) to your Chef nodes so that you 
can connect to them from your workstation using ``ssh`` from ``knife``
without having to provide a password. To generate a key pair without 
a passphrase, execute the following on your Chef workstation. :: 

	ssh-keygen
	Generating public/private key pair.
	Enter file in which to save the key (/ceph-admin/.ssh/id_rsa): 
	Enter passphrase (empty for no passphrase): 
	Enter same passphrase again: 
	Your identification has been saved in /ceph-admin/.ssh/id_rsa.
	Your public key has been saved in /ceph-admin/.ssh/id_rsa.pub.

You may use RSA or DSA keys. Once you generate your keys, copy them to each 
OSD host. For example:: 

	ssh-copy-id chef@your-node

Consider modifying your ``~/.ssh/config`` file so that it defaults to 
logging in as ``chef`` when no username is specified. :: 

	Host myserver01
		Hostname myserver01.fqdn-or-ip-address.com
		User chef
	Host myserver02
		Hostname myserver02.fqdn-or-ip-address.com
		User chef

.. _installruby:

Installing Ruby
===============

Chef requires you to install Ruby. Use the version applicable to your current 
Linux distribution and install Ruby on all of your hosts. ::

	sudo apt-get update
	sudo apt-get install ruby

.. _installchefserver:

Installing Chef and Chef Server on a Server
===========================================

If you plan on hosting your `Chef Server at Opscode`_ you may skip this step, 
but you must make a note of the the fully qualified domain name or IP address
of your Chef Server for ``knife`` and ``chef-client``.

First, add Opscode packages to your APT configuration. For example:: 

	sudo tee /etc/apt/sources.list.d/chef.list << EOF
	deb http://apt.opscode.com/ $(lsb_release -cs)-0.10 main  
	deb-src http://apt.opscode.com/ $(lsb_release -cs)-0.10 main
	EOF

Next, you must request keys so that APT can verify the packages. Copy
and paste the following line into your command line:: 

	sudo touch /etc/apt/trusted.gpg.d/opscode-keyring.gpg && sudo gpg --fetch-key http://apt.opscode.com/packages@opscode.com.gpg.key && sudo gpg --export 83EF826A | sudo apt-key --keyring /etc/apt/trusted.gpg.d/opscode-keyring.gpg add - && sudo gpg --yes --delete-key 83EF826A

The key is only used by ``apt``, so remove it from the ``root`` keyring by
typing ``Y`` when prompted to delete it.

Install the Opscode keyring, Chef and Chef server on the host designated
as your Chef Server. ::

	sudo apt-get update && sudo apt-get upgrade && sudo apt-get install opscode-keyring chef chef-server

Enter the fully qualified domain name or IP address for your Chef server. For example::

	http://fqdn-or-ip-address.com:4000

The Chef server installer will prompt you to enter a temporary password. Enter
a temporary password (*e.g.,* ``foo``) and proceed with the installation. 

.. tip:: When prompted for a temporary password, you may press **OK**.
   The installer wants you to re-enter the password to confirm it. To 
   re-enter the password, you must press the **ESC** key.

Once the installer finishes and activates the Chef server, you may enter the 
fully qualified domain name or IP address in a browser to launch the 
Chef web UI. For example:: 

	http://fqdn-or-ip-address.com:4000

The Chef web UI will prompt you to enter the username and password.

- **login:** ``admin``
- **password:** ``foo``

Once you have entered the temporary password, the Chef web UI will prompt you
to enter a new password.

.. _installchef:

Install Chef on all Remaining Hosts
===================================

Install Chef on all Chef Nodes and on the Chef Workstation (if it is not the 
same host as the Chef Server). See `Installing Chef Client on Ubuntu or Debian`_
for details.

First, add Opscode packages to your APT configuration. For example:: 

	sudo tee /etc/apt/sources.list.d/chef.list << EOF
	deb http://apt.opscode.com/ $(lsb_release -cs)-0.10 main  
	deb-src http://apt.opscode.com/ $(lsb_release -cs)-0.10 main
	EOF

Next, you must request keys so that APT can verify the packages. Copy
and paste the following line into your command line:: 

	sudo touch /etc/apt/trusted.gpg.d/opscode-keyring.gpg && sudo gpg --fetch-key http://apt.opscode.com/packages@opscode.com.gpg.key && sudo gpg --export 83EF826A | sudo apt-key --keyring /etc/apt/trusted.gpg.d/opscode-keyring.gpg add - && sudo gpg --yes --delete-key 83EF826A

The key is only used by ``apt``, so remove it from the ``root`` keyring by
typing ``Y`` when prompted to delete it.

Install the Opscode keyring and Chef on all hosts other than the Chef Server. ::

	sudo apt-get update && sudo apt-get upgrade && sudo apt-get install opscode-keyring chef

Enter the fully qualified domain name or IP address for your Chef server. 
For example::

	http://fqdn-or-ip-address.com:4000

.. _configknife:

Configuring Knife
=================

Once you complete the Chef server installation, install ``knife`` on the your
Chef Workstation. If the Chef server is a remote host, use ``ssh`` to connect. :: 

	ssh chef@fqdn-or-ip-address.com

In the ``/home/chef`` directory, create a hidden Chef directory. :: 

	mkdir -p ~/.chef

The server generates validation and web UI certificates with read/write 
permissions for the user that installed the Chef server. Copy them from the
``/etc/chef`` directory to the ``~/.chef`` directory. Then, change their 
ownership to the current user. ::
	
	sudo cp /etc/chef/validation.pem /etc/chef/webui.pem ~/.chef && sudo chown $(id -u):$(id -g) ~/.chef/*.pem

From the current user's home directory, configure ``knife`` with an initial 
API client. :: 

	knife configure -i

The configuration will prompt you for inputs. Answer accordingly: 

*Where should I put the config file? [~/.chef/knife.rb]* Press **Enter** 
to accept the default value.

*Please enter the chef server URL:* If you are installing the 
client on the same host as the server, enter ``http://localhost:4000``. 
Otherwise, enter an appropriate URL for the server.

*Please enter a clientname for the new client:* Press **Enter** 
to accept the default value.

*Please enter the existing admin clientname:* Press **Enter** 
to accept the default value.

*Please enter the location of the existing admin client's private key:* 
Override the default value so that it points to the ``.chef`` directory. 
(*e.g.,* ``/home/chef/.chef/webui.pem``)

*Please enter the validation clientname:* Press **Enter** to accept 
the default value.

*Please enter the location of the validation key:* Override the 
default value so that it points to the ``.chef`` directory. 
(*e.g.,* ``/home/chef/.chef/validation.pem``)

*Please enter the path to a chef repository (or leave blank):*
Leave the entry field blank and press **Enter**.

.. _addcbpath:

Add a Cookbook Path
===================

Add ``cookbook_path`` to the ``~/.chef/knife.rb`` configuration file
on your Chef workstation. For example::

	cookbook_path '/home/{user-name}/chef-cookbooks/'
	
Then create the path if it doesn't already exist. ::

	mkdir /home/{user-name}/chef-cookbooks
	
This is where you will store local copies of cookbooks before uploading
them to the Chef server.

.. _cpvalpem:

Copy ``validation.pem`` to Nodes
================================

Copy the ``/etc/chef/validation.pem`` file from your Chef server to
each Chef Node. In a command line shell on the Chef Server, for each node, 
replace ``{nodename}`` in the following line with the node's host name and 
execute it. ::

	sudo cat /etc/chef/validation.pem | ssh {nodename} "exec sudo tee /etc/chef/validation.pem >/dev/null"

.. _runchefcli:

Run ``chef-client`` on each Chef Node
=====================================

Run the ``chef-client`` on each Chef Node so that the nodes
register with the Chef server. :: 

	ssh chef-node
	sudo chef-client

.. _verifynodes:

Verify Nodes
============

Verify that you have setup all the hosts you want to use as 
Chef nodes. :: 

	knife node list

A list of the nodes you've configured should appear.

See the `Deploy With Chef <../../config-cluster/chef>`_ section for information
on using Chef to deploy your Ceph cluster.

.. _Chef Architecture Introduction: http://wiki.opscode.com/display/chef/Architecture+Introduction
.. _Chef Server at Opscode: http://www.opscode.com/hosted-chef/
.. _Installing Chef Client on Ubuntu or Debian: http://wiki.opscode.com/display/chef/Installing+Chef+Client+on+Ubuntu+or+Debian
.. _Installing Chef Server on Debian or Ubuntu using Packages: http://wiki.opscode.com/display/chef/Installing+Chef+Server+on+Debian+or+Ubuntu+using+Packages
.. _Knife Bootstrap: http://wiki.opscode.com/display/chef/Knife+Bootstrap