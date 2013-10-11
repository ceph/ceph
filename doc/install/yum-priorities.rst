===========================
 Installing YUM Priorities
===========================

Ceph builds packages for Apache and FastCGI (for 100-continue support) and
QEMU (for ``rbd`` support). You must set priorities in your ``.repo`` 
files to ensure that ``yum`` installs the Ceph packages instead of the 
standard packages. The ``priorities`` setting requires you to install  
and enable ``yum-plugin-priorities``.

#. Install ``yum-plugin-priorities``. ::

	sudo yum install yum-plugin-priorities

#. Ensure ``/etc/yum/pluginconf.d/priorities.conf`` exists. :: 

#. Ensure ``priorities.conf`` enables the plugin. :: 

	[main]
	enabled = 1
