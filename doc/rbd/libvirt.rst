========================
 Installing ``libvirt``
========================

Installing ``libvirt`` on Ubuntu 12.04 Precise
----------------------------------------------
``libvirt`` packages are incorporated into the Ubuntu 12.04 precise 
distribution. To install ``libvirt`` on precise, execute the following:: 

	sudo apt-get update && sudo apt-get install libvirt-dev

Installing ``libvirt`` on Earlier Versions of Ubuntu
----------------------------------------------------
For Ubuntu distributions 11.10 oneiric and earlier, you must build 
``libvirt`` from source using the following procedure::

	cd {your-development-directory}
	git clone git://libvirt.org/libvirt.git
	cd libvirt
	./autogen.sh --PREFIX=$USR/install-directory
	make
	sudo make install 

See `libvirt Installation`_ for details. 

.. _libvirt Installation: http://www.libvirt.org/compiling.html 