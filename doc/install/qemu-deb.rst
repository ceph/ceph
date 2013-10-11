=================
 Installing QEMU
=================



Installing QEMU (12.04 Precise and later)
=========================================

QEMU packages are incorporated into Ubuntu 12.04 Precise Pangolin and later
versions. To  install QEMU, execute the following:: 

	sudo apt-get install qemu

Installing QEMU (11.10 Oneric and earlier)
==========================================

For Ubuntu distributions 11.10 Oneiric and earlier, you must install 
the 0.15 version of QEMU or later. To build QEMU from source, use the
following procedure::

	cd {your-development-directory}
	git clone git://git.qemu.org/qemu.git
	cd qemu
	./configure --enable-rbd
	make; make install
