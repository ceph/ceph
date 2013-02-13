=================================
 Using ``libvirt`` with Ceph RBD
=================================

The ``libvirt`` library creates a virtual machine abstraction layer between 
hypervisor interfaces and the software applications that use them. With 
``libvirt``, developers and system administrators can focus on a common 
management framework, common API, and common shell interface (i.e., ``virsh``)
to many different hypervisors, including: 

- QEMU/KVM
- XEN
- LXC
- VirtualBox
- etc.

Ceph block devices support QEMU/KVM. You can use Ceph block devices with
software that interfaces with ``libvirt``. The following stack diagram
illustrates how ``libvirt`` and QEMU use Ceph block devices via ``librbd``. 


.. ditaa::  +---------------------------------------------------+
            |                     libvirt                       |
            +------------------------+--------------------------+
                                     |
                                     | configures
                                     v
            +---------------------------------------------------+
            |                       QEMU                        |
            +---------------------------------------------------+
            |                      librbd                       |
            +------------------------+-+------------------------+
            |          OSDs          | |        Monitors        |
            +------------------------+ +------------------------+


The most common ``libvirt`` use case involves providing Ceph block devices to
cloud solutions like OpenStack or CloudStack. The cloud solution uses
``libvirt`` to  interact with QEMU/KVM, and QEMU/KVM interacts with Ceph block
devices via  ``librbd``. See `Block Devices and OpenStack`_ and `Block Devices
and CloudStack`_ for details.

You can also use Ceph block devices with ``libvirt``, ``virsh`` and the
``libvirt`` API. See `libvirt Virtualization API`_ for details.

Prerequisites
=============

- `Install`_ and `configure`_ a Ceph cluster
- `Install and configure`_ QEMU/KVM


Installing ``libvirt`` on Ubuntu 12.04 Precise
==============================================

``libvirt`` packages are incorporated into the Ubuntu 12.04 precise 
distribution. To install ``libvirt`` on precise, execute the following:: 

	sudo apt-get update && sudo apt-get install libvirt-bin


Installing ``libvirt`` on Earlier Versions of Ubuntu
====================================================

For Ubuntu distributions 11.10 oneiric and earlier, you must build  ``libvirt``
from source. Clone the ``libvirt`` repository, and use `AutoGen`_ to generate
the build. Then, execute ``make`` and ``make install`` to complete the
installation. For example::

	git clone git://libvirt.org/libvirt.git
	cd libvirt
	./autogen.sh
	make
	sudo make install 

See `libvirt Installation`_ for details.


Using Ceph with Virtual Machines
================================

To create VMs that use Ceph block devices, use the procedures in the following
sections. In the exemplary embodiment, we've used ``libvirt-pool`` for the pool
name, ``client.libvirt`` for the user name, and ``new-libvirt-image`` for  the
image name. You may use any value you like, but ensure you replace those values
when executing commands in the subsequent procedures.


Configuring Ceph
----------------

To configure Ceph for use with ``libvirt``, perform the following steps:

#. `Create a pool`_ (or use the default). The following example uses the 
   pool name ``libvirt-pool`` with 128 placement groups. ::

	ceph osd pool create libvirt-pool 128 128

   Verify the pool exists. :: 

	ceph osd lspools

#. `Create a Ceph Name`_ (or use ``client.admin`` for version 0.9.7 and earlier).
   The following example uses the Ceph name ``client.libvirt`` and references
   ``libvirt-pool``. ::

	ceph auth get-or-create client.libvirt mon 'allow r' osd 'allow class-read object_prefix rbd_children, allow rwx pool=libvirt-pool'
	
   Verify the name exists. :: 
   
	ceph auth list

   **NOTE**: ``libvirt`` will access Ceph using the ID ``libvirt``, 
   not the Ceph name ``client.libvirt``. See `Cephx Commandline`_ for detailed
   explanation of the difference between ID and name.	

#. Use QEMU to `create an image`_ in your RBD pool. 
   The following example uses the image name ``new-libvirt-image``
   and references ``libvirt-pool``. ::

	qemu-img create -f rbd rbd:libvirt-pool/new-libvirt-image 2G

   Verify the image exists. :: 

	rbd -p libvirt-pool ls

   **NOTE:** You can also use `rbd create`_ to create an image, but we
   recommend ensuring that QEMU is working properly.



Preparing the VM Manager
------------------------

You may use ``libvirt`` without a VM manager, but you may find it simpler to
create your first domain with ``virt-manager``. 

#. Install a virtual machine manager. See `KVM/VirtManager`_ for details. ::

	sudo apt-get install virt-manager

#. Download an OS image (if necessary).

#. Launch the virtual machine manager. :: 

	sudo virt-manager



Creating a VM
-------------

To create a VM with ``virt-manager``, perform the following steps:

#. Press the **Create New Virtual Machine** button. 

#. Name the new virtual machine domain. In the exemplary embodiment, we
   use the name ``libvirt-virtual-machine``. You may use any name you wish,
   but ensure you replace ``libvirt-virtual-machine`` with the name you 
   choose in subsequent commandline and configuration examples. :: 

	libvirt-virtual-machine

#. Import the image. ::

	/path/to/image/recent-linux.img

   **NOTE:** Import a recent image. Some older images may not rescan for 
   virtual devices properly.
   
#. Configure and start the VM.

#. You may use ``virsh list`` to verify the VM domain exists. ::

	sudo virsh list

#. Login to the VM (root/root)

#. Stop the VM before configuring it for use with Ceph.


Configuring the VM
------------------

When configuring the VM for use with Ceph, it is important  to use ``virsh``
where appropriate. Additionally, ``virsh`` commands often require root
privileges  (i.e., ``sudo``) and will not return appropriate results or notify
you that that root privileges are required. For a reference of ``virsh``
commands, refer to `Virsh Command Reference`_.


#. Open the configuration file with ``virsh edit``. :: 

	sudo virsh edit {vm-domain-name}

   Under ``<devices>`` there should be a ``<disk>`` entry. :: 

	<devices>
		<emulator>/usr/bin/kvm</emulator>
		<disk type='file' device='disk'>
			<driver name='qemu' type='raw'/>
			<source file='/path/to/image/recent-linux.img'/>
			<target dev='hda' bus='ide'/>
			<address type='drive' controller='0' bus='0' unit='0'/>
		</disk>


   Replace ``/path/to/image/recent-linux.img`` with the path to the OS image.

   **IMPORTANT:** Use ``sudo virsh edit`` instead of a text editor. If you edit 
   the configuration file under ``/etc/libvirt/qemu`` with a text editor, 
   ``libvirt`` may not recognize the change. If there is a discrepancy between 
   the contents of the XML file under ``/etc/libvirt/qemu`` and the result of 
   ``sudo virsh dumpxml {vm-domain-name}``, then your VM may not work 
   properly.
   

#. Add the Ceph RBD image you created as a ``<disk>`` entry. :: 

	<disk type='network' device='disk'>
		<source protocol='rbd' name='libvirt-pool/new-libvirt-image'>
			<host name='{monitor-host}' port='6789'/>
		</source>
		<target dev='hdb' bus='ide'/>
	</disk>

   Replace ``{monitor-host}`` with the name of your host, and replace the 
   pool and/or image name as necessary. You may add multiple ``<host>`` 
   entries for your Ceph monitors. The ``dev`` attribute is the logical
   device name that will appear under the ``/dev`` directory of your 
   VM. The optional ``bus`` attribute indicates the type of disk device to 
   emulate. The valid settings are driver specific (e.g., "ide", "scsi", 
   "virtio", "xen", "usb" or "sata").
   
   See `Disks`_ for details of the ``<disk>`` element, and its child elements
   and attributes.
	
#. Save the file.

#. If you are using `Ceph Authentication`_, you must generate a secret. :: 

	cat > secret.xml <<EOF
	<secret ephemeral='no' private='no'>
		<usage type='ceph'>
			<name>client.libvirt secret</name>
		</usage>
	</secret>
	EOF

#. Define the secret. ::

	sudo virsh secret-define --file secret.xml
	<uuid of secret is output here>

#. Get the ``client.libvirt`` key and save the key string to a file. ::

	sudo ceph auth list
	vim client.libvirt.key

#. Set the UUID of the secret. :: 

	sudo virsh secret-set-value --secret {uuid of secret} --base64 $(cat client.libvirt.key) && rm client.libvirt.key secret.xml

   You must also set the secret manually by adding the following ``<auth>`` 
   entry to the ``<disk>`` element you entered earlier (replacing the
   ``uuid`` value with the result from the command line example above). ::

	sudo virsh edit {vm-domain-name}

   Then, add ``<auth></auth>`` element to the domain configuration file::

	...
	</source>
	<auth username='libvirt'>
		<secret type='ceph' uuid='9ec59067-fdbc-a6c0-03ff-df165c0587b8'/>
	</auth>
	<target ... 


   **NOTE:** The exemplary ID is ``libvirt``, not the Ceph name 
   ``client.libvirt`` as generated at step 2 of `Configuring Ceph`_. Ensure 
   you use the ID component of the Ceph name you generated. If for some reason 
   you need to regenerate the secret, you will have to execute 
   ``sudo virsh secret-undefine {uuid}`` before executing 
   ``sudo virsh secret-set-value`` again.


Summary
-------

Once you have configured the VM for use with Ceph, you can start the VM.
To verify that the VM and Ceph are communicating, you may perform the
following procedures.


#. Check to see if Ceph is running:: 

	ceph health

#. Check to see if the VM is running. :: 

	sudo virsh list

#. Check to see if the VM is communicating with Ceph. Replace 
   ``{vm-domain-name}`` with the name of your VM domain:: 

	sudo virsh qemu-monitor-command --hmp {vm-domain-name} 'info block'

#. Check to see if the device from ``<target dev='hdb' bus='ide'/>`` appears
   under ``/dev`` or under ``proc/partitions``. :: 
   
	ls dev
	cat proc/partitions

If everything looks okay, you may begin using the Ceph block device 
within your VM.



.. _AutoGen: http://www.gnu.org/software/autogen/
.. _libvirt Installation: http://www.libvirt.org/compiling.html
.. _libvirt Virtualization API: http://www.libvirt.org
.. _Install: ../../install
.. _configure: ../../rados/configuration
.. _Install and configure: ../qemu-rbd
.. _Block Devices and OpenStack: ../rbd-openstack
.. _Block Devices and CloudStack: ../rbd-cloudstack
.. _Create a pool: ../../rados/operations/pools#create-a-pool
.. _Create a Ceph Name: ../../rados/operations/authentication#add-a-key
.. _create an image: ../qemu-rbd#creating-images-with-qemu
.. _Virsh Command Reference: http://www.libvirt.org/virshcmdref.html
.. _KVM/VirtManager: https://help.ubuntu.com/community/KVM/VirtManager
.. _Ceph Authentication: ../../rados/operations/auth-intro
.. _Disks: http://www.libvirt.org/formatdomain.html#elementsDisks
.. _rbd create: ../rados-rbd-cmds#creating-a-block-device-image
.. _Cephx Commandline: ../../rados/operations/authentication#cephx-commandline-options