----------------------------------
The iSCSI Initiator for VMware ESX
----------------------------------

**Prerequisite:**

-  VMware ESX 6.0 or later

**iSCSI Discovery and Multipath Device Setup:**

#. From vSphere, open the Storage Adapters, on the Configuration tab. Right click
   on the iSCSI Software Adapter and select Properties.

#. In the General tab click the "Advanced" button and in the "Advanced Settings"
   set RecoveryTimeout to 25.

#. If CHAP was setup on the iSCSI gateway, in the General tab click the "CHAP…​"
   button. If CHAP is not being used, skip to step 4.

#. On the CHAP Credentials windows, select “Do not use CHAP unless required by target”,
   and enter the "Name" and "Secret" values used on the initial setup for the iSCSI
   gateway, then click on the "OK" button.

#. On the Dynamic Discovery tab, click the "Add…​" button, and enter the IP address
   and port of one of the iSCSI target portals. Click on the "OK" button.

#. Close the iSCSI Initiator Properties window. A prompt will ask to rescan the
   iSCSI software adapter. Select Yes.

#. In the Details pane, the LUN on the iSCSI target will be displayed. Right click
   on a device and select "Manage Paths".

#. On the Manage Paths window, select “Most Recently Used (VMware)” for the policy
   path selection. Close and repeat for the other disks.

Now the disks can be used for datastores.
