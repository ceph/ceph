-------------------------------------
iSCSI Initiator for Microsoft Windows
-------------------------------------

**Prerequisite:**

-  Microsoft Windows Server 2016

**iSCSI Initiator, Discovery and Setup:**

#. Install the iSCSI initiator driver and MPIO tools.

#. Launch the MPIO program, click on the "Discover Multi-Paths" tab, check the
   "Add support for iSCSI devices” box, and click "Add". This will require a
   reboot.

#. On the iSCSI Initiator Properties window, on the "Discovery" tab, add a target
   portal. Enter the IP address or DNS name and Port of the Ceph iSCSI gateway.

#. On the “Targets” tab, select the target and click on “Connect”.

#. On the “Connect To Target” window, select the “Enable multi-path” option, and
   click the “Advanced” button.

#. Under the "Connet using" section, select a “Target portal IP” . Select the
   “Enable CHAP login on” and enter the "Name" and "Target secret" values from the
   Ceph iSCSI Ansible client credentials section, and click OK.

#. Repeat steps 5 and 6 for each target portal defined when setting up
   the iSCSI gateway.

**Multipath IO Setup:**

Configuring the MPIO load balancing policy, setting the timeout and
retry options are using PowerShell with the ``mpclaim`` command. The
rest is done in the iSCSI Initiator tool.

.. note::
  It is recommended to increase the ``PDORemovePeriod`` option to 120
  seconds from PowerShell. This value might need to be adjusted based
  on the application. When all paths are down, and 120 seconds
  expires, the operating system will start failing IO requests.

::

    Set-MPIOSetting -NewPDORemovePeriod 120

::

    mpclaim.exe -l -m 1

::

    mpclaim -s -m
    MSDSM-wide Load Balance Policy: Fail Over Only

#. Using the iSCSI Initiator tool, from the “Targets” tab, click on
   the “Devices...” button.

#. From the Devices window, select a disk and click the
   “MPIO...” button.

#. On the "Device Details" window the paths to each target portal is
   displayed. If using the ``ceph-ansible`` setup method, the
   iSCSI gateway will use ALUA to tell the iSCSI initiator which path
   and iSCSI gateway should be used as the primary path. The Load
   Balancing Policy “Fail Over Only” must be selected

::

    mpclaim -s -d $MPIO_DISK_ID

.. note::
  For the ``ceph-ansible`` setup method, there will be one
  Active/Optimized path which is the path to the iSCSI gateway node
  that owns the LUN, and there will be an Active/Unoptimized path for
  each other iSCSI gateway node.

**Tuning:**

Consider using the following registry settings:

-  Windows Disk Timeout

   ::

       HKEY_LOCAL_MACHINE\System\CurrentControlSet\Services\Disk

   ::

       TimeOutValue = 65

-  Microsoft iSCSI Initiator Driver

   ::

       HKEY_LOCAL_MACHINE\\SYSTEM\CurrentControlSet\Control\Class\{4D36E97B-E325-11CE-BFC1-08002BE10318}\<Instance_Number>\Parameters

   ::
   
       LinkDownTime = 25
       SRBTimeoutDelta = 15
