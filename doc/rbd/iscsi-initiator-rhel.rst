------------------------------------------------
The iSCSI Initiator for Red Hat Enterprise Linux
------------------------------------------------

**Prerequisite:**

-  Package ``iscsi-initiator-utils-6.2.0.873-35`` or newer must be
   installed

-  Package ``device-mapper-multipath-0.4.9-99`` or newer must be
   installed

**Installing:**

Install the iSCSI initiator and multipath tools:

   ::

       # yum install iscsi-initiator-utils
       # yum install device-mapper-multipath

**Configuring:**

#. Create the default ``/etc/multipath.conf`` file and enable the
   ``multiapthd`` service:

   ::

       # mpathconf --enable --with_multipathd y

#. Add the following to ``/etc/multipath.conf`` file:

   ::

       devices {
               device {
                       vendor                 "LIO-ORG"
                       hardware_handler       "1 alua"
                       path_grouping_policy   "failover"
                       path_selector          "queue-length 0"
                       failback               60
                       path_checker           tur
                       prio                   alua
                       prio_args              exclusive_pref_bit
                       fast_oi_fail_tmo       25
                       no_path_retry          queue
               }
       }

#. Restart the ``multipathd`` service:

   ::

       # systemctl reload multipathd

**iSCSI Discovery and Setup:**

#. Discover the target portals:

   ::

       # iscsiadm -m discovery -t -st 192.168.56.101
       192.168.56.101:3260,1 iqn.2003-01.org.linux-iscsi.rheln1
       192.168.56.102:3260,2 iqn.2003-01.org.linux-iscsi.rheln1

#. Login to target:

   ::

       # iscsiadm -m node -T iqn.2003-01.org.linux-iscsi.rheln1 -l

**Multipath IO Setup:**

The multipath daemon (``multipathd``), will set up devices automatically
based on the ``multipath.conf`` settings. Running the ``multipath``
command show devices setup in a failover configuration with a priority
group for each path.

::

    # multipath -ll
    mpathbt (360014059ca317516a69465c883a29603) dm-1 LIO-ORG ,IBLOCK
    size=1.0G features='0' hwhandler='1 alua' wp=rw
    |-+- policy='queue-length 0' prio=50 status=active
    | `- 28:0:0:1 sde  8:64  active ready running
    `-+- policy='queue-length 0' prio=10 status=enabled
      `- 29:0:0:1 sdc  8:32  active ready running

You should now be able to use the RBD image like you would a normal
multipath’d iSCSI disk.
