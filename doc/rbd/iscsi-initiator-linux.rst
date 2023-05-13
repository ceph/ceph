-------------------------
iSCSI Initiator for Linux
-------------------------

**Prerequisite:**

-  Package ``iscsi-initiator-utils``

-  Package ``device-mapper-multipath``

**Installing:**

Install the iSCSI initiator and multipath tools:

.. prompt:: bash #

   yum install iscsi-initiator-utils
   yum install device-mapper-multipath

**Configuring:**

#. Create the default ``/etc/multipath.conf`` file and enable the
   ``multipathd`` service:

   .. prompt:: bash #

      mpathconf --enable --with_multipathd y

#. Add the following to the ``/etc/multipath.conf`` file:

   ::

       devices {
               device {
                       vendor                 "LIO-ORG"
                       product                "TCMU device"
                       hardware_handler       "1 alua"
                       path_grouping_policy   "failover"
                       path_selector          "queue-length 0"
                       failback               60
                       path_checker           tur
                       prio                   alua
                       prio_args              exclusive_pref_bit
                       fast_io_fail_tmo       25
                       no_path_retry          queue
               }
       }

#. Restart the ``multipathd`` service:

   .. prompt:: bash #

      systemctl reload multipathd

**iSCSI Discovery and Setup:**

#. Enable CHAP authentication and provide the initiator CHAP username
   and password by uncommenting and setting the following options in
   the ``/etc/iscsi/iscsid.conf`` file:

   ::

       node.session.auth.authmethod = CHAP
       node.session.auth.username = myusername
       node.session.auth.password = mypassword

   If you intend to use mutual (bidirectional) authentication, provide the
   target CHAP username and password:

   ::

       node.session.auth.username_in = mytgtusername
       node.session.auth.password_in = mytgtpassword

#. Discover the target portals:

   .. prompt:: bash #

      iscsiadm -m discovery -t st -p 192.168.56.101

   ::

       192.168.56.101:3260,1 iqn.2003-01.org.linux-iscsi.rheln1
       192.168.56.102:3260,2 iqn.2003-01.org.linux-iscsi.rheln1

#. Log in to the target:

   .. prompt:: bash #

      iscsiadm -m node -T iqn.2003-01.org.linux-iscsi.rheln1 -l

**Multipath IO Setup:**

#. The multipath daemon (``multipathd``) uses the ``multipath.conf`` settings
   to set up devices automatically. Running the ``multipath`` command shows
   that the devices have been set up in a failover configuration. Notice that
   each path has been placed into its own priority group: 

   .. prompt:: bash #

      multipath -ll

   ::

       mpathbt (360014059ca317516a69465c883a29603) dm-1 LIO-ORG ,IBLOCK
       size=1.0G features='0' hwhandler='1 alua' wp=rw
       |-+- policy='queue-length 0' prio=50 status=active
       | `- 28:0:0:1 sde  8:64  active ready running
       `-+- policy='queue-length 0' prio=10 status=enabled
         `- 29:0:0:1 sdc  8:32  active ready running

   You should now be able to use the RBD image in the same way that you would
   use a normal multipath iSCSI disk.

#. Log out of the target:

   .. prompt:: bash #

      iscsiadm -m node -T iqn.2003-01.org.linux-iscsi.rheln1 -u
