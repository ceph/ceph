.. _rados-troubleshooting-mon:

=================================
 Troubleshooting Monitors
=================================

.. index:: monitor, high availability

When a cluster encounters monitor-related troubles there's a tendency to
panic, and sometimes with good reason. Losing one or more monitors doesn't
necessarily mean that your cluster is down, so long as a majority are up,
running, and form a quorum.
Regardless of how bad the situation is, the first thing you should do is to
calm down, take a breath, and step through the below troubleshooting steps.


Initial Troubleshooting
========================


**Are the monitors running?**

  First of all, we need to make sure the monitor (*mon*) daemon processes
  (``ceph-mon``) are running.  You would be amazed by how often Ceph admins
  forget to start the mons, or to restart them after an upgrade. There's no
  shame, but try to not lose a couple of hours looking for a deeper problem.
  When running Kraken or later releases also ensure that the manager
  daemons (``ceph-mgr``) are running, usually alongside each ``ceph-mon``.
 

**Are you able to reach to the mon nodes?**

  Doesn't happen often, but sometimes there are ``iptables`` rules that
  block accesse to mon nodes or TCP ports. These may be leftovers from
  prior stress-testing or rule development. Try SSHing into
  the server and, if that succeeds, try connecting to the monitor's ports
  (``tcp/3300`` and ``tcp/6789``) using a ``telnet``, ``nc``, or similar tools.

**Does ceph -s run and obtain a reply from the cluster?**

  If the answer is yes then your cluster is up and running.  One thing you
  can take for granted is that the monitors will only answer to a ``status``
  request if there is a formed quorum.  Also check that at least one ``mgr``
  daemon is reported as running, ideally all of them.

  If ``ceph -s`` hangs without obtaining a reply from the cluster
  or showing ``fault`` messages, then it is likely that your monitors
  are either down completely or just a fraction are up -- a fraction
  insufficient to form a majority quorum.  This check will connect to an
  arbitrary mon; in rare cases it may be illuminating to bind to specific
  mons in sequence by adding e.g. ``-m mymon1`` to the command.

**What if ceph -s doesn't come back?**

  If you haven't gone through all the steps so far, please go back and do.

  You can contact each monitor individually asking them for their status,
  regardless of a quorum being formed. This can be achieved using
  ``ceph tell mon.ID mon_status``, ID being the monitor's identifier. You should
  perform this for each monitor in the cluster. In section `Understanding
  mon_status`_ we will explain how to interpret the output of this command.

  You may instead SSH into each mon node and query the daemon's admin socket.


Using the monitor's admin socket
=================================

The admin socket allows you to interact with a given daemon directly using a
Unix socket file. This file can be found in your monitor's ``run`` directory.
By default, the admin socket will be kept in ``/var/run/ceph/ceph-mon.ID.asok``
but this may be elsewhere if you have overridden the default directory. If you
don't find it there, check your ``ceph.conf`` for an alternative path or
run::

  ceph-conf --name mon.ID --show-config-value admin_socket

Bear in mind that the admin socket will be available only while the monitor
daemon is running. When the monitor is properly shut down, the admin socket
will be removed. If however the monitor is not running and the admin socket
persists, it is likely that the monitor was improperly shut down.
Regardless, if the monitor is not running, you will not be able to use the
admin socket, with ``ceph`` likely returning ``Error 111: Connection Refused``.

Accessing the admin socket is as simple as running ``ceph tell`` on the daemon
you are interested in. For example::

  ceph tell mon.<id> mon_status

Under the hood, this passes the command ``help`` to the running MON daemon
``<id>`` via its "admin socket", which is a file ending in ``.asok``
somewhere under ``/var/run/ceph``. Once you know the full path to the file,
you can even do this yourself::

  ceph --admin-daemon <full_path_to_asok_file> <command>

Using ``help`` as the command to the ``ceph`` tool will show you the
supported commands available through the admin socket. Please take a look
at ``config get``, ``config show``, ``mon stat`` and ``quorum_status``,
as those can be enlightening when troubleshooting a monitor.


Understanding mon_status
=========================

``mon_status`` can always be obtained via the admin socket. This command will
output a multitude of information about the monitor, including the same output
you would get with ``quorum_status``.

Take the following example output of ``ceph tell mon.c mon_status``::

  
  { "name": "c",
    "rank": 2,
    "state": "peon",
    "election_epoch": 38,
    "quorum": [
          1,
          2],
    "outside_quorum": [],
    "extra_probe_peers": [],
    "sync_provider": [],
    "monmap": { "epoch": 3,
        "fsid": "5c4e9d53-e2e1-478a-8061-f543f8be4cf8",
        "modified": "2013-10-30 04:12:01.945629",
        "created": "2013-10-29 14:14:41.914786",
        "mons": [
              { "rank": 0,
                "name": "a",
                "addr": "127.0.0.1:6789\/0"},
              { "rank": 1,
                "name": "b",
                "addr": "127.0.0.1:6790\/0"},
              { "rank": 2,
                "name": "c",
                "addr": "127.0.0.1:6795\/0"}]}}

A couple of things are obvious: we have three monitors in the monmap (*a*, *b*
and *c*), the quorum is formed by only two monitors, and *c* is in the quorum
as a *peon*.

Which monitor is out of the quorum?

  The answer would be **a**.

Why?

  Take a look at the ``quorum`` set. We have two monitors in this set: *1*
  and *2*. These are not monitor names. These are monitor ranks, as established
  in the current monmap. We are missing the monitor with rank 0, and according
  to the monmap that would be ``mon.a``.

By the way, how are ranks established?

  Ranks are (re)calculated whenever you add or remove monitors and follow a
  simple rule: the **greater** the ``IP:PORT`` combination, the **lower** the
  rank is. In this case, considering that ``127.0.0.1:6789`` is lower than all
  the remaining ``IP:PORT`` combinations, ``mon.a`` has rank 0.

Most Common Monitor Issues
===========================

Have Quorum but at least one Monitor is down
---------------------------------------------

When this happens, depending on the version of Ceph you are running,
you should be seeing something similar to::

      $ ceph health detail
      [snip]
      mon.a (rank 0) addr 127.0.0.1:6789/0 is down (out of quorum)

How to troubleshoot this?

  First, make sure ``mon.a`` is running.

  Second, make sure you are able to connect to ``mon.a``'s node from the
  other mon nodes. Check the TCP ports as well. Check ``iptables`` and
  ``nf_conntrack`` on all nodes and ensure that you are not
  dropping/rejecting connections.

  If this initial troubleshooting doesn't solve your problems, then it's
  time to go deeper.

  First, check the problematic monitor's ``mon_status`` via the admin
  socket as explained in `Using the monitor's admin socket`_ and
  `Understanding mon_status`_.

  If the monitor is out of the quorum, its state should be one of
  ``probing``, ``electing`` or ``synchronizing``. If it happens to be either
  ``leader`` or ``peon``, then the monitor believes to be in quorum, while
  the remaining cluster is sure it is not; or maybe it got into the quorum
  while we were troubleshooting the monitor, so check you ``ceph -s`` again
  just to make sure. Proceed if the monitor is not yet in the quorum.

What if the state is ``probing``?

  This means the monitor is still looking for the other monitors. Every time
  you start a monitor, the monitor will stay in this state for some time
  while trying to connect the rest of the monitors specified in the ``monmap``.
  The time a monitor will spend in this state can vary. For instance, when on
  a single-monitor cluster (never do this in production),
  the monitor will pass through the probing state almost instantaneously.
  In a multi-monitor cluster, the monitors will stay in this state until they
  find enough monitors to form a quorum -- this means that if you have 2 out
  of 3 monitors down, the one remaining monitor will stay in this state
  indefinitely until you bring one of the other monitors up.

  If you have a quorum the starting daemon should be able to find the
  other monitors quickly, as long as they can be reached. If your
  monitor is stuck probing and you have gone through with all the communication
  troubleshooting, then there is a fair chance that the monitor is trying
  to reach the other monitors on a wrong address. ``mon_status`` outputs the
  ``monmap`` known to the monitor: check if the other monitor's locations
  match reality. If they don't, jump to
  `Recovering a Monitor's Broken monmap`_; if they do, then it may be related
  to severe clock skews amongst the monitor nodes and you should refer to
  `Clock Skews`_ first, but if that doesn't solve your problem then it is
  the time to prepare some logs and reach out to the community (please refer
  to `Preparing your logs`_ on how to best prepare your logs).


What if state is ``electing``?

  This means the monitor is in the middle of an election. With recent Ceph
  releases these typically complete quickly, but at times the monitors can
  get stuck in what is known as an *election storm*. This can indicate
  clock skew among the monitor nodes; jump to
  `Clock Skews`_ for more information. If all your clocks are properly
  synchronized, you should search the mailing lists and tracker.
  This is not a state that is likely to persist and aside from
  (*really*) old bugs there is not an obvious reason besides clock skews on
  why this would happen.  Worst case, if there are enough surviving mons,
  down the problematic one while you investigate.

What if state is ``synchronizing``?

  This means the monitor is catching up with the rest of the cluster in
  order to join the quorum. Time to synchronize is a function of the size
  of your monitor store and thus of cluster size and state, so if you have a
  large or degraded cluster this may take a while.

  If you notice that the monitor jumps from ``synchronizing`` to
  ``electing`` and then back to ``synchronizing``, then you do have a
  problem: the cluster state may be advancing (i.e., generating new maps)
  too fast for the synchronization process to keep up. This was a more common
  thing in early days (Cuttlefish), but since then the synchronization process
  has been refactored and enhanced to avoid this dynamic. If you experience
  this in later versions please let us know via a bug tracker. And bring some logs
  (see `Preparing your logs`_).

What if state is ``leader`` or ``peon``?

  This should not happen:  famous last words.  If it does, however, it likely
  has a lot to do with clock skew -- see `Clock Skews`_. If you are not
  suffering from clock skew, then please prepare your logs (see
  `Preparing your logs`_) and reach out to the community.


Recovering a Monitor's Broken ``monmap``
----------------------------------------

This is how a ``monmap`` usually looks, depending on the number of
monitors::


      epoch 3
      fsid 5c4e9d53-e2e1-478a-8061-f543f8be4cf8
      last_changed 2013-10-30 04:12:01.945629
      created 2013-10-29 14:14:41.914786
      0: 127.0.0.1:6789/0 mon.a
      1: 127.0.0.1:6790/0 mon.b
      2: 127.0.0.1:6795/0 mon.c
      
This may not be what you have however. For instance, in some versions of
early Cuttlefish there was a bug that could cause your ``monmap``
to be nullified.  Completely filled with zeros. This means that not even
``monmaptool`` would be able to make sense of cold, hard, inscrutable zeros.
It's also possible to end up with a monitor with a severely outdated monmap,
notably if the node has been down for months while you fight with your vendor's
TAC.  The subject ``ceph-mon`` daemon might be unable to find the surviving
monitors (e.g., say ``mon.c`` is down; you add a new monitor ``mon.d``,
then remove ``mon.a``, then add a new monitor ``mon.e`` and remove
``mon.b``; you will end up with a totally different monmap from the one
``mon.c`` knows).

In this situation you have two possible solutions:

Scrap the monitor and redeploy

  You should only take this route if you are positive that you won't
  lose the information kept by that monitor; that you have other monitors
  and that they are running just fine so that your new monitor is able
  to synchronize from the remaining monitors. Keep in mind that destroying
  a monitor, if there are no other copies of its contents, may lead to
  loss of data.

Inject a monmap into the monitor

  Usually the safest path. You should grab the monmap from the remaining
  monitors and inject it into the monitor with the corrupted/lost monmap.

  These are the basic steps:

  1. Is there a formed quorum? If so, grab the monmap from the quorum::

      $ ceph mon getmap -o /tmp/monmap

  2. No quorum? Grab the monmap directly from another monitor (this
     assumes the monitor you are grabbing the monmap from has id ID-FOO
     and has been stopped)::

      $ ceph-mon -i ID-FOO --extract-monmap /tmp/monmap

  3. Stop the monitor you are going to inject the monmap into.

  4. Inject the monmap::

      $ ceph-mon -i ID --inject-monmap /tmp/monmap

  5. Start the monitor

  Please keep in mind that the ability to inject monmaps is a powerful
  feature that can cause havoc with your monitors if misused as it will
  overwrite the latest, existing monmap kept by the monitor.


Clock Skews
------------

Monitor operation can be severely affected by clock skew among the quorum's
mons, as the PAXOS consensus algorithm requires tight time alignment.
Skew can result in weird behavior with no obvious
cause. To avoid such issues, you must run a clock synchronization tool
on your monitor nodes:  ``Chrony`` or the legacy ``ntpd``.  Be sure to
configure the mon nodes with the `iburst` option and multiple peers:

* Each other
* Internal ``NTP`` servers
* Multiple external, public pool servers

For good measure, *all* nodes in your cluster should also sync against
internal and external servers, and perhaps even your mons.  ``NTP`` servers
should run on bare metal; VM virtualized clocks are not suitable for steady
timekeeping.  Visit `https://www.ntp.org <https://www.ntp.org>`_ for more info.  Your
organization may already have quality internal ``NTP`` servers you can use.  
Sources for ``NTP`` server appliances include:

* Microsemi (formerly Symmetricom) `https://microsemi.com <https://www.microsemi.com/product-directory/3425-timing-synchronization>`_
* EndRun `https://endruntechnologies.com <https://endruntechnologies.com/products/ntp-time-servers>`_
* Netburner `https://www.netburner.com <https://www.netburner.com/products/network-time-server/pk70-ex-ntp-network-time-server>`_


What's the maximum tolerated clock skew?

  By default the monitors will allow clocks to drift up to 0.05 seconds (50 ms).


Can I increase the maximum tolerated clock skew?

  The maximum tolerated clock skew is configurable via the
  ``mon-clock-drift-allowed`` option, and
  although you *CAN* you almost certainly *SHOULDN'T*. The clock skew mechanism
  is in place because clock-skewed monitors are liely to misbehave. We, as
  developers and QA aficionados, are comfortable with the current default
  value, as it will alert the user before the monitors get out hand. Changing
  this value may cause unforeseen effects on the
  stability of the monitors and overall cluster health.

How do I know there's a clock skew?

  The monitors will warn you via the cluster status ``HEALTH_WARN``. ``ceph health
  detail`` or ``ceph status`` should show something like::

      mon.c addr 10.10.0.1:6789/0 clock skew 0.08235s > max 0.05s (latency 0.0045s)

  That means that ``mon.c`` has been flagged as suffering from a clock skew.

  On releases beginning with Luminous you can issue the
  ``ceph time-sync-status`` command to check status.  Note that the lead mon
  is typically the one with the numerically lowest IP address.  It will always
  show ``0``: the reported offsets of other mons are relative to
  the lead mon, not to any external reference source.


What should I do if there's a clock skew?

  Synchronize your clocks. Running an NTP client may help. If you are already
  using one and you hit this sort of issues, check if you are using some NTP
  server remote to your network and consider hosting your own NTP server on
  your network.  This last option tends to reduce the amount of issues with
  monitor clock skews.


Client Can't Connect or Mount
------------------------------

Check your IP tables. Some OS install utilities add a ``REJECT`` rule to
``iptables``. The rule rejects all clients trying to connect to the host except
for ``ssh``. If your monitor host's IP tables have such a ``REJECT`` rule in
place, clients connecting from a separate node will fail to mount with a timeout
error. You need to address ``iptables`` rules that reject clients trying to
connect to Ceph daemons.  For example, you would need to address rules that look
like this appropriately::

	REJECT all -- anywhere anywhere reject-with icmp-host-prohibited

You may also need to add rules to IP tables on your Ceph hosts to ensure
that clients can access the ports associated with your Ceph monitors (i.e., port
6789 by default) and Ceph OSDs (i.e., 6800 through 7300 by default). For
example::

	iptables -A INPUT -m multiport -p tcp -s {ip-address}/{netmask} --dports 6789,6800:7300 -j ACCEPT

Monitor Store Failures
======================

Symptoms of store corruption
----------------------------

Ceph monitor stores the :term:`Cluster Map` in a key/value store such as LevelDB. If
a monitor fails due to the key/value store corruption, following error messages
might be found in the monitor log::

  Corruption: error in middle of record

or::

  Corruption: 1 missing files; e.g.: /var/lib/ceph/mon/mon.foo/store.db/1234567.ldb

Recovery using healthy monitor(s)
---------------------------------

If there are any survivors, we can always :ref:`replace <adding-and-removing-monitors>` the corrupted one with a
new one. After booting up, the new joiner will sync up with a healthy
peer, and once it is fully sync'ed, it will be able to serve the clients.

.. _mon-store-recovery-using-osds:

Recovery using OSDs
-------------------

But what if all monitors fail at the same time? Since users are encouraged to
deploy at least three (and preferably five) monitors in a Ceph cluster, the chance of simultaneous
failure is rare. But unplanned power-downs in a data center with improperly
configured disk/fs settings could fail the underlying file system, and hence
kill all the monitors. In this case, we can recover the monitor store with the
information stored in OSDs.::

  ms=/root/mon-store
  mkdir $ms
  
  # collect the cluster map from stopped OSDs
  for host in $hosts; do
    rsync -avz $ms/. user@$host:$ms.remote
    rm -rf $ms
    ssh user@$host <<EOF
      for osd in /var/lib/ceph/osd/ceph-*; do
        ceph-objectstore-tool --data-path \$osd --no-mon-config --op update-mon-db --mon-store-path $ms.remote
      done
  EOF
    rsync -avz user@$host:$ms.remote/. $ms
  done
  
  # rebuild the monitor store from the collected map, if the cluster does not
  # use cephx authentication, we can skip the following steps to update the
  # keyring with the caps, and there is no need to pass the "--keyring" option.
  # i.e. just use "ceph-monstore-tool $ms rebuild" instead
  ceph-authtool /path/to/admin.keyring -n mon. \
    --cap mon 'allow *'
  ceph-authtool /path/to/admin.keyring -n client.admin \
    --cap mon 'allow *' --cap osd 'allow *' --cap mds 'allow *'
  # add one or more ceph-mgr's key to the keyring. in this case, an encoded key
  # for mgr.x is added, you can find the encoded key in
  # /etc/ceph/${cluster}.${mgr_name}.keyring on the machine where ceph-mgr is
  # deployed
  ceph-authtool /path/to/admin.keyring --add-key 'AQDN8kBe9PLWARAAZwxXMr+n85SBYbSlLcZnMA==' -n mgr.x \
    --cap mon 'allow profile mgr' --cap osd 'allow *' --cap mds 'allow *'
  # if your monitors' ids are not single characters like 'a', 'b', 'c', please
  # specify them in the command line by passing them as arguments of the "--mon-ids"
  # option. if you are not sure, please check your ceph.conf to see if there is any
  # sections named like '[mon.foo]'. don't pass the "--mon-ids" option, if you are
  # using DNS SRV for looking up monitors.
  ceph-monstore-tool $ms rebuild -- --keyring /path/to/admin.keyring --mon-ids alpha beta gamma
  
  # make a backup of the corrupted store.db just in case!  repeat for
  # all monitors.
  mv /var/lib/ceph/mon/mon.foo/store.db /var/lib/ceph/mon/mon.foo/store.db.corrupted

  # move rebuild store.db into place.  repeat for all monitors.
  mv $ms/store.db /var/lib/ceph/mon/mon.foo/store.db
  chown -R ceph:ceph /var/lib/ceph/mon/mon.foo/store.db

The steps above

#. collect the map from all OSD hosts,
#. then rebuild the store,
#. fill the entities in keyring file with appropriate caps
#. replace the corrupted store on ``mon.foo`` with the recovered copy.

Known limitations
~~~~~~~~~~~~~~~~~

Following information are not recoverable using the steps above:

- **some added keyrings**: all the OSD keyrings added using ``ceph auth add`` command
  are recovered from the OSD's copy. And the ``client.admin`` keyring is imported
  using ``ceph-monstore-tool``. But the MDS keyrings and other keyrings are missing
  in the recovered monitor store. You might need to re-add them manually.

- **creating pools**: If any RADOS pools were in the process of being creating, that state is lost.  The recovery tool assumes that all pools have been created.  If there are PGs that are stuck in the 'unknown' after the recovery for a partially created pool, you can force creation of the *empty* PG with the ``ceph osd force-create-pg`` command.  Note that this will create an *empty* PG, so only do this if you know the pool is empty.

- **MDS Maps**: the MDS maps are lost.



Everything Failed! Now What?
=============================

Reaching out for help
----------------------

You can find us on IRC at #ceph and #ceph-devel at OFTC (server irc.oftc.net)
and on ``ceph-devel@vger.kernel.org`` and ``ceph-users@lists.ceph.com``. Make
sure you have grabbed your logs and have them ready if someone asks: the faster
the interaction and lower the latency in response, the better chances everyone's
time is optimized.


Preparing your logs
---------------------

Monitor logs are, by default, kept in ``/var/log/ceph/ceph-mon.FOO.log*``. We
may want them. However, your logs may not have the necessary information. If
you don't find your monitor logs at their default location, you can check
where they should be by running::

  ceph-conf --name mon.FOO --show-config-value log_file

The amount of information in the logs are subject to the debug levels being
enforced by your configuration files. If you have not enforced a specific
debug level then Ceph is using the default levels and your logs may not
contain important information to track down you issue.
A first step in getting relevant information into your logs will be to raise
debug levels. In this case we will be interested in the information from the
monitor.
Similarly to what happens on other components, different parts of the monitor
will output their debug information on different subsystems.

You will have to raise the debug levels of those subsystems more closely
related to your issue. This may not be an easy task for someone unfamiliar
with troubleshooting Ceph. For most situations, setting the following options
on your monitors will be enough to pinpoint a potential source of the issue::

      debug mon = 10
      debug ms = 1

If we find that these debug levels are not enough, there's a chance we may
ask you to raise them or even define other debug subsystems to obtain infos
from -- but at least we started off with some useful information, instead
of a massively empty log without much to go on with.

Do I need to restart a monitor to adjust debug levels?
------------------------------------------------------

No. You may do it in one of two ways:

You have quorum

  Either inject the debug option into the monitor you want to debug::

        ceph tell mon.FOO config set debug_mon 10/10

  or into all monitors at once::

        ceph tell mon.* config set debug_mon 10/10

No quorum

  Use the monitor's admin socket and directly adjust the configuration
  options::

      ceph daemon mon.FOO config set debug_mon 10/10


Going back to default values is as easy as rerunning the above commands
using the debug level ``1/10`` instead.  You can check your current
values using the admin socket and the following commands::

      ceph daemon mon.FOO config show

or::

      ceph daemon mon.FOO config get 'OPTION_NAME'


Reproduced the problem with appropriate debug levels. Now what?
----------------------------------------------------------------

Ideally you would send us only the relevant portions of your logs.
We realise that figuring out the corresponding portion may not be the
easiest of tasks. Therefore, we won't hold it to you if you provide the
full log, but common sense should be employed. If your log has hundreds
of thousands of lines, it may get tricky to go through the whole thing,
specially if we are not aware at which point, whatever your issue is,
happened. For instance, when reproducing, keep in mind to write down
current time and date and to extract the relevant portions of your logs
based on that.

Finally, you should reach out to us on the mailing lists, on IRC or file
a new issue on the `tracker`_.

.. _tracker: http://tracker.ceph.com/projects/ceph/issues/new
