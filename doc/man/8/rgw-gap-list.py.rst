:orphan:

=============================================================================
 rgw-gap-list.py -- List bucket entries with missing RADOS backing objects
=============================================================================

.. program:: rgw-gap-list.py

Synopsis
========

| **rgw-gap-list.py**

Description
===========

:program:`rgw-gap-list.py` tool is designed to identify gaps in Ceph 
RADOS Gateway (RGW) bucket data, perform consistency checks across buckets, 
and generate reports on bucket scrub meta‐data. It supports multi-run / 
multi-host environments for large-scale deployments.

This tool helps maintain data integrity in Ceph RGW systems by detecting 
missing or inconsistent objects across buckets. It can operate on all buckets 
or specific buckets, with options to exclude certain buckets from processing.

Options
=======
.. option:: -h, --help

   Show help message and exit.

.. option:: -a MAXAGE, --maxage MAXAGE

   Maximum age (in seconds) of last scan before rescan occurs. Default 7 days.

.. option:: -b BUCKETLIST, --bucketlist BUCKETLIST

   Optional: Bucket(s) to operate on, default is all buckets, quoted space 
   separated list is supported. Supercedes -l.

.. option:: -e EXCLUDELIST, --excludelist EXCLUDELIST

   Optional: Bucket(s) to skip, default is process all buckets, quoted space 
   separated list is supported. Supercedes -f.

.. option:: -f EXCLUDEFILE, --excludefile EXCLUDEFILE

   Optional: File with list of bucket(s) to skip, should be one bucket name
   per line.

.. option:: -c CONF, --conf CONF

   Ceph conf file to use, default '/etc/ceph/ceph.conf'.

.. option:: --ceph_client_name CLIENT_NAME

   Ceph Client Name to use, default 'client.admin'.

.. option:: --ceph_client_keyring CLIENT_KEYRING

   Ceph Client Keyring file to use, 
   default '/etc/ceph/ceph.client.admin.keyring'.

.. option:: -d, --delete

   Remove all sync and results objects then Exit. Used to clear all tool 
   generated data.

.. option:: -g, --gaps

   Dump the gap results from RADOS object contents. All other options are 
   ignore ( except -j ).

.. option:: -i INFLIGHT, --inflight INFLIGHT

   Maximum number of in-flight ops to allow without a response. 
   Default: 15000.

.. option:: -l LISTFILE, --listfile LISTFILE

   Optional: Bucket list file, should be one bucket name per line.

.. option:: -m MATCH, --match MATCH

   Specify a prefix match for the object names. Only objects matching this 
   prefix will be checked for gaps.

.. option:: -n, --norandom

   By default, the script randomizes the list of buckets before processing. 
   On large bucket count environments, this may cause significant delay 
   before start of processing due to the way the randomizing occurs. Set 
   '-n' to Not Randomize the list to remove this delay.

.. option:: --namespace NAMESPACE

   What namespace to use for sync / results objects. Default: rgw-gap-list.

.. option:: -p POOL, --pool POOL

   Bucket Data Pool(s) as a quoted space separated list, default 
   'default.rgw.buckets.data default.rgw.buckets.non-ec'

.. option:: --promexport

   Prometheus Export.  All other processing and output options are ignored if
   this flag is specified.

.. option:: -s SYNCPOOL, --syncpool SYNCPOOL

   Synchronization / Queuing pool for the script ot use, 
   default 'default.rgw.buckets.index'.

.. option:: -r, --report

   Generate bucket scrub metadata report.

.. option:: -j, --json

   Use JSON format for bucket scrub metadata report. Only considered with -g,
   -r and -x.

.. option:: -v, --verbosity

   Optional: Verbosity level, multiple -v's are supported for higher 
   verbosity, example: -vvv.

.. option:: -x, --verify

   Used to verify the results from a prior run.

Examples
========

Check all buckets with default settings::

         $ rgw-gap-list.py

Check specific buckets with verbosity::

         $ rgw-gap-list.py -b "bucket1 bucket2" -v -v

Exclude certain buckets from processing::

         $ rgw-gap-list.py -e "bucket3 bucket4"

Generate a JSON report for all buckets::

         $ rgw-gap-list.py -r -j

Check gaps in RADOS object contents::

         $ rgw-gap-list.py -g

Clear all synchronized bucket status data::

         $ rgw-gap-list.py -d

Availability
============

:program:`rgw-gap-list.py` is part of Ceph, a massively scalable, open-source,
distributed storage system.  Please refer to the Ceph documentation at
https://docs.ceph.com for more information.


See also
========

:doc:`radosgw-admin <radosgw-admin>`\(8)
:doc:`rgw-orphan-list <rgw-orphan-list>`\(8)
:doc:`rgw-gap-list <rgw-gap-list>`\(8)
