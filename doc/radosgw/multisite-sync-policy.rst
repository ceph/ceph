.. _radosgw-multisite-sync-policy:

=====================
Multisite Sync Policy
=====================

.. versionadded:: Octopus

Multisite bucket-granularity sync policy provides fine grained control of data movement between buckets in different zones. It extends the zone sync mechanism. Previously buckets were being treated symmetrically, that is -- each (data) zone holds a mirror of that bucket that should be the same as all the other zones. Whereas leveraging the bucket-granularity sync policy is possible for buckets to diverge, and a bucket can pull data from other buckets (ones that don't share its name or its ID) in different zone.  The sync process was assuming therefore that the bucket sync source and the bucket sync destination were always referring to the same bucket, now that is not the case anymore.

The sync policy supersedes the old zonegroup coarse configuration (sync_from*). The sync policy can be configured at the zonegroup level (and if it is configured it replaces the old style config), but it can also be configured at the bucket level.

In the sync policy multiple groups that can contain lists of data-flow configurations can be defined, as well as lists of pipe configurations. The data-flow defines the flow of data between the different zones. It can define symmetrical data flow, in which multiple zones sync data from each other, and it can define directional data flow, in which the data moves in one way from one zone to another.

A pipe defines the actual buckets that can use these data flows, and the properties that are associated with it (for example: source object prefix).

A sync policy group can be in 3 states:

+----------------------------+----------------------------------------+
|  Value                     | Description                            |
+============================+========================================+
| ``enabled``                | sync is allowed and enabled            |
+----------------------------+----------------------------------------+
| ``allowed``                | sync is allowed                        |
+----------------------------+----------------------------------------+
| ``forbidden``              | sync (as defined by this group) is not |
|                            | allowed and can override other groups  |
+----------------------------+----------------------------------------+

A policy can be defined at the bucket level. A bucket level sync policy inherits the data flow of the zonegroup policy, and can only define a subset of what the zonegroup allows.

A wildcard zone, and a wildcard bucket parameter in the policy defines all relevant zones, or all relevant buckets. In the context of a bucket policy it means the current bucket instance.  A disaster recovery configuration where entire zones are mirrored doesn't require configuring anything on the buckets. However, for a fine grained bucket sync it would be better to configure the pipes to be synced by allowing (status=allowed) them at the zonegroup level (e.g., using wildcards), but only enable the specific sync at the bucket level (status=enabled). If needed, the policy at the bucket level can limit the data movement to specific relevant zones.

.. important:: Any changes to the zonegroup policy needs to be applied on the
               zonegroup master zone, and require period update and commit. Changes
               to the bucket policy needs to be applied on the zonegroup master
               zone. The changes are dynamically handled by rgw.


S3 Replication API
~~~~~~~~~~~~~~~~~~

The S3 bucket replication api has also been implemented, and allows users to create replication rules between different buckets. Note though that while the AWS replication feature allows bucket replication within the same zone, rgw does not allow it at the moment.  However, the rgw api also added a new 'Zone' array that allows users to select to what zones the specific bucket will be synced.


Sync Policy Control Reference
=============================


Get Sync Policy
~~~~~~~~~~~~~~~

To retrieve the current zonegroup sync policy, or a specific bucket policy:

::

     # radosgw-admin sync policy get [--bucket=<bucket>]


Create Sync Policy Group
~~~~~~~~~~~~~~~~~~~~~~~~

To create a sync policy group:

::

      # radosgw-admin sync group create [--bucket=<bucket>]                      \
                                        --group-id=<group-id>                    \
                                        --status=<enabled | allowed | forbidden> \
                                       
      
Modify Sync Policy Group
~~~~~~~~~~~~~~~~~~~~~~~~

To modify a sync policy group:

::

      # radosgw-admin sync group modify [--bucket=<bucket>]                      \
                                        --group-id=<group-id>                    \
                                        --status=<enabled | allowed | forbidden> \


Show Sync Policy Group
~~~~~~~~~~~~~~~~~~~~~~~~

To show a sync policy group:

::

      # radosgw-admin sync group get [--bucket=<bucket>]       \
                                        --group-id=<group-id>
                                       

Remove Sync Policy Group
~~~~~~~~~~~~~~~~~~~~~~~~

To remove a sync policy group:

::

      # radosgw-admin sync group remove [--bucket=<bucket>]    \
                                        --group-id=<group-id>



Create Sync Flow
~~~~~~~~~~~~~~~~

- To create or update directional sync flow:

::

      # radosgw-admin sync group flow create [--bucket=<bucket>]          \
                                             --group-id=<group-id>        \
                                             --flow-id=<flow-id>          \
                                             --flow-type=directional      \
                                             --source-zone=<source_zone>  \
                                             --dest-zone=<dest_zone>


- To create or update symmetrical sync flow:

::

      # radosgw-admin sync group flow create [--bucket=<bucket>]          \
                                             --group-id=<group-id>        \
                                             --flow-id=<flow-id>          \
                                             --flow-type=symmetrical      \
                                             --zones=<zones>


Where zones are a comma separated list of all the zones that need to add to the flow.


Remove Sync Flow Zones
~~~~~~~~~~~~~~~~~~~~~~

- To remove directional sync flow:

::

      # radosgw-admin sync group flow remove [--bucket=<bucket>]          \
                                             --group-id=<group-id>        \
                                             --flow-id=<flow-id>          \
                                             --flow-type=directional      \
                                             --source-zone=<source_zone>  \
                                             --dest-zone=<dest_zone>


- To remove specific zones from symmetrical sync flow:

::

      # radosgw-admin sync group flow remove [--bucket=<bucket>]          \
                                             --group-id=<group-id>        \
                                             --flow-id=<flow-id>          \
                                             --flow-type=symmetrical      \
                                             --zones=<zones>


Where zones are a comma separated list of all zones to remove from the flow.

                                             
- To remove symmetrical sync flow:

::

      # radosgw-admin sync group flow remove [--bucket=<bucket>]          \
                                             --group-id=<group-id>        \
                                             --flow-id=<flow-id>          \
                                             --flow-type=symmetrical


Create Sync Pipe
~~~~~~~~~~~~~~~~

To create sync group pipe, or update its parameters:


::

      # radosgw-admin sync group pipe create [--bucket=<bucket>]                      \
                                             --group-id=<group-id>                    \
                                             --pipe-id=<pipe-id>                      \
                                             --source-zones=<source_zones>            \
                                             [--source-bucket=<source_buckets>]       \
                                             [--source-bucket-id=<source_bucket_id>]  \
                                             --dest-zones=<dest_zones>                \
                                             [--dest-bucket=<dest_buckets>]           \
                                             [--dest-bucket-id=<dest_bucket_id>]      \
                                             [--prefix=<source_prefix>]               \
                                             [--prefix-rm]                            \
                                             [--tags-add=<tags>]                      \
                                             [--tags-rm=<tags>]                       \
                                             [--dest-owner=<owner>]                   \
                                             [--storage-class=<storage_class>]        \
                                             [--mode=<system | user>]                 \
                                             [--uid=<user_id>]


Zones are either a list of zones, or '*' (wildcard). Wildcard zones mean any zone that matches the sync flow rules.
Buckets are either a bucket name, or '*' (wildcard). Wildcard bucket means the current bucket
Prefix can be defined to filter source objects.
Tags are passed by a comma separated list of 'key=value'.
Destination owner can be set to force a destination owner of the objects. If user mode is selected, only the destination bucket owner can be set.
Destination storage class can also be configured.
User id can be set for user mode, and will be the user under which the sync operation will be executed (for permissions validation).


Remove Sync Pipe
~~~~~~~~~~~~~~~~

To remove specific sync group pipe params, or the entire pipe:


::

      # radosgw-admin sync group pipe remove [--bucket=<bucket>]                     \
                                             --group-id=<group-id>                   \
                                             --pipe-id=<pipe-id>                     \
                                             [--source-zones=<source_zones>]         \
                                             [--source-bucket=<source_buckets>]      \
                                             [--source-bucket-id=<source_bucket_id>] \
                                             [--dest-zones=<dest_zones>]             \
                                             [--dest-bucket=<dest_buckets>]          \
                                             [--dest-bucket-id=<dest_bucket_id>]


Sync Info
~~~~~~~~~

To get information about the expected sync sources and targets (as defined by the sync policy):

::

      # radosgw-admin sync info [--bucket=<bucket>]             \
                                [--effective-zone-name=<zone>]


Since a bucket can define a policy that defines data movement from it towards a different bucket at a different zone, when the policy is created we also generate a list of bucket dependencies that are used as hints when a sync of any particular bucket happens. The fact that a bucket references another bucket does not mean it actually syncs to/from it, as the data flow might not permit it.  


Examples
========

The system in these examples includes 3 zones: ``us-east`` (the master zone), ``us-west``, ``us-west-2``.

Example 1: Two Zones, Complete Mirror
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is similar to older (pre ``Octopus``) sync capabilities, but being done via the new sync policy engine. Note that changes to the zonegroup sync policy require a period update and commit.


::

      [us-east] $ radosgw-admin sync group create --group-id=group1 --status=allowed
      [us-east] $ radosgw-admin sync group flow create --group-id=group1 \
                                --flow-id=flow-mirror --flow-type=symmetrical \
                                --zones=us-east,us-west
      [us-east] $ radosgw-admin sync group pipe create --group-id=group1 \
                                --pipe-id=pipe1 --source-zones='*' \
                                --source-bucket='*' --dest-zones='*' \
                                --dest-bucket='*'
      [us-east] $ radosgw-admin sync group modify --group-id=group1 --status=enabled
      [us-east] $ radosgw-admin period update --commit

      $ radosgw-admin sync info --bucket=buck
      {
          "sources": [
              {
                  "id": "pipe1",
                  "source": {
                      "zone": "us-west",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                  "dest": {
                      "zone": "us-east",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                  "params": {
      ...
                  }
              }
          ],
          "dests": [
              {
                  "id": "pipe1",
                  "source": {
                      "zone": "us-east",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                  "dest": {
                      "zone": "us-west",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                 ...
              }
          ],
          ...
          }
      }


Note that the "id" field in the output above reflects the pipe rule
that generated that entry, a single rule can generate multiple sync
entries as can be seen in the example.

::

      [us-west] $ radosgw-admin sync info --bucket=buck
      {
          "sources": [
              {
                  "id": "pipe1",
                  "source": {
                      "zone": "us-east",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                  "dest": {
                      "zone": "us-west",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                  ...
              }
          ],
          "dests": [
              {
                  "id": "pipe1",
                  "source": {
                      "zone": "us-west",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                  "dest": {
                      "zone": "us-east",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                 ...
              }
          ],
          ...
      }



Example 2: Directional, Entire Zone Backup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Also similar to older sync capabilities. In here we add a third zone, ``us-west-2`` that will be a replica of ``us-west``, but data will not be replicated back from it.

::

      [us-east] $ radosgw-admin sync group flow create --group-id=group1 \
                                --flow-id=us-west-backup --flow-type=directional \
                                --source-zone=us-west --dest-zone=us-west-2
      [us-east] $ radosgw-admin period update --commit


Note that us-west has two dests:

::

      [us-west] $ radosgw-admin sync info --bucket=buck
      {
          "sources": [
              {
                  "id": "pipe1",
                  "source": {
                      "zone": "us-east",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                  "dest": {
                      "zone": "us-west",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                 ...
              }
          ],
          "dests": [
              {
                  "id": "pipe1",
                  "source": {
                      "zone": "us-west",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                  "dest": {
                      "zone": "us-east",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                 ...
              },
              {
                  "id": "pipe1",
                  "source": {
                      "zone": "us-west",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                  "dest": {
                      "zone": "us-west-2",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                 ...
              }
          ],
          ...
      }


Whereas us-west-2 has only source and no destinations:

::

      [us-west-2] $ radosgw-admin sync info --bucket=buck
      {
          "sources": [
              {
                  "id": "pipe1",
                  "source": {
                      "zone": "us-west",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                  "dest": {
                      "zone": "us-west-2",
                      "bucket": "buck:115b12b3-....4409.1"
                  },
                 ...
              }
          ],
          "dests": [],
          ...
      }

      
      
Example 3: Mirror a Specific Bucket
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using the same group configuration, but this time switching it to ``allowed`` state, which means that sync is allowed but not enabled.

::

      [us-east] $ radosgw-admin sync group modify --group-id=group1 --status=allowed
      [us-east] $ radosgw-admin period update --commit


And we will create a bucket level policy rule for existing bucket ``buck2``. Note that the bucket needs to exist before being able to set this policy, and admin commands that modify bucket policies need to run on the master zone, however, they do not require period update.  There is no need to change the data flow, as it is inherited from the zonegroup policy. A bucket policy flow will only be a subset of the flow defined in the zonegroup policy. Same goes for pipes, although a bucket policy can enable pipes that are not enabled (albeit not forbidden) at the zonegroup policy.

::

      [us-east] $ radosgw-admin sync group create --bucket=buck2 \
                                --group-id=buck2-default --status=enabled

      [us-east] $ radosgw-admin sync group pipe create --bucket=buck2 \
                                --group-id=buck2-default --pipe-id=pipe1 \
                                --source-zones='*' --dest-zones='*'



Example 4: Limit Bucket Sync To Specific Zones
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This will only sync ``buck3`` to ``us-east`` (from any zone that flow allows to sync into ``us-east``).

::

      [us-east] $ radosgw-admin sync group create --bucket=buck3 \
                                --group-id=buck3-default --status=enabled

      [us-east] $ radosgw-admin sync group pipe create --bucket=buck3 \
                                --group-id=buck3-default --pipe-id=pipe1 \
                                --source-zones='*' --dest-zones=us-east



Example 5: Sync From a Different Bucket
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Note that bucket sync only works (currently) across zones and not within the same zone.

Set ``buck4`` to pull data from ``buck5``:

::

      [us-east] $ radosgw-admin sync group create --bucket=buck4 '
                                --group-id=buck4-default --status=enabled

      [us-east] $ radosgw-admin sync group pipe create --bucket=buck4 \
                                --group-id=buck4-default --pipe-id=pipe1 \
                                --source-zones='*' --source-bucket=buck5 \
                                --dest-zones='*'


can also limit it to specific zones, for example the following will
only sync data originated in us-west:

::

      [us-east] $ radosgw-admin sync group pipe modify --bucket=buck4 \
                                --group-id=buck4-default --pipe-id=pipe1 \
                                --source-zones=us-west --source-bucket=buck5 \
                                --dest-zones='*'


Checking the sync info for ``buck5`` on ``us-west`` is interesting:

::

      [us-west] $ radosgw-admin sync info --bucket=buck5
      {
          "sources": [],
          "dests": [],
          "hints": {
              "sources": [],
              "dests": [
                  "buck4:115b12b3-....14433.2"
              ]
          },
          "resolved-hints-1": {
              "sources": [],
              "dests": [
                  {
                      "id": "pipe1",
                      "source": {
                          "zone": "us-west",
                          "bucket": "buck5"
                      },
                      "dest": {
                          "zone": "us-east",
                          "bucket": "buck4:115b12b3-....14433.2"
                      },
                      ...
                  },
                  {
                      "id": "pipe1",
                      "source": {
                          "zone": "us-west",
                          "bucket": "buck5"
                      },
                      "dest": {
                          "zone": "us-west-2",
                          "bucket": "buck4:115b12b3-....14433.2"
                      },
                      ...
                  }
              ]
          },
          "resolved-hints": {
              "sources": [],
              "dests": []
          }
      }


Note that there are resolved hints, which means that the bucket ``buck5`` found about ``buck4`` syncing from it indirectly, and not from its own policy (the policy for ``buck5`` itself is empty).


Example 6: Sync To Different Bucket
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The same mechanism can work for configuring data to be synced to (vs.  synced from as in the previous example). Note that internally data is still pulled from the source at the destination zone:

Set ``buck6`` to "push" data to ``buck5``:

::

      [us-east] $ radosgw-admin sync group create --bucket=buck6 \
                                --group-id=buck6-default --status=enabled

      [us-east] $ radosgw-admin sync group pipe create --bucket=buck6 \
                                --group-id=buck6-default --pipe-id=pipe1 \
                                --source-zones='*' --source-bucket='*' \
                                --dest-zones='*' --dest-bucket=buck5


A wildcard bucket name means the current bucket in the context of bucket sync policy.

Combined with the configuration in Example 5, we can now write data to ``buck6`` on ``us-east``, data will sync to ``buck5`` on ``us-west``, and from there it will be distributed to ``buck4`` on ``us-east``, and on ``us-west-2``.

Example 7: Source Filters
~~~~~~~~~~~~~~~~~~~~~~~~~

Sync from ``buck8`` to ``buck9``, but only objects that start with ``foo/``:

::

      [us-east] $ radosgw-admin sync group create --bucket=buck8 \
                                --group-id=buck8-default --status=enabled

      [us-east] $ radosgw-admin sync group pipe create --bucket=buck8 \
                                --group-id=buck8-default --pipe-id=pipe-prefix \
                                --prefix=foo/ --source-zones='*' --dest-zones='*' \
                                --dest-bucket=buck9


Also sync from ``buck8`` to ``buck9`` any object that has the tags ``color=blue`` or ``color=red``:

::

      [us-east] $ radosgw-admin sync group pipe create --bucket=buck8 \
                                --group-id=buck8-default --pipe-id=pipe-tags \
                                --tags-add=color=blue,color=red --source-zones='*' \
                                --dest-zones='*' --dest-bucket=buck9


And we can check the expected sync in ``us-east`` (for example):

::

      [us-east] $ radosgw-admin sync info --bucket=buck8
      {
          "sources": [],
          "dests": [
              {
                  "id": "pipe-prefix",
                  "source": {
                      "zone": "us-east",
                      "bucket": "buck8:115b12b3-....14433.5"
                  },
                  "dest": {
                      "zone": "us-west",
                      "bucket": "buck9"
                  },
                  "params": {
                      "source": {
                          "filter": {
                              "prefix": "foo/",
                              "tags": []
                          }
                      },
                      ...
                  }
              },
              {
                  "id": "pipe-tags",
                  "source": {
                      "zone": "us-east",
                      "bucket": "buck8:115b12b3-....14433.5"
                  },
                  "dest": {
                      "zone": "us-west",
                      "bucket": "buck9"
                  },
                  "params": {
                      "source": {
                          "filter": {
                              "tags": [
                                  {
                                      "key": "color",
                                      "value": "blue"
                                  },
                                  {
                                      "key": "color",
                                      "value": "red"
                                  }
                              ]
                          }
                      },
                      ...
                  }
              }
          ],
          ...
      }


Note that there aren't any sources, only two different destinations (one for each configuration). When the sync process happens it will select the relevant rule for each object it syncs.

Prefixes and tags can be combined, in which object will need to have both in order to be synced. The priority param can also be passed, and it can be used to determine when there are multiple different rules that are matched (and have the same source and destination), to determine which of the rules to be used.


Example 8: Destination Params: Storage Class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Storage class of the destination objects can be configured:

::

      [us-east] $ radosgw-admin sync group create --bucket=buck10 \
                                --group-id=buck10-default --status=enabled

      [us-east] $ radosgw-admin sync group pipe create --bucket=buck10 \
                                --group-id=buck10-default \
                                --pipe-id=pipe-storage-class \
                                --source-zones='*' --dest-zones=us-west-2 \
                                --storage-class=CHEAP_AND_SLOW


Example 9: Destination Params: Destination Owner Translation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set the destination objects owner as the destination bucket owner.
This requires specifying the uid of the destination bucket:

::

      [us-east] $ radosgw-admin sync group create --bucket=buck11 \
                                --group-id=buck11-default --status=enabled

      [us-east] $ radosgw-admin sync group pipe create --bucket=buck11 \
                                --group-id=buck11-default --pipe-id=pipe-dest-owner \
                                --source-zones='*' --dest-zones='*' \
                                --dest-bucket=buck12 --dest-owner=joe

Example 10: Destination Params: User Mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

User mode makes sure that the user has permissions to both read the objects, and write to the destination bucket. This requires that the uid of the user (which in its context the operation executes) is specified.

::

      [us-east] $ radosgw-admin sync group pipe modify --bucket=buck11 \
                                --group-id=buck11-default --pipe-id=pipe-dest-owner \
                                --mode=user --uid=jenny



