.. _erasure-code-profiles:

=====================
Erasure code profiles
=====================

Erasure code is defined by a **profile** and is used when creating an
erasure coded pool and the associated CRUSH rule.

The **default** erasure code profile (which is created when the Ceph
cluster is initialized) will split the data into 2 equal-sized chunks,
and have 2 parity chunks of the same size. It will take as much space
in the cluster as a 2-replica pool but can sustain the data loss of 2
chunks out of 4. It is described as a profile with **k=2** and **m=2**,
meaning the information is spread over four OSD (k+m == 4) and two of
them can be lost.

To improve redundancy without increasing raw storage requirements, a
new profile can be created. For instance, a profile with **k=10** and
**m=4** can sustain the loss of four (**m=4**) OSDs by distributing an
object on fourteen (k+m=14) OSDs. The object is first divided in
**10** chunks (if the object is 10MB, each chunk is 1MB) and **4**
coding chunks are computed, for recovery (each coding chunk has the
same size as the data chunk, i.e. 1MB). The raw space overhead is only
40% and the object will not be lost even if four OSDs break at the
same time.

.. _list of available plugins:

.. toctree::
	:maxdepth: 1

	erasure-code-jerasure
	erasure-code-isa
	erasure-code-lrc
	erasure-code-shec
	erasure-code-clay

osd erasure-code-profile set
============================

To create a new erasure code profile::

	ceph osd erasure-code-profile set {name} \
             [{directory=directory}] \
             [{plugin=plugin}] \
             [{stripe_unit=stripe_unit}] \
             [{key=value} ...] \
             [--force]

Where:

``{directory=directory}``

:Description: Set the **directory** name from which the erasure code
              plugin is loaded.

:Type: String
:Required: No.
:Default: /usr/lib/ceph/erasure-code

``{plugin=plugin}``

:Description: Use the erasure code **plugin** to compute coding chunks
              and recover missing chunks. See the `list of available
              plugins`_ for more information.

:Type: String
:Required: No.
:Default: jerasure

``{stripe_unit=stripe_unit}``

:Description: The amount of data in a data chunk, per stripe. For
              example, a profile with 2 data chunks and stripe_unit=4K
              would put the range 0-4K in chunk 0, 4K-8K in chunk 1,
              then 8K-12K in chunk 0 again. This should be a multiple
              of 4K for best performance. The default value is taken
              from the monitor config option
              ``osd_pool_erasure_code_stripe_unit`` when a pool is
              created.  The stripe_width of a pool using this profile
              will be the number of data chunks multiplied by this
              stripe_unit.

:Type: String
:Required: No.

``{key=value}``

:Description: The semantic of the remaining key/value pairs is defined
              by the erasure code plugin.

:Type: String
:Required: No.

``--force``

:Description: Override an existing profile by the same name, and allow
              setting a non-4K-aligned stripe_unit.

:Type: String
:Required: No.

osd erasure-code-profile rm
============================

To remove an erasure code profile::

	ceph osd erasure-code-profile rm {name}

If the profile is referenced by a pool, the deletion will fail.

.. warning:: Removing an erasure code profile using ``osd erasure-code-profile rm`` does not automatically delete the associated CRUSH rule associated with the erasure code profile. It is recommended to manually remove the associated CRUSH rule using ``ceph osd crush rule remove {rule-name}`` to avoid unexpected behavior.

osd erasure-code-profile get
============================

To display an erasure code profile::

	ceph osd erasure-code-profile get {name}

osd erasure-code-profile ls
===========================

To list the names of all erasure code profiles::

	ceph osd erasure-code-profile ls

