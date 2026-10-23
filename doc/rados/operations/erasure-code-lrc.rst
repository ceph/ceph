======================================
Locally Repairable Erasure Code Plugin
======================================

With the *isa* plugin, when an erasure coded object is stored on
multiple OSDs, recovering from the loss of one OSD requires reading
from *k* others. For instance if *isa* is configured with
*k=8* and *m=4*, recovering from the loss of one OSD requires reading
from eight others.

The *lrc* erasure code plugin creates local parity chunks to enable
recovery using fewer surviving OSDs. For instance if *lrc* is configured with
*k=8*, *m=4* and *l=4*, it will create an additional parity chunk for
every four OSDs. When a single OSD is lost, it can be recovered with
only four OSDs instead of eight.

Erasure Code Profile Examples
=============================

Reduce Recovery Bandwidth Between Hosts
---------------------------------------

Although it is probably not an interesting use case when all hosts are
connected to the same switch, reduced bandwidth usage can actually be
observed:

.. prompt:: bash #

   ceph osd erasure-code-profile set LRCprofile \
      plugin=lrc \
      k=4 m=2 l=3 \
      crush-failure-domain=host
   ceph osd pool create lrcpool erasure LRCprofile


Reduce Recovery Bandwidth Between Racks
---------------------------------------

In Firefly, bandwidth reduction will only be observed if the primary
OSD is in the same rack as the lost chunk:

.. prompt:: bash #

   ceph osd erasure-code-profile set LRCprofile \
      plugin=lrc \
      k=4 m=2 l=3 \
      crush-locality=rack \
      crush-failure-domain=host
   ceph osd pool create lrcpool erasure LRCprofile


Create an LRC Profile
=====================

To create a new LRC erasure code profile:

.. prompt:: bash #

   ceph osd erasure-code-profile set {name} \
      plugin=lrc \
      k={data-chunks} \
      m={coding-chunks} \
      l={locality} \
      [crush-root={root}] \
      [crush-locality={bucket-type}] \
      [crush-failure-domain={bucket-type}] \
      [crush-device-class={device-class}] \
      [directory={directory}] \
      [--force]

Where:

``k={data-chunks}``

:Description: Each object is split in ``data-chunks`` parts,
              each stored on a different OSD.

:Type: Integer
:Required: Yes.
:Example: 4

``m={coding-chunks}``

:Description: Compute ``coding-chunks`` for each object and store them
              on different OSDs. The number of coding chunks is also
              the number of OSDs that can be down without losing data.

:Type: Integer
:Required: Yes.
:Example: 2

``l={locality}``

:Description: Group the coding and data chunks into sets of size
              ``locality``. For instance, for *k=4* and *m=2*,
              when *locality=3* two groups of three are created.
              Each set can be recovered without reading chunks
              from another set.

:Type: Integer
:Required: Yes.
:Example: 3

``crush-root={root}``

:Description: The name of the CRUSH bucket used for the first step of
              the CRUSH rule. For instance ``step take default``.

:Type: String
:Required: No.
:Default: default

``crush-locality={bucket-type}``

:Description: The type of the CRUSH bucket in which each set of chunks
              defined by *l* will be stored. For instance, if it is
              set to ``rack``, each group of *l* chunks will be
              placed in a different rack. It is used to create a
              CRUSH rule step such as ``step choose rack``. If it is not
              set, no such grouping is done.

:Type: String
:Required: No.

``crush-failure-domain={bucket-type}``

:Description: Ensure that no two chunks are in a bucket with the same
              failure domain. For instance, if the failure domain is
              ``host``, no two chunks will be stored on the same
              host. It is used to create a CRUSH rule step such as ``step
              chooseleaf host``.

:Type: String
:Required: No.
:Default: host

``crush-device-class={device-class}``

:Description: Restrict placement to devices of a specific class (e.g.,
              ``ssd`` or ``hdd``), using the CRUSH device class names
              in the CRUSH map.

:Type: String
:Required: No.
:Default:

``directory={directory}``

:Description: Set the ``directory`` name from which the erasure code
              plugin is loaded.

:Type: String
:Required: No.
:Default: /usr/lib/ceph/erasure-code

``layer-plugin={plugin}``

:Description: The erasure code plugin used for any layer that does not set
              its own ``plugin`` (see `Low Level Plugin Configuration`_).
              ``jerasure`` and ``isa`` are *not* interchangeable: they agree
              on the first coding chunk but diverge on later ones, so a pool
              must always be read and recovered with the same sub-plugin it
              was written with. Only ``jerasure`` and ``isa`` are
              accepted, and only on an LRC profile. See
              `Choosing the layer sub-plugin`_.

:Type: String
:Required: No.
:Default: isa

``--force``

:Description: Override an existing profile by the same name.

:Type: String
:Required: No.

Choosing the layer sub-plugin
=============================

Each LRC layer is itself an erasure code computed by another plugin. By
default the *lrc* plugin uses *isa* for any layer that does not set its own
``plugin``. The two candidate sub-plugins, ``jerasure`` and ``isa``, compute
identical first coding chunks but **diverge on subsequent coding chunks**.
A pool must therefore always be recovered and read with the same sub-plugin
it was written with.

To make this unambiguous, the sub-plugin is recorded in the profile under the
``layer-plugin`` key. New LRC profiles created without an explicit value
default to ``isa``, and overriding a profile (``--force``) without
``layer-plugin`` keeps the value it already records. The sub-plugin of a
profile with no ``layer-plugin`` set is never guessed: creating a pool against
it is rejected, overriding it requires ``layer-plugin`` to be given explicitly,
and OSDs will not serve the PGs of its pools until it is set. A value other
than ``jerasure`` or ``isa`` is treated the same way; the monitor rejects one,
but releases without the ``layer-plugin`` key stored whatever was given. The
monitors raise the ``LRC_LAYER_PLUGIN_UNSET`` health check while any LRC
profile has no valid ``layer-plugin``. Changing the value on a profile that a
pool uses prints a warning, because data already written is then decoded with
the new sub-plugin.

.. note::

   Releases before Tentacle did not record ``layer-plugin`` and always used
   ``jerasure``. Before upgrading a cluster that has LRC pools from a release
   without layer-plugin support to one with it, set ``layer-plugin`` on every
   LRC profile so the correct sub-plugin is preserved. Use the value the data
   was actually written with:

   * ``jerasure`` if the profile's existing pools were written before Tentacle;
   * ``isa`` otherwise, including profiles created on an existing Tentacle
     cluster -- every Tentacle release defaulted to ``isa``,
     so any pool created on Tentacle was written with ``isa``.

   A profile that no pool uses holds no data, so either value is correct for
   it. You can also remove it with ``ceph osd erasure-code-profile rm {name}``.

   Because ``erasure-code-profile set`` replaces the whole profile, read the
   current settings first and re-specify them all:

   .. prompt:: bash #

      ceph osd erasure-code-profile get {name}
      ceph osd erasure-code-profile set {name} <all existing settings> \
         layer-plugin=<jerasure|isa> --force --yes-i-really-mean-it

   A release without layer-plugin support does not check the value, so if you
   set it on one, confirm with ``ceph osd erasure-code-profile get {name}``
   that it reads exactly ``layer-plugin=jerasure`` or ``layer-plugin=isa``. A
   release with layer-plugin support rejects any other value, and raises the
   ``LRC_LAYER_PLUGIN_UNSET`` health check for every LRC profile that still
   needs setting.

   cephadm blocks ``ceph orch upgrade start`` (and pauses an in-progress
   upgrade with the ``UPGRADE_LRC_LAYER_PLUGIN_UNSET`` health alert) while any
   LRC profile is missing ``layer-plugin``. Other upgrade methods (manual
   package upgrades, Rook, ceph-ansible) have no such check. Once the monitors
   are upgraded they raise the ``LRC_LAYER_PLUGIN_UNSET`` health check; set the
   key before restarting any OSD.

.. warning::

   Upgrade directly to a release that contains the ``layer-plugin`` key.
   Releases without it, including earlier Tentacle releases, ignore
   ``layer-plugin`` and always use their built-in default: ``jerasure`` before
   Tentacle, ``isa`` on earlier Tentacle releases. Upgrading to such a release,
   or rolling back to one, after setting ``layer-plugin=jerasure`` reads
   ``jerasure`` data with ``isa`` and corrupts it on recovery and degraded
   reads.

.. note::

   If a cluster was upgraded without setting ``layer-plugin`` first, an
   upgraded OSD that holds PGs of an affected LRC pool does not boot: it stays
   down and logs a cluster error naming the profile(s). While it waits, it
   serves none of its PGs, including those of other pools, so upgrading a whole
   host or failure domain of such OSDs degrades every pool on them. When
   upgrading OSDs manually, restart a single OSD first and check that it boots.
   Upgraded OSDs also refuse to create PGs of the affected pools rather than
   guess. OSDs that have not been upgraded keep serving those pools, so the
   pools become unavailable as more OSDs are upgraded. To recover, set
   ``layer-plugin`` on the affected profile(s) as above. The waiting OSDs then
   load their PGs and boot on their own, without a restart.

.. warning::

   Changing an existing ``layer-plugin`` value, for example to correct a
   mistake, only takes effect on an OSD when it restarts. Running OSDs keep the
   sub-plugin they loaded the pool's PGs with. While OSDs disagree, recovery
   and degraded reads can return or write corrupt data, and restarting OSDs
   degrades PGs. So stop client I/O and recovery first, then change the value,
   restart every OSD that serves the pool, and resume I/O and recovery.

.. note::

   Do not create new LRC pools during an upgrade until every OSD has been
   upgraded. OSDs running a release without the ``layer-plugin`` key ignore it
   and write the layers with their built-in default (``jerasure`` before
   Tentacle, ``isa`` on earlier Tentacle releases), whatever the profile says.
   Data written in that window would then be read with a different sub-plugin
   once those OSDs upgrade. Wait until the OSD upgrade is complete before
   creating LRC pools.

Low Level Plugin Configuration
==============================

The sum of *k* and *m* must be a multiple of the *l* parameter.
The low level configuration parameters however do not enforce this
restriction and it may be advantageous to use them for specific
purposes. It is for instance possible to define two groups, one with 4
chunks and another with 3 chunks. It is also possible to recursively
define locality sets, for instance datacenters and racks into
datacenters. The *k*\ /\ *m*\ /\ *l* are implemented by generating a low-level
configuration.

The *lrc* erasure code plugin recursively applies erasure code
techniques so that recovering from the loss of some chunks only
requires a subset of the available chunks, most of the time.

For instance, when three coding steps are described as::

   chunk nr    01234567
   step 1      _cDD_cDD
   step 2      cDDD____
   step 3      ____cDDD

where ``c`` are coding chunks calculated from the data chunks ``D``, the
loss of chunk *7* can be recovered with the last four chunks. And the
loss of chunk *2* can be recovered with the first four
chunks.

Erasure Code Profile Examples Using Low-Level Configuration
===========================================================

Minimal Testing
---------------

The following example is strictly equivalent to using a *k=2*, *m=1* erasure code profile. The ``DD``
implies *k=2*, the ``c`` implies *m=1* and the *isa* plugin is used
by default:

.. prompt:: bash #

   ceph osd erasure-code-profile set LRCprofile \
      plugin=lrc \
      mapping=DD_ \
      layers='[ [ "DDc", "" ] ]'
   ceph osd pool create lrcpool erasure LRCprofile

Reduce Recovery Bandwidth Between Hosts
---------------------------------------

Although it is probably not an interesting use case when all hosts are
connected to the same switch, reduced bandwidth usage can actually be
observed. It is equivalent to *k=4*, *m=2* and *l=3* although
the layout of the chunks is different.

.. prompt:: bash # auto

   # ceph osd erasure-code-profile set LRCprofile \
      plugin=lrc \
      mapping=__DD__DD \
      layers='[
                [ "_cDD_cDD", "" ],
                [ "cDDD____", "" ],
                [ "____cDDD", "" ],
              ]'
   # ceph osd pool create lrcpool erasure LRCprofile


Reduce Recovery Bandwidth Between Racks
---------------------------------------

In Firefly, the reduced bandwidth will only be observed if the primary OSD is in
the same rack as the lost chunk.

.. prompt:: bash # auto

   # ceph osd erasure-code-profile set LRCprofile \
      plugin=lrc \
      mapping=__DD__DD \
      layers='[
                [ "_cDD_cDD", "" ],
                [ "cDDD____", "" ],
                [ "____cDDD", "" ],
              ]' \
      crush-steps='[
                      [ "choose", "rack", 2 ],
                      [ "chooseleaf", "host", 4 ],
                     ]'

   # ceph osd pool create lrcpool erasure LRCprofile

Testing with Different Erasure Code Backends
--------------------------------------------

LRC now uses ISA as the default EC backend. It is possible to
specify the EC backend/algorithm on a per-layer basis using the low-level
configuration. The second argument in ``layers='[ [ "DDc", "" ] ]'``
is actually an erasure code profile to be used for this level. The
example below specifies the Jerasure backend with the cauchy technique to
be used in the pool named ``lrcpool``:

.. prompt:: bash #

   ceph osd erasure-code-profile set LRCprofile \
      plugin=lrc \
      mapping=DD_ \
      layers='[ [ "DDc", "plugin=jerasure technique=cauchy" ] ]'
   ceph osd pool create lrcpool erasure LRCprofile

You could also use a different erasure code profile for each
layer:

.. prompt:: bash # auto

   # ceph osd erasure-code-profile set LRCprofile \
      plugin=lrc \
      mapping=__DD__DD \
      layers='[
                [ "_cDD_cDD", "plugin=isa technique=cauchy" ],
                [ "cDDD____", "plugin=isa" ],
                [ "____cDDD", "plugin=jerasure" ],
              ]'
   # ceph osd pool create lrcpool erasure LRCprofile


Erasure Coding and Decoding Algorithm
=====================================

The steps found in the layers description::

   chunk nr    01234567

   step 1      _cDD_cDD
   step 2      cDDD____
   step 3      ____cDDD

are applied in order. For instance, if a 4K object is encoded, it will
first go through *step 1* and be divided in four 1K chunks (the four
uppercase ``D``). They are stored in the chunks *2*, *3*, *6* and *7*, in
order. From these, two coding chunks are calculated (the two lowercase
``c``). The coding chunks are stored in the chunks *1* and *5*, respectively.

The *step 2* re-uses the content created by *step 1* in a similar
fashion and stores a single coding chunk ``c`` at position *0*. The last four
chunks, marked with an underscore (``_``) for readability, are ignored.

The *step 3* stores a single coding chunk ``c`` at position *4*. The three
chunks created by *step 1* are used to compute this coding chunk,
i.e. the coding chunk from *step 1* becomes a data chunk in *step 3*.

If chunk *2* is lost::

   chunk nr    01234567

   step 1      _c D_cDD
   step 2      cD D____
   step 3      __ _cDDD

decoding will attempt to recover it by walking the steps in reverse
order: *step 3*, then *step 2*, and finally *step 1*.

The *step 3* knows nothing about chunk *2* (i.e. it is an underscore)
and is skipped.

The coding chunk from *step 2*, stored in chunk *0*, allows it to
recover the content of chunk *2*. There are no more chunks to recover
and the process stops, without considering *step 1*.

Recovering chunk *2* requires reading chunks *0*, *1*, *3* and writing
back chunk *2*.

If chunk *2, 3, 6* are lost::

   chunk nr    01234567

   step 1      _c  _c D
   step 2      cD  __ _
   step 3      __  cD D

The *step 3* can recover the content of chunk *6*::

   chunk nr    01234567

   step 1      _c  _cDD
   step 2      cD  ____
   step 3      __  cDDD

The *step 2* fails to recover and is skipped because there are two
chunks missing (*2*, *3*) and it can only recover from one missing
chunk.

The coding chunk from *step 1*, stored in chunk *1*, *5*, allows it to
recover the content of chunk *2*, *3*::

   chunk nr    01234567

   step 1      _cDD_cDD
   step 2      cDDD____
   step 3      ____cDDD

Controlling CRUSH Placement
===========================

The default CRUSH rule provides OSDs that are on different hosts. For instance::

   chunk nr    01234567

   step 1      _cDD_cDD
   step 2      cDDD____
   step 3      ____cDDD

needs exactly 8 OSDs, one for each chunk. If the hosts are in two
adjacent racks, the first four chunks can be placed in the first rack,
and the last four in the second rack, so that recovering from the loss
of a single OSD does not require using bandwidth between the two
racks.

For instance::

   crush-steps='[ [ "choose", "rack", 2 ], [ "chooseleaf", "host", 4 ] ]'

will create a rule that will select two CRUSH buckets of type
``rack`` and for each of them choose four OSDs, each of them located in
different buckets of type ``host``.

The CRUSH rule can also be manually crafted for finer control.
