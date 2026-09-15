==============================
Fail-slow OSD Detection Design
==============================

Background
==========

Some production clusters have seen severe cluster-wide performance
degradation caused by a very small number of storage devices with abnormally
high I/O latency. These devices may still respond within one or two seconds,
which is slow enough to hurt client workloads but not slow enough for the OSD
to be marked down.

This is especially impactful for VM workloads because VM data is distributed
across many OSDs. A single slow device can therefore add latency to requests
for many VMs.

The goal of this work is to detect OSDs, or physical devices backing OSDs, that
are likely in a fail-slow state and surface them to the cluster operator. A
later policy decision may decide whether Ceph should only report these OSDs or
also take automatic action such as marking them out.

Problem Statement
=================

A fail-slow detector should identify OSDs whose device-side latency is
abnormally high compared with similar healthy OSDs. It should avoid flagging
OSDs that are temporarily slow because of expected internal background work,
like recovery/backfill/deep-scrub.

The detector should be robust against outliers. A small number of bad OSDs
should not move the baseline far enough that they stop being detected.

Non-goals
=========

This design does not try to classify every cause of cluster-wide latency. In
particular, higher-level background work such as RGW garbage collection,
cross-site replication, deduplication, or pool migration can affect performance
without being directly visible to the OSD. Those workloads may need separate
signals or operator context.

Initial automated remediation policy is also out of scope. The detector should
first produce a reliable suspicion signal.

Detection Model
===============

Use a robust reference baseline instead of a simple average or an adaptive
per-OSD sliding-window baseline.

An adaptive per-OSD baseline has two problems:

* it can include samples collected during recovery, backfill, scrub, or other
  temporary work;
* it can adapt to the degraded state and eventually stop flagging the slow OSD.

For each comparison cohort, calculate the median and median absolute deviation
of the steady-state OSDs, then score each candidate OSD against that cohort:

::

   osd_score = (commit_latency_ms[osd] - cohort_median) / cohort_MAD

``cohort_MAD`` is preferred over standard deviation because standard deviation
is pulled by extreme outliers.

Comparison Cohorts
==================

The baseline should not necessarily be global across the entire cluster. A
cluster may contain very different hardware classes, and it is common for pools
to use different hardware. Comparing HDD-backed pools directly with NVMe-backed
pools would produce misleading results.

The preferred comparison is therefore per pool, or per another cohort that maps
to similar storage hardware. The exact cohort selection should be explicit in
the implementation so that mixed-hardware clusters do not make healthy slow
media look anomalous merely because faster media exists elsewhere.

Sample Filtering
================

Only steady-state OSDs should contribute to the reference statistics. Exclude
OSDs that are known to be affected by OSD-visible background work, including:

* recovery;
* backfill;
* deep scrub;
* possibly PG split and merge.

The same filtering should be considered when deciding whether a specific OSD is
a candidate for fail-slow detection. An OSD that is temporarily slow because of
known background work should not be treated the same way as a steady-state OSD
with unexpectedly high device latency.

Primary Signal
==============

For bottom-up OSD or device detection, use device-oriented latency such as
``commit_latency_ms`` as the primary signal. This keeps the first detector
focused on storage-device slowness.

Signals such as operation latency or queue-age histograms are still valuable,
but they are better suited as top-down triggers. They can tell the system that
clients are experiencing latency, after which the detector can look for a
device-level explanation.

Physical Device Grouping
========================

Some deployments run multiple OSDs on the same physical device. After scoring
individual OSDs, group scores by physical device when that mapping is available
from OSD metadata.

The primary device-level score should be the median of the already-normalized
OSD scores for that device:

::

   device_score = median(osd_score[osd] for osd on device)

This helps identify the physical device as the likely failing unit rather than
treating each affected OSD independently. Since ``osd_score`` is already
normalized against the comparison cohort, subtracting the median score of OSDs
not on the device is unnecessary for the primary detector. A leave-one-device-out
contrast may still be useful as a secondary diagnostic, but it should not be
the main score used for thresholding.

Failure Classification
======================

Additional latency metrics can help distinguish storage-device failures from
network or failure-domain problems. For example, elevated ``subop_w_latency``
for a rack, host, or OSD may indicate a network issue in that entity or failure
domain rather than a slow disk.

This classification should be secondary to the first fail-slow detector. The
initial detector should produce a strong suspicion signal for abnormal storage
latency; later work can classify the failure domain more precisely.
