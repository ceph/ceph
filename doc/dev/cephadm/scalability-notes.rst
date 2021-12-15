#############################################
 Notes and Thoughts on Cephadm's scalability
#############################################

*********************
 About this document
*********************

This document does NOT define a specific proposal or some future work.
Instead it merely lists a few thoughts that MIGHT be relevant for future
cephadm enhancements.

*******
 Intro
*******

Current situation:

Cephadm manages all registered hosts. This means that it periodically
scrapes data from each host to identify changes on the host like:

-  disk added/removed
-  daemon added/removed
-  host network/firewall etc has changed

Currently, cephadm scrapes each host (up to 10 in parallel) every 6
minutes, unless a refresh is forced manually.

Refreshes for disks (ceph-volume), daemons (podman/docker), etc, happen
in sequence.

With the cephadm exporter, we have now reduced the time to scan hosts
considerably, but the question remains:

Is the cephadm-exporter sufficient to solve all future scalability
issues?

***********************************************
 Considerations of cephadm-exporter's REST API
***********************************************

The cephadm-exporter uses HTTP to serve an endpoint to the hosts
metadata. We MIGHT encounter some issues with this approach, which need
to be mitigated at some point.

-  With the cephadm-exporter we use SSH and HTTP to connect to each
   host. Having two distinct transport layers feels odd, and we might
   want to consider reducing it to only a single protocol.

-  The current approach of delivering ``bin/cephadm`` to the host doesn't
   allow the use of external dependencies. This means that we're stuck
   with the built-in HTTP server lib, which isn't great for providing a
   good developer experience. ``bin/cephadm`` needs to be packaged and
   distributed (one way or the other) for us to make use of a better
   http server library.

************************
 MON's config-key store
************************

After the ``mgr/cephadm`` queried metadata from each host, cephadm stores
the data within the mon's k-v store.

If each host would be allowed to write their own metadata to the store,
``mgr/cephadm`` would no longer be required to gather the data.

Some questions arise:

-  ``mgr/cephadm`` now needs to query data from the config-key store,
   instead of relying on cached data.

-  cephadm knows three different types of data: (1) Data that is
   critical and needs to be stored in the config-key store. (2) Data
   that can be kept in memory only. (3) Data that can be stored in
   RADOS pool. How can we apply this idea to those different types of
   data.

*******************************
 Increase the worker pool size
*******************************

``mgr/cephadm`` is currently able to scrape 10 nodes at the same time.

The scrape of a individual host takes the same amount of time persists.
We'd just reduce the overall execution time.

At best we can reach O(hosts) + O(daemons).

*************************
 Backwards compatibility
*************************

Any changes need to be backwards compatible or completely isolated from
any existing functionality. There are running cephadm clusters out there
that require an upgrade path.
