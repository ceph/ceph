Cephadm Design Docs
===================


**Housekeeping note:**

If you want to see things added to this document, please add it via Github's `suggestion[Suggest Changes]` feature.
This allows me to just hit the button in order for the comment to end up in the document.


Intro
-----

Current situation:

Cephadm manages all registered hosts. This means that it needs to periodically scrape data from each
host to identify changes on the host like:

* disk added/removed
* daemon added/removed
* host network/firewall etc has changed


Currently, cephadm scrapes each host (up to 10 in parallel[0]) every 6 minutes[1], unless a refresh is forced manually.

Refreshes for disks (ceph-volume), daemons (podman/docker), etc, happen in sequence.

While the refresh model isn't ideal in the first place, the execution time vastly increases with the cluster size > 10

This certainly isn't acceptable since ceph itself scales massively and so has the management part.

There are a couple of solutions to this:


1. Pre fetching/caching data on the host
----------------------------------------

This would allow bin/cephadm to autonomously scrape data that can later be retrieved by mgr/cephadm.

i.e. `cephadm ls` would access an underlying (transparent) cache.

This, btw assumes a **pull** model.

There is at least one possible solutions on *how* to do this.

1.1 daemonize bin/cephadm
--------------------------

A daemon is able to execute scraping tasks more frequently (as opposed to only when asked by mgr/cephadm)


Downsides:

We're still `scp'ing` bin/cephadm from the ceph-mgr to the host which is then executed.
A daemon on the other hand needs to run continuously on the target system. If ceph-mgr (and the containing bin/cephadm
script) is updated, the daemon needs to be informed of this event and get updated/restarted as well.


1.1.1 HTTP:
___________

There is a implementation by Paul Cuzner [2] that uses HTTP to serve an endpoint to the hosts metadata.

Upside:

* We could utilize a proper REST API to CRUD all of the daemons/disks etc.
  Currently it feels a bit weird to call a binary from a remote host (that's another topic though)


Downside:

* This is quite some shift in architecture.
* We still use SSH to bootstrap a node (in order to install bin/cephadm for the first time)
  Having two distinct transport layers feels odd.
* The current approach of delivering bin/cephadm to the host doesn't allow the use of external dependencies.
  This means that we're stuck with the built-in HTTP server lib, which isn't ment for production purposes.
  bin/cephadm needs to be packaged and distributed (one way or the other) for us to make use of a production ready
  http server.

Requirements:

* https
* authentication
* documentation
* api versioning (keep it stable)


1.1.2 Keep bin/cephadm
______________________

Daemonizing bin/cephadm allows us to pre-fetch data and store it in some shape or form.

Currently we're only interested in speeding up metadata retrieval. This could mean that we don't even
need to introduce a new transport layer but rather transparently return cached content via the `cephadm ls`
or the `cephadm ceph-volume ls` command.

The technical details need to be figured out. (format of data storage etc.)

Upside:

* No new transport layer.
  Also means no extra auth, security layer is needed

Downside:

* May feel a bit hacky
* Needs locks (if a refresh is currently happening)


1.1.3 Use k-v store
___________________

After the mgr/cephadm queried metadata from each host, we usually save it in the mon's k-v store.

If each host would be allowed to write their own metadata to the store, we'd not need to explicitly call
for a refresh.

This needs research on some topics:

* Is the mon's k-v store capable of accepting **many** requests (CRUD)
* Does the auth/keyring mechanism allow to reduce access to certain areas of the mon store?


2 Push or Pub-Sub
-----------------

This is a change of paradigm. However:

Upside:
* Events can be captured and forwarded. Instant notifications
* This is a proper and fast implementation for such a system

Downside:

* This is would require a major rewrite (also how mgr/cephadm operates)


4 Increase the worker pool size
--------------------------------

As we identified in the `Intro` section, mgr/cephadm is currently able to scrape 10 nodes at the same time.

I'm sure we hit limitations somewhere so that we can't just increase this number (please comment if you know)

The obvious downside that the scrape of a individual host takes the same amount of time persists. We'd just
reduce the overall execution time.

At best we can reach O(hosts) + O(daemons).

Imho, this is not an option.



General notes:
--------------

We should make it absolutely clear that any changes need to be backwards compatible or completely isolated from any
existing functionality. There are running octopus clusters out there that use cephadm.

Anything that will be decided here will probably stick for a while. Lets choose carefully with maintainability in mind.


[0] https://github.com/ceph/ceph/blob/d65092f0fa616e623993a422176fb51ecf1245bf/src/pybind/mgr/cephadm/utils.py#L42
[1] https://github.com/ceph/ceph/blob/1fb9082b0907ca51baafe43293750d93ddde133c/src/pybind/mgr/cephadm/module.py#L499
[2] https://github.com/ceph/ceph/pull/37130
