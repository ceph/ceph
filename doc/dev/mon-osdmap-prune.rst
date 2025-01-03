===========================
FULL OSDMAP VERSION PRUNING
===========================

For each incremental osdmap epoch, the monitor will keep a full osdmap
epoch in the store.

While this is great when serving osdmap requests from clients, allowing
us to fulfill their request without having to recompute the full osdmap
from a myriad of incrementals, it can also become a burden once we start
keeping an unbounded number of osdmaps.

The monitors will attempt to keep a bounded number of osdmaps in the store.
This number is defined (and configurable) via ``mon_min_osdmap_epochs``, and
defaults to 500 epochs. Generally speaking, we will remove older osdmap
epochs once we go over this limit.

However, there are a few constraints to removing osdmaps. These are all
defined in ``OSDMonitor::get_trim_to()``.

In the event one of these conditions is not met, we may go over the bounds
defined by ``mon_min_osdmap_epochs``. And if the cluster does not meet the
trim criteria for some time (e.g., unclean pgs), the monitor may start
keeping a lot of osdmaps. This can start putting pressure on the underlying
key/value store, as well as on the available disk space.

One way to mitigate this problem would be to stop keeping full osdmap
epochs on disk. We would have to rebuild osdmaps on-demand, or grab them
from cache if they had been recently served. We would still have to keep
at least one osdmap, and apply all incrementals on top of either this
oldest map epoch kept in the store or a more recent map grabbed from cache.
While this would be feasible, it seems like a lot of cpu (and potentially
IO) would be going into rebuilding osdmaps.

Additionally, this would prevent the aforementioned problem going forward,
but would do nothing for stores currently in a state that would truly
benefit from not keeping osdmaps.

This brings us to full osdmap pruning.

Instead of not keeping full osdmap epochs, we are going to prune some of
them when we have too many.

Deciding whether we have too many will be dictated by a configurable option
``mon_osdmap_full_prune_min`` (default: 10000). The pruning algorithm will be
engaged once we go over this threshold.

We will not remove all ``mon_osdmap_full_prune_min`` full osdmap epochs
though. Instead, we are going to poke some holes in the sequence of full
maps. By default, we will keep one full osdmap per 10 maps since the last
map kept; i.e., if we keep epoch 1, we will also keep epoch 10 and remove
full map epochs 2 to 9. The size of this interval is configurable with
``mon_osdmap_full_prune_interval``.

Essentially, we are proposing to keep ~10% of the full maps, but we will
always honour the minimum number of osdmap epochs, as defined by
``mon_min_osdmap_epochs``, and these won't be used for the count of the
minimum versions to prune. For instance, if we have on-disk versions
[1..50000], we would allow the pruning algorithm to operate only over
osdmap epochs [1..49500); but, if have on-disk versions [1..10200], we
won't be pruning because the algorithm would only operate on versions
[1..9700), and this interval contains less versions than the minimum
required by ``mon_osdmap_full_prune_min``.


ALGORITHM
=========

Say we have 50,000 osdmap epochs in the store, and we're using the
defaults for all configurable options.

::

    -----------------------------------------------------------
    |1|2|..|10|11|..|100|..|1000|..|10000|10001|..|49999|50000|
    -----------------------------------------------------------
     ^ first                                            last ^

We will prune when all the following constraints are met:

1. number of versions is greater than ``mon_min_osdmap_epochs``;

2. the number of versions between ``first`` and ``prune_to`` is greater (or
   equal) than ``mon_osdmap_full_prune_min``, with ``prune_to`` being equal to
   ``last`` minus ``mon_min_osdmap_epochs``.

If any of these conditions fails, we will *not* prune any maps.

Furthermore, if it is known that we have been pruning, but since then we
are no longer satisfying at least one of the above constraints, we will
not continue to prune. In essence, we only prune full osdmaps if the
number of epochs in the store so warrants it.

As pruning will create gaps in the sequence of full maps, we need to keep
track of the intervals of missing maps. We do this by keeping a manifest of
pinned maps -- i.e., a list of maps that, by being pinned, are not to be
pruned.

While pinned maps are not removed from the store, maps between two consecutive
pinned maps will; and the number of maps to be removed will be dictated by the
configurable option ``mon_osdmap_full_prune_interval``. The algorithm makes an
effort to keep pinned maps apart by as many maps as defined by this option,
but in the event of corner cases it may allow smaller intervals. Additionally,
as this is a configurable option that is read any time a prune iteration
occurs, there is the possibility this interval will change if the user changes
this config option.

Pinning maps is performed lazily: we will be pinning maps as we are removing
maps. This grants us more flexibility to change the prune interval while
pruning is happening, but also simplifies considerably the algorithm, as well
as the information we need to keep in the manifest. Below we show a simplified
version of the algorithm:::

    manifest.pin(first)
    last_to_prune = last - mon_min_osdmap_epochs

    while manifest.get_last_pinned() + prune_interval < last_to_prune AND
          last_to_prune - first > mon_min_osdmap_epochs AND
          last_to_prune - first > mon_osdmap_full_prune_min AND
          num_pruned < mon_osdmap_full_prune_txsize:
      
      last_pinned = manifest.get_last_pinned()
      new_pinned = last_pinned + prune_interval
      manifest.pin(new_pinned)
      for e in (last_pinned .. new_pinned):
        store.erase(e)
        ++num_pruned

In essence, the algorithm ensures that the first version in the store is
*always* pinned. After all, we need a starting point when rebuilding maps, and
we can't simply remove the earliest map we have; otherwise we would be unable
to rebuild maps for the very first pruned interval.

Once we have at least one pinned map, each iteration of the algorithm can
simply base itself on the manifest's last pinned map (which we can obtain by
reading the element at the tail of the manifest's pinned maps list).

We'll next need to determine the interval of maps to be removed: all the maps
from ``last_pinned`` up to ``new_pinned``, which in turn is nothing more than
``last_pinned`` plus ``mon_osdmap_full_prune_interval``. We know that all maps
between these two values, ``last_pinned`` and ``new_pinned`` can be removed,
considering ``new_pinned`` has been pinned.

The algorithm ceases to execute as soon as one of the two initial
preconditions is not met, or if we do not meet two additional conditions that
have no weight on the algorithm's correctness:

1. We will stop if we are not able to create a new pruning interval properly
   aligned with ``mon_osdmap_full_prune_interval`` that is lower than
   ``last_pruned``. There is no particular technical reason why we enforce
   this requirement, besides allowing us to keep the intervals with an
   expected size, and preventing small, irregular intervals that would be
   bound to happen eventually (e.g., pruning continues over the course of
   several iterations, removing one or two or three maps each time).

2. We will stop once we know that we have pruned more than a certain number of
   maps. This value is defined by ``mon_osdmap_full_prune_txsize``, and
   ensures we don't spend an unbounded number of cycles pruning maps. We don't
   enforce this value religiously (deletes do not cost much), but we make an
   effort to honor it.

We could do the removal in one go, but we have no idea how long that would
take. Therefore, we will perform several iterations, removing at most
``mon_osdmap_full_prune_txsize`` osdmaps per iteration.

In the end, our on-disk map sequence will look similar to::

    ------------------------------------------
    |1|10|20|30|..|49500|49501|..|49999|50000|
    ------------------------------------------
     ^ first                           last ^


Because we are not pruning all versions in one go, we need to keep state
about how far along on our pruning we are. With that in mind, we have
created a data structure, ``osdmap_manifest_t``, that holds the set of pinned
maps:::

    struct osdmap_manifest_t:
        set<version_t> pinned;

Given we are only pinning maps while we are pruning, we don't need to keep
track of additional state about the last pruned version. We know as a matter
of fact that we have pruned all the intermediate maps between any two
consecutive pinned maps.

The question one could ask, though, is how can we be sure we pruned all the
intermediate maps if, for instance, the monitor crashes. To ensure we are
protected against such an event, we always write the osdmap manifest to disk
on the same transaction that is deleting the maps. This way we have the
guarantee that, if the monitor crashes, we will read the latest version of the
manifest: either containing the newly pinned maps, meaning we also pruned the
in-between maps; or we will find the previous version of the osdmap manifest,
which will not contain the maps we were pinning at the time we crashed, given
the transaction on which we would be writing the updated osdmap manifest was
not applied (alongside with the maps removal).

The osdmap manifest will be written to the store each time we prune, with an
updated list of pinned maps. It is written in the transaction effectively
pruning the maps, so we guarantee the manifest is always up to date. As a
consequence of this criteria, the first time we will write the osdmap manifest
is the first time we prune. If an osdmap manifest does not exist, we can be
certain we do not hold pruned map intervals.

We will rely on the manifest to ascertain whether we have pruned maps
intervals. In theory, this will always be the on-disk osdmap manifest, but we
make sure to read the on-disk osdmap manifest each time we update from paxos;
this way we always ensure having an up to date in-memory osdmap manifest.

Once we finish pruning maps, we will keep the manifest in the store, to
allow us to easily find which maps have been pinned (instead of checking
the store until we find a map). This has the added benefit of allowing us to
quickly figure out which is the next interval we need to prune (i.e., last
pinned plus the prune interval). This doesn't however mean we will forever
keep the osdmap manifest: the osdmap manifest will no longer be required once
the monitor trims osdmaps and the earliest available epoch in the store is
greater than the last map we pruned.

The same conditions from ``OSDMonitor::get_trim_to()`` that force the monitor
to keep a lot of osdmaps, thus requiring us to prune, may eventually change
and allow the monitor to remove some of its oldest maps.

MAP TRIMMING
------------

If the monitor trims maps, we must then adjust the osdmap manifest to
reflect our pruning status, or remove the manifest entirely if it no longer
makes sense to keep it. For instance, take the map sequence from before, but
let us assume we did not finish pruning all the maps.::

    -------------------------------------------------------------
    |1|10|20|30|..|490|500|501|502|..|49500|49501|..|49999|50000|
    -------------------------------------------------------------
     ^ first            ^ pinned.last()                   last ^

    pinned = {1, 10, 20, ..., 490, 500}

Now let us assume that the monitor will trim up to epoch 501. This means
removing all maps prior to epoch 501, and updating the ``first_committed``
pointer to ``501``. Given removing all those maps would invalidate our
existing pruning efforts, we can consider our pruning has finished and drop
our osdmap manifest. Doing so also simplifies starting a new prune, if all
the starting conditions are met once we refreshed our state from the
store.

We would then have the following map sequence: ::

    ---------------------------------------
    |501|502|..|49500|49501|..|49999|50000|
    ---------------------------------------
     ^ first                        last ^

However, imagine a slightly more convoluted scenario: the monitor will trim
up to epoch 491. In this case, epoch 491 has been previously pruned from the
store.

Given we will always need to have the oldest known map in the store, before
we trim we will have to check whether that map is in the prune interval
(i.e., if said map epoch belongs to ``[ pinned.first()..pinned.last() )``).
If so, we need to check if this is a pinned map, in which case we don't have
much to be concerned aside from removing lower epochs from the manifest's
pinned list. On the other hand, if the map being trimmed to is not a pinned
map, we will need to rebuild said map and pin it, and only then will we remove
the pinned maps prior to the map's epoch. 

In this case, we would end up with the following sequence:::

    -----------------------------------------------
    |491|500|501|502|..|49500|49501|..|49999|50000|
    -----------------------------------------------
     ^   ^- pinned.last()                   last ^
     `- first

There is still an edge case that we should mention. Consider that we are
going to trim up to epoch 499, which is the very last pruned epoch.

Much like the scenario above, we would end up writing osdmap epoch 499 to
the store; but what should we do about pinned maps and pruning?

The simplest solution is to drop the osdmap manifest. After all, given we
are trimming to the last pruned map, and we are rebuilding this map, we can
guarantee that all maps greater than e 499 are sequential (because we have
not pruned any of them). In essence, dropping the osdmap manifest in this
case is essentially the same as if we were trimming over the last pruned
epoch: we can prune again later if we meet the required conditions.

And, with this, we have fully dwelled into full osdmap pruning. Later in this
document one can find detailed `REQUIREMENTS, CONDITIONS & INVARIANTS` for the
whole algorithm, from pruning to trimming. Additionally, the next section
details several additional checks to guarantee the sanity of our configuration
options. Enjoy.


CONFIGURATION OPTIONS SANITY CHECKS
-----------------------------------

We perform additional checks before pruning to ensure all configuration
options involved are sane:

1. If ``mon_osdmap_full_prune_interval`` is zero we will not prune; we
   require an actual positive number, greater than one, to be able to prune
   maps. If the interval is one, we would not actually be pruning any maps, as
   the interval between pinned maps would essentially be a single epoch. This
   means we would have zero maps in-between pinned maps, hence no maps would
   ever be pruned.

2. If ``mon_osdmap_full_prune_min`` is zero we will not prune; we require a
   positive, greater than zero, value so we know the threshold over which we
   should prune. We don't want to guess.

3. If ``mon_osdmap_full_prune_interval`` is greater than
   ``mon_osdmap_full_prune_min`` we will not prune, as it is impossible to
   ascertain a proper prune interval.

4. If ``mon_osdmap_full_prune_txsize`` is lower than
   ``mon_osdmap_full_prune_interval`` we will not prune; we require a
   ``txsize`` with a value at least equal than ``interval``, and (depending on
   the value of the latter) ideally higher.


REQUIREMENTS, CONDITIONS & INVARIANTS
-------------------------------------

REQUIREMENTS
~~~~~~~~~~~~

* All monitors in the quorum need to support pruning.

* Once pruning has been enabled, monitors not supporting pruning will not be
  allowed in the quorum, nor will be allowed to synchronize.

* Removing the osdmap manifest results in disabling the pruning feature quorum
  requirement. This means that monitors not supporting pruning will be allowed
  to synchronize and join the quorum, granted they support any other features
  required.


CONDITIONS & INVARIANTS
~~~~~~~~~~~~~~~~~~~~~~~

* Pruning has never happened, or we have trimmed past its previous
  intervals:::

    invariant: first_committed > 1

    condition: pinned.empty() AND !store.exists(manifest)


* Pruning has happened at least once:::

    invariant: first_committed > 0
    invariant: !pinned.empty())
    invariant: pinned.first() == first_committed
    invariant: pinned.last() < last_committed

      precond: pinned.last() < prune_to AND
               pinned.last() + prune_interval < prune_to

     postcond: pinned.size() > old_pinned.size() AND
               (for each v in [pinned.first()..pinned.last()]:
                 if pinned.count(v) > 0: store.exists_full(v)
                 else: !store.exists_full(v)
               )


* Pruning has finished:::

    invariant: first_committed > 0
    invariant: !pinned.empty()
    invariant: pinned.first() == first_committed
    invariant: pinned.last() < last_committed

    condition: pinned.last() == prune_to OR
               pinned.last() + prune_interval < prune_to


* Pruning intervals can be trimmed:::

    precond:   OSDMonitor::get_trim_to() > 0

    condition: !pinned.empty()

    invariant: pinned.first() == first_committed
    invariant: pinned.last() < last_committed
    invariant: pinned.first() <= OSDMonitor::get_trim_to()
    invariant: pinned.last() >= OSDMonitor::get_trim_to()

* Trim pruned intervals:::

    invariant: !pinned.empty()
    invariant: pinned.first() == first_committed
    invariant: pinned.last() < last_committed
    invariant: pinned.first() <= OSDMonitor::get_trim_to()
    invariant: pinned.last() >= OSDMonitor::get_trim_to()

    postcond:  pinned.empty() OR
               (pinned.first() == OSDMonitor::get_trim_to() AND
                pinned.last() > pinned.first() AND
                (for each v in [0..pinned.first()]:
                  !store.exists(v) AND
                  !store.exists_full(v)
                ) AND
                (for each m in [pinned.first()..pinned.last()]:
                  if pinned.count(m) > 0: store.exists_full(m)
                  else: !store.exists_full(m) AND store.exists(m)
                )
               )
    postcond:  !pinned.empty() OR
               (!store.exists(manifest) AND
                (for each v in [pinned.first()..pinned.last()]:
                  !store.exists(v) AND
                  !store.exists_full(v)
                )
               )

