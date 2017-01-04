==================
PGPool
==================

PGPool is a structure used to manage and update the status of removed
snapshots.  It does this by maintaining two fields, cached_removed_snaps - the
current removed snap set and newly_removed_snaps - newly removed snaps in the
last epoch. In OSD::load_pgs the osd map is recovered from the pg's file store
and passed down to OSD::_get_pool where a PGPool object is initialised with the
map.

With each new map we receive we call PGPool::update with the new map. In that
function we build a list of newly removed snaps
(pg_pool_t::build_removed_snaps) and merge that with our cached_removed_snaps.
This function included checks to make sure we only do this update when things
have changed or there has been a map gap.

When we activate the pg we initialise the snap trim queue from
cached_removed_snaps and subtract the purged_snaps we have already purged
leaving us with the list of snaps that need to be trimmed. Trimming is later
performed asynchronously by the snap_trim_wq.

