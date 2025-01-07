#include "crimson/common/coroutine.h"
#include "crimson/osd/object_context_loader.h"
#include "osd/osd_types_fmt.h"
#include "osd/object_state_fmt.h"

SET_SUBSYS(osd);

namespace crimson::osd {

using crimson::common::local_conf;


ObjectContextLoader::load_and_lock_fut
ObjectContextLoader::load_and_lock_head(Manager &manager, RWState::State lock_type)
{
  LOG_PREFIX(ObjectContextLoader::load_and_lock_head);
  DEBUGDPP("{} {}", dpp, manager.target, lock_type);
  auto releaser = manager.get_releaser();
  ceph_assert(manager.target.is_head());

  if (manager.head_state.is_empty()) {
    auto [obc, _] = obc_registry.get_cached_obc(manager.target);
    manager.set_state_obc(manager.head_state, obc);
  }
  ceph_assert(manager.target_state.is_empty());
  manager.set_state_obc(manager.target_state, manager.head_state.obc);

  if (manager.target_state.obc->loading_started) {
    co_await manager.target_state.lock_to(lock_type);
  } else {
    manager.target_state.lock_excl_sync();
    manager.target_state.obc->loading_started = true;
    co_await load_obc(manager.target_state.obc);
    manager.target_state.demote_excl_to(lock_type);
  }
  releaser.cancel();
}

ObjectContextLoader::load_and_lock_fut
ObjectContextLoader::load_and_lock_clone(
  Manager &manager, RWState::State lock_type, bool lock_head)
{
  LOG_PREFIX(ObjectContextLoader::load_and_lock_clone);
  DEBUGDPP("{} {}", dpp, manager.target, lock_type);
  auto releaser = manager.get_releaser();

  ceph_assert(!manager.target.is_head());
  ceph_assert(manager.target_state.is_empty());

  if (manager.head_state.is_empty()) {
    auto [obc, _] = obc_registry.get_cached_obc(manager.target.get_head());
    manager.set_state_obc(manager.head_state, obc);
  }

  if (!manager.head_state.obc->loading_started) {
    // caller is responsible for pre-populating a loaded obc if lock_head is
    // false
    ceph_assert(lock_head);
    manager.head_state.lock_excl_sync();
    manager.head_state.obc->loading_started = true;
    co_await load_obc(manager.head_state.obc);
    manager.head_state.demote_excl_to(RWState::RWREAD);
  } else if (lock_head) {
    co_await manager.head_state.lock_to(RWState::RWREAD);
  }

  if (manager.options.resolve_clone) {
    auto resolved_oid = resolve_oid(
      manager.head_state.obc->get_head_ss(),
      manager.target);
    if (!resolved_oid) {
      ERRORDPP("clone {} not found", dpp, manager.target);
      co_await load_obc_iertr::future<>(
	crimson::ct_error::enoent::make()
      );
    }
    // note: might be head if snap was taken after most recent write!
    manager.target = *resolved_oid;
  }

  if (manager.target.is_head()) {
    /* Yes, we assert at the top that manager.target is not head.  However, it's
     * possible that the requested snap (the resolve_clone path above) actually
     * maps to head (a read on an rbd snapshot more recent than the most recent
     * write on this specific rbd block, for example).
     *
     * In such an event, it's hypothetically possible that lock_type isn't
     * RWREAD, in which case we need to drop and reacquire the lock.  However,
     * this case is at present impossible.  Actual client requests cannot write
     * to a snapshot and will therefore always be RWREAD.  The pathways that
     * actually can mutate a clone do not set resolve_clone, so target will not
     * become head here.
     */
    manager.set_state_obc(manager.target_state, manager.head_state.obc);
    if (lock_type != manager.head_state.state) {
      // This case isn't actually possible at the moment for the above reason.
      manager.head_state.release_lock();
      co_await manager.target_state.lock_to(lock_type);
    } else {
      manager.target_state.state = manager.head_state.state;
      manager.head_state.state = RWState::RWNONE;
    }
  } else {
    auto [obc, _] = obc_registry.get_cached_obc(manager.target);
    manager.set_state_obc(manager.target_state, obc);

    if (manager.target_state.obc->loading_started) {
      co_await manager.target_state.lock_to(RWState::RWREAD);
    } else {
      manager.target_state.lock_excl_sync();
      manager.target_state.obc->loading_started = true;
      co_await load_obc(manager.target_state.obc);
      manager.target_state.obc->set_clone_ssc(manager.head_state.obc->ssc);
      manager.target_state.demote_excl_to(RWState::RWREAD);
    }
  }

  releaser.cancel();
}

ObjectContextLoader::load_and_lock_fut
ObjectContextLoader::load_and_lock(Manager &manager, RWState::State lock_type)
{
  LOG_PREFIX(ObjectContextLoader::load_and_lock);
  DEBUGDPP("{} {}", dpp, manager.target, lock_type);
  if (manager.target.is_head()) {
    return load_and_lock_head(manager, lock_type);
  } else {
    return load_and_lock_clone(manager, lock_type);
  }
}

ObjectContextLoader::load_obc_iertr::future<>
ObjectContextLoader::load_obc(ObjectContextRef obc)
{
  LOG_PREFIX(ObjectContextLoader::load_obc);
  return backend.load_metadata(obc->get_oid())
    .safe_then_interruptible(
      [FNAME, this, obc=std::move(obc)](auto md)
      -> load_obc_ertr::future<> {
	const hobject_t& oid = md->os.oi.soid;
	DEBUGDPP("loaded obs {} for {}", dpp, md->os.oi, oid);
	if (oid.is_head()) {
	  if (!md->ssc) {
	    ERRORDPP("oid {} missing snapsetcontext", dpp, oid);
	    return crimson::ct_error::object_corrupted::make();
	  }
	  obc->set_head_state(std::move(md->os),
			      std::move(md->ssc));
	} else {
	  // we load and set the ssc only for head obc.
	  // For clones, the head's ssc will be referenced later.
	  // See set_clone_ssc
	  obc->set_clone_state(std::move(md->os));
	}
	DEBUGDPP("loaded obc {} for {}", dpp, obc->obs.oi, obc->obs.oi.soid);
	return seastar::now();
      });
}

void ObjectContextLoader::notify_on_change(bool is_primary)
{
  LOG_PREFIX(ObjectContextLoader::notify_on_change);
  DEBUGDPP("is_primary: {}", dpp, is_primary);
  for (auto& obc : obc_set_accessing) {
    DEBUGDPP("interrupting obc: {}", dpp, obc.get_oid());
    obc.interrupt(::crimson::common::actingset_changed(is_primary));
  }
}
}
