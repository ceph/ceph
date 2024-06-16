#include "crimson/osd/object_context_loader.h"
#include "osd/osd_types_fmt.h"

SET_SUBSYS(osd);

namespace crimson::osd {

using crimson::common::local_conf;

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_head_obc(const hobject_t& oid,
                                     with_obc_func_t&& func)
  {
    LOG_PREFIX(ObjectContextLoader::with_head_obc);
    auto [obc, existed] = obc_registry.get_cached_obc(oid);
    DEBUGDPP("object {} existed {}",
             dpp, obc->get_oid(), existed);
    assert(obc->is_head());
    obc->append_to(obc_set_accessing);
    return obc->with_lock<State, IOInterruptCondition>(
      [existed=existed, obc=obc, func=std::move(func), this] {
      return get_or_load_obc<State>(obc, existed)
      .safe_then_interruptible(
        [func = std::move(func)](auto obc) {
        // The template with_obc_func_t wrapper supports two obcs (head and clone).
        // In the 'with_head_obc' case, however, only the head is in use.
        // Pass the same head obc twice in order to
        // to support the generic with_obc sturcture.
        return std::move(func)(obc, obc);
      });
    }).finally([FNAME, this, obc=std::move(obc)] {
      DEBUGDPP("released object {}", dpp, obc->get_oid());
      obc->remove_from(obc_set_accessing);
    });
  }

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_clone_obc(const hobject_t& oid,
                                      with_obc_func_t&& func,
                                      bool resolve_clone)
  {
    LOG_PREFIX(ObjectContextLoader::with_clone_obc);
    assert(!oid.is_head());
    return with_head_obc<RWState::RWREAD>(
      oid.get_head(),
      [FNAME, oid, func=std::move(func), resolve_clone, this]
      (auto head, auto) mutable -> load_obc_iertr::future<> {
      if (!head->obs.exists) {
	ERRORDPP("head doesn't exist for object {}", dpp, head->obs.oi.soid);
        return load_obc_iertr::future<>{
          crimson::ct_error::enoent::make()
        };
      }
      return this->with_clone_obc_only<State>(std::move(head),
                                              oid,
                                              std::move(func),
                                              resolve_clone);
    });
  }

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_clone_obc_only(ObjectContextRef head,
                                           hobject_t clone_oid,
                                           with_obc_func_t&& func,
                                           bool resolve_clone)
  {
    LOG_PREFIX(ObjectContextLoader::with_clone_obc_only);
    DEBUGDPP("{}", clone_oid);
    assert(!clone_oid.is_head());
    if (resolve_clone) {
      auto resolved_oid = resolve_oid(head->get_head_ss(), clone_oid);
      if (!resolved_oid) {
        ERRORDPP("clone {} not found", dpp, clone_oid);
        return load_obc_iertr::future<>{
          crimson::ct_error::enoent::make()
        };
      }
      if (resolved_oid->is_head()) {
        // See resolve_oid
        return std::move(func)(head, head);
      }
      clone_oid = *resolved_oid;
    }
    auto [clone, existed] = obc_registry.get_cached_obc(clone_oid);
    return clone->template with_lock<State, IOInterruptCondition>(
      [existed=existed, clone=std::move(clone),
       func=std::move(func), head=std::move(head), this]() mutable
      -> load_obc_iertr::future<> {
      return get_or_load_obc<State>(clone, existed
      ).safe_then_interruptible(
        [func = std::move(func), head=std::move(head)](auto clone) mutable {
        clone->set_clone_ssc(head->ssc);
        return std::move(func)(std::move(head), std::move(clone));
      });
    });
  }

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_obc(hobject_t oid,
                                with_obc_func_t&& func,
                                bool resolve_clone)
  {
    if (oid.is_head()) {
      return with_head_obc<State>(oid, std::move(func));
    } else {
      return with_clone_obc<State>(oid, std::move(func), resolve_clone);
    }
  }

  ObjectContextLoader::load_obc_iertr::future<ObjectContextRef>
  ObjectContextLoader::load_obc(ObjectContextRef obc)
  {
    LOG_PREFIX(ObjectContextLoader::load_obc);
    return backend.load_metadata(obc->get_oid())
    .safe_then_interruptible(
      [FNAME, this, obc=std::move(obc)](auto md)
      -> load_obc_ertr::future<ObjectContextRef> {
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
      DEBUGDPP("returning obc {} for {}", dpp, obc->obs.oi, obc->obs.oi.soid);
      return load_obc_ertr::make_ready_future<ObjectContextRef>(obc);
    });
  }

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<ObjectContextRef>
  ObjectContextLoader::get_or_load_obc(ObjectContextRef obc,
                                       bool existed)
  {
    LOG_PREFIX(ObjectContextLoader::get_or_load_obc);
    DEBUGDPP("{} -- fully_loaded={}, "
             "invalidated_by_interval_change={}",
             dpp, obc->get_oid(),
             obc->fully_loaded,
             obc->invalidated_by_interval_change);
    if (existed) {
      // obc is already loaded - avoid loading_mutex usage
      DEBUGDPP("cache hit on {}", dpp, obc->get_oid());
      return get_obc(obc, existed);
    }
    // See ObjectContext::_with_lock(),
    // this function must be able to support atomicity before
    // acquiring the lock
    if (obc->loading_mutex.try_lock()) {
      return _get_or_load_obc<State>(obc, existed
      ).finally([obc]{
        obc->loading_mutex.unlock();
      });
    } else {
      return interruptor::with_lock(obc->loading_mutex,
      [this, obc, existed, FNAME] {
        // Previous user already loaded the obc
        DEBUGDPP("{} finished waiting for loader, cache hit on {}",
                 dpp, FNAME, obc->get_oid());
        return get_obc(obc, existed);
      });
    }
  }

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<ObjectContextRef>
  ObjectContextLoader::_get_or_load_obc(ObjectContextRef obc,
                                        bool existed)
  {
    LOG_PREFIX(ObjectContextLoader::_get_or_load_obc);
    if (existed) {
      DEBUGDPP("cache hit on {}", dpp, obc->get_oid());
      return get_obc(obc, existed);
    } else {
      DEBUGDPP("cache miss on {}", dpp, obc->get_oid());
      return obc->template with_promoted_lock<State, IOInterruptCondition>(
        [obc, this] {
        return load_obc(obc);
      });
    }
  }

  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::reload_obc(ObjectContext& obc) const
  {
    LOG_PREFIX(ObjectContextLoader::reload_obc);
    assert(obc.is_head());
    return backend.load_metadata(obc.get_oid())
    .safe_then_interruptible<false>(
      [FNAME, this, &obc](auto md)-> load_obc_ertr::future<> {
      DEBUGDPP("reloaded obs {} for {}", dpp, md->os.oi, obc.get_oid());
      if (!md->ssc) {
	ERRORDPP("oid {} missing snapsetcontext", dpp, obc.get_oid());
        return crimson::ct_error::object_corrupted::make();
      }
      obc.set_head_state(std::move(md->os), std::move(md->ssc));
      return load_obc_ertr::now();
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

  // explicitly instantiate the used instantiations
  template ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_obc<RWState::RWNONE>(hobject_t,
                                                 with_obc_func_t&&,
                                                 bool resolve_clone);

  template ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_obc<RWState::RWREAD>(hobject_t,
                                                 with_obc_func_t&&,
                                                 bool resolve_clone);

  template ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_obc<RWState::RWWRITE>(hobject_t,
                                                  with_obc_func_t&&,
                                                 bool resolve_clone);

  template ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_obc<RWState::RWEXCL>(hobject_t,
                                                 with_obc_func_t&&,
                                                 bool resolve_clone);
}
