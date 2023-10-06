#include "crimson/osd/object_context_loader.h"
#include "osd/osd_types_fmt.h"

SET_SUBSYS(osd);

namespace crimson::osd {

using crimson::common::local_conf;

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_head_obc(ObjectContextRef obc,
                                     bool existed,
                                     with_obc_func_t&& func)
  {
    LOG_PREFIX(ObjectContextLoader::with_head_obc);
    DEBUGDPP("object {}", dpp, obc->get_oid());
    assert(obc->is_head());
    obc->append_to(obc_set_accessing);
    return obc->with_lock<State, IOInterruptCondition>(
      [existed=existed, obc=obc, func=std::move(func), this] {
      return get_or_load_obc<State>(obc, existed)
      .safe_then_interruptible(
        [func = std::move(func)](auto obc) {
        return std::move(func)(obc, obc);
      });
    }).finally([FNAME, this, obc=std::move(obc)] {
      DEBUGDPP("released object {}", dpp, obc->get_oid());
      obc->remove_from(obc_set_accessing);
    });
  }

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_clone_obc(hobject_t oid,
                                      with_obc_func_t&& func)
  {
    LOG_PREFIX(ObjectContextLoader::with_clone_obc);
    assert(!oid.is_head());
    return with_obc<RWState::RWREAD>(
      oid.get_head(),
      [FNAME, oid, func=std::move(func), this](auto head, auto) mutable
      -> load_obc_iertr::future<> {
      if (!head->obs.exists) {
	ERRORDPP("head doesn't exist for object {}", dpp, head->obs.oi.soid);
        return load_obc_iertr::future<>{
          crimson::ct_error::enoent::make()
        };
      }
      return this->with_clone_obc_only<State>(std::move(head),
                                              oid,
                                              std::move(func));
    });
  }

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_clone_obc_only(ObjectContextRef head,
                                           hobject_t oid,
                                           with_obc_func_t&& func)
  {
    LOG_PREFIX(ObjectContextLoader::with_clone_obc_only);
    auto coid = resolve_oid(head->get_head_ss(), oid);
    if (!coid) {
      ERRORDPP("clone {} not found", dpp, oid);
      return load_obc_iertr::future<>{
        crimson::ct_error::enoent::make()
      };
    }
    auto [clone, existed] = obc_registry.get_cached_obc(*coid);
    return clone->template with_lock<State, IOInterruptCondition>(
      [existed=existed, clone=std::move(clone),
       func=std::move(func), head=std::move(head), this]() mutable
      -> load_obc_iertr::future<> {
      auto loaded = get_or_load_obc<State>(clone, existed);
      return loaded.safe_then_interruptible(
        [func = std::move(func), head=std::move(head)](auto clone) mutable {
        return std::move(func)(std::move(head), std::move(clone));
      });
    });
  }

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_clone_obc_direct(
    hobject_t oid,
    with_obc_func_t&& func)
  {
    LOG_PREFIX(ObjectContextLoader::with_clone_obc_direct);
    assert(!oid.is_head());
    return with_obc<RWState::RWREAD>(
      oid.get_head(),
      [FNAME, oid, func=std::move(func), this](auto head, auto) mutable
      -> load_obc_iertr::future<> {
      if (!head->obs.exists) {
        ERRORDPP("head doesn't exist for object {}", dpp, head->obs.oi.soid);
        return load_obc_iertr::future<>{
          crimson::ct_error::enoent::make()
        };
      }
#ifndef NDEBUG
      auto &ss = head->get_head_ss();
      auto cit = std::find(
	std::begin(ss.clones), std::end(ss.clones), oid.snap);
      assert(cit != std::end(ss.clones));
#endif
      auto [clone, existed] = obc_registry.get_cached_obc(oid);
      return clone->template with_lock<State, IOInterruptCondition>(
        [existed=existed, clone=std::move(clone),
         func=std::move(func), head=std::move(head), this]()
        -> load_obc_iertr::future<> {
        auto loaded = get_or_load_obc<State>(clone, existed);
        return loaded.safe_then_interruptible(
          [func = std::move(func), head=std::move(head)](auto clone) {
          return std::move(func)(std::move(head), std::move(clone));
        });
      });
    });
  }

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_obc(hobject_t oid,
                                with_obc_func_t&& func)
  {
    if (oid.is_head()) {
      auto [obc, existed] =
        obc_registry.get_cached_obc(std::move(oid));
      return with_head_obc<State>(std::move(obc),
                                  existed,
                                  std::move(func));
    } else {
      return with_clone_obc<State>(oid, std::move(func));
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
        obc->set_clone_state(std::move(md->os));
      }
      obc->attr_cache = std::move(md->attr_cache);
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
    auto loaded =
      load_obc_iertr::make_ready_future<ObjectContextRef>(obc);
    if (existed) {
      if (!obc->is_loaded_and_valid()) {
	ERRORDPP(
	  "obc for {} invalid -- fully_loaded={}, "
	  "invalidated_by_interval_change={}",
	  dpp, obc->get_oid(),
	  obc->fully_loaded, obc->invalidated_by_interval_change
	);
      }
      ceph_assert(obc->is_loaded_and_valid());
      DEBUGDPP("cache hit on {}", dpp, obc->get_oid());
    } else {
      DEBUGDPP("cache miss on {}", dpp, obc->get_oid());
      loaded =
        obc->template with_promoted_lock<State, IOInterruptCondition>(
        [obc, this] {
        return load_obc(obc);
      });
    }
    return loaded;
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
      obc.attr_cache = std::move(md->attr_cache);
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
                                                 with_obc_func_t&&);

  template ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_obc<RWState::RWREAD>(hobject_t,
                                                 with_obc_func_t&&);

  template ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_obc<RWState::RWWRITE>(hobject_t,
                                                  with_obc_func_t&&);

  template ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_obc<RWState::RWEXCL>(hobject_t,
                                                 with_obc_func_t&&);

  template ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_clone_obc_direct<RWState::RWWRITE>(
    hobject_t,
    with_obc_func_t&&);
}
