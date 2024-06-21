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
    return with_locked_obc<State, true /* track */>(
      oid,
      [func=std::move(func)](auto obc) {
        // The template with_obc_func_t wrapper supports two obcs (head and clone).
        // In the 'with_head_obc' case, however, only the head is in use.
        // Pass the same head obc twice in order to
        // to support the generic with_obc sturcture.
	return std::invoke(std::move(func), obc, obc);
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
    DEBUGDPP("{}", dpp, clone_oid);
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
    return with_locked_obc<State, false /* don't track */>(
      clone_oid,
      [head=std::move(head), func=std::move(func)](auto clone) {
        clone->set_clone_ssc(head->ssc);
        return std::move(func)(std::move(head), std::move(clone));
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

  template<RWState::State State, bool track, typename Func>
  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_locked_obc(const hobject_t& oid,
		  Func&& func)
  {
    LOG_PREFIX(ObjectContextLoader::with_locked_obc);
    auto [obc, existed] = obc_registry.get_cached_obc(oid);
    DEBUGDPP("object {} existed {}",
             dpp, obc->get_oid(), existed);
    if constexpr (track) {
      obc->append_to(obc_set_accessing);
    }
    if (existed) {
      return obc->with_lock<State, IOInterruptCondition>(
	[func=std::move(func), obc=ObjectContextRef(obc)] {
	  return std::invoke(std::move(func), obc);
	}
      ).finally([FNAME, this, obc=ObjectContextRef(obc)] {
	DEBUGDPP("released object {}", dpp, obc->get_oid());
	if constexpr (track) {
	  obc->remove_from(obc_set_accessing);
	}
      });
    } else {
      return obc->load_then_with_lock<State> (
	[this, obc=ObjectContextRef(obc)] {
	  return load_obc(obc);
	},
	[func=std::move(func), obc=ObjectContextRef(obc)] {
	  return std::invoke(std::move(func), obc);
	}
      ).finally([FNAME, this, obc=ObjectContextRef(obc)] {
	DEBUGDPP("released object {}", dpp, obc->get_oid());
	if constexpr (track) {
	  obc->remove_from(obc_set_accessing);
	}
      });
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
