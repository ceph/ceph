#pragma once

#include <seastar/core/future.hh>
#include <seastar/util/defer.hh>
#include "crimson/common/coroutine.h"
#include "crimson/common/errorator.h"
#include "crimson/common/log.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/pg_backend.h"
#include "osd/object_state_fmt.h"

namespace crimson::osd {
class ObjectContextLoader {
public:
  using obc_accessing_list_t = boost::intrusive::list<
    ObjectContext,
    ObjectContext::obc_accessing_option_t>;

  ObjectContextLoader(
    ObjectContextRegistry& _obc_services,
    PGBackend& _backend,
    DoutPrefixProvider& dpp)
    : obc_registry{_obc_services},
      backend{_backend},
      dpp{dpp}
    {}

  using load_obc_ertr = crimson::errorator<
    crimson::ct_error::enoent,
    crimson::ct_error::object_corrupted>;
  using load_obc_iertr =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      load_obc_ertr>;

  class Manager {
    ObjectContextLoader &loader;
    hobject_t target;

    Manager() = delete;
    template <typename T>
    Manager(ObjectContextLoader &loader, T &&t)
      : loader(loader), target(std::forward<T>(t)) {}
    Manager(const Manager &) = delete;
    Manager &operator=(const Manager &o) = delete;

    struct options_t {
      bool resolve_clone = true;
    } options;

    struct state_t {
      RWState::State state = RWState::RWNONE;
      ObjectContextRef obc;
      bool is_empty() const { return !obc; }

      void lock_excl_sync() {
	bool locked = obc->lock.try_lock_for_excl();
	ceph_assert(locked);
	state = RWState::RWEXCL;
      }

      void demote_excl_to(RWState::State lock_type) {
	assert(state == RWState::RWEXCL);
	switch (lock_type) {
	case RWState::RWWRITE:
	  obc->lock.demote_to_write();
	  state = RWState::RWWRITE;
	  break;
	case RWState::RWREAD:
	  obc->lock.demote_to_read();
	  state = RWState::RWREAD;
	  break;
	case RWState::RWNONE:
	  obc->lock.unlock_for_excl();
	  state = RWState::RWNONE;
	  break;
	case RWState::RWEXCL:
	  //noop
	  break;
	default:
	  ceph_assert(0 == "impossible");
	}
      }

      auto lock_to(RWState::State lock_type) {
	assert(state == RWState::RWNONE);
	switch (lock_type) {
	case RWState::RWWRITE:
	  return interruptor::make_interruptible(
	    obc->lock.lock_for_write().then([this] {
	      state = RWState::RWWRITE;
	    }));
	case RWState::RWREAD:
	  return interruptor::make_interruptible(
	    obc->lock.lock_for_read().then([this] {
	      state = RWState::RWREAD;
	    }));
	case RWState::RWNONE:
	  // noop
	  return interruptor::now();
	case RWState::RWEXCL:
	  return interruptor::make_interruptible(
	    obc->lock.lock_for_excl().then([this] {
	      state = RWState::RWEXCL;
	    }));
	default:
	  ceph_assert(0 == "impossible");
	  return interruptor::now();
	}
      }

      void release_lock() {
	switch (state) {
	case RWState::RWREAD:
	  obc->lock.unlock_for_read();
	  break;
	case RWState::RWWRITE:
	  obc->lock.unlock_for_write();
	  break;
	case RWState::RWEXCL:
	  obc->lock.unlock_for_excl();
	  break;
	case RWState::RWNONE:
	  // noop
	  break;
	default:
	  ceph_assert(0 == "invalid");
	}
	state = RWState::RWNONE;
      }
    };
    state_t head_state;
    state_t target_state;

    friend ObjectContextLoader;

    void set_state_obc(state_t &s, ObjectContextRef _obc) {
      ceph_assert(s.is_empty());
      s.obc = std::move(_obc);
      s.obc->append_to(loader.obc_set_accessing);
    }

    void release_state(state_t &s) {
      LOG_PREFIX(ObjectContextLoader::release_state);
      if (s.is_empty()) return;

      s.release_lock();
      SUBDEBUGDPP(osd, "releasing obc {}, {}", loader.dpp, *(s.obc), s.obc->obs);
      s.obc->remove_from(loader.obc_set_accessing);
      s = state_t();
    }
  public:
    Manager(Manager &&rhs) : loader(rhs.loader) {
      std::swap(target, rhs.target);
      std::swap(options, rhs.options);
      std::swap(head_state, rhs.head_state);
      std::swap(target_state, rhs.target_state);
    }

    Manager &operator=(Manager &&o) {
      this->~Manager();
      new(this) Manager(std::move(o));
      return *this;
    }

    void lock_excl_sync() {
      target_state.lock_excl_sync();
    }

    ObjectContextRef &get_obc() {
      ceph_assert(!target_state.is_empty());
      ceph_assert(target_state.obc->is_loaded());
      return target_state.obc;
    }

    ObjectContextRef &get_head_obc() {
      ceph_assert(!head_state.is_empty());
      ceph_assert(head_state.obc->is_loaded());
      return head_state.obc;
    }

    void release() {
      release_state(head_state);
      release_state(target_state);
    }

    auto get_releaser() {
      return seastar::defer([this] {
	release();
      });
    }

    ~Manager() {
      release();
    }
  };

  class Orderer {
    friend ObjectContextLoader;
    ObjectContextRef orderer_obc;
  public:
    CommonOBCPipeline &obc_pp() {
      ceph_assert(orderer_obc);
      return orderer_obc->obc_pipeline;
    }

    ~Orderer() {
      LOG_PREFIX(ObjectContextLoader::~Orderer);
      SUBDEBUG(osd, "releasing obc {}", *(orderer_obc));
    }
  };

  /**
   * create_cached_obc_from_push_data
   *
   * Creates a fresh cached obc from passed oi and ssc.
   * Overwrites any obc already in cache for this object.
   *
   * Note, this interface may be used to create a clone obc
   * with a null ssc.  The capability is useful when handling
   * a clone push on a replica -- we don't necessarily have
   * a valid local copy of the head since the primary may not
   * push the head first.  That obc with a null ssc may
   * remain in the cache.  ObjectContextLoader::load_and_lock_clone
   * will fix the ssc member if null, but users of any other
   * access mechanism must be aware that ssc on a clone obc may be
   * null.
   */
  ObjectContextRef create_cached_obc_from_push_data(
    const object_info_t &oi,
    SnapSetContextRef ssc) {
    auto obc = obc_registry.get_cached_obc(oi.soid).first;
    if (oi.soid.is_head()) {
      ceph_assert(ssc); // head, ssc may not be null
      obc->set_head_state(ObjectState(oi, true), SnapSetContextRef(ssc));
    } else {
      // clone, ssc may be null
      obc->set_clone_state(ObjectState(oi, true));
      obc->set_clone_ssc(SnapSetContextRef(ssc));
    }
    return obc;
  }

  Orderer get_obc_orderer(const hobject_t &oid) {
    Orderer ret;
    std::tie(ret.orderer_obc, std::ignore) =
      obc_registry.get_cached_obc(oid.get_head());
    return ret;
  }

  Manager get_obc_manager(const hobject_t &oid, bool resolve_clone = true) {
    Manager ret(*this, oid);
    ret.options.resolve_clone = resolve_clone;
    return ret;
  }

  Manager get_obc_manager(ObjectContextRef obc) {
    Manager ret = get_obc_manager(obc->obs.oi.soid, false);
    ret.set_state_obc(ret.target_state, obc);
    return ret;
  }

  Manager get_obc_manager(
    Orderer &orderer, const hobject_t &oid, bool resolve_clone = true) {
    Manager ret = get_obc_manager(oid, resolve_clone);
    ret.set_state_obc(ret.head_state, orderer.orderer_obc);
    return ret;
  }

  using load_and_lock_ertr = load_obc_ertr;
  using load_and_lock_iertr = interruptible::interruptible_errorator<
    IOInterruptCondition, load_and_lock_ertr>;
  using load_and_lock_fut = load_and_lock_iertr::future<>;
private:
  load_and_lock_fut load_and_lock_head(Manager &, RWState::State);
  load_and_lock_fut load_and_lock_clone(Manager &, RWState::State, bool lock_head=true);
public:
  load_and_lock_fut load_and_lock(Manager &, RWState::State);

  using interruptor = ::crimson::interruptible::interruptor<
    ::crimson::osd::IOInterruptCondition>;

  using with_obc_func_t =
    std::function<load_obc_iertr::future<> (ObjectContextRef, ObjectContextRef)>;

  // Use this variant by default
  // If oid is a clone object, the clone obc *and* it's
  // matching head obc will be locked and can be used in func.
  // resolve_clone: For SnapTrim, it may be possible that it
  //                won't be possible to resolve the clone.
  // See SnapTrimObjSubEvent::remove_or_update - in_removed_snaps_queue usage.
  template<RWState::State State>
  load_obc_iertr::future<> with_obc(hobject_t oid,
                                    with_obc_func_t func,
                                    bool resolve_clone = true) {
    auto manager = get_obc_manager(oid, resolve_clone);
    co_await load_and_lock(manager, State);
    co_await std::invoke(
      func, manager.get_head_obc(), manager.get_obc());
  }

  // Use this variant in the case where the head object
  // obc is already locked and only the clone obc is needed.
  // Avoid nesting with_head_obc() calls by using with_clone_obc()
  // with an already locked head.
  template<RWState::State State>
  load_obc_iertr::future<> with_clone_obc_only(ObjectContextRef head,
                                               hobject_t clone_oid,
                                               with_obc_func_t func,
                                               bool resolve_clone = true) {
    LOG_PREFIX(ObjectContextLoader::with_clone_obc_only);
    SUBDEBUGDPP(osd, "{}", dpp, clone_oid);
    auto manager = get_obc_manager(clone_oid, resolve_clone);
    // We populate head_state here with the passed obc assuming that
    // it has been loaded and locked appropriately.  We do not populate
    // head_state.state because we won't be taking or releasing any
    // locks on head as part of this call.
    manager.head_state.obc = head;
    manager.head_state.obc->append_to(obc_set_accessing);
    co_await load_and_lock_clone(manager, State, false);
    co_await std::invoke(func, head, manager.get_obc());
  }

  void notify_on_change(bool is_primary);

private:
  ObjectContextRegistry& obc_registry;
  PGBackend& backend;
  DoutPrefixProvider& dpp;
  obc_accessing_list_t obc_set_accessing;

  load_obc_iertr::future<> load_obc(ObjectContextRef obc);
};

using ObjectContextManager = ObjectContextLoader::Manager;

}
