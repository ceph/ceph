// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <optional>
#include <utility>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>

#include "common/intrusive_lru.h"
#include "osd/object_state.h"
#include "crimson/common/exception.h"
#include "crimson/common/tri_mutex.h"
#include "crimson/osd/osd_operation.h"

namespace ceph {
  class Formatter;
}

namespace crimson::common {
  class ConfigProxy;
}

namespace crimson::osd {

class Watch;
struct SnapSetContext;
using SnapSetContextRef = boost::intrusive_ptr<SnapSetContext>;

template <typename OBC>
struct obc_to_hoid {
  using type = hobject_t;
  const type &operator()(const OBC &obc) {
    return obc.obs.oi.soid;
  }
};

struct SnapSetContext :
  public boost::intrusive_ref_counter<SnapSetContext,
                                     boost::thread_unsafe_counter>
{
  hobject_t oid;
  SnapSet snapset;
  bool exists = false;
  /**
   * exists
   *
   * Because ObjectContext's are cached, we need to be able to express the case
   * where the object to which a cached ObjectContext refers does not exist.
   * ObjectContext's for yet-to-be-created objects are initialized with exists=false.
   * The ObjectContext for a deleted object will have exists set to false until it falls
   * out of cache (or another write recreates the object).
   */
  explicit SnapSetContext(const hobject_t& o) :
    oid(o), exists(false) {}
};

class ObjectContext : public ceph::common::intrusive_lru_base<
  ceph::common::intrusive_lru_config<
    hobject_t, ObjectContext, obc_to_hoid<ObjectContext>>>
{
private:
  tri_mutex lock;
  bool recovery_read_marker = false;

public:
  ObjectState obs;
  SnapSetContextRef ssc;
  // the watch / notify machinery rather stays away from the hot and
  // frequented paths. std::map is used mostly because of developer's
  // convenience.
  using watch_key_t = std::pair<uint64_t, entity_name_t>;
  std::map<watch_key_t, seastar::shared_ptr<crimson::osd::Watch>> watchers;

  // obc loading is a concurrent phase. In case this obc is being loaded,
  // make other users of this obc to await for the loading to complete.
  seastar::shared_mutex loading_mutex;

  ObjectContext(hobject_t hoid) : lock(hoid.oid.name),
                                  obs(std::move(hoid)) {}

  const hobject_t &get_oid() const {
    return obs.oi.soid;
  }

  bool is_head() const {
    return get_oid().is_head();
  }

  hobject_t get_head_oid() const {
    return get_oid().get_head();
  }

  const SnapSet &get_head_ss() const {
    ceph_assert(is_head());
    ceph_assert(ssc);
    return ssc->snapset;
  }

  void set_head_state(ObjectState &&_obs, SnapSetContextRef &&_ssc) {
    ceph_assert(is_head());
    obs = std::move(_obs);
    ssc = std::move(_ssc);
    fully_loaded = true;
  }

  void set_clone_state(ObjectState &&_obs) {
    ceph_assert(!is_head());
    obs = std::move(_obs);
    fully_loaded = true;
  }

  void set_clone_ssc(SnapSetContextRef head_ssc) {
    ceph_assert(!is_head());
    ssc = head_ssc;
  }

  /// pass the provided exception to any waiting consumers of this ObjectContext
  template<typename Exception>
  void interrupt(Exception ex) {
    lock.abort(std::move(ex));
    if (recovery_read_marker) {
      drop_recovery_read();
    }
  }

  bool is_loaded() const {
    return fully_loaded;
  }

  bool is_valid() const {
    return !invalidated_by_interval_change;
  }

private:
  template <typename Lock, typename Func>
  auto _with_lock(Lock& lock, Func&& func) {
    Ref obc = this;
    auto maybe_fut = lock.lock();
    return seastar::futurize_invoke([
        maybe_fut=std::move(maybe_fut),
        func=std::forward<Func>(func)]() mutable {
      if (maybe_fut) {
        return std::move(*maybe_fut
        ).then([func=std::forward<Func>(func)]() mutable {
          return seastar::futurize_invoke(func);
        });
      } else {
        // atomically calling func upon locking
        return seastar::futurize_invoke(func);
      }
    }).finally([&lock, obc] {
      lock.unlock();
    });
  }

  template <typename Lock, typename Func>
  auto _with_promoted_lock(Lock& lock, Func&& func) {
    Ref obc = this;
    lock.lock();
    return seastar::futurize_invoke(func).finally([&lock, obc] {
      lock.unlock();
    });
  }

  boost::intrusive::list_member_hook<> obc_accessing_hook;
  uint64_t list_link_cnt = 0;
  bool fully_loaded = false;
  bool invalidated_by_interval_change = false;

  friend class ObjectContextRegistry;
  friend class ObjectContextLoader;
public:

  template <typename ListType>
  void append_to(ListType& list) {
    if (list_link_cnt++ == 0) {
      list.push_back(*this);
    }
  }

  template <typename ListType>
  void remove_from(ListType&& list) {
    assert(list_link_cnt > 0);
    if (--list_link_cnt == 0) {
      list.erase(std::decay_t<ListType>::s_iterator_to(*this));
    }
  }

  using obc_accessing_option_t = boost::intrusive::member_hook<
    ObjectContext,
    boost::intrusive::list_member_hook<>,
    &ObjectContext::obc_accessing_hook>;

  template<RWState::State Type, typename InterruptCond = void, typename Func>
  auto with_lock(Func&& func) {
    if constexpr (!std::is_void_v<InterruptCond>) {
      auto wrapper = ::crimson::interruptible::interruptor<InterruptCond>::wrap_function(std::forward<Func>(func));
      switch (Type) {
      case RWState::RWWRITE:
	return _with_lock(lock.for_write(), std::move(wrapper));
      case RWState::RWREAD:
	return _with_lock(lock.for_read(), std::move(wrapper));
      case RWState::RWEXCL:
	return _with_lock(lock.for_excl(), std::move(wrapper));
      case RWState::RWNONE:
	return seastar::futurize_invoke(std::move(wrapper));
      default:
	assert(0 == "noop");
      }
    } else {
      switch (Type) {
      case RWState::RWWRITE:
	return _with_lock(lock.for_write(), std::forward<Func>(func));
      case RWState::RWREAD:
	return _with_lock(lock.for_read(), std::forward<Func>(func));
      case RWState::RWEXCL:
	return _with_lock(lock.for_excl(), std::forward<Func>(func));
      case RWState::RWNONE:
	return seastar::futurize_invoke(std::forward<Func>(func));
      default:
	assert(0 == "noop");
      }
    }
  }
  template<RWState::State Type, typename InterruptCond = void, typename Func>
  auto with_promoted_lock(Func&& func) {
    if constexpr (!std::is_void_v<InterruptCond>) {
      auto wrapper = ::crimson::interruptible::interruptor<InterruptCond>::wrap_function(std::forward<Func>(func));
      switch (Type) {
      case RWState::RWWRITE:
	return _with_promoted_lock(lock.excl_from_write(), std::move(wrapper));
      case RWState::RWREAD:
	return _with_promoted_lock(lock.excl_from_read(), std::move(wrapper));
      case RWState::RWEXCL:
	return seastar::futurize_invoke(std::move(wrapper));
      case RWState::RWNONE:
	return _with_lock(lock.for_excl(), std::move(wrapper));
       default:
	assert(0 == "noop");
      }
    } else {
      switch (Type) {
      case RWState::RWWRITE:
	return _with_promoted_lock(lock.excl_from_write(), std::forward<Func>(func));
      case RWState::RWREAD:
	return _with_promoted_lock(lock.excl_from_read(), std::forward<Func>(func));
      case RWState::RWEXCL:
	return seastar::futurize_invoke(std::forward<Func>(func));
      case RWState::RWNONE:
	return _with_lock(lock.for_excl(), std::forward<Func>(func));
       default:
	assert(0 == "noop");
      }
    }
  }

  bool empty() const {
    return !lock.is_acquired();
  }
  bool is_request_pending() const {
    return lock.is_acquired();
  }

  bool get_recovery_read() {
    if (lock.try_lock_for_read()) {
      recovery_read_marker = true;
      return true;
    } else {
      return false;
    }
  }
  void wait_recovery_read() {
    assert(lock.get_readers() > 0);
    recovery_read_marker = true;
  }
  void drop_recovery_read() {
    assert(recovery_read_marker);
    recovery_read_marker = false;
  }
  bool maybe_get_excl() {
    return lock.try_lock_for_excl();
  }
};
using ObjectContextRef = ObjectContext::Ref;

class ObjectContextRegistry : public md_config_obs_t  {
  ObjectContext::lru_t obc_lru;

public:
  ObjectContextRegistry(crimson::common::ConfigProxy &conf);
  ~ObjectContextRegistry();

  std::pair<ObjectContextRef, bool> get_cached_obc(const hobject_t &hoid) {
    return obc_lru.get_or_create(hoid);
  }
  ObjectContextRef maybe_get_cached_obc(const hobject_t &hoid) {
    return obc_lru.get(hoid);
  }

  void clear_range(const hobject_t &from,
                   const hobject_t &to) {
    obc_lru.clear_range(from, to);
  }

  void invalidate_on_interval_change() {
    obc_lru.clear([](auto &obc) {
      obc.invalidated_by_interval_change = true;
    });
  }

  template <class F>
  void for_each(F&& f) {
    obc_lru.for_each(std::forward<F>(f));
  }

  const char** get_tracked_conf_keys() const final;
  void handle_conf_change(const crimson::common::ConfigProxy& conf,
                          const std::set <std::string> &changed) final;
};

std::optional<hobject_t> resolve_oid(const SnapSet &ss,
                                     const hobject_t &oid);

} // namespace crimson::osd
