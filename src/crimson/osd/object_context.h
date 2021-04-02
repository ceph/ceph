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

template <typename OBC>
struct obc_to_hoid {
  using type = hobject_t;
  const type &operator()(const OBC &obc) {
    return obc.obs.oi.soid;
  }
};

class ObjectContext : public ceph::common::intrusive_lru_base<
  ceph::common::intrusive_lru_config<
    hobject_t, ObjectContext, obc_to_hoid<ObjectContext>>>
{
public:
  Ref head; // Ref defined as part of ceph::common::intrusive_lru_base
  ObjectState obs;
  std::optional<SnapSet> ss;
  bool loaded : 1;
  // the watch / notify machinery rather stays away from the hot and
  // frequented paths. std::map is used mostly because of developer's
  // convenience.
  using watch_key_t = std::pair<uint64_t, entity_name_t>;
  std::map<watch_key_t, seastar::shared_ptr<crimson::osd::Watch>> watchers;

  ObjectContext(const hobject_t &hoid) : obs(hoid), loaded(false) {}

  const hobject_t &get_oid() const {
    return obs.oi.soid;
  }

  bool is_head() const {
    return get_oid().is_head();
  }

  const SnapSet &get_ro_ss() const {
    if (is_head()) {
      ceph_assert(ss);
      return *ss;
    } else {
      ceph_assert(head);
      return head->get_ro_ss();
    }
  }

  void set_head_state(ObjectState &&_obs, SnapSet &&_ss) {
    ceph_assert(is_head());
    obs = std::move(_obs);
    ss = std::move(_ss);
    loaded = true;
  }

  void set_clone_state(ObjectState &&_obs, Ref &&_head) {
    ceph_assert(!is_head());
    obs = std::move(_obs);
    head = _head;
    loaded = true;
  }

  /// pass the provided exception to any waiting consumers of this ObjectContext
  template<typename Exception>
  void interrupt(Exception ex) {
    lock.abort(std::move(ex));
    if (recovery_read_marker) {
      drop_recovery_read();
    }
  }

private:
  tri_mutex lock;
  bool recovery_read_marker = false;

  template <typename Lock, typename Func>
  auto _with_lock(Lock&& lock, Func&& func) {
    Ref obc = this;
    return lock.lock().then([&lock, func = std::forward<Func>(func), obc]() mutable {
      return seastar::futurize_invoke(func).finally([&lock, obc] {
	lock.unlock();
      });
    });
  }

  boost::intrusive::list_member_hook<> list_hook;
  uint64_t list_link_cnt = 0;

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
    &ObjectContext::list_hook>;

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
	return _with_lock(lock.excl_from_write(), std::move(wrapper));
      case RWState::RWREAD:
	return _with_lock(lock.excl_from_read(), std::move(wrapper));
      case RWState::RWEXCL:
	return _with_lock(lock.excl_from_excl(), std::move(wrapper));
      case RWState::RWNONE:
	return _with_lock(lock.for_excl(), std::move(wrapper));
       default:
	assert(0 == "noop");
      }
    } else {
      switch (Type) {
      case RWState::RWWRITE:
	return _with_lock(lock.excl_from_write(), std::forward<Func>(func));
      case RWState::RWREAD:
	return _with_lock(lock.excl_from_read(), std::forward<Func>(func));
      case RWState::RWEXCL:
	return _with_lock(lock.excl_from_excl(), std::forward<Func>(func));
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

  std::pair<ObjectContextRef, bool> get_cached_obc(const hobject_t &hoid) {
    return obc_lru.get_or_create(hoid);
  }
  ObjectContextRef maybe_get_cached_obc(const hobject_t &hoid) {
    return obc_lru.get(hoid);
  }

  const char** get_tracked_conf_keys() const final;
  void handle_conf_change(const crimson::common::ConfigProxy& conf,
                          const std::set <std::string> &changed) final;
};

} // namespace crimson::osd
