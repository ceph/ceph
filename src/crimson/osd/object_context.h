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

class ObjectContext : public Blocker,
		      public ceph::common::intrusive_lru_base<
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

  const char *get_type_name() const final {
    return "ObjectContext";
  }
  void dump_detail(Formatter *f) const final;

  template <typename LockF>
  seastar::future<> get_lock(
    Operation *op,
    LockF &&lockf) {
    return op->with_blocking_future(
      make_blocking_future(std::forward<LockF>(lockf)));
  }

public:
  seastar::future<> get_lock_type(Operation *op, RWState::State type) {
    switch (type) {
    case RWState::RWWRITE:
      return get_lock(op, lock.lock_for_write(false));
    case RWState::RWREAD:
      return get_lock(op, lock.lock_for_read());
    case RWState::RWEXCL:
      return get_lock(op, lock.lock_for_excl());
    case RWState::RWNONE:
      return seastar::make_ready_future<>();
    default:
      assert(0 == "invalid lock type");
    }
  }

  void put_lock_type(RWState::State type) {
    switch (type) {
    case RWState::RWWRITE:
      return lock.unlock_for_write();
    case RWState::RWREAD:
      return lock.unlock_for_read();
    case RWState::RWEXCL:
      return lock.unlock_for_excl();
    case RWState::RWNONE:
      return;
    default:
      assert(0 == "invalid lock type");
    }
  }

  void degrade_excl_to(RWState::State type) {
    // assume we already hold an excl lock
    lock.unlock_for_excl();
    bool success = false;
    switch (type) {
    case RWState::RWWRITE:
      success = lock.try_lock_for_write(false);
      break;
    case RWState::RWREAD:
      success = lock.try_lock_for_read();
      break;
    case RWState::RWEXCL:
      success = lock.try_lock_for_excl();
      break;
    case RWState::RWNONE:
      success = true;
      break;
    default:
      assert(0 == "invalid lock type");
      break;
    }
    ceph_assert(success);
  }

  bool empty() const {
    return !lock.is_acquired();
  }
  bool is_request_pending() const {
    return lock.is_acquired();
  }

  template <typename F>
  seastar::future<> get_write_greedy(Operation *op) {
    return get_lock(op, [this] {
      return lock.lock_for_write(true);
    });
  }

  bool get_recovery_read() {
    if (lock.try_lock_for_read()) {
      recovery_read_marker = true;
      return true;
    } else {
      return false;
    }
  }
  seastar::future<> wait_recovery_read() {
    return lock.lock_for_read().then([this] {
      recovery_read_marker = true;
    });
  }
  void drop_recovery_read() {
    assert(recovery_read_marker);
    lock.unlock_for_read();
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
