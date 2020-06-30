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

private:
  RWState rwstate;
  seastar::shared_mutex wait_queue;
  std::optional<seastar::shared_promise<>> wake;

  template <typename F>
  seastar::future<> with_queue(F &&f) {
    return wait_queue.lock().then([this, f=std::move(f)] {
      ceph_assert(!wake);
      return seastar::repeat([this, f=std::move(f)]() {
	if (f()) {
	  wait_queue.unlock();
	  return seastar::make_ready_future<seastar::stop_iteration>(
	    seastar::stop_iteration::yes);
	} else {
	  rwstate.inc_waiters();
	  wake = seastar::shared_promise<>();
	  return wake->get_shared_future().then([this, f=std::move(f)] {
	    wake = std::nullopt;
	    rwstate.dec_waiters(1);
	    return seastar::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::no);
	  });
	}
      });
    });
  }


  const char *get_type_name() const final {
    return "ObjectContext";
  }
  void dump_detail(Formatter *f) const final;

  template <typename LockF>
  seastar::future<> get_lock(
    Operation *op,
    LockF &&lockf) {
    return op->with_blocking_future(
      make_blocking_future(with_queue(std::forward<LockF>(lockf))));
  }

  template <typename UnlockF>
  void put_lock(
    UnlockF &&unlockf) {
    if (unlockf() && wake) wake->set_value();
  }
public:
  seastar::future<> get_lock_type(Operation *op, RWState::State type) {
    switch (type) {
    case RWState::RWWRITE:
      return get_lock(op, [this] { return rwstate.get_write_lock(); });
    case RWState::RWREAD:
      return get_lock(op, [this] { return rwstate.get_read_lock(); });
    case RWState::RWEXCL:
      return get_lock(op, [this] { return rwstate.get_excl_lock(); });
    case RWState::RWNONE:
      return seastar::now();
    default:
      ceph_abort_msg("invalid lock type");
      return seastar::now();
    }
  }

  void put_lock_type(RWState::State type) {
    switch (type) {
    case RWState::RWWRITE:
      return put_lock([this] { return rwstate.put_write(); });
    case RWState::RWREAD:
      return put_lock([this] { return rwstate.put_read(); });
    case RWState::RWEXCL:
      return put_lock([this] { return rwstate.put_excl(); });
    case RWState::RWNONE:
      return;
    default:
      ceph_abort_msg("invalid lock type");
      return;
    }
  }

  void degrade_excl_to(RWState::State type) {
    // assume we already hold an excl lock
    bool put = rwstate.put_excl();
    bool success = false;
    switch (type) {
    case RWState::RWWRITE:
      success = rwstate.get_write_lock();
      break;
    case RWState::RWREAD:
      success = rwstate.get_read_lock();
      break;
    case RWState::RWEXCL:
      success = rwstate.get_excl_lock();
      break;
    case RWState::RWNONE:
      success = true;
      break;
    default:
      ceph_abort_msg("invalid lock type");
      break;
    }
    ceph_assert(success);
    if (put && wake) {
      wake->set_value();
    }
  }

  bool empty() const { return rwstate.empty(); }

  template <typename F>
  seastar::future<> get_write_greedy(Operation *op) {
    return get_lock(op, [this] { return rwstate.get_write_lock(true); });
  }

  bool try_get_read_lock() {
    return rwstate.get_read_lock();
  }
  void drop_read() {
    return put_lock_type(RWState::RWREAD);
  }
  seastar::future<bool> get_recovery_read(bool can_wait = false) {
    if (!can_wait) {
      return seastar::make_ready_future<bool>(rwstate.get_recovery_read());
    }
    return with_queue([this] {
      return rwstate.get_recovery_read();
    }).then([] {return seastar::make_ready_future<bool>(true); });
  }
  void drop_recovery_read() {
    ceph_assert(rwstate.recovery_read_marker);
    drop_read();
    rwstate.recovery_read_marker = false;
  }
  bool maybe_get_excl() {
    return rwstate.get_excl_lock();
  }

  bool is_request_pending() const {
    return !rwstate.empty();
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

  const char** get_tracked_conf_keys() const final;
  void handle_conf_change(const crimson::common::ConfigProxy& conf,
                          const std::set <std::string> &changed) final;
};

} // namespace crimson::osd
