// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <type_traits>
#include <vector>

#include "crimson/common/exception.h"
#include "crimson/common/log.h"
#include "include/ceph_assert.h"

namespace crimson::common {

class Gated {
 public:
  Gated() : sid(seastar::this_shard_id()) {}
  Gated(const seastar::shard_id sid) : sid(sid) {}
  Gated(const Gated&) = delete;
  Gated& operator=(const Gated&) = delete;
  Gated(Gated&&) = default;
  Gated& operator=(Gated&&) = delete;
  virtual ~Gated() = default;
  
  static seastar::logger& gated_logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }

  template <typename Func, typename T>
  inline void dispatch_in_background(const char* what, T& who, Func&& func) {
    ceph_assert(seastar::this_shard_id() == sid);
    (void) dispatch(what, who, std::forward<Func>(func));
  }

  template <typename Func, typename T>
  inline seastar::future<> dispatch(const char* what, T& who, Func&& func) {
    ceph_assert(seastar::this_shard_id() == sid);
    return seastar::with_gate(pending_dispatch, std::forward<Func>(func)
    ).handle_exception([what, &who] (std::exception_ptr eptr) {
      if (*eptr.__cxa_exception_type() == typeid(system_shutdown_exception)) {
	gated_logger().debug(
	    "{}, {} skipped, system shutdown", who, what);
	return;
      }
      try {
	std::rethrow_exception(eptr);
      } catch (std::exception& e) {
	gated_logger().error(
          "{} dispatch() {} caught exception: {}", who, what, e.what());
      }
      assert(*eptr.__cxa_exception_type()
	== typeid(seastar::gate_closed_exception));
    });
  }

  template <typename Func>
  auto simple_dispatch(const char* what, Func&& func) {
    ceph_assert(seastar::this_shard_id() == sid);
    return seastar::with_gate(pending_dispatch, std::forward<Func>(func));
  }

  seastar::future<> close() {
    ceph_assert(seastar::this_shard_id() == sid);
    return pending_dispatch.close();
  }

  bool is_closed() const {
    return pending_dispatch.is_closed();
  }

  seastar::shard_id get_shard_id() const {
    return sid;
  }
 private:
  seastar::gate pending_dispatch;
  const seastar::shard_id sid;
};

// gate_per_shard is a class that provides a gate for each shard.
// It was introduced to provide a way to have gate for each shard
// in a seastar application since gates are not supposed to be shared
// across shards. ( https://tracker.ceph.com/issues/64332 )
class gate_per_shard {
 public:
  gate_per_shard() : gates(seastar::smp::count) {
    std::vector<seastar::future<>> futures;
    for (unsigned shard = 0; shard < seastar::smp::count; ++shard) {
      futures.push_back(seastar::smp::submit_to(shard, [this, shard] {
        gates[shard] = std::make_unique<Gated>();
      }));
    }
    seastar::when_all_succeed(futures.begin(), futures.end()).get();
  }
  //explicit gate_per_shard(size_t shard_count) : gates(shard_count) {}
  gate_per_shard(const gate_per_shard&) = delete;
  gate_per_shard& operator=(const gate_per_shard&) = delete;
  gate_per_shard(gate_per_shard&&) = default;
  gate_per_shard& operator=(gate_per_shard&&) = default;
  ~gate_per_shard() = default;

  template <typename Func, typename T>
  inline void dispatch_in_background(const char* what, T& who, Func&& func) {
    (void) dispatch(what, who, std::forward<Func>(func));
  }

  template <typename Func, typename T>
  inline auto dispatch(const char* what, T& who, Func&& func) {
    return gates[seastar::this_shard_id()]->dispatch(what, who, std::forward<Func>(func));
  }

  template <typename Func>
  auto simple_dispatch(const char* what, Func&& func) {
    return gates[seastar::this_shard_id()]->simple_dispatch(what, std::forward<Func>(func));
  }

  bool is_closed() const {
    return gates[seastar::this_shard_id()]->is_closed();
  }

  seastar::future<> close_all() {
    ceph_assert(gates.size() == seastar::smp::count);
    return seastar::parallel_for_each(gates.begin(), gates.end(), [] (std::unique_ptr<Gated>& gate_ptr) {
      return seastar::smp::submit_to(gate_ptr->get_shard_id(), [gate = gate_ptr.get()] {
        return gate->close();
      });
    });
  }

 private:
  std::vector<std::unique_ptr<Gated>> gates;
};

} // namespace crimson::common
