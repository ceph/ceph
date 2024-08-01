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
  Gated() = default;
  Gated(const Gated&) = delete;
  Gated& operator=(const Gated&) = delete;
  Gated(Gated&&) = default;
  Gated& operator=(Gated&&) = default;
  virtual ~Gated() = default;
  
  static seastar::logger& gated_logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }

  template <typename Func, typename T>
  inline void dispatch_in_background(const char* what, T& who, Func&& func) {
    (void) dispatch(what, who, std::forward<Func>(func));
  }

  template <typename Func, typename T>
  inline seastar::future<> dispatch(const char* what, T& who, Func&& func) {
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
  auto submit_with_gate(const char* what, Func&& func) {
    return seastar::with_gate(pending_dispatch, std::forward<Func>(func));
  }

  virtual seastar::future<> close() {
    return pending_dispatch.close();
  }
  bool is_closed() const {
    return pending_dispatch.is_closed();
  }
 private:
  seastar::gate pending_dispatch;
};

class gate_per_shard : public Gated {
 public:
  gate_per_shard() : gates(seastar::smp::count) {}
  explicit gate_per_shard(size_t shard_count) : gates(shard_count) {}
  gate_per_shard(const gate_per_shard&) = delete;
  gate_per_shard& operator=(const gate_per_shard&) = delete;
  gate_per_shard(gate_per_shard&&) = default;
  gate_per_shard& operator=(gate_per_shard&&) = default;
  ~gate_per_shard() override = default;

  template <typename Func, typename T>
  inline auto dispatch_in_background(const char* what, T& who, Func&& func) {
    return dispatch(what, who, std::forward<Func>(func));
  }

  template <typename Func, typename T>
  inline auto dispatch(const char* what, T& who, Func&& func) {
    return gates[seastar::this_shard_id()].dispatch(what, who, std::forward<Func>(func));
  }

  template <typename Func>
  auto submit_with_gate(const char* what, Func&& func) {
    return gates[seastar::this_shard_id()].submit_with_gate(what, std::forward<Func>(func));
  }

  bool is_closed() const {
    return gates[seastar::this_shard_id()].is_closed();
  }

  seastar::future<> close_all() {
    return seastar::parallel_for_each(gates, [](Gated& gated) {
      return gated.close();
    });
  }

 private:
  std::vector<Gated> gates;
};

} // namespace crimson::common
