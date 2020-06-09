// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>

#include "crimson/common/exception.h"
#include "crimson/common/log.h"
#include "include/ceph_assert.h"

namespace crimson::common {

class Gated {
 public:
  static seastar::logger& gated_logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
  template <typename Func, typename T>
  inline void dispatch_in_background(const char* what, T& who, Func&& func) {
    (void) dispatch(what, who, func);
  }
  template <typename Func, typename T>
  inline seastar::future<> dispatch(const char* what, T& who, Func&& func) {
    return seastar::with_gate(pending_dispatch, std::forward<Func>(func)
    ).handle_exception([this, what, &who] (std::exception_ptr eptr) {
      if (*eptr.__cxa_exception_type() == typeid(system_shutdown_exception)) {
	gated_logger().debug(
	    "{}, {} skipped, system shutdown", who, what);
	return seastar::now();
      }
      gated_logger().error(
          "{} dispatch() {} caught exception: {}", who, what, eptr);
      ceph_abort("unexpected exception from dispatch()");
    });
  }

  seastar::future<> close() {
    return pending_dispatch.close();
  }
  bool is_closed() const {
    return pending_dispatch.is_closed();
  }
 private:
  seastar::gate pending_dispatch;
};

}// namespace crimson::common
