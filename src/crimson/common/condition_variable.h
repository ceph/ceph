// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/loop.hh>

#include "crimson/common/interruptible_future.h"

namespace crimson {

class condition_variable : public seastar::condition_variable {
public:
  template <typename Pred, typename Func>
  auto wait(
    Pred&& pred,
    Func&& action) noexcept {
    using func_result_t = std::invoke_result_t<Func>;
    using intr_errorator_t = typename func_result_t::interrupt_errorator_type;
    using intr_cond_t = typename func_result_t::interrupt_cond_type;
    using interruptor = crimson::interruptible::interruptor<intr_cond_t>;
    return interruptor::repeat(
      [this, pred=std::forward<Pred>(pred),
      action=std::forward<Func>(action)]()
      -> typename intr_errorator_t::template future<seastar::stop_iteration> {
      if (!pred()) {
	return seastar::condition_variable::wait().then([] {
	  return seastar::make_ready_future<
	    seastar::stop_iteration>(seastar::stop_iteration::no);
	});
      } else {
	return action().si_then([] {
	  return seastar::make_ready_future<
	    seastar::stop_iteration>(seastar::stop_iteration::yes);
	});
      }
    });
  }
};

} // namespace crimson
