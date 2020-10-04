// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <exception>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>

#include "crimson/common/log.h"
#include "crimson/common/interruptible_future.h"

namespace crimson::common {

class interruption : public std::exception
{};

class system_shutdown_exception final : public interruption{
public:
  const char* what() const noexcept final {
    return "system shutting down";
  }
};

class actingset_changed final : public interruption {
public:
  actingset_changed(bool sp) : still_primary(sp) {}
  const char* what() const noexcept final {
    return "acting set changed";
  }
  bool is_primary() const {
    return still_primary;
  }
private:
  const bool still_primary;
};

template <typename InterruptCond, bool may_loop,
	  typename... InterruptExceptions,
	  typename OpFunc, typename OnInterrupt>
inline seastar::future<> with_interruption(
  OpFunc&& opfunc, OnInterrupt&& efunc) {
  if constexpr (may_loop) {
    return ::seastar::repeat(
      [opfunc=std::move(opfunc), efunc=std::move(efunc)]() mutable {
      return ::crimson::interruptible::interruptor<InterruptCond>::template futurize<
	std::result_of_t<OpFunc()>>::apply(std::move(opfunc), std::make_tuple())
	.template handle_interruption<InterruptExceptions...>(std::move(efunc));
    });
  } else {
      return ::crimson::interruptible::interruptor<InterruptCond>::template futurize<
	std::result_of_t<OpFunc()>>::apply(std::move(opfunc), std::make_tuple())
	.template handle_interruption<InterruptExceptions...>(std::move(efunc));
  }
}

template<typename Func, typename... Args>
inline seastar::future<> handle_system_shutdown(Func&& func, Args&&... args)
{
  return seastar::futurize_invoke(std::forward<Func>(func),
				  std::forward<Args>(args)...)
  .handle_exception([](std::exception_ptr eptr) {
    if (*eptr.__cxa_exception_type() ==
	typeid(crimson::common::system_shutdown_exception)) {
	crimson::get_logger(ceph_subsys_osd).debug(
	    "operation skipped, system shutdown");
	return seastar::now();
    }
    std::rethrow_exception(eptr);
  });
}

}
