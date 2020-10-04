// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "include/types.h"
#include "crimson/common/errorator.h"
#include "crimson/common/exception.h"
#include "crimson/common/type_helpers.h"

namespace crimson::osd {

class PG;

class IOInterruptCondition {
public:
  IOInterruptCondition(Ref<PG>& pg);

  epoch_t get_current_osdmap_epoch();

  bool is_stopping();

  bool is_primary();

  template <typename T>
  std::pair<bool, std::optional<T>> may_interrupt() {
    if (e != get_current_osdmap_epoch()) {
      return std::pair<bool, std::optional<T>>(
		true, seastar::futurize<T>::make_exception_future(
			  ::crimson::common::actingset_changed(is_primary())));
    }
    if (is_stopping()) {
      return {true, seastar::futurize<Fut>::make_exception_future(
        ::crimson::common::system_shutdown_exception())};
    }
    return {false, std::optional<Fut>()};
  }

  template <typename T>
  static constexpr bool is_interruption_v =
    std::is_same_v<T, ::crimson::common::actingset_changed>
    || std::is_same_v<T, ::crimson::common::system_shutdown_exception>;

  bool is_interruption(std::exception_ptr& eptr) {
    return (*eptr.__cxa_exception_type() ==
            typeid(::crimson::common::actingset_changed) ||
            *eptr.__cxa_exception_type() ==
            typeid(::crimson::common::system_shutdown_exception));
  }

private:
  Ref<PG> pg;
  epoch_t e;
};

} // namespace crimson::osd
