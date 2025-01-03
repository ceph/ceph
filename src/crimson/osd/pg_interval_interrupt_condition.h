// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "include/types.h"
#include "crimson/common/errorator.h"
#include "crimson/common/exception.h"
#include "crimson/common/type_helpers.h"

namespace crimson::osd {

class PG;

/**
 * IOInterruptCondition
 *
 * Encapsulates logic for determining whether a continuation chain
 * started at <epoch_started> should be halted for once of two reasons:
 * 1. PG instance is stopping (includes if OSD is shutting down)
 * 2. A map advance has caused an interval change since <epoch_started>
 *
 * <epoch_started> should be the epoch at which the operation was logically
 * started, which may or may not pg->get_osdmap_epoch() at the time at which
 * with_interruption is actually invoked.
 */
class IOInterruptCondition {
public:
  IOInterruptCondition(Ref<PG>& pg, epoch_t epoch_started);
  ~IOInterruptCondition();

  /**
   * new_interval_created()
   *
   * Returns true iff the pg has entered a new interval since <epoch_started>
   * (<epoch_started> < pg->get_interval_start_epoch())
   */
  bool new_interval_created();

  /// true iff pg->stopping
  bool is_stopping();

  /**
   * is_primary
   *
   * True iff the pg is still primary.  Used to populate
   * ::crimson::common::actingset_changed upon interval change
   * to indicate whether client IOs should be requeued.
   */
  bool is_primary();

  template <typename Fut>
  std::optional<Fut> may_interrupt() {
    if (new_interval_created()) {
      return seastar::futurize<Fut>::make_exception_future(
        ::crimson::common::actingset_changed(is_primary()));
    }
    if (is_stopping()) {
      return seastar::futurize<Fut>::make_exception_future(
        ::crimson::common::system_shutdown_exception());
    }
    return std::optional<Fut>();
  }

  template <typename T>
  static constexpr bool is_interruption_v =
    std::is_same_v<T, ::crimson::common::actingset_changed>
    || std::is_same_v<T, ::crimson::common::system_shutdown_exception>;

  static bool is_interruption(std::exception_ptr& eptr) {
    return (*eptr.__cxa_exception_type() ==
            typeid(::crimson::common::actingset_changed) ||
            *eptr.__cxa_exception_type() ==
            typeid(::crimson::common::system_shutdown_exception));
  }

private:
  Ref<PG> pg;
  epoch_t epoch_started;
};

} // namespace crimson::osd
