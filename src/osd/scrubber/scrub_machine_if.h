// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
/**
 * \file the FSM API used by the scrubber
 */
#include <boost/statechart/event.hpp>
#include <boost/statechart/event_base.hpp>
#include <boost/statechart/simple_state.hpp>
#include <boost/statechart/state.hpp>
#include <boost/statechart/state_machine.hpp>

#include "osd/scrubber_common.h"

namespace Scrub {

namespace sc = ::boost::statechart;


/// the FSM API used by the scrubber
class ScrubFsmIf {
 public:
  virtual ~ScrubFsmIf() = default;

  virtual void process_event(const sc::event_base& evt) = 0;

  /// 'true' if the FSM state is PrimaryActive/PrimaryIdle
  [[nodiscard]] virtual bool is_primary_idle() const = 0;

  /// 'true' if the FSM state is PrimaryActive/Session/ReservingReplicas
  [[nodiscard]] virtual bool is_reserving() const = 0;

  /// 'true' if the FSM state is PrimaryActive/Session/Act/WaitLastUpdate
  [[nodiscard]] virtual bool is_accepting_updates() const = 0;

  /// verify state is not any substate of PrimaryActive/Session
  virtual void assert_not_in_session() const = 0;

  /**
   * time passed since entering the current scrubbing session.
   * Specifically - since the Session ctor has completed.
   */
  virtual ceph::timespan get_time_scrubbing() const = 0;

  /**
   * if we are in the ReservingReplicas state - fetch the reservation status.
   * The returned data names the last request sent to the replicas, and
   * how many replicas responded / are yet expected to respond.
   */
  virtual std::optional<pg_scrubbing_status_t> get_reservation_status()
      const = 0;

  /// "initiate" the state machine (an internal state_chart function)
  virtual void initiate() = 0;
};

}  // namespace Scrub

