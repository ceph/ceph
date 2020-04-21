// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/statechart/custom_reaction.hpp>
#include <boost/statechart/event.hpp>
#include <boost/statechart/event_base.hpp>
#include <boost/statechart/simple_state.hpp>
#include <boost/statechart/state.hpp>
#include <boost/statechart/state_machine.hpp>
#include <boost/statechart/transition.hpp>

#include "osd/recovery_types.h"

namespace crimson::osd {

namespace sc = boost::statechart;

struct BackfillState {
  struct BackfillListener;

  // events comes first
  struct PrimaryScanned : sc::event<PrimaryScanned> {
    BackfillInterval result;
  };

  struct ReplicaScanned : sc::event<ReplicaScanned> {
    pg_shard_t from;
    BackfillInterval result;
  };

  struct Flushed : sc::event<Flushed> {
  };

  struct Triggered : sc::event<Triggered> {
  };

  struct Initial;
  struct Enqueuing;

  class BackfillMachine : public sc::state_machine<BackfillMachine, Initial> {
  };

  // states
  struct Crashed : sc::state<Crashed, BackfillMachine>, NamedState {
  };

  struct Initial : sc::state<Initial, BackfillMachine>, NamedState {
    using reactions = boost::mpl::list<
      sc::custom_reaction<Triggered>,
      sc::transition<sc::event_base, Crashed>>;
    // initialize after triggering backfill by on_activate_complete().
    // transit to Enqueuing.
    sc::result react(const Triggered&);
  };

  struct Enqueuing : sc::state<Enqueuing, BackfillMachine>, NamedState {
    using reactions = boost::mpl::list<
      sc::transition<sc::event_base, Crashed>>;
  };

  struct PrimaryScanning : sc::state<PrimaryScanning, BackfillMachine>,
                           NamedState {
    using reactions = boost::mpl::list<
      sc::custom_reaction<PrimaryScanned>,
      sc::transition<sc::event_base, Crashed>>;
    // collect scanning result and transit to Enqueuing.
    sc::result react(const PrimaryScanned&);
  };

  struct ReplicasScanning : sc::state<ReplicasScanning, BackfillMachine>,
                            NamedState {
    using reactions = boost::mpl::list<
      sc::custom_reaction<ReplicaScanned>,
      sc::transition<sc::event_base, Crashed>>;
    // collect scanning result; if all results are collected, transition
    // to Enqueuing will happen.
    sc::result react(const ReplicaScanned&);
  };

  struct Flushing : sc::simple_state<Flushing, BackfillMachine>,
                    NamedState {
    using reactions = boost::mpl::list<
      sc::transition<Flushed, Enqueuing>,
      sc::transition<sc::event_base, Crashed>>;
  };
};

struct BackfillState::BackfillListener {
  virtual void request_replica_scan(
    const pg_shard_t& target,
    const hobject_t& begin,
    const hobject_t& end) = 0;

  virtual void request_primary_scan(
    const hobject_t& begin) = 0;

  virtual void enqueue_push(
    const pg_shard_t& target,
    const hobject_t& obj,
    const eversion_t& v) = 0;

  virtual void enqueue_drop(
    const pg_shard_t& target,
    const hobject_t& obj,
    const eversion_t& v) = 0;

  virtual void update_peers_last_backfill(
    const hobject_t& new_last_backfill) = 0;

  virtual bool budget_available() const = 0;

  virtual void backfilled() = 0;

  virtual ~BackfillListener() = default;
};

} // namespace crimson::osd
