// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <optional>

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
  struct PeeringFacade;
  struct PGFacade;

  // events comes first
  struct PrimaryScanned : sc::event<PrimaryScanned> {
    BackfillInterval result;
    PrimaryScanned(BackfillInterval&& result)
      : result(std::move(result)) {
    }
  };

  struct ReplicaScanned : sc::event<ReplicaScanned> {
    pg_shard_t from;
    BackfillInterval result;
    ReplicaScanned(pg_shard_t from, BackfillInterval&& result)
      : from(std::move(from)),
        result(std::move(result)) {
    }
  };

  struct ObjectPushed : sc::event<ObjectPushed> {
    // TODO: implement replica management; I don't want to follow
    // current convention where the backend layer is responsible
    // for tracking replicas.
    hobject_t object;
    pg_stat_t stat;
    ObjectPushed(hobject_t object)
      : object(std::move(object)) {
    }
  };

  struct Triggered : sc::event<Triggered> {
  };

  struct RequestDone : sc::event<RequestDone> {
  };

  struct CancelBackfill : sc::event<CancelBackfill> {
  };

private:
  // internal events
  struct RequestPrimaryScanning : sc::event<RequestPrimaryScanning> {
  };

  struct RequestReplicasScanning : sc::event<RequestReplicasScanning> {
  };

  struct RequestWaiting : sc::event<RequestWaiting> {
  };

  class ProgressTracker;

public:

  struct Initial;
  struct Enqueuing;
  struct PrimaryScanning;
  struct ReplicasScanning;
  struct Waiting;
  struct Done;

  struct BackfillMachine : sc::state_machine<BackfillMachine, Initial> {
    BackfillMachine(BackfillState& backfill_state,
                    BackfillListener& backfill_listener,
                    std::unique_ptr<PeeringFacade> peering_state,
                    std::unique_ptr<PGFacade> pg);
    ~BackfillMachine();
    BackfillState& backfill_state;
    BackfillListener& backfill_listener;
    std::unique_ptr<PeeringFacade> peering_state;
    std::unique_ptr<PGFacade> pg;
  };

private:
  template <class S>
  struct StateHelper {
    StateHelper();
    ~StateHelper();

    BackfillState& backfill_state() {
      return static_cast<S*>(this) \
        ->template context<BackfillMachine>().backfill_state;
    }
    BackfillListener& backfill_listener() {
      return static_cast<S*>(this) \
        ->template context<BackfillMachine>().backfill_listener;
    }
    PeeringFacade& peering_state() {
      return *static_cast<S*>(this) \
        ->template context<BackfillMachine>().peering_state;
    }
    PGFacade& pg() {
      return *static_cast<S*>(this)->template context<BackfillMachine>().pg;
    }

    const PeeringFacade& peering_state() const {
      return *static_cast<const S*>(this) \
        ->template context<BackfillMachine>().peering_state;
    }
    const BackfillState& backfill_state() const {
      return static_cast<const S*>(this) \
        ->template context<BackfillMachine>().backfill_state;
    }
  };

public:

  // states
  struct Crashed : sc::simple_state<Crashed, BackfillMachine>,
                   StateHelper<Crashed> {
    explicit Crashed();
  };

  struct Cancelled : sc::state<Cancelled, BackfillMachine>,
                     StateHelper<Cancelled> {
    using reactions = boost::mpl::list<
      sc::custom_reaction<Triggered>,
      sc::custom_reaction<PrimaryScanned>,
      sc::custom_reaction<ReplicaScanned>,
      sc::custom_reaction<ObjectPushed>,
      sc::transition<sc::event_base, Crashed>>;
    explicit Cancelled(my_context);
    // resume after triggering backfill by on_activate_complete().
    // transit to Enqueuing.
    sc::result react(const Triggered&);
    sc::result react(const PrimaryScanned&) {
      return discard_event();
    }
    sc::result react(const ReplicaScanned&) {
      return discard_event();
    }
    sc::result react(const ObjectPushed&) {
      return discard_event();
    }
  };

  struct Initial : sc::state<Initial, BackfillMachine>,
                   StateHelper<Initial> {
    using reactions = boost::mpl::list<
      sc::custom_reaction<Triggered>,
      sc::transition<CancelBackfill, Cancelled>,
      sc::transition<sc::event_base, Crashed>>;
    explicit Initial(my_context);
    // initialize after triggering backfill by on_activate_complete().
    // transit to Enqueuing.
    sc::result react(const Triggered&);
  };

  struct Enqueuing : sc::state<Enqueuing, BackfillMachine>,
                     StateHelper<Enqueuing> {
    using reactions = boost::mpl::list<
      sc::transition<CancelBackfill, Cancelled>,
      sc::transition<RequestPrimaryScanning, PrimaryScanning>,
      sc::transition<RequestReplicasScanning, ReplicasScanning>,
      sc::transition<RequestWaiting, Waiting>,
      sc::transition<RequestDone, Done>,
      sc::transition<CancelBackfill, Cancelled>,
      sc::transition<sc::event_base, Crashed>>;
    explicit Enqueuing(my_context);

    // indicate whether there is any remaining work to do when it comes
    // to comparing the hobject_t namespace between primary and replicas.
    // true doesn't necessarily mean backfill is done -- there could be
    // in-flight pushes or drops which had been enqueued but aren't
    // completed yet.
    static bool all_enqueued(
      const PeeringFacade& peering_state,
      const BackfillInterval& backfill_info,
      const std::map<pg_shard_t, BackfillInterval>& peer_backfill_info);

  private:
    void maybe_update_range();
    void trim_backfill_infos();

    // these methods take BackfillIntervals instead of extracting them from
    // the state to emphasize the relationships across the main loop.
    bool all_emptied(
      const BackfillInterval& local_backfill_info,
      const std::map<pg_shard_t, BackfillInterval>& peer_backfill_info) const;
    hobject_t earliest_peer_backfill(
      const std::map<pg_shard_t, BackfillInterval>& peer_backfill_info) const;
    bool should_rescan_replicas(
      const std::map<pg_shard_t, BackfillInterval>& peer_backfill_info,
      const BackfillInterval& backfill_info) const;
    // indicate whether a particular acting primary needs to scanned again
    // to process next piece of the hobject_t's namespace.
    // the logic is per analogy to replica_needs_scan(). See comments there.
    bool should_rescan_primary(
      const std::map<pg_shard_t, BackfillInterval>& peer_backfill_info,
      const BackfillInterval& backfill_info) const;

    // the result_t is intermediary between {remove,update}_on_peers() and
    // updating BackfillIntervals in trim_backfilled_object_from_intervals.
    // This step is important because it affects the main loop's condition,
    // and thus deserves to be exposed instead of being called deeply from
    // {remove,update}_on_peers().
    struct [[nodiscard]] result_t {
      std::set<pg_shard_t> pbi_targets;
      hobject_t new_last_backfill_started;
    };
    void trim_backfilled_object_from_intervals(
      result_t&&,
      hobject_t& last_backfill_started,
      std::map<pg_shard_t, BackfillInterval>& peer_backfill_info);
    result_t remove_on_peers(const hobject_t& check);
    result_t update_on_peers(const hobject_t& check);
  };

  struct PrimaryScanning : sc::state<PrimaryScanning, BackfillMachine>,
                           StateHelper<PrimaryScanning> {
    using reactions = boost::mpl::list<
      sc::custom_reaction<ObjectPushed>,
      sc::custom_reaction<PrimaryScanned>,
      sc::transition<RequestDone, Done>,
      sc::transition<CancelBackfill, Cancelled>,
      sc::transition<sc::event_base, Crashed>>;
    explicit PrimaryScanning(my_context);
    sc::result react(ObjectPushed);
    // collect scanning result and transit to Enqueuing.
    sc::result react(PrimaryScanned);
  };

  struct ReplicasScanning : sc::state<ReplicasScanning, BackfillMachine>,
                            StateHelper<ReplicasScanning> {
    using reactions = boost::mpl::list<
      sc::custom_reaction<ObjectPushed>,
      sc::custom_reaction<ReplicaScanned>,
      sc::custom_reaction<CancelBackfill>,
      sc::transition<RequestDone, Done>,
      sc::transition<sc::event_base, Crashed>>;
    explicit ReplicasScanning(my_context);
    // collect scanning result; if all results are collected, transition
    // to Enqueuing will happen.
    sc::result react(ObjectPushed);
    sc::result react(ReplicaScanned);
    sc::result react(CancelBackfill);

    // indicate whether a particular peer should be scanned to retrieve
    // BackfillInterval for new range of hobject_t namespace.
    // true when bi.objects is exhausted, replica bi's end is not MAX,
    // and primary bi'begin is further than the replica's one.
    static bool replica_needs_scan(
      const BackfillInterval& replica_backfill_info,
      const BackfillInterval& local_backfill_info);

  private:
    std::set<pg_shard_t> waiting_on_backfill;
  };

  struct Waiting : sc::state<Waiting, BackfillMachine>,
                   StateHelper<Waiting> {
    using reactions = boost::mpl::list<
      sc::custom_reaction<ObjectPushed>,
      sc::transition<RequestDone, Done>,
      sc::transition<CancelBackfill, Cancelled>,
      sc::transition<sc::event_base, Crashed>>;
    explicit Waiting(my_context);
    sc::result react(ObjectPushed);
  };

  struct Done : sc::state<Done, BackfillMachine>,
                StateHelper<Done> {
    using reactions = boost::mpl::list<
      sc::transition<sc::event_base, Crashed>>;
    explicit Done(my_context);
  };

  BackfillState(BackfillListener& backfill_listener,
                std::unique_ptr<PeeringFacade> peering_state,
                std::unique_ptr<PGFacade> pg);
  ~BackfillState();

  void process_event(
    boost::intrusive_ptr<const sc::event_base> evt) {
    backfill_machine.process_event(*std::move(evt));
  }

  hobject_t get_last_backfill_started() const {
    return last_backfill_started;
  }

  void backfill_target_done() {
    ceph_assert(replicas_in_backfill > 0);
    replicas_in_backfill--;
    if (!replicas_in_backfill) {
      backfill_machine.process_event(RequestDone{});
    }
  }
private:
  hobject_t last_backfill_started;
  BackfillInterval backfill_info;
  std::map<pg_shard_t, BackfillInterval> peer_backfill_info;
  BackfillMachine backfill_machine;
  std::unique_ptr<ProgressTracker> progress_tracker;
  size_t replicas_in_backfill = 0;
};

// BackfillListener -- an interface used by the backfill FSM to request
// low-level services like issueing `MOSDPGPush` or `MOSDPGBackfillRemove`.
// The goals behind the interface are: 1) unittestability; 2) possibility
// to retrofit classical OSD with BackfillState. For the second reason we
// never use `seastar::future` -- instead responses to the requests are
// conveyed as events; see ObjectPushed as an example.
struct BackfillState::BackfillListener {
  virtual void request_replica_scan(
    const pg_shard_t& target,
    const hobject_t& begin,
    const hobject_t& end) = 0;

  virtual void request_primary_scan(
    const hobject_t& begin) = 0;

  virtual void enqueue_push(
    const hobject_t& obj,
    const eversion_t& v,
    const std::vector<pg_shard_t> &peers) = 0;

  virtual void enqueue_drop(
    const pg_shard_t& target,
    const hobject_t& obj,
    const eversion_t& v) = 0;

  virtual void maybe_flush() = 0;

  virtual void update_peers_last_backfill(
    const hobject_t& new_last_backfill) = 0;

  virtual bool budget_available() const = 0;

  virtual void backfilled() = 0;

  virtual ~BackfillListener() = default;
};

// PeeringFacade -- a facade (in the GoF-defined meaning) simplifying
// the interface of PeeringState. The motivation is to have an inventory
// of behaviour that must be provided by a unit test's mock.
struct BackfillState::PeeringFacade {
  virtual hobject_t earliest_backfill() const = 0;
  virtual const std::set<pg_shard_t>& get_backfill_targets() const = 0;
  virtual const hobject_t& get_peer_last_backfill(pg_shard_t peer) const = 0;
  virtual const eversion_t& get_last_update() const = 0;
  virtual const eversion_t& get_log_tail() const = 0;

  // the performance impact of `std::function` has not been considered yet.
  // If there is any proof (from e.g. profiling) about its significance, we
  // can switch back to the template variant.
  using scan_log_func_t = std::function<void(const pg_log_entry_t&)>;
  virtual void scan_log_after(eversion_t, scan_log_func_t) const = 0;

  virtual bool is_backfill_target(pg_shard_t peer) const = 0;
  virtual void update_complete_backfill_object_stats(const hobject_t &hoid,
                                             const pg_stat_t &stats) = 0;
  virtual bool is_backfilling() const = 0;
  virtual void prepare_backfill_for_missing(
    const hobject_t &soid,
    const eversion_t &v,
    const std::vector<pg_shard_t> &peers) = 0;
  virtual ~PeeringFacade() {}
};

// PGFacade -- a facade (in the GoF-defined meaning) simplifying the huge
// interface of crimson's PG class. The motivation is to have an inventory
// of behaviour that must be provided by a unit test's mock.
struct BackfillState::PGFacade {
  virtual const eversion_t& get_projected_last_update() const = 0;
  virtual ~PGFacade() {}
};

class BackfillState::ProgressTracker {
  // TODO: apply_stat,
  enum class op_stage_t {
    enqueued_push,
    enqueued_drop,
    completed_push,
  };

  struct registry_item_t {
    op_stage_t stage;
    std::optional<pg_stat_t> stats;
  };

  BackfillMachine& backfill_machine;
  std::map<hobject_t, registry_item_t> registry;

  BackfillState& backfill_state() {
    return backfill_machine.backfill_state;
  }
  PeeringFacade& peering_state() {
    return *backfill_machine.peering_state;
  }
  BackfillListener& backfill_listener() {
    return backfill_machine.backfill_listener;
  }

public:
  ProgressTracker(BackfillMachine& backfill_machine)
    : backfill_machine(backfill_machine) {
  }

  bool tracked_objects_completed() const;

  bool enqueue_push(const hobject_t&);
  void enqueue_drop(const hobject_t&);
  void complete_to(const hobject_t&, const pg_stat_t&, bool may_push_to_max);
};

} // namespace crimson::osd
