// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "include/int_types.h"

#include <string>
#include <ranges>

#include <boost/optional.hpp>
#include <boost/statechart/custom_reaction.hpp>
#include <boost/statechart/deferral.hpp>
#include <boost/statechart/event.hpp>
#include <boost/statechart/event_base.hpp>
#include <boost/statechart/in_state_reaction.hpp>
#include <boost/statechart/simple_state.hpp>
#include <boost/statechart/state.hpp>
#include <boost/statechart/state_machine.hpp>
#include <boost/statechart/transition.hpp>

#include "common/fmt_common.h"
#include "common/hobject.h"
#include "crimson/common/log.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber_common.h"
#include "osd/scrubber/scrub_reservations.h"
#include "scrub_validator.h"
#include "scrub_metrics.h"

namespace crimson::osd {
  class PG;
}

namespace crimson::osd::scrub {

// Forward declaration to break circular dependency
enum class reservation_status_t {
  unreserved,
  requested_or_granted ///< i.e. must be released
};

class PGScrubber;

// Event tracking stubs (no-ops for now)
inline void on_event_creation(const char*) {}
inline void on_event_discard(const char*) {}

/* Development Notes
 *
 * Notes:
 * - We're leaving out all of the throttle waits.  We actually want to handle
 *   that using crimson's operation throttler machinery.
 *
 * TODOs:
 * - Leaving SnapMapper validation to later work
 *   - Note, each replica should validate and repair locally as the SnapMapper
 *     is meant to be a local index of the local object contents
 * - Leaving preemption for later
 * - Leaving scheduling for later, for now the only way to trigger a scrub
 *   is via the ceph tell <pgid> [deep_]scrub command
 */

namespace sc = boost::statechart;

template <typename T>
struct simple_event_t : sc::event<T> {
  template <typename FormatContext>
  auto fmt_print_ctx(FormatContext & ctx) const {
    return fmt::format_to(ctx.out(), "{}", T::event_name);
  }
};

template <typename T, fmt::formattable V>
struct value_event_t : sc::event<T> {
  const V value;

  template <typename... Args>
  value_event_t(Args&&... args) : value(std::forward<Args>(args)...) {}

  value_event_t(const value_event_t &) = default;
  value_event_t(value_event_t &&) = default;
  value_event_t &operator=(const value_event_t&) = default;
  value_event_t &operator=(value_event_t&&) = default;

  template <typename FormatContext>
  auto fmt_print_ctx(FormatContext & ctx) const {
    return fmt::format_to(ctx.out(), "{}", T::event_name);
  }
};


#define SIMPLE_EVENT(T) struct T : simple_event_t<T> {			\
    static constexpr const char * event_name = #T;			\
  };

#define VALUE_EVENT(T, V) struct T : value_event_t<T, V> {		\
    static constexpr const char * event_name = #T;			\
									\
    template <typename... Args>						\
    T(Args&&... args) : value_event_t(					\
      std::forward<Args>(args)...) {}					\
  };

 template <typename EV>
 struct OpCarryingEvent : sc::event<EV> {
  static constexpr const char* event_name = "<>";
  const Message &m;
  const pg_shard_t m_from;
  OpCarryingEvent(Message &_m, pg_shard_t from) : m{_m}, m_from{from}
  {
    on_event_creation(static_cast<EV*>(this)->event_name);
  }

  OpCarryingEvent(const OpCarryingEvent&) = default;
  OpCarryingEvent(OpCarryingEvent&&) = default;
  OpCarryingEvent& operator=(const OpCarryingEvent&) = default;
  OpCarryingEvent& operator=(OpCarryingEvent&&) = default;

  void print(std::ostream* out) const
  {
    *out << fmt::format("{} (from: {})", EV::event_name, m_from);
  }
  std::string fmt_print() const
  {
    return fmt::format("{} (from: {})", EV::event_name, m_from);
  }
  std::string_view print() const { return EV::event_name; }
  ~OpCarryingEvent() { on_event_discard(EV::event_name); }
};
#define OP_EV(T)                                                     \
  struct T : OpCarryingEvent<T> {                                    \
    static constexpr const char* event_name = #T;                    \
    template <typename... Args>                                      \
    T(Args&&... args) : OpCarryingEvent(std::forward<Args>(args)...) \
    {                                                                \
    }                                                                \
  }
/**
 * ScrubContext
 *
 * Interface to external PG/OSD/IO machinery.
 *
 * Methods which may take time return immediately and define an event which
 * will be asynchronously delivered to the state machine with the result.  This
 * is a bit clumsy to use, but should render this component highly testable.
 *
 * Events sent as a completion to a ScrubContext interface method are defined
 * within ScrubContext.  Other events are defined within ScrubMachine.
 */
struct ScrubContext {
  /// return ids to scrub
  virtual const std::set<pg_shard_t> &get_ids_to_scrub() const = 0;

  /// iterates over each pg_shard_t to scrub
  template <typename F>
  void foreach_id_to_scrub(F &&f) {
    for (const auto &id : get_ids_to_scrub()) {
      std::invoke(f, id);
    }
  }

  virtual void schedule_scrub_with_osd() = 0;
  virtual void update_scrub_job() = 0;
  virtual void rm_from_osd_scrubbing() = 0;
  virtual void clear_pgscrub_state() = 0;

  /// return struct defining chunk validation rules
  virtual chunk_validation_policy_t get_policy() const = 0;

  /// notifies implementation of scrub start
  virtual void notify_scrub_start(bool deep) = 0;

  /// requests range to scrub starting at start
  struct request_range_result_t {
    hobject_t start;
    hobject_t end;

    request_range_result_t(
      const hobject_t &start,
      const hobject_t &end) : start(start), end(end) {}

    auto fmt_print_ctx(auto &ctx) const -> decltype(ctx.out()) {
      return fmt::format_to(ctx.out(), "start: {}, end: {}", start, end);
    }
  };
  VALUE_EVENT(request_range_complete_t, request_range_result_t);
  virtual void request_range(
    const hobject_t &start) = 0;

  /// reserves range [start, end)
  VALUE_EVENT(reserve_range_complete_t, eversion_t);
  virtual void reserve_range(
    const hobject_t &start,
    const hobject_t &end) = 0;

  /// waits until implementation has committed up to version
  SIMPLE_EVENT(await_update_complete_t);
  virtual bool await_update(
    const eversion_t &version) = 0;

  /// cancel in progress or currently reserved range
  virtual void release_range() = 0;

  /// scans [begin, end) on target as of version
  struct scan_range_value_t {
    pg_shard_t from;
    ScrubMap map;

    template <typename Map>
    scan_range_value_t(
      pg_shard_t from,
      Map &&map) : from(from), map(std::forward<Map>(map)) {}

    auto to_pair() const { return std::make_pair(from, map); }
    auto fmt_print_ctx(auto &ctx) const -> decltype(ctx.out()) {
      return fmt::format_to(ctx.out(), "from: {}", from);
    }
  };
  VALUE_EVENT(scan_range_complete_t, scan_range_value_t);
  virtual void scan_range(
    pg_shard_t target,
    eversion_t version,
    bool deep,
    const hobject_t &start,
    const hobject_t &end) = 0;

  /// instructs implmentatino to scan [begin, end) and emit result to primary
  SIMPLE_EVENT(generate_and_submit_chunk_result_complete_t);
  virtual void generate_and_submit_chunk_result(
    const hobject_t &begin,
    const hobject_t &end,
    bool deep) = 0;

  /// notifies implementation of chunk scrub results
  virtual void emit_chunk_result(
    const request_range_result_t &range,
    chunk_result_t &&result) = 0;

  SIMPLE_EVENT(digest_updates_complete_t);

  /// notifies implementation of full scrub results
  virtual void emit_scrub_result(
    bool deep,
    object_stat_sum_t scrub_stats) = 0;

  /// get dpp instance for logging
  virtual DoutPrefixProvider &get_dpp() = 0;
};

struct Crash;
struct Inactive;

namespace events {
/// reset ScrubMachine
SIMPLE_EVENT(reset_t);

/// abort scrub and return to AwaitScrub (stays in PrimaryActive)
SIMPLE_EVENT(abort_t);

/// internal event to schedule next chunk after sleep
SIMPLE_EVENT(internal_sched_scrub_t);

/// start (deep) scrub
struct start_scrub_event_t {
  bool deep = false;

  start_scrub_event_t(bool deep) : deep(deep) {}

  auto fmt_print_ctx(auto &ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "deep: {}", deep);
  }
};
VALUE_EVENT(start_scrub_t, start_scrub_event_t);

/// notifies ScrubMachine about a write on oid resulting in delta_stats
struct op_stat_event_t {
  hobject_t oid;
  object_stat_sum_t delta_stats;

  op_stat_event_t(
    hobject_t oid,
    object_stat_sum_t delta_stats) : oid(oid), delta_stats(delta_stats) {}

  auto fmt_print_ctx(auto &ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "oid: {}", oid);
  }
};
VALUE_EVENT(op_stats_t, op_stat_event_t);

/// Prepares statemachine for primary events
SIMPLE_EVENT(primary_activate_t);

/// Prepares statemachine for replica events
SIMPLE_EVENT(replica_activate_t);

/// Instructs replica to (deep) scrub [start, end) as of version version
struct replica_scan_event_t {
  hobject_t start;
  hobject_t end;
  eversion_t version;
  bool deep = false;

  replica_scan_event_t() = default;

  replica_scan_event_t(
    hobject_t start,
    hobject_t end,
    eversion_t version,
    bool deep) : start(start), end(end), version(version), deep(deep) {}

  auto fmt_print_ctx(auto &ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(), "start: {}, end: {}, version: {}, deep: {}",
      start, end, version, deep);
  }
};
VALUE_EVENT(replica_scan_t, replica_scan_event_t);

/// Primary requests reservation from replica
OP_EV(replica_reserve_request_t);

/// Primary releases reservation from replica
OP_EV(replica_release_t);

/// Replica has granted reservation request
OP_EV(replica_grant_t);

/// Replica has rejected reservation request
OP_EV(replica_reject_t);

/// All replicas have granted reservation
SIMPLE_EVENT(remotes_reserved_t);

/// the async-reserver granted our reservation request
VALUE_EVENT(reserver_granted_t, AsyncScrubResData);
}
/**
 * ScrubMachine
 *
 * Manages orchestration of rados's distributed scrub process.
 *
 * There are two general ways in which ScrubMachine may need to release
 * resources:
 * - interval_change_t -- represents case where PG as a whole undergoes
 *   a distributed mapping change.  Distributed resources are released
 *   implicitly as remote PG instances receive the new map.  Local
 *   resources are still released by ScrubMachine via ScrubContext methods
 *   generally via state destructors
 * - otherwise, ScrubMachine is responsible for notifying remote PG
 *   instances via the appropriate ScrubContext methods again generally
 *   from state destructors.
 *
 * TODO: interval_change_t will be added with remote reservations.
 */
class ScrubMachine
  : public sc::state_machine<ScrubMachine, Inactive> {
public:
  static constexpr std::string_view full_name = "ScrubMachine";

  ScrubContext &context;
  PGScrubber* m_scrbr;
  spg_t m_pg_id;
  ScrubMachine(ScrubContext &context, PGScrubber* scrbr) : context(context), m_scrbr(scrbr) {}
};

/**
 * ScrubState
 *
 * Template defining machinery/state common to all scrub state machine
 * states.
 */
template <typename S, typename P, typename... T>
struct ScrubState : sc::state<S, P, T...> {
  using sc_base = sc::state<S, P, T...>;
  DoutPrefixProvider &dpp;

  /* machinery for populating a full_name member for each ScrubState with
   * ScrubMachine/.../ParentState/ChildState full_name */
  template <std::string_view const &PN, typename PI,
	    std::string_view const &CN, typename CI>
  struct concat;

  template <std::string_view const &PN, std::size_t... PI,
	    std::string_view const &CN, std::size_t... CI>
  struct concat<PN, std::index_sequence<PI...>, CN, std::index_sequence<CI...>> {
    static constexpr size_t value_size = PN.size() + CN.size() + 1;
    static constexpr const char value[value_size]{PN[PI]..., '/', CN[CI]...};
  };

  template <std::string_view const &PN, std::string_view const &CN>
  struct join {
    using conc = concat<
      PN, std::make_index_sequence<PN.size()>,
      CN, std::make_index_sequence<CN.size()>>;
    static constexpr std::string_view value{
      conc::value,
      conc::value_size
    };
  };

  /// Populated with ScrubMachine/.../Parent/Child for each state Child
  static constexpr std::string_view full_name =
    join<P::full_name, S::state_name>::value;

  template <typename C>
  explicit ScrubState(C ctx) : sc_base(ctx), dpp(get_scrub_context().get_dpp()) {
    LOG_PREFIX(ScrubState::ScrubState);
    SUBDEBUGDPP(osd, "entering state {}", dpp, full_name);
  }

  ~ScrubState() {
    LOG_PREFIX(ScrubState::~ScrubState);
    SUBDEBUGDPP(osd, "exiting state {}", dpp, full_name);
  }

  auto &get_scrub_context() {
    return sc_base::template context<ScrubMachine>().context;
  }
};

struct Crash : ScrubState<Crash, ScrubMachine> {
  static constexpr std::string_view state_name = "Crash";
  explicit Crash(my_context ctx) : ScrubState(ctx) {
    ceph_abort_msg("Crash state impossible");
  }

};

struct PrimaryActive;
struct ReplicaActive;
struct Inactive : ScrubState<Inactive, ScrubMachine> {
  static constexpr std::string_view state_name = "Inactive";
  explicit Inactive(my_context ctx) : ScrubState(ctx) {}

  using reactions = boost::mpl::list<
    sc::transition<events::primary_activate_t, PrimaryActive>,
    sc::transition<events::replica_activate_t, ReplicaActive>,
    sc::custom_reaction<events::reset_t>,
    sc::custom_reaction<events::start_scrub_t>,
    sc::custom_reaction<events::op_stats_t>,
    sc::transition< boost::statechart::event_base, Crash >
    >;

  sc::result react(const events::reset_t &) {
    return discard_event();
  }
  sc::result react(const events::start_scrub_t &) {
    return discard_event();
  }
  sc::result react(const events::op_stats_t &) {
    return discard_event();
  }
};

struct AwaitScrub;
struct PrimaryActive : ScrubState<PrimaryActive, ScrubMachine, AwaitScrub> {
  static constexpr std::string_view state_name = "PrimaryActive";
  explicit PrimaryActive(my_context ctx) : ScrubState(ctx), scrub_context(get_scrub_context()) {
    scrub_context.schedule_scrub_with_osd();
  }

  ~PrimaryActive() {
    LOG_PREFIX(PrimaryActive::~PrimaryActive);
    SUBDEBUGDPP(osd, "destructor called, cleanup_done={}", dpp, cleanup_done);
    // Ensure cleanup happens even if exit() wasn't called.
    // This can happen when the state machine is terminated without a proper
    // state transition (e.g., during PG shutdown). We must clean up resources
    // to prevent memory leaks.
    if (!cleanup_done) {
      do_cleanup();
    }
  }

  void exit() {
    LOG_PREFIX(PrimaryActive::exit);
    SUBDEBUGDPP(osd, "exit called, cleanup_done={}", dpp, cleanup_done);
    // Match classic OSD behavior: clear scrub state before removing from OSD queue
    // exit() is called during normal state transitions while context is valid
    if (!cleanup_done) {
      do_cleanup();
    }
  }

private:
  ScrubContext& scrub_context;
  bool cleanup_done = false;

  void do_cleanup() {
    LOG_PREFIX(PrimaryActive::do_cleanup);
    SUBDEBUGDPP(osd, "performing cleanup", dpp);
    scrub_context.clear_pgscrub_state();
    scrub_context.rm_from_osd_scrubbing();
    cleanup_done = true;
  }

public:
  bool local_reservation_held = false;
  std::set<pg_shard_t> remote_reservations_held;
  reservation_nonce_t last_request_sent_nonce{1};

  using reactions = boost::mpl::list<
    sc::transition<events::reset_t, Inactive>,
    sc::transition<events::abort_t, AwaitScrub>,
    sc::custom_reaction<events::op_stats_t>,
    sc::transition< boost::statechart::event_base, Crash >
    >;

  sc::result react(const events::op_stats_t &) {
    return discard_event();
  }
};

namespace internal_events {
VALUE_EVENT(set_deep_t, bool);
}

struct Scrubbing;
struct AwaitScrub : ScrubState<AwaitScrub, PrimaryActive> {
  static constexpr std::string_view state_name = "AwaitScrub";
  explicit AwaitScrub(my_context ctx) : ScrubState(ctx) {}

  using reactions = boost::mpl::list<
    sc::custom_reaction<events::start_scrub_t>,
    sc::custom_reaction<events::primary_activate_t>
    >;

  sc::result react(const events::start_scrub_t &event) {
    post_event(internal_events::set_deep_t{event.value.deep});
    return transit<Scrubbing>();
  }

  sc::result react(const events::primary_activate_t &) {
    // Already in PrimaryActive state, discard redundant activation event
    // This can happen when PG transitions to Active+Clean after recovery
    return discard_event();
  }
};

struct ReservingReplicas;
struct ChunkState;
struct Scrubbing : ScrubState<Scrubbing, PrimaryActive, ReservingReplicas> {
  static constexpr std::string_view state_name = "Scrubbing";
  explicit Scrubbing(my_context ctx);

  ~Scrubbing() {
    // Match classic OSD behavior: release reservations and clear state
    // See Session::~Session() in src/osd/scrubber/scrub_machine.cc
    if (m_reservations) {
      m_reservations.reset();
    }
    get_scrub_context().clear_pgscrub_state();
  }

  using reactions = boost::mpl::list<
    sc::custom_reaction<internal_events::set_deep_t>,
    sc::custom_reaction<events::op_stats_t>
    >;

  chunk_validation_policy_t policy;
  std::optional<ReplicaReservations> m_reservations{std::nullopt};

  /// hobjects < current have been scrubbed
  hobject_t current;

  /// true for deep scrub
  bool deep = false;

  /// stats for objects < current, maintained via events::op_stats_t
  object_stat_sum_t stats;

  /// timestamp when scrubbing started, for elapsed time calculation (using ScrubClock)
  ScrubTimePoint scrub_start_time;

  void advance_current(const hobject_t &next) {
    current = next;
  }

  // Access metrics for dumping - returns pointer from PGScrubber
  ScrubMetrics* get_metrics();

  sc::result react(const internal_events::set_deep_t &event) {
    deep = event.value;
    get_scrub_context().notify_scrub_start(deep);
    return discard_event();
  }

  void exit() {
    // Release replica reservations if they were acquired
    if (m_reservations) {
      m_reservations.reset();
    }
  }

  sc::result react(const events::op_stats_t &event) {
    if (event.value.oid < current) {
      stats.add(event.value.delta_stats);
    }
    return discard_event();
  }
};

struct ReservingReplicas : ScrubState<ReservingReplicas, Scrubbing> {
  static constexpr std::string_view state_name = "ReservingReplicas";
  explicit ReservingReplicas(my_context ctx);
  ~ReservingReplicas() = default;

  /// Track which replicas have granted reservations
  std::set<pg_shard_t> granted_replicas;

  /// Track total replicas we're waiting for
  unsigned waiting_on = 0;

  using reactions = boost::mpl::list<
    sc::custom_reaction<events::replica_grant_t>,
    sc::custom_reaction<events::replica_reject_t>,
    sc::custom_reaction<events::remotes_reserved_t>,
    sc::transition<events::abort_t, AwaitScrub>
    >;

  sc::result react(const events::replica_grant_t &);
  sc::result react(const events::replica_reject_t &);
  sc::result react(const events::remotes_reserved_t &);
};

struct PendingTimer;
struct GetRange;
struct ChunkState : ScrubState<ChunkState, Scrubbing, GetRange> {
  static constexpr std::string_view state_name = "ChunkState";
  explicit ChunkState(my_context ctx) : ScrubState(ctx) {}

  /// Current chunk includes objects in [range_start, range_end)
  boost::optional<ScrubContext::request_range_result_t> range;

  /// true once we have requested that the range be reserved
  bool range_reserved = false;

  /// version of last update for the reserved chunk
  eversion_t version;

  void exit() {
    if (range_reserved) {
      get_scrub_context().release_range();
    }
  }
};

/// State between chunks - sleeps for osd_scrub_sleep duration before next chunk
struct PendingTimer : ScrubState<PendingTimer, Scrubbing> {
  static constexpr std::string_view state_name = "PendingTimer";
  explicit PendingTimer(my_context ctx);

  using reactions = boost::mpl::list<
    sc::transition<events::internal_sched_scrub_t, ChunkState>
    >;
};

struct WaitUpdate;
struct GetRange : ScrubState<GetRange, ChunkState> {
  static constexpr std::string_view state_name = "GetRange";
  explicit GetRange(my_context ctx) : ScrubState(ctx) {
    get_scrub_context().request_range(context<Scrubbing>().current);
  }

  using reactions = boost::mpl::list<
    sc::custom_reaction<ScrubContext::request_range_complete_t>
    >;

  sc::result react(const ScrubContext::request_range_complete_t &event) {
    context<ChunkState>().range = event.value;
    return transit<WaitUpdate>();
  }
};

struct ScanRange;
struct WaitUpdate : ScrubState<WaitUpdate, ChunkState> {
  static constexpr std::string_view state_name = "WaitUpdate";
  explicit WaitUpdate(my_context ctx);

  using reactions = boost::mpl::list<
    sc::custom_reaction<ScrubContext::reserve_range_complete_t>
    >;

  sc::result react(const ScrubContext::reserve_range_complete_t &e);
};

struct ScanRange : ScrubState<ScanRange, ChunkState> {
  static constexpr std::string_view state_name = "ScanRange";
  explicit ScanRange(my_context ctx);

  scrub_map_set_t maps;
  unsigned waiting_on = 0;

  using reactions = boost::mpl::list<
    sc::custom_reaction<ScrubContext::scan_range_complete_t>
    >;

  sc::result react(const ScrubContext::scan_range_complete_t &);
};

struct WaitDigestUpdate : ScrubState<WaitDigestUpdate, ChunkState> {
  static constexpr std::string_view state_name = "WaitDigestUpdate";
  explicit WaitDigestUpdate(my_context ctx);

  using reactions = boost::mpl::list<
    sc::custom_reaction<ScrubContext::digest_updates_complete_t>
    >;

  sc::result react(const ScrubContext::digest_updates_complete_t &);
};

// -------- for replicas -----------------------------------------------------
struct ReplicaIdle;
struct ReplicaActive :
    ScrubState<ReplicaActive, ScrubMachine, ReplicaIdle> {
  static constexpr std::string_view state_name = "ReplicaActive";
  explicit ReplicaActive(my_context ctx) : ScrubState(ctx) {}
  ~ReplicaActive();

  void clear_remote_reservation(bool warn_if_no_reservation);
  using reactions = boost::mpl::list<
    sc::transition<events::reset_t, Inactive>,
    sc::custom_reaction<events::start_scrub_t>,
    sc::custom_reaction<events::op_stats_t>,
    sc::custom_reaction<events::reserver_granted_t>,
    sc::custom_reaction<events::replica_reserve_request_t>,
    sc::custom_reaction<events::replica_release_t>,
    sc::transition< boost::statechart::event_base, Crash >
    >;

  sc::result react(const events::start_scrub_t &) {
    return discard_event();
  }

  sc::result react(const events::op_stats_t &) {
    return discard_event();
  }
  sc::result react(const events::reserver_granted_t &);
  sc::result react(const events::replica_reserve_request_t &);
  sc::result react(const events::replica_release_t &);

  MOSDScrubReserve::reservation_nonce_t pending_reservation_nonce{0};
  private:
    bool reservation_granted{false};

    reservation_status_t m_reservation_status{reservation_status_t::unreserved};
    void handle_reservation_request(const events::replica_reserve_request_t& event);

    struct RtReservationCB : public Context {
    crimson::osd::PG &pg;
    AsyncScrubResData res_data;

    explicit RtReservationCB(crimson::osd::PG& pg, AsyncScrubResData request_details)
 : pg{pg}
 , res_data{request_details}
    {}

    void finish(int) override;
  };
};

struct ReplicaChunkState;
struct ReplicaIdle : ScrubState<ReplicaIdle, ReplicaActive> {
  static constexpr std::string_view state_name = "ReplicaIdle";
  explicit ReplicaIdle(my_context ctx) : ScrubState(ctx) {}

  using reactions = boost::mpl::list<
    sc::custom_reaction<events::replica_scan_t>
    >;

  sc::result react(const events::replica_scan_t &event);
};

struct ReplicaWaitUpdate;
struct ReplicaChunkState : ScrubState<ReplicaChunkState, ReplicaActive, ReplicaWaitUpdate> {
  static constexpr std::string_view state_name = "ReplicaChunkState";
  explicit ReplicaChunkState(my_context ctx) : ScrubState(ctx) {}

  using reactions = boost::mpl::list<
    sc::custom_reaction<events::replica_scan_t>,
    sc::custom_reaction<events::replica_release_t>
    >;


  events::replica_scan_event_t to_scan;

  sc::result react(const events::replica_scan_t &event);
  sc::result react(const events::replica_release_t &event);

};

struct ReplicaScanChunk;
struct ReplicaWaitUpdate : ScrubState<ReplicaWaitUpdate, ReplicaChunkState> {
  static constexpr std::string_view state_name = "ReplicaWaitUpdate";
  explicit ReplicaWaitUpdate(my_context ctx) : ScrubState(ctx) {}

  using reactions = boost::mpl::list<
    sc::transition<ScrubContext::await_update_complete_t, ReplicaScanChunk>
    >;
};

struct ReplicaScanChunk : ScrubState<ReplicaScanChunk, ReplicaChunkState> {
  static constexpr std::string_view state_name = "ReplicaScanChunk";
  explicit ReplicaScanChunk(my_context ctx);

  using reactions = boost::mpl::list<
    sc::transition<ScrubContext::generate_and_submit_chunk_result_complete_t,
		   ReplicaIdle>
    >;
};

#undef SIMPLE_EVENT
#undef VALUE_EVENT

}
