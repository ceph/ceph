/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM, Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#pragma once
#include <string>
#include <map>
#include <unordered_map>
#include <optional>
#include <vector>
#include <ranges>

#include "mds/mdstypes.h"
#include "common/ceph_time.h"

// NB! The order of the states in the enum is important!
// There are places in the code that aggregate multiple states
// via min or max, depending on the task.
// The order of states represents the natural lifecycle
// of a set and its members, this is specifically important
// for the active states.
enum QuiesceState: uint8_t {
  QS__INVALID,

  // these states are considered "active"
  QS_QUIESCING, QS__ACTIVE = QS_QUIESCING,
  QS_QUIESCED,
  QS_RELEASING,

  // the below states are all terminal, or "inactive"
  QS_RELEASED, QS__TERMINAL = QS_RELEASED,
  // the below states are all about types of failure
  QS_EXPIRED, QS__FAILURE = QS_EXPIRED,

  QS_FAILED,

  // the below states aren't allowed for roots, only for sets
  QS_CANCELED, QS__SET_ONLY = QS_CANCELED,
  QS_TIMEDOUT,

  QS__MAX,
};

template <class CharT, class Traits>
static std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const QuiesceState& qs)
{
  switch (qs) {
  case QS__INVALID:
    return os << "QS__INVALID (" << (int)qs << ")";
  case QS_QUIESCING:
    return os << "QS_QUIESCING (" << (int)qs << ")";
  case QS_QUIESCED:
    return os << "QS_QUIESCED (" << (int)qs << ")";
  case QS_RELEASING:
    return os << "QS_RELEASING (" << (int)qs << ")";
  case QS_RELEASED:
    return os << "QS_RELEASED (" << (int)qs << ")";
  case QS_FAILED:
    return os << "QS_FAILED (" << (int)qs << ")";
  case QS_CANCELED:
    return os << "QS_CANCELED (" << (int)qs << ")";
  case QS_TIMEDOUT:
    return os << "QS_TIMEDOUT (" << (int)qs << ")";
  case QS_EXPIRED:
    return os << "QS_EXPIRED (" << (int)qs << ")";
  default:
    return os << "!Unknown quiesce state! (" << (int)qs << ")";
  }
};

inline const char * quiesce_state_name(QuiesceState state) {
  switch (state) {
  case QS__INVALID:
    return "<invalid>";
  case QS_QUIESCING:
    return "QUIESCING";
  case QS_QUIESCED:
    return "QUIESCED";
  case QS_RELEASING:
    return "RELEASING";
  case QS_RELEASED:
    return "RELEASED";
  case QS_FAILED:
    return "FAILED";
  case QS_CANCELED:
    return "CANCELED";
  case QS_TIMEDOUT:
    return "TIMEDOUT";
  case QS_EXPIRED:
    return "EXPIRED";
  default:
    return "<unknown>";
  }
}

// Since MDS clock is not syncrhonized, and the quiesce db has to be replicated,
// we measure all events in the quiesce database relative to the database age.
// The age of the database is just large enough to have earliest events carry
// a non-negative age stamp.
// This is sufficient because we only care to honor the timeouts that are relative
// to the other recorded database events.
// This approach also relieves us from storing or transfering absolute time stamps:
// every client can deduce the lower boundary of event's absolute time given the 
// message roundrip timing - if they bother enough. Otherwise, they can just subtract
// the received database age from now() and get their own absolute time reference.

using QuiesceClock = ceph::coarse_real_clock;
using QuiesceTimePoint = QuiesceClock::time_point;
using QuiesceTimeInterval = QuiesceClock::duration;
using QuiesceSetId = std::string;
using QuiesceRoot = std::string;
using QuiesceSetVersion = uint64_t;

namespace QuiesceInterface {
  using PeerId = mds_gid_t;
}

struct QuiesceDbVersion {
  epoch_t epoch;
  QuiesceSetVersion set_version;
  auto operator<=>(QuiesceDbVersion const& other) const = default;
  QuiesceDbVersion& operator+(unsigned int delta) {
    set_version += delta;
    return *this;
  }
};

inline auto operator==(int const& set_version, QuiesceDbVersion const& db_version)
{
  return db_version.set_version == (QuiesceSetVersion)set_version;
}

template <class CharT, class Traits>
static std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const QuiesceDbVersion& dbv)
{
  return os << "(" << dbv.epoch << ":" << dbv.set_version << ")";
}

struct QuiesceTimeIntervalSec {
  const QuiesceTimeInterval interval;
  QuiesceTimeIntervalSec(const QuiesceTimeInterval &interval) : interval(interval) {}
};

template <class CharT, class Traits>
static std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const QuiesceTimeIntervalSec& sec)
{
  using std::chrono::duration_cast;
  using dd = std::chrono::duration<double>;
  const auto precision = os.precision();
  const auto flags = os.flags();

  os
    << std::fixed
    << std::setprecision(1)
    << duration_cast<dd>(sec.interval).count()
    << std::setprecision(precision);

  os.flags(flags);
  return os;
}

struct RecordedQuiesceState {
  QuiesceState state;
  QuiesceTimeInterval at_age;

  operator QuiesceState() {
    return state;
  }

  bool update(const QuiesceState &state, const QuiesceTimeInterval &at_age) {
    if (state != this->state) {
      this->state = state;
      this->at_age = at_age;
      return true;
    }
    return false;
  }

  RecordedQuiesceState(QuiesceState state, QuiesceTimeInterval at_age) : state(state), at_age(at_age) {}
  RecordedQuiesceState() : RecordedQuiesceState (QS__INVALID, QuiesceTimeInterval::zero()) {}
  RecordedQuiesceState(RecordedQuiesceState const&) = default;
  RecordedQuiesceState(RecordedQuiesceState &&) = default;
  RecordedQuiesceState& operator=(RecordedQuiesceState const&) = default;
  RecordedQuiesceState& operator=(RecordedQuiesceState &&) = default;
};

template <class CharT, class Traits>
static std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const RecordedQuiesceState& rstate)
{
  return os << rstate.state;
}

/// @brief  `QuiesceSet` is the only record type in the quiesce database
///         It encodes sufficient information to have the db taken over by
///         a new manager and correctly decide on the next state transition.
struct QuiesceSet {
  /// @brief  A member of a set represents a single root this set wants quiesced
  ///         It carries the information about the current known state of this root
  ///         and whether it got excluded from this set.
  ///         It's possible that holding on to excluded members is an overkill.
  struct MemberInfo {
    RecordedQuiesceState rstate;
    bool excluded = false;
    MemberInfo(QuiesceState state, QuiesceTimeInterval at_age)
        : rstate(state, at_age)
        , excluded(false)
    {
    }
    MemberInfo(QuiesceTimeInterval at_age)
        : MemberInfo(QS_QUIESCING, at_age)
    {
    }
    MemberInfo() = default;
    MemberInfo(MemberInfo const& o) = default;
    MemberInfo(MemberInfo &&) = default;
    MemberInfo& operator=(MemberInfo const& o) = default;
    MemberInfo& operator=(MemberInfo &&) = default;

    bool is_quiescing() const { return rstate.state < QS_QUIESCED; }
    bool is_failed() const { return rstate.state >= QS__FAILURE; }
  };
  
  /// @brief  The db version when this set got modified last
  QuiesceSetVersion version = 0;
  /// @brief  The last recorded state change of this set
  RecordedQuiesceState rstate;
  /// @brief  How much time to give every new member to reach the quiesced state
  ///         By default the value is zero. It means that new sets which don't
  ///         have this field explicitly updated will immediately reach QS_TIMEDOUT.
  QuiesceTimeInterval timeout = QuiesceTimeInterval::zero();
  /// @brief  How much time to allow the set to spend in QS_QUIESCED or QS_RELEASING states
  ///         The expiration timer is reset every time a successful await is executed
  ///         on a QS_QUIESCED set.
  QuiesceTimeInterval expiration = QuiesceTimeInterval::zero();
  using Members = std::unordered_map<QuiesceRoot, MemberInfo>;
  Members members;

  /// @brief The effective state of a member is just a max of its parent set state and its own state
  ///        The exception is when the set is releasing: we want to consider any ack from peers
  ///        that confirms quiesced state of the member to be treated as RELEASED.
  /// @param member_state the reported state of the member
  /// @return the effective member state
  QuiesceState get_effective_member_state(QuiesceState reported_state) const
  {
    if (is_releasing()) {
      if (reported_state >= QS_QUIESCED && reported_state <= QS_RELEASED) {
        return QS_RELEASED;
      }
    }
    if (is_quiesced() && reported_state < QS_QUIESCED) {
      // we need to change back to quiescing
      return QS_QUIESCING;
    }
    return std::max(reported_state, rstate.state);
  }

  /// @brief The requested state of a member is what we send to the agents for 
  ///        executing the quiesce protocol. This state is deliberately reduced
  ///        to provoke clients to ack back and thus confirm their current state
  /// @param set_state the state of the set this member is from
  /// @return the effective member state
  QuiesceState get_requested_member_state() const
  {
    if (rstate.state >= QS__TERMINAL) {
      return rstate.state;
    }
    if (rstate.state <= QS_QUIESCED) {
      // request quiescing even if we are already quiesced
      return QS_QUIESCING;
    }
    // we can't have anything else unless the state enum was changed
    // which will have to be addressed here.
    ceph_assert(rstate.state == QS_RELEASING);
    return QS_RELEASING;
  }

  bool is_active() const {
    return
      rstate.state > QS__INVALID
      && rstate.state < QS__TERMINAL;
  }

  QuiesceState next_state(QuiesceState min_member_state) const;

  bool is_quiescing() const { return rstate.state < QS_QUIESCED; }
  bool is_quiesced() const { return rstate.state == QS_QUIESCED; }
  bool is_releasing() const { return rstate.state == QS_RELEASING; }
  bool is_released() const { return rstate.state == QS_RELEASED; }

  QuiesceSet() = default;
  QuiesceSet(QuiesceSet const &) = default;
  QuiesceSet(QuiesceSet &&) = default;
  QuiesceSet& operator=(QuiesceSet const &) = default;
  QuiesceSet& operator=(QuiesceSet &&) = default;
};

template <class CharT, class Traits>
static std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const QuiesceSet::MemberInfo& member)
{
  return os << (member.excluded ? "(excluded)" : "") << member.rstate;
}

template <class CharT, class Traits>
static std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const QuiesceSet& set)
{
  size_t active = 0, inactive = 0;

  for (auto && [_, m]: set.members) {
    if (m.excluded) {
      ++inactive;
    } else {
      ++active;
    }
  }

  return os << "q-set[" << set.rstate << " v:" << set.version << ", m:" << active << "/" << inactive
    << ", t:" << QuiesceTimeIntervalSec(set.timeout) << ", e:" << QuiesceTimeIntervalSec(set.expiration) << "]";
}

/// @brief QuiesceDbRequest is the only client interface to the database.
///        This structure alone should be capable of encoding the full variety
///        of different requests that can be submitted by the client.
struct QuiesceDbRequest {
  /// @brief `RootsOp` should be considered together with the `roots` set below
  ///        to know the operation. Each name in the enum has two verbs: first
  ///        verb is for the case when `roots` is not empty, and the second is
  ///        for when `roots` is empty
  enum RootsOp: uint8_t {
    INCLUDE_OR_QUERY,
    EXCLUDE_OR_RELEASE,
    RESET_OR_CANCEL,
    __INVALID
  };

  enum Flags: uint8_t {
    NONE = 0,
    VERBOSE = 1,
    EXCLUSIVE = 2,
  };

  struct Control {
    union {
      struct {
        RootsOp roots_op;
        Flags flags;
      };
      uint64_t raw;
    };
    Control() : raw(0) {}
    Control(RootsOp op) : raw(0) {
      roots_op = op;
    }
    bool operator==(const Control& other) const {
      return other.raw == raw;
    }
  };

  Control control;

  /// @brief `set_id` is optional to allow for the following operations:
  ///         * including roots without providing a set id will generate a new set with a unique id
  ///           ** NB! the new set id will stored in this field by the db manager
  ///         * querying without a set id will return the full db
  ///         * cancelling without a set id will cancel all active sets
  std::optional<std::string> set_id;
  /// @brief When `if_version` provided, the request will only be executed
  ///        if the named set has exactly the version, otherwise ESTALE is returned
  ///        and no set modification is performed.
  ///        Requires a set_id.
  std::optional<QuiesceSetVersion> if_version;
  /// @brief Updates the quiesce timeout of an active set.
  ///        Requires a set_id. Attempt to update an inactive set will result in EPERM
  std::optional<QuiesceTimeInterval> timeout;
  /// @brief Updates the quiesce expiration of an active set.
  ///        Requires a set_id. Attempt to update an inactive set will result in EPERM
  std::optional<QuiesceTimeInterval> expiration;
  /// @brief When `await` is non-null, then after performing other encoded operations
  ///        this request is put on the await queue of the given set.
  ///        The value of this member defines the await timeout.
  ///        Requires a set id. The result code is one of the following:
  ///         EPERM     - the set is not in one of the awaitable states:
  ///                     [QS_QUIESCING, QS_QUIESCED, QS_RELEASING, QS_RELEASED]
  ///         SUCCESS   - the set is currently QS_QUIESCED or QS_RELEASED.
  ///                     When an await is completed successfully for a QS_QUIESCED set,
  ///                     this set's quiesce expiration timer is reset.
  ///         EINTR     - the set had a change in members or in state
  ///         ECANCELED - the set was canceled
  ///         ETIMEDOUT - at least one of the set members failed to quiesce 
  ///                     within the configured quiesce timeout.
  ///                     OR the set is RELEASING and it couldn't reach RELEASED before it expired
  ///                     NB: the quiesce timeout is measured for every member separately
  ///                     from the moment that member is included.
  ///         EINPROGRESS - the time limit configured for this await call has elapsed 
  ///                     before the set changed state.
  std::optional<QuiesceTimeInterval> await;
  using Roots = std::unordered_set<QuiesceRoot>;
  /// @brief `roots` help identify the wanted operation as well as providing
  ///        the actual roots to mutate the members of the set.
  Roots roots;

  bool operator==(const QuiesceDbRequest&) const = default;

  bool is_valid() const {
    return control.roots_op < __INVALID
        && (is_awaitable() || !await) 
        && (
          // Everything goes if a set id is provided
          set_id
          // or it's a new set creation, in which case the request should be including roots
          || includes_roots()
          // Otherwise, the allowed wildcard operations are: query and cancel all.
          // Also, one can't await a wildcard
          || ((is_cancel_all() || is_query()) && !await && !timeout && !expiration && !if_version)
        )
    ;
  }

  bool is_awaitable() const { return !(is_query() || is_cancel()); }
  bool is_mutating() const { return (control.roots_op != INCLUDE_OR_QUERY) || !roots.empty() || timeout || expiration; }
  bool is_cancel_all() const { return !set_id && is_cancel(); }
  bool excludes_roots() const { return control.roots_op == RESET_OR_CANCEL || (control.roots_op == EXCLUDE_OR_RELEASE && !roots.empty()); }
  bool includes_roots() const { return (control.roots_op == RESET_OR_CANCEL || control.roots_op == INCLUDE_OR_QUERY) && !roots.empty(); }

  bool is_include() const { return control.roots_op == INCLUDE_OR_QUERY && !roots.empty(); }
  bool is_query() const { return control.roots_op == INCLUDE_OR_QUERY && roots.empty(); }
  bool is_exclude() const { return control.roots_op == EXCLUDE_OR_RELEASE && !roots.empty(); }
  bool is_release() const { return control.roots_op == EXCLUDE_OR_RELEASE && roots.empty(); }
  bool is_reset() const { return control.roots_op == RESET_OR_CANCEL && !roots.empty(); }
  bool is_cancel() const { return control.roots_op == RESET_OR_CANCEL && roots.empty(); }

  bool is_verbose() const { return control.flags & Flags::VERBOSE; }
  bool is_exclusive() const { return control.flags & Flags::EXCLUSIVE; }

  bool should_exclude(QuiesceRoot root) const {
    switch (control.roots_op) {
    case INCLUDE_OR_QUERY:
      return false;
    case EXCLUDE_OR_RELEASE:
      return roots.contains(root);
    case RESET_OR_CANCEL:
      return !roots.contains(root);
      default: ceph_abort("unknown roots_op"); return false;
    }
  }

  void reset(std::invocable<QuiesceDbRequest&> auto const &config)
  {
    set_id.reset();
    if_version.reset();
    timeout.reset();
    expiration.reset();
    await.reset();
    roots.clear();
    control.raw = 0; // implies roots_op == INCLUDE_OR_QUERY;

    config(*this);
  }
  void clear() {
    reset([](auto&r){});
  }

  template<typename R = Roots>
  requires requires ( R&& roots) {
    Roots(std::forward<R>(roots));
  }
  void set_roots(RootsOp op, R&& roots) {
    control.roots_op = op;
    this->roots = Roots(std::forward<R>(roots));
  }

  template <std::ranges::range R>
  void set_roots(RootsOp op, const R& roots_range)
  {
    control.roots_op = op;
    this->roots = Roots(roots_range.begin(), roots_range.end());
  }

  template <typename R = Roots>
  void include_roots(R&& roots)
  {
    set_roots(INCLUDE_OR_QUERY, std::forward<R>(roots));
  }

  template <typename R = Roots>
  void exclude_roots(R&& roots)
  {
    set_roots(EXCLUDE_OR_RELEASE, std::forward<R>(roots));
  }

  void release_roots() {
    set_roots(EXCLUDE_OR_RELEASE, {});
  }

  template <typename R = Roots>
  void reset_roots(R&& roots)
  {
    set_roots(RESET_OR_CANCEL, std::forward<R>(roots));
  }

  void cancel_roots()
  {
    set_roots(RESET_OR_CANCEL, {});
  }

  template <typename S = std::string>
  void query(S&& set_id) {
    reset([set_id](auto &r){
      r.set_id = std::forward<S>(set_id);
    });
  }

  const char * op_string() const {
    switch (control.roots_op) {
    case INCLUDE_OR_QUERY:
      return roots.empty() ? "query" : "include";
    case EXCLUDE_OR_RELEASE:
      return roots.empty() ? "release" : "exclude";
    case RESET_OR_CANCEL:
      return roots.empty() ? "cancel" : "reset";
    default:
      return "<unknown>";
    }
  }


  QuiesceDbRequest() {}
  QuiesceDbRequest(const QuiesceDbRequest &) = default;
  QuiesceDbRequest(QuiesceDbRequest &&) = default;
  QuiesceDbRequest(std::invocable<QuiesceDbRequest&> auto const &config) {
    reset(config);
  }
};

template <class CharT, class Traits>
static std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const QuiesceDbRequest& req)
{
  os << "q-req[" << req.op_string();

  if (req.set_id) {
    os << " \"" << req.set_id << "\"";
  }

  if (req.if_version) {
    os << " ?v:" << req.if_version;
  }

  if (req.await) {
    os << " a:" << QuiesceTimeIntervalSec(*req.await);
  }

  return os << " roots:" << req.roots.size() << "]";
}

/// @brief  A `QuiesceDbListing` represents a subset of the database, up to
///         a full database. The contents of the listing is decided by the leader
///         based on the acks it got from every given replica: the update will
///         contain all sets that have their version > than the last acked by the peer.
struct QuiesceDbListing {
  QuiesceDbVersion db_version = {0, 0};
  /// @brief  Crucially, the precise `db_age` must be included in every db listing
  ///         This data is used by all replicas to update their calculated DB TIME ZERO.
  ///         All events in the database are measured relative to the DB TIME ZERO
  QuiesceTimeInterval db_age = QuiesceTimeInterval::zero();
  std::unordered_map<QuiesceSetId, QuiesceSet> sets;

  void clear() {
    db_version = {0, 0};
    db_age = QuiesceTimeInterval::zero();
    sets.clear();
  }

  QuiesceDbListing(epoch_t epoch) : db_version {epoch, 0} {}
  QuiesceDbListing() = default;
  QuiesceDbListing(QuiesceDbListing const&) = default;
  QuiesceDbListing(QuiesceDbListing &&) = default;
  QuiesceDbListing& operator=(QuiesceDbListing const&) = default;
  QuiesceDbListing& operator=(QuiesceDbListing &&) = default;
};

template <class CharT, class Traits>
static std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const QuiesceDbListing& dbl)
{
  size_t active = 0, inactive = 0;

  for (auto&& [_, s] : dbl.sets) {
    if (s.is_active()) {
      ++active;
    } else {
      ++inactive;
    }
  }

  return os << "q-db[v:" << dbl.db_version << " sets:" << active << "/" << inactive << "]";
}

struct QuiesceDbPeerListing {
  QuiesceInterface::PeerId origin;
  QuiesceDbListing db;
};

template <class CharT, class Traits>
static std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const QuiesceDbPeerListing& dbl)
{
  return os << dbl.db << " from " << dbl.origin;
}

/// @brief  `QuiesceMap` is a root-centric representation of the quiesce database
///         It lists roots with their effective states as of particular version.
///         Additionally, the same structure is used by the peers when reporting
///         actual roots states which are different from what the DB version encodes
struct QuiesceMap {
  QuiesceDbVersion db_version;
  struct RootInfo {
    QuiesceState state;
    QuiesceTimeInterval ttl = QuiesceTimeInterval::zero();
    bool is_valid() const { return state > QS__INVALID && state < QS__SET_ONLY; }
    RootInfo() : RootInfo(QS__INVALID) {}
    RootInfo(QuiesceState state) : RootInfo(state,QuiesceTimeInterval::zero()) {}
    RootInfo(QuiesceState state, QuiesceTimeInterval ttl)
        : state(state)
        , ttl(ttl)
    {
    }
    inline bool operator==(const RootInfo& other) const {
      return state == other.state && ttl == other.ttl;
    }

    RootInfo(RootInfo const&) = default;
    RootInfo(RootInfo &&) = default;
    RootInfo& operator=(RootInfo const&) = default;
    RootInfo& operator=(RootInfo &&) = default;
  };
  using Roots = std::unordered_map<QuiesceRoot, RootInfo>;
  Roots roots;
  void clear() {
    db_version = {0, 0};
    roots.clear();
  }

  QuiesceMap() : db_version({0, 0}), roots() { }
  QuiesceMap(QuiesceDbVersion db_version) : db_version(db_version), roots() { }
  QuiesceMap(QuiesceDbVersion db_version, Roots &&roots) : db_version(db_version), roots(roots) { }
  QuiesceMap(QuiesceDbVersion db_version, Roots const& roots) : db_version(db_version), roots(roots) { }

  QuiesceMap(QuiesceMap const&) = default;
  QuiesceMap(QuiesceMap &&) = default;
  QuiesceMap& operator=(QuiesceMap const&) = default;
  QuiesceMap& operator=(QuiesceMap &&) = default;
};

template <class CharT, class Traits>
static std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const QuiesceMap& map)
{

  size_t active = 0, inactive = 0;

  for (auto&& [_, r] : map.roots) {
    if (r.state < QS__TERMINAL) {
      ++active;
    } else {
      ++inactive;
    }
  }

  return os << "q-map[v:" << map.db_version << " roots:" << active << "/" << inactive << "]";
}

struct QuiesceDbPeerAck {
  QuiesceInterface::PeerId origin;
  QuiesceMap diff_map;
};

template <class CharT, class Traits>
static std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const QuiesceDbPeerAck& ack)
{
  return os << "ack " << ack.diff_map << " from " << ack.origin;
}

inline QuiesceTimeInterval interval_saturate_add(QuiesceTimeInterval lhs, QuiesceTimeInterval rhs)
{
  // assuming an unsigned time interval.
  // TODO: make this function generic and also saturate add signed values
  assert(std::is_unsigned_v<QuiesceTimeInterval::rep>);

  QuiesceTimeInterval result = lhs + rhs;

  // the sum can't be smaller than either part
  // since we're working with an unsigned value
  if (result < lhs || result < rhs) {
    // this must have been an overflow
    return QuiesceTimeInterval::max();
  }

  return result;
};

inline QuiesceTimePoint interval_saturate_add_now(QuiesceTimeInterval interval) {
  return QuiesceTimePoint(interval_saturate_add(QuiesceClock::now().time_since_epoch(), interval));
};

namespace QuiesceInterface {
  /// @brief  A callback from the manager to the agent with an up-to-date root list
  ///         The map is mutable and will be used as synchronous agent ack if the return value is true
  using AgentNotify = std::function<bool(QuiesceMap&)>;
  /// @brief  Used to send asyncrhonous acks from agents about changes to the root states
  ///         The transport layer should include sufficient information to know the sender of the ack
  using AgentAck = std::function<int(QuiesceMap&&)>;
  /// @brief  Used by the leader to replicate the DB changes to its peers
  using DbPeerUpdate = std::function<int(PeerId, QuiesceDbListing&&)>;

  using RequestHandle = metareqid_t;
  /// @brief  Used by the agent to initiate an ongoing quiesce request for the given quiesce root
  ///         The context will be completed when the quiescing is achieved by this rank. The IO pause
  ///         should continue until the request is canceled.
  ///         Repeated requests for the same root should succeed, returning a _new_ request id;
  ///         the old context should be completed with an error EINTR, and the old request id should be invalidated.
  ///         If the root has already reached quiescence by the time the repeated request is submitted
  ///         then the new context should be immediately (syncrhonously) completed with success and then discarded.
  ///         Syncrhonous errors should be reported by completing the supplied context, and the return value
  ///         should be std::nullopt in such cases
  using RequestSubmit = std::function<std::optional<RequestHandle>(QuiesceRoot, Context*)>;
  /// @brief  Cancels the quiesce request. May be called at any time after the request got submitted
  using RequestCancel = std::function<int(const RequestHandle&)>;
};
