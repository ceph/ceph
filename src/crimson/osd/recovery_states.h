// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/statechart/custom_reaction.hpp>
#include <boost/statechart/event.hpp>
#include <boost/statechart/event_base.hpp>
#include <boost/statechart/simple_state.hpp>
#include <boost/statechart/state.hpp>
#include <boost/statechart/transition.hpp>

#include "recovery_machine.h"
#include "recovery_events.h"

// Initial
// Reset
// Start
//   Started
//     Primary
//       WaitActingChange
//       Peering
//         GetInfo
//         GetLog
//         GetMissing
//         WaitUpThru
//         Incomplete
//       Active
//         Activating
//         Clean
//         Recovered
//         Backfilling
//         WaitRemoteBackfillReserved
//         WaitLocalBackfillReserved
//         NotBackfilling
//         NotRecovering
//         Recovering
//         WaitRemoteRecoveryReserved
//         WaitLocalRecoveryReserved
//     ReplicaActive
//       RepNotRecovering
//       RepRecovering
//       RepWaitBackfillReserved
//       RepWaitRecoveryReserved
//     Stray
//     ToDelete
//       WaitDeleteReserved
//       Deleting
// Crashed

namespace recovery {

struct Crashed : boost::statechart::state<Crashed, Machine> {
  explicit Crashed(my_context ctx);
};

struct Reset;

struct Initial : boost::statechart::state<Initial, Machine> {
  explicit Initial(my_context ctx);
  void exit();

  using reactions = boost::mpl::list <
    boost::statechart::transition<Initialize, Reset>,
    boost::statechart::custom_reaction<NullEvt>,
    boost::statechart::transition<boost::statechart::event_base, Crashed>
    >;

  boost::statechart::result react(const MNotifyRec&);
  boost::statechart::result react(const MInfoRec&);
  boost::statechart::result react(const MLogRec&);
  boost::statechart::result react(const boost::statechart::event_base&) {
    return discard_event();
  }
};

struct Reset : boost::statechart::state<Reset, Machine> {
  explicit Reset(my_context ctx);
  void exit();

  using reactions = boost::mpl::list <
    boost::statechart::custom_reaction<AdvMap>,
    boost::statechart::custom_reaction<ActMap>,
    boost::statechart::custom_reaction<NullEvt>,
    boost::statechart::transition<boost::statechart::event_base, Crashed>
    >;
  boost::statechart::result react(const AdvMap&);
  boost::statechart::result react(const ActMap&);
  boost::statechart::result react(const boost::statechart::event_base&) {
    return discard_event();
  }
};

struct Start;

struct Started : boost::statechart::state<Started, Machine, Start> {
  explicit Started(my_context ctx);
  void exit();

  using reactions = boost::mpl::list <
    boost::statechart::custom_reaction<AdvMap>,
    // ignored
    boost::statechart::custom_reaction<NullEvt>,
    // crash
    boost::statechart::transition<boost::statechart::event_base, Crashed>
    >;
  boost::statechart::result react(const AdvMap&);
  boost::statechart::result react(const boost::statechart::event_base&) {
    return discard_event();
  }
};

struct Primary;
struct Stray;

struct MakePrimary : boost::statechart::event<MakePrimary> {};
struct MakeStray : boost::statechart::event<MakeStray> {};

struct Start : boost::statechart::state<Start, Started> {
  explicit Start(my_context ctx);
  void exit();

  using reactions = boost::mpl::list <
    boost::statechart::transition<MakePrimary, Primary>,
    boost::statechart::transition<MakeStray, Stray>
    >;
};

struct Peering;
struct WaitActingChange;

struct Primary : boost::statechart::state<Primary, Started, Peering> {
  explicit Primary(my_context ctx);
  void exit();

  using reactions = boost::mpl::list <
    boost::statechart::custom_reaction<ActMap>,
    boost::statechart::custom_reaction<MNotifyRec>
    >;
  boost::statechart::result react(const ActMap&);
  boost::statechart::result react(const MNotifyRec&);
};

struct GetInfo;
struct Active;

struct Peering : boost::statechart::state<Peering, Primary, GetInfo> {
  PastIntervals::PriorSet prior_set;
  /// need osd_find_best_info_ignore_history_les
  bool history_les_bound = false;

  explicit Peering(my_context ctx);
  void exit();

  using reactions = boost::mpl::list <
    boost::statechart::transition<Activate, Active>,
    boost::statechart::custom_reaction<AdvMap>
    >;
  boost::statechart::result react(const AdvMap &advmap);
};

struct Activating;

struct AllReplicasActivated : boost::statechart::event<AllReplicasActivated> {};

struct Active : boost::statechart::state<Active, Primary, Activating> {
  explicit Active(my_context ctx);
  void exit();

  bool all_replicas_activated = false;

  using reactions = boost::mpl::list <
    boost::statechart::custom_reaction< ActMap >,
    boost::statechart::custom_reaction< AdvMap >,
    boost::statechart::custom_reaction< MInfoRec >,
    boost::statechart::custom_reaction< MNotifyRec >,
    boost::statechart::custom_reaction< MLogRec >,
    boost::statechart::custom_reaction< AllReplicasActivated >
    >;
  boost::statechart::result react(const ActMap&);
  boost::statechart::result react(const AdvMap&);
  boost::statechart::result react(const MInfoRec& infoevt);
  boost::statechart::result react(const MNotifyRec& notevt);
  boost::statechart::result react(const MLogRec& logevt);
  boost::statechart::result react(const AllReplicasActivated&);
};

struct GoClean : boost::statechart::event<GoClean> {};

struct Clean : boost::statechart::state<Clean, Active> {
  explicit Clean(my_context ctx);
  void exit();
  boost::statechart::result react(const boost::statechart::event_base&) {
    return discard_event();
  }
};

struct Recovered : boost::statechart::state<Recovered, Active> {
  using reactions = boost::mpl::list<
    boost::statechart::transition<GoClean, Clean>,
    boost::statechart::custom_reaction<AllReplicasActivated>
    >;
  explicit Recovered(my_context ctx);
  void exit();
  boost::statechart::result react(const AllReplicasActivated&);
};

struct AllReplicasRecovered : boost::statechart::event<AllReplicasRecovered>
{};

struct Activating : boost::statechart::state<Activating, Active> {
  using reactions = boost::mpl::list <
    boost::statechart::transition< AllReplicasRecovered, Recovered >
    >;
  explicit Activating(my_context ctx);
  void exit();
};

struct RepNotRecovering;

struct ReplicaActive : boost::statechart::state<ReplicaActive, Started, RepNotRecovering> {
  explicit ReplicaActive(my_context ctx);
  void exit();

  using reactions = boost::mpl::list <
    boost::statechart::custom_reaction<ActMap>,
    boost::statechart::custom_reaction<MQuery>,
    boost::statechart::custom_reaction<MInfoRec>,
    boost::statechart::custom_reaction<MLogRec>,
    boost::statechart::custom_reaction<Activate>
    >;
  boost::statechart::result react(const MInfoRec& infoevt);
  boost::statechart::result react(const MLogRec& logevt);
  boost::statechart::result react(const ActMap&);
  boost::statechart::result react(const MQuery&);
  boost::statechart::result react(const Activate&);
};

struct RepNotRecovering : boost::statechart::state<RepNotRecovering, ReplicaActive> {
  explicit RepNotRecovering(my_context ctx);
  void exit();
};

struct Stray : boost::statechart::state<Stray, Started> {
  explicit Stray(my_context ctx);
  void exit();

  using reactions = boost::mpl::list <
    boost::statechart::custom_reaction<MQuery>,
    boost::statechart::custom_reaction<MLogRec>,
    boost::statechart::custom_reaction<MInfoRec>,
    boost::statechart::custom_reaction<ActMap>
    >;
  boost::statechart::result react(const MQuery& query);
  boost::statechart::result react(const MLogRec& logevt);
  boost::statechart::result react(const MInfoRec& infoevt);
  boost::statechart::result react(const ActMap&);
};

struct GetLog;
struct Down;

struct GotInfo : boost::statechart::event<GotInfo> {};
struct IsDown : boost::statechart::event<IsDown> {};

struct GetInfo : boost::statechart::state<GetInfo, Peering> {
  std::set<pg_shard_t> peer_info_requested;

  explicit GetInfo(my_context ctx);
  void exit();
  void get_infos();

  using reactions = boost::mpl::list <
    boost::statechart::transition<GotInfo, GetLog>,
    boost::statechart::custom_reaction<MNotifyRec>,
    boost::statechart::transition<IsDown, Down>
    >;
  boost::statechart::result react(const MNotifyRec& infoevt);
};

struct GotLog : boost::statechart::event<GotLog> {};

struct GetLog : boost::statechart::state<GetLog, Peering> {
  pg_shard_t auth_log_shard;
  boost::intrusive_ptr<MOSDPGLog> msg;

  explicit GetLog(my_context ctx);
  void exit();

  using reactions = boost::mpl::list <
    boost::statechart::custom_reaction<MLogRec>,
    boost::statechart::custom_reaction<GotLog>,
    boost::statechart::custom_reaction<AdvMap>
    >;
  boost::statechart::result react(const AdvMap&);
  boost::statechart::result react(const MLogRec& logevt);
  boost::statechart::result react(const GotLog&);
};

struct NeedUpThru : boost::statechart::event<NeedUpThru> {};
struct WaitUpThru;

struct GetMissing : boost::statechart::state<GetMissing, Peering> {
  explicit GetMissing(my_context ctx);
  void exit();

  using reactions = boost::mpl::list <
    boost::statechart::custom_reaction<MLogRec>,
    boost::statechart::transition<NeedUpThru, WaitUpThru>
    >;
  boost::statechart::result react(const MLogRec& logevt);
};

struct WaitUpThru : boost::statechart::state<WaitUpThru, Peering> {
  explicit WaitUpThru(my_context ctx);
  void exit();

  using reactions = boost::mpl::list <
    boost::statechart::custom_reaction<ActMap>,
    boost::statechart::custom_reaction<MLogRec>
    >;
  boost::statechart::result react(const ActMap& am);
  boost::statechart::result react(const MLogRec& logrec);
};

struct Down : boost::statechart::state<Down, Peering> {
  explicit Down(my_context ctx);
  using reactions = boost::mpl::list <
    boost::statechart::custom_reaction<MNotifyRec>
    >;
  boost::statechart::result react(const MNotifyRec& infoevt);
  void exit();
};

}
