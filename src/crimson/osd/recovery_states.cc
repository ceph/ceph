// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "recovery_states.h"

#include "crimson/common/log.h"
#include "messages/MOSDPGLog.h"
#include "osd/OSDMap.h"
#include "pg.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

namespace recovery {

/*------Crashed-------*/
Crashed::Crashed(my_context ctx)
  : my_base(ctx)
{
  ceph_abort_msg("we got a bad state machine event");
}

/*------Initial-------*/

Initial::Initial(my_context ctx)
  : my_base(ctx)
{
  logger().info("Initial");
}

boost::statechart::result Initial::react(const MNotifyRec& notify)
{
  logger().info("Active <MNotifyRec>");
  // todo: process info from replica
  auto& pg = context<Machine>().pg;
  pg.update_last_peering_reset();
  return transit<Primary>();
}

boost::statechart::result Initial::react(const MInfoRec& i)
{
  // todo
  logger().info("<MInfoRec> transitioning from Initial to Stray");
  post_event(i);
  return transit<Stray>();
}

boost::statechart::result Initial::react(const MLogRec& i)
{
  // todo
  logger().info("<MLogRec> transitioning from Initial to Stray");
  post_event(i);
  return transit<Stray>();
}

void Initial::exit()
{}

/*--------Reset---------*/
Reset::Reset(my_context ctx)
  : my_base(ctx)
{
  logger().info("Entering Reset");
  auto& pg = context<Machine>().pg;
  pg.update_last_peering_reset();
}

boost::statechart::result Reset::react(const AdvMap& advmap)
{
  logger().info("Reset advmap");
  auto& pg = context<Machine>().pg;
  if (pg.should_restart_peering(advmap.up_primary,
				advmap.acting_primary,
				advmap.new_up,
				advmap.new_acting,
				advmap.last_map,
				advmap.osdmap)) {
    pg.start_peering_interval(advmap.up_primary,
			      advmap.acting_primary,
			      advmap.new_up,
			      advmap.new_acting,
			      advmap.last_map);
  }
  return discard_event();
}

boost::statechart::result Reset::react(const ActMap&)
{
  auto& pg = context<Machine>().pg;
  if (pg.should_send_notify()) {
    context<Machine>().send_notify(pg.get_primary(),
				   pg.get_notify(pg.get_osdmap_epoch()),
				   pg.get_past_intervals());
  }
  return transit<Started>();
}

void Reset::exit()
{
  logger().info("Leaving Reset");
}

/*------Started-------*/
Started::Started(my_context ctx)
  : my_base(ctx)
{
  logger().info("Entering Started");
}

boost::statechart::result Started::react(const AdvMap& advmap)
{
  auto& pg = context<Machine>().pg;
  logger().info("Started <AdvMap>");
  if (pg.should_restart_peering(advmap.up_primary,
				advmap.acting_primary,
				advmap.new_up,
				advmap.new_acting,
				advmap.last_map,
				advmap.osdmap)) {
    logger().info("should_restart_peering, transitioning to Reset");
    post_event(advmap);
    return transit<Reset>();
  }
  return discard_event();
}

void Started::exit()
{
  logger().info("Leaving Started");
}

/*-------Start---------*/
Start::Start(my_context ctx)
  : my_base(ctx)
{
  auto& pg = context<Machine>().pg;
  if (pg.is_primary()) {
    logger().info("Start transitioning to Primary");
    post_event(MakePrimary{});
  } else { // is_stray
    logger().info("Start transitioning to Stray");
    post_event(MakeStray{});
  }
}

void Start::exit()
{
  logger().info("Leaving Start");
}

/*---------Primary--------*/
Primary::Primary(my_context ctx)
  : my_base(ctx)
{
  logger().info("Entering Primary");
}

boost::statechart::result Primary::react(const MNotifyRec& notevt)
{
  // todo
  auto& pg = context<Machine>().pg;
  pg.proc_replica_info(notevt.from,
		       notevt.notify.info,
		       notevt.notify.epoch_sent);
  logger().info("Primary <MNotifyRec> {}", pg);
  return discard_event();
}

boost::statechart::result Primary::react(const ActMap&)
{
  // todo
  auto& pg = context<Machine>().pg;
  logger().info("Primary <ActMap> {}", pg);
  return discard_event();
}

void Primary::exit()
{
  auto& pg = context<Machine>().pg;
  pg.clear_primary_state();
  pg.clear_state(PG_STATE_CREATING);
  logger().info("Leaving Primary: {}", pg);
}

/*---------Peering--------*/
Peering::Peering(my_context ctx)
  : my_base(ctx)
{
  auto& pg = context<Machine>().pg;
  pg.set_state(PG_STATE_PEERING);
  logger().info("Entering Peering");
}

boost::statechart::result Peering::react(const AdvMap& advmap)
{
  logger().info("Peering <AdvMap>");
  context<Machine>().pg.update_need_up_thru(advmap.osdmap.get());
  return forward_event();
}

void Peering::exit()
{
  auto& pg = context<Machine>().pg;
  logger().info("Leaving Peering: {}", pg);
  pg.clear_state(PG_STATE_PEERING);
}

/*--------GetInfo---------*/
GetInfo::GetInfo(my_context ctx)
  : my_base(ctx)
{
  logger().info("Entering GetInfo");
  context<Machine>().pg.update_need_up_thru();
  post_event(GotInfo{});
}

boost::statechart::result GetInfo::react(const MNotifyRec& infoevt)
{
  logger().info("GetInfo <MNotifyRec>");
  // todo: depends on get_infos()
  post_event(GotInfo());
  return discard_event();
}

void GetInfo::exit()
{
  logger().info("Leaving GetInfo");
  // todo
}

/*------GetLog------------*/
GetLog::GetLog(my_context ctx)
  : my_base(ctx)
{
  logger().info("Entering GetLog");
  auto& pg = context<Machine>().pg;

  PG::choose_acting_t adjust_acting;
  tie(adjust_acting, auth_log_shard) = pg.choose_acting();
  switch (adjust_acting) {
  case PG::choose_acting_t::dont_change:
    break;
  case PG::choose_acting_t::should_change:
    // post_event(NeedActingChange());
    return;
  case PG::choose_acting_t::pg_incomplete:
    // post_event(IsIncomplete());
    return;
  }
  // am i the best?
  if (auth_log_shard == pg.get_whoami()) {
    post_event(GotLog{});
    return;
  } else {
    // todo: request log from peer
    return;
  }
}

boost::statechart::result GetLog::react(const AdvMap& advmap)
{
  // make sure our log source didn't go down.  we need to check
  // explicitly because it may not be part of the prior set, which
  // means the Peering state check won't catch it going down.
  if (!advmap.osdmap->is_up(auth_log_shard.osd)) {
    logger().info("GetLog: auth_log_shard osd.{} went down",
		  auth_log_shard.osd);
    post_event(advmap);
    return transit<Reset>();
  }

  // let the Peering state do its checks.
  return forward_event();
}

boost::statechart::result GetLog::react(const MLogRec& logevt)
{
  assert(!msg);
  if (logevt.from != auth_log_shard) {
    logger().info("GetLog: discarding log from non-auth_log_shard osd.{}",
		  logevt.from);
    return discard_event();
  }
  logger().info("GetLog: received master log from osd.{}",
		logevt.from);
  msg = logevt.msg;
  post_event(GotLog{});
  return discard_event();
}

boost::statechart::result GetLog::react(const GotLog&)
{
  logger().info("leaving GetLog");
  return transit<GetMissing>();
}

void GetLog::exit()
{}

Recovered::Recovered(my_context ctx)
  : my_base(ctx)
{
  logger().info("Entering Recovered");
  if (context<Active>().all_replicas_activated) {
    logger().info("all_replicas_activated");
    post_event(GoClean{});
  }
}

boost::statechart::result Recovered::react(const AllReplicasActivated&)
{
  logger().info("Recovered <AllReplicasActivated>");
  post_event(GoClean{});
  return forward_event();
}

void Recovered::exit()
{
  logger().info("Leaving Recovered");
}

Clean::Clean(my_context ctx)
  : my_base(ctx)
{
  logger().info("Entering Clean");
  auto& pg = context<Machine>().pg;
  pg.maybe_mark_clean();
}

void Clean::exit()
{
  logger().info("Leaving Clean");
  auto& pg = context<Machine>().pg;
  pg.clear_state(PG_STATE_CLEAN);
}

/*---------Active---------*/
Active::Active(my_context ctx)
  : my_base(ctx)
{
  logger().info("Entering Activate");
  auto& pg = context<Machine>().pg;
  assert(pg.is_primary());
  pg.activate(pg.get_osdmap_epoch());
  logger().info("Activate Finished");
}

boost::statechart::result Active::react(const AdvMap& advmap)
{
  auto& pg = context<Machine>().pg;
  if (pg.should_restart_peering(advmap.up_primary,
				advmap.acting_primary,
				advmap.new_up,
				advmap.new_acting,
				advmap.last_map,
				advmap.osdmap)) {
    logger().info("Active advmap interval change, fast return");
    return forward_event();
  }
  logger().info("Active advmap");
  return forward_event();
}

boost::statechart::result Active::react(const ActMap&)
{
  return forward_event();
}

boost::statechart::result Active::react(const MNotifyRec& notevt)
{
  logger().info("Active <MNotifyRec>");
  auto& pg = context<Machine>().pg;
  pg.proc_replica_info(notevt.from,
		       notevt.notify.info,
		       notevt.notify.epoch_sent);
  return discard_event();
}

boost::statechart::result Active::react(const MInfoRec& infoevt)
{
  logger().info("Active <MInfoRec>");
  auto& pg = context<Machine>().pg;
  assert(pg.is_primary());

  if (pg.is_last_activated_peer(infoevt.from)) {
    post_event(AllReplicasActivated{});
  }
  return discard_event();
}

boost::statechart::result Active::react(const MLogRec& logevt)
{
  logger().info("Active <MLogRec>");
  auto& pg = context<Machine>().pg;
  pg.proc_replica_log(logevt.from,
		      logevt.msg->info,
		      logevt.msg->log,
		      logevt.msg->missing);
  // todo
  return discard_event();
}

boost::statechart::result Active::react(const AllReplicasActivated &evt)
{
  logger().info("Active <AllReplicasActivated>");
  all_replicas_activated = true;
  auto& pg = context<Machine>().pg;
  pg.clear_state(PG_STATE_ACTIVATING);
  pg.clear_state(PG_STATE_CREATING);
  pg.on_activated();
  pg.share_pg_info();
  post_event(AllReplicasRecovered{});
  return discard_event();
}

void Active::exit()
{
  auto& pg = context<Machine>().pg;
  pg.clear_state(PG_STATE_ACTIVATING);
  pg.clear_state(PG_STATE_DEGRADED);
  pg.clear_state(PG_STATE_UNDERSIZED);

  logger().info("Leaving Active");
}

/*------Activating--------*/
Activating::Activating(my_context ctx)
  : my_base(ctx)
{
  logger().info("Entering Activating");
}

void Activating::exit()
{
  logger().info("Leaving Activating");
}

/*------ReplicaActive-----*/
ReplicaActive::ReplicaActive(my_context ctx)
  : my_base(ctx)
{
  logger().info("Entering ReplicaActive");
}


boost::statechart::result ReplicaActive::react(const Activate& actevt)
{
  auto& pg = context<Machine>().pg;
  logger().info("In ReplicaActive, about to call activate");
  pg.activate(actevt.activation_epoch);
  logger().info("Activate Finished");
  return discard_event();
}

boost::statechart::result ReplicaActive::react(const MInfoRec& infoevt)
{
  return discard_event();
}

boost::statechart::result ReplicaActive::react(const MLogRec& logevt)
{
  return discard_event();
}

boost::statechart::result ReplicaActive::react(const ActMap&)
{
  auto& pg = context<Machine>().pg;
  if (pg.should_send_notify()) {
    context<Machine>().send_notify(pg.get_primary(),
				   pg.get_notify(pg.get_osdmap_epoch()),
				   pg.get_past_intervals());
  };
  return discard_event();
}

boost::statechart::result ReplicaActive::react(const MQuery& query)
{
  auto& pg = context<Machine>().pg;
  context<Machine>().send_notify(query.from,
				 pg.get_notify(query.query_epoch),
				 pg.get_past_intervals());
  return discard_event();
}

void ReplicaActive::exit()
{}

/*---RepNotRecovering----*/
RepNotRecovering::RepNotRecovering(my_context ctx)
  : my_base(ctx)
{}

void RepNotRecovering::exit()
{}

/*-------Stray---*/
Stray::Stray(my_context ctx)
  : my_base(ctx)
{
  auto& pg = context<Machine>().pg;
  assert(!pg.is_primary());
  logger().info("{}, Entering Stray", pg);
}

boost::statechart::result Stray::react(const MLogRec& logevt)
{
  auto& pg = context<Machine>().pg;
  logger().info("{} Stray <MLogRec>", pg);
  MOSDPGLog* msg = logevt.msg.get();
  logger().info("got info+log from osd.{} {} {}",
                logevt.from, msg->info, msg->log);
  if (msg->info.last_backfill == hobject_t()) {
    // todo: restart backfill
  } else {
    // todo: merge log
  }
  post_event(Activate{logevt.msg->info.last_epoch_started});
  return transit<ReplicaActive>();
}

boost::statechart::result Stray::react(const MInfoRec& infoevt)
{
  auto& pg = context<Machine>().pg;
  logger().info("{} Stray <MInfoRec>", pg);
  logger().info("got info from osd.{} {}",
                infoevt.from, infoevt.info);
  // todo
  post_event(Activate{infoevt.info.last_epoch_started});
  return transit<ReplicaActive>();
}

boost::statechart::result Stray::react(const MQuery& query)
{
  auto& pg = context<Machine>().pg;
  logger().info("{} Stray <MQuery>", pg);
  pg.reply_pg_query(query, context<Machine>().get_context());
  return discard_event();
}

boost::statechart::result Stray::react(const ActMap&)
{
  auto& pg = context<Machine>().pg;
  logger().info("{} Stray <ActMap>", pg);
  if (pg.should_send_notify()) {
    context<Machine>().send_notify(pg.get_primary(),
				   pg.get_notify(pg.get_osdmap_epoch()),
				   pg.get_past_intervals());
  }
  return discard_event();
}

void Stray::exit()
{
  logger().info("Leaving Stray");
}

/*------Down--------*/
Down::Down(my_context ctx)
  : my_base(ctx)
{}

void Down::exit()
{
}

boost::statechart::result Down::react(const MNotifyRec& infoevt)
{
  // todo
  return discard_event();
}

/*------GetMissing--------*/
GetMissing::GetMissing(my_context ctx)
  : my_base(ctx)
{
  logger().info("Entering GetMissing");
  auto& pg = context<Machine>().pg;
  if (pg.get_need_up_thru() != 0) {
    logger().info("still need up_thru update before going active");
    post_event(NeedUpThru{});
  } else {
    // all good!
    post_event(Activate{pg.get_osdmap_epoch()});
  }
}

boost::statechart::result GetMissing::react(const MLogRec& logevt)
{
  logger().info("GetMissing <MLogRec>");
  auto& pg = context<Machine>().pg;
  pg.proc_replica_log(logevt.from,
		      logevt.msg->info,
		      logevt.msg->log,
		      logevt.msg->missing);
  if (pg.get_need_up_thru() != 0) {
    logger().info(" still need up_thru update before going active");
    post_event(NeedUpThru{});
  } else {
    logger().info("Got last missing, don't need missing posting Activate");
    post_event(Activate{pg.get_osdmap_epoch()});
  }
  return discard_event();
}

void GetMissing::exit()
{
  logger().info("Leaving GetMissing");
}

/*------WaitUpThru--------*/
WaitUpThru::WaitUpThru(my_context ctx)
  : my_base(ctx)
{
  logger().info("Entering WaitUpThru");
}

boost::statechart::result WaitUpThru::react(const ActMap& am)
{
  logger().info("WaitUpThru <ActMap>");
  auto& pg = context<Machine>().pg;
  if (!pg.get_need_up_thru()) {
    logger().info("WaitUpThru: no need up thru!");
    post_event(Activate{pg.get_osdmap_epoch()});
  }
  return forward_event();
}

boost::statechart::result WaitUpThru::react(const MLogRec& logevt)
{
  logger().info("WaitUpThru: <MLogRec>");
  auto& pg = context<Machine>().pg;
  pg.proc_replica_log(logevt.from,
		      logevt.msg->info,
		      logevt.msg->log,
		      logevt.msg->missing);
  return discard_event();
}

void WaitUpThru::exit()
{
  logger().info("Leaving WaitUpThru");
}

}
