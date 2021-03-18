// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Copyright(c) 2020, Intel Corporation
 *
 * Author: Changcheng Liu <changcheng.liu@aliyun.com>
 */

#include "ReplicaMonitor.h"
#include "messages/MReplicaDaemonMap.h"
#include "messages/MReplicaDaemonBlink.h"
#include "messages/MMonGetReplicaDaemonMap.h"
#include "common/cmdparse.h"

using TOPNSPC::common::bad_cmd_get;

#define FN_NAME (__CEPH_ASSERT_FUNCTION == nullptr ? __func__ : __CEPH_ASSERT_FUNCTION)
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cache_replica
#undef dout_prefix
#define dout_prefix _prefix(_dout, FN_NAME, mon, this)
static ostream& _prefix(std::ostream *_dout,
                        std::string_view func_name,
                        const Monitor& mon,
                        const ReplicaMonitor *replica_monitor) {
  return *_dout << func_name << ": " << "mon." << mon.name << "@" << mon.rank
                << "(" << mon.get_state_name() << ").ReplicaMonitor: ";
}

ReplicaMonitor::ReplicaMonitor(Monitor& monitor, Paxos& paxos, std::string service_name)
  : PaxosService(monitor, paxos, service_name)
{
}

void ReplicaMonitor::init()
{
  dout(10) << dendl;
}

void ReplicaMonitor::create_initial()
{
  cur_cache_replicadaemon_map.set_epoch(0);
  pending_cache_replicadaemon_map = cur_cache_replicadaemon_map;
  pending_cache_replicadaemon_map.set_epoch(cur_cache_replicadaemon_map.get_epoch() + 1);
}

void ReplicaMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t latest_commit_version = get_last_committed();
  auto cur_commited_version = cur_cache_replicadaemon_map.get_epoch();
  ceph_assert(latest_commit_version >= cur_commited_version);

  if (latest_commit_version == cur_commited_version) {
    return;
  }

  dout(10) << "lastest commit epoch: " << latest_commit_version
           << ", ReplicaDaemonMap epoch: " << cur_commited_version << dendl;

  load_health();

  // read ReplicaDaemonMap
  bufferlist cache_replicadaemon_map_bl;
  int err = get_version(latest_commit_version, cache_replicadaemon_map_bl);
  ceph_assert(err == 0);

  ceph_assert(cache_replicadaemon_map_bl.length() > 0);
  dout(10) << "got " << latest_commit_version << dendl;

  decode_replicadaemon_map(cache_replicadaemon_map_bl);

  // output new ReplicaDaemonMap
  print_map<10>(cur_cache_replicadaemon_map);

  check_subs();
}

void ReplicaMonitor::create_pending()
{
  pending_cache_replicadaemon_map = cur_cache_replicadaemon_map;
  pending_cache_replicadaemon_map.set_epoch(cur_cache_replicadaemon_map.get_epoch() + 1);
  dout(10) << "cur_cache epoch: " << cur_cache_replicadaemon_map.get_epoch() << ", "
           << "pening_cache epoch: " << pending_cache_replicadaemon_map.get_epoch()
           << dendl;
}

void ReplicaMonitor::encode_pending(MonitorDBStore::TransactionRef mon_dbstore_tran)
{
  version_t version = get_last_committed() + 1;

#if 0
  if (pending_cache_replicadaemon_map.get_epoch() != version) {
    derr << "should not update the db, always continue here, need refine" << dendl;
  }
  ceph_assert(version == pending_cache_replicadaemon_map.get_epoch());
#endif

  // apply to paxos
  bufferlist pending_bl;
  pending_cache_replicadaemon_map.encode(pending_bl, mon.get_quorum_con_features());

  // clear pending ReplicaDaemonMap after being encoded into pending_bl
  pending_cache_replicadaemon_map = {};

  // put everything in the transaction
  put_version(mon_dbstore_tran, version, pending_bl);
  put_last_committed(mon_dbstore_tran, version);
}

void ReplicaMonitor::encode_full(MonitorDBStore::TransactionRef mon_dbstore_tran)
{
// Empty function
}

bool ReplicaMonitor::preprocess_query(MonOpRequestRef mon_op_req)
{
  auto replica_msg = mon_op_req->get_req<PaxosServiceMessage>();

  switch (replica_msg->get_type()) {
  case MSG_MON_COMMAND:
    try {
      return preprocess_command(mon_op_req);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon.reply_command(mon_op_req, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }

  case MSG_REPLICADAEMON_BLINK:
    return preprocess_blink(mon_op_req);

  case CEPH_MSG_MON_GET_REPLICADAEMONMAP:
    return false;

  default:
    ceph_abort();
    return true;
  }

  return false;
}

bool ReplicaMonitor::prepare_update(MonOpRequestRef mon_op_req)
{
  auto replica_msg = mon_op_req->get_req<PaxosServiceMessage>();

  switch (replica_msg->get_type()) {
  case MSG_MON_COMMAND:
    try {
      return prepare_command(mon_op_req);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon.reply_command(mon_op_req, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }

  case MSG_REPLICADAEMON_BLINK:
    return prepare_blink(mon_op_req);

  case CEPH_MSG_MON_GET_REPLICADAEMONMAP:
    return get_replicadaemon_map(mon_op_req);

  default:
    mon.no_reply(mon_op_req);
    derr << "Unhandled message type " << replica_msg->get_type() << dendl;
    return true;
  }
  return false;
}

bool ReplicaMonitor::prepare_blink(MonOpRequestRef mon_op_req)
{
  auto blink_msg = mon_op_req->get_req<MReplicaDaemonBlink>();

  bool peer_updated = true;
  const auto& recv_replicadaemon_state = blink_msg->get_replicadaemon_stateref();
#if 0
  if (recv_replicadaemon_state.daemon_status == STATE_BOOTING) {
    blink_msg->update_replicadaemon_status(STATE_ACTIVE);
    peer_updated = true;
  } else if (recv_replicadaemon_state.daemon_status == STATE_STOPPING) {
    blink_msg->update_replicadaemon_status(STATE_DOWN);
    peer_updated = true;
  }
#endif

  if (peer_updated == false) {
    return false;
  }

  auto temp_replicadaemon_map = cur_cache_replicadaemon_map;
  temp_replicadaemon_map.update_daemonmap(recv_replicadaemon_state);

  pending_cache_replicadaemon_map = std::move(temp_replicadaemon_map);

  return true;
}

bool ReplicaMonitor::prepare_command(MonOpRequestRef mon_op_req)
{
  // TODO: deal with replica module commands
  return false;
}

bool ReplicaMonitor::preprocess_blink(MonOpRequestRef mon_op_req)
{
  auto session = mon_op_req->get_session();
  mon.no_reply(mon_op_req);
  if (!session) {
    dout(10) << __func__ << "no monitor session!" << dendl;
    return true;
  }

  // forward blink to the leader's prepare_beacon
  if (!is_leader()) {
    return false;
  }

  return false;
}

bool ReplicaMonitor::preprocess_command(MonOpRequestRef mon_op_req)
{
  // TODO: deal with replica module commands
  return false;
}

bool ReplicaMonitor::should_propose(double& delay)
{
  if (!pending_cache_replicadaemon_map.empty()) {
    return true;
  }

  return PaxosService::should_propose(delay);
}

bool ReplicaMonitor::get_replicadaemon_map(MonOpRequestRef mon_op_req)
{
  auto get_replica_msg = mon_op_req->get_req<MMonGetReplicaDaemonMap>();
  auto const& req_daemon_info = get_replica_msg->req_daemon_info;

  // construct reply message
  auto reply_daemons_info = cur_cache_replicadaemon_map.get_replica_daemons(req_daemon_info.replicas, req_daemon_info.replica_size);
  ReplicaDaemonMap replica_daemon_map(reply_daemons_info);

  ReplicaDaemonMap *replicadaemon_map = &replica_daemon_map;
  auto reply_msg = make_message<MReplicaDaemonMap>(*replicadaemon_map);
  mon.send_reply(mon_op_req, reply_msg.detach());

  //TODO: 1. update ReplicaDaemonMap 2. go through paxos

  return true;
}

void ReplicaMonitor::on_restart()
{
// TODO: Clear the pending map
}

void ReplicaMonitor::check_subs()
{
  auto& all_subs = mon.session_map.subs;
  auto replicamap_subs_it = all_subs.find("replicamap");
  if (replicamap_subs_it == all_subs.end()) {
    return;
  }
  auto replicamap_sub_it = replicamap_subs_it->second->begin();
  while (!replicamap_sub_it.end()) {
    auto replicamap_sub = *replicamap_sub_it;
    ++replicamap_sub_it;
    check_sub(replicamap_sub);
  }
}

void ReplicaMonitor::check_sub(Subscription *sub)
{
  // Only support subscribe "replicamap"
  ceph_assert(sub->type == "replicamap");

  ReplicaDaemonMap *replicadaemon_map = nullptr;
  ReplicaDaemonMap reply_map;
  //TODO build reply_map according to cur_cache_replicadaemon_map;
  replicadaemon_map = &cur_cache_replicadaemon_map;

  //reply subscription
  auto reply_msg = make_message<MReplicaDaemonMap>(*replicadaemon_map);
  sub->session->con->send_message(reply_msg.detach());
  if (sub->onetime) {
    mon.session_map.remove_sub(sub);
  } else {
    sub->next = cur_cache_replicadaemon_map.get_epoch() + 1;
  }
}

void ReplicaMonitor::decode_replicadaemon_map(bufferlist &replicadaemon_map_bl)
{
  cur_cache_replicadaemon_map.decode(replicadaemon_map_bl);
}

template<int dbg_level>
void ReplicaMonitor::print_map(const ReplicaDaemonMap& replicadaemon_map) const
{
  dout(dbg_level) << "print map\n";
  replicadaemon_map.print_map(*_dout);
  *_dout << dendl;
}
