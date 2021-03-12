// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Copyright(c) 2020, Intel Corporation
 *
 * Author: Changcheng Liu <changcheng.liu@aliyun.com>
 */

#include "include/ceph_assert.h"
#include "ReplicaDaemon.h"
#include "messages/MReplicaDaemonMap.h"
#include "messages/MReplicaDaemonBlink.h"

#define FN_NAME (__CEPH_ASSERT_FUNCTION == nullptr ? __func__ : __CEPH_ASSERT_FUNCTION)
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cache_replica
#undef dout_prefix
#define dout_prefix *_dout << "replica." << name << ' ' << FN_NAME << " : "

ReplicaDaemon::ReplicaDaemon(std::string_view name,
                             int32_t daemon_id,
                             std::string& rnic_addr,
                             Messenger *msgr_public,
                             MonClient *mon_client,
                             boost::asio::io_context& ioctx) :
  Dispatcher(msgr_public->cct),
  name(name),
  msgr_public(msgr_public),
  mon_client(mon_client),
  self_state{0, daemon_id, -1, rnic_addr, 0},
  ioctx(ioctx),
  log_client(msgr_public->cct, msgr_public, &mon_client->monmap, LogClient::NO_FLAGS),
  clog(log_client.create_channel()),
  replicadaemon_map(new ReplicaDaemonMap)
{
}

bool ReplicaDaemon::clean_cache()
{
  // TODO
  self_state.set_free_size(-1);
  return true;
}
bool ReplicaDaemon::init_connection()
{
  // TODO
  self_state.set_rnic_bind_port(-1);
  return true;
}

int ReplicaDaemon::init()
{
  msgr_public->add_dispatcher_tail(this);

  //init monc_client
  mon_client->set_messenger(msgr_public);
  int r = mon_client->init();
  if (r < 0) {
    derr << "ERROR: failed to init monc: " << cpp_strerror(-r) << dendl;
    return r;
  }

  // Note: msgr_public auth_client has been already set up through mon_client->init();
  msgr_public->set_auth_server(mon_client);
  mon_client->set_handle_authentication_dispatcher(this);

  // tell mon_client about log_clien, so it will known about monitor session resets.
  mon_client->set_log_client(&log_client);

  // authenticate and create active connection with monitor
  r = mon_client->authenticate();
  if (r < 0) {
    derr << "ERROR: failed to authenticate: " << cpp_strerror(-r) << dendl;
    return r;
  }
  ceph_assert(mon_client->is_connected());

  ceph_assert(clean_cache());
  ceph_assert(init_connection());

  mon_client->sub_want("replicamap", 0, 0); // do we still need replicamap???
  mon_client->renew_subs(); // does daemon need to subscribe the replicamap with all ReplicaDaemons's info
  MReplicaDaemonBlink *blink_msg = new MReplicaDaemonBlink(self_state); // reconstruct the message which should include: 1) ID 2) RNIC IP+PORT 3) Size 4) free_size (no need to report free_size currently)
  mon_client->send_mon_message(blink_msg); // send the second(first) message to ReplicaMonitor

  return 0;
}

//parent implement: ceph_abort
void ReplicaDaemon::ms_fast_dispatch(Message *m)
{
  derr << "TODO: deal with received message" << dendl;
}

bool ReplicaDaemon::ms_dispatch(Message *m)
{
  switch (m->get_type()) {
  case CEPH_MSG_REPLICADAEMON_MAP:
    {
    auto replicadaemon_map_msg = static_cast<MReplicaDaemonMap*>(m);
    auto replicadaemon_map_ref = replicadaemon_map_msg->get_map();
    }
    break;
  default:
    ceph_abort();
  }
  return true;
}

//parent pure virtual function
bool ReplicaDaemon::ms_handle_reset(Connection *con)
{
  return false;
}

//parent pure virtual function
void ReplicaDaemon::ms_handle_remote_reset(Connection *con)
{
  return;
}

//parent pure virtual function
bool ReplicaDaemon::ms_handle_refused(Connection *con)
{
  return false;
}
