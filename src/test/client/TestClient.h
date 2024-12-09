// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gtest/gtest.h"

#include "common/async/context_pool.h"
#include "global/global_context.h"

#include "msg/Messenger.h"
#include "mon/MonClient.h"
#include "osdc/ObjectCacher.h"
#include "osdc/Objecter.h"
#include "client/MetaRequest.h"
#include "client/Client.h"
#include "messages/MClientReclaim.h"
#include "messages/MClientSession.h"
#include "common/async/blocked_completion.h"

#define dout_subsys ceph_subsys_client

namespace bs = boost::system;
namespace ca = ceph::async;

class ClientScaffold : public Client {  
public:
    ClientScaffold(Messenger *m, MonClient *mc, Objecter *objecter_) : Client(m, mc, objecter_) {}
    virtual ~ClientScaffold()
    { }
    int check_dummy_op(const UserPerm& perms){
      RWRef_t mref_reader(mount_state, CLIENT_MOUNTING);
      if (!mref_reader.is_state_satisfied()) {
        return -CEPHFS_ENOTCONN;
      }
      std::scoped_lock l(client_lock);
      MetaRequest *req = new MetaRequest(CEPH_MDS_OP_DUMMY);
      int res = make_request(req, perms);
      ldout(cct, 10) << __func__ << " result=" << res << dendl;
      return res;
    }
    int send_unknown_session_op(int op) {
      RWRef_t mref_reader(mount_state, CLIENT_MOUNTING);
      if (!mref_reader.is_state_satisfied()) {
        return -CEPHFS_ENOTCONN;
      }
      std::scoped_lock l(client_lock);
      auto session = _get_or_open_mds_session(0);
      auto msg = make_message<MClientSession>(op, session->seq);
      int res = session->con->send_message2(std::move(msg));
      ldout(cct, 10) << __func__ << " result=" << res << dendl;
      return res;
    }
    bool check_client_blocklisted() {
      RWRef_t mref_reader(mount_state, CLIENT_MOUNTING);
      if (!mref_reader.is_state_satisfied()) {
        return -CEPHFS_ENOTCONN;
      }
      std::scoped_lock l(client_lock);
      bs::error_code ec;
      ldout(cct, 20) << __func__ << ": waiting for latest osdmap" << dendl;
      objecter->wait_for_latest_osdmap(ca::use_blocked[ec]);
      ldout(cct, 20) << __func__ << ": got latest osdmap: " << ec << dendl;
      const auto myaddrs = messenger->get_myaddrs();
      return objecter->with_osdmap([&](const OSDMap& o) {return o.is_blocklisted(myaddrs);});
    }
    bool check_unknown_reclaim_flag(uint32_t flag) {
      RWRef_t mref_reader(mount_state, CLIENT_MOUNTING);
      if (!mref_reader.is_state_satisfied()) {
        return -CEPHFS_ENOTCONN;
      }
      std::scoped_lock l(client_lock);
      char uuid[256];
      sprintf(uuid, "unknownreclaimflag:%x", getpid());
      auto session = _get_or_open_mds_session(0);
      auto m = make_message<MClientReclaim>(uuid, flag);
      ceph_assert(session->con->send_message2(std::move(m)) == 0);
      wait_on_list(waiting_for_reclaim);
      return session->reclaim_state == MetaSession::RECLAIM_FAIL ? true : false;
    }
};

class TestClient : public ::testing::Test {
public:
    static void SetUpTestSuite() {
      icp.start(g_ceph_context->_conf.get_val<std::uint64_t>("client_asio_thread_count"));
    }
    static void TearDownTestSuite() {
      icp.stop();
    }
    void SetUp() override {
      messenger = Messenger::create_client_messenger(g_ceph_context, "client");
      if (messenger->start() != 0) {
        throw std::runtime_error("failed to start messenger");
      }

      mc = new MonClient(g_ceph_context, icp);
      if (mc->build_initial_monmap() < 0) {
        throw std::runtime_error("build monmap");
      }
      mc->set_messenger(messenger);
      mc->set_want_keys(CEPH_ENTITY_TYPE_MDS | CEPH_ENTITY_TYPE_OSD);
      if (mc->init() < 0) {
        throw std::runtime_error("init monclient");
      }

      objecter = new Objecter(g_ceph_context, messenger, mc, icp);
      objecter->set_client_incarnation(0);
      objecter->init();
      messenger->add_dispatcher_tail(objecter);
      objecter->start();

      client = new ClientScaffold(messenger, mc, objecter);
      client->init();
      client->mount("/", myperm, true);
    }
    void TearDown() override {
      if (client->is_mounted())
        client->unmount();
      client->shutdown();
      objecter->shutdown();
      mc->shutdown();
      messenger->shutdown();
      messenger->wait();

      delete client;
      client = nullptr;
      delete objecter;
      objecter = nullptr;
      delete mc;
      mc = nullptr;
      delete messenger;
      messenger = nullptr;
    }
protected:
    static inline ceph::async::io_context_pool icp;
    static inline UserPerm myperm{0,0};
    MonClient* mc = nullptr;
    Messenger* messenger = nullptr;
    Objecter* objecter = nullptr;
    ClientScaffold* client = nullptr;
};
