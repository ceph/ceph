// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2014 Red Hat
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <sstream>
#include <time.h>
#include <stdlib.h>
#include <map>

#include "global/global_init.h"
#include "global/global_context.h"
#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "common/version.h"
#include "common/dout.h"
#include "common/debug.h"
#include "common/ceph_mutex.h"
#include "common/Timer.h"
#include "common/errno.h"
#include "mon/MonClient.h"
#include "msg/Dispatcher.h"
#include "include/err.h"
#include <boost/scoped_ptr.hpp>

#include "gtest/gtest.h"

#include "common/config.h"
#include "include/ceph_assert.h"

#include "messages/MMonProbe.h"
#include "messages/MRoute.h"
#include "messages/MGenericMessage.h"
#include "messages/MMonJoin.h"
#include "messages/MMonSubscribe.h"
#include "messages/MKVData.h"
#include "messages/MMDSMap.h"
#include "messages/MOSDMap.h"
#include "messages/MFSMap.h"
#include "messages/MOSDPGCreate2.h"
#include "auth/KeyRing.h"
#include "common/Cond.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_
#undef dout_prefix
#define dout_prefix *_dout << "test-mon-msg "

using namespace std;

class MonClientHelper : public Dispatcher
{
protected:
  CephContext *cct;
  ceph::async::io_context_pool poolctx;
  Messenger *msg;
  MonClient monc;

  ceph::mutex lock = ceph::make_mutex("mon-msg-test::lock");
  ceph::condition_variable cond;

  set<int> wanted;
  int reply_type = 0;
  Message *reply_msg = nullptr;

public:

  explicit MonClientHelper(CephContext *cct_)
    : Dispatcher(cct_),
      cct(cct_),
      poolctx(1),
      msg(NULL),
      monc(cct_, poolctx)
  { }

  ~MonClientHelper() override {
    if (reply_msg) {
      reply_msg->put();
      reply_msg = nullptr;
    }
  }

  int post_init() {
    dout(1) << __func__ << dendl;
    if (!msg)
      return -EINVAL;
    msg->add_dispatcher_tail(this);
    return 0;
  }

  int init_messenger() {
    dout(1) << __func__ << dendl;

    std::string public_msgr_type = cct->_conf->ms_public_type.empty() ? cct->_conf.get_val<std::string>("ms_type") : cct->_conf->ms_public_type;
    msg = Messenger::create(cct, public_msgr_type, entity_name_t::CLIENT(-1),
                            "test-mon-msg", 0);
    ceph_assert(msg != NULL);
    msg->set_default_policy(Messenger::Policy::lossy_client(0));
    dout(0) << __func__ << " starting messenger at "
            << msg->get_myaddrs() << dendl;
    msg->start();
    return 0;
  }

  int init_monc() {
    dout(1) << __func__ << dendl;
    ceph_assert(msg != NULL);
    int err = monc.build_initial_monmap();
    if (err < 0) {
      derr << __func__ << " error building monmap: "
           << cpp_strerror(err) << dendl;
      return err;
    }

    monc.set_messenger(msg);
    msg->add_dispatcher_head(&monc);

    monc.set_want_keys(CEPH_ENTITY_TYPE_MON);
    err = monc.init();
    if (err < 0) {
      derr << __func__ << " monc init failed: "
           << cpp_strerror(err) << dendl;
      goto fail;
    }

    err = monc.authenticate();
    if (err < 0) {
      derr << __func__ << " monc auth failed: "
           << cpp_strerror(err) << dendl;
      goto fail_monc;
    }
    monc.wait_auth_rotating(30.0);
    monc.renew_subs();
    dout(0) << __func__ << " finished" << dendl;
    return 0;

fail_monc:
    derr << __func__ << " failing monc" << dendl;
    monc.shutdown();
fail:
    return err;
  }

  void shutdown_messenger() {
    dout(0) << __func__ << dendl;
    msg->shutdown();
    msg->wait();
  }

  void shutdown_monc() {
    dout(0) << __func__ << dendl;
    monc.shutdown();
  }

  void shutdown() {
    dout(0) << __func__ << dendl;
    shutdown_monc();
    shutdown_messenger();
  }

  MonMap *get_monmap() {
    return &monc.monmap;
  }

  int init() {
    int err = init_messenger();
    if (err < 0)
      goto fail;
    err = init_monc();
    if (err < 0)
      goto fail_msgr;
    err = post_init();
    if (err < 0)
      goto fail_monc;
    return 0;
fail_monc:
    shutdown_monc();
fail_msgr:
    shutdown_messenger();
fail:
    return err;
  }

  virtual void handle_wanted(Message *m) {
    std::lock_guard l{lock};
    // caller will put() after they call us, so hold on to a ref
    m->get();
    if (reply_msg) {
      reply_msg->put();
    }
    reply_msg = m;
    cond.notify_all();
  }

  Message *send_wait_reply(Message *m, int t, double timeout=30.0) {
    std::unique_lock l{lock};
    reply_type = t;
    if (reply_msg) {
      reply_msg->put();
      reply_msg = nullptr;
    }
    add_wanted(t);
    send_message(m);

    std::cv_status status = std::cv_status::no_timeout;
    if (timeout > 0) {
      utime_t s = ceph_clock_now();
      status = cond.wait_for(l, ceph::make_timespan(timeout));
      utime_t e = ceph_clock_now();
      dout(20) << __func__ << " took " << (e-s) << " seconds" << dendl;
    } else {
      cond.wait(l);
    }
    rm_wanted(t);
    l.unlock();
    if (status == std::cv_status::timeout) {
      dout(20) << __func__ << " error: " << cpp_strerror(ETIMEDOUT) << dendl;
      return (Message*)((long)-ETIMEDOUT);
    }

    if (!reply_msg)
      dout(20) << __func__ << " reply_msg is nullptr" << dendl;
    else
      dout(20) << __func__ << " reply_msg " << *reply_msg << dendl;

    Message *ret = reply_msg;
    reply_msg = nullptr;
    return ret;
  }

  bool handle_message(Message *m) {
    dout(1) << __func__ << " " << *m << dendl;
    if (!is_wanted(m)) {
      dout(10) << __func__ << " not wanted" << dendl;
      return false;
    }
    handle_wanted(m);
    m->put();

    return true;
  }

  bool ms_dispatch(Message *m) override {
    return handle_message(m);  
  }
  void ms_handle_connect(Connection *con) override { }
  void ms_handle_remote_reset(Connection *con) override { }
  bool ms_handle_reset(Connection *con) override { return false; }
  bool ms_handle_refused(Connection *con) override { return false; }

  bool is_wanted(Message *m) {
    dout(20) << __func__ << " " << *m << " type " << m->get_type() << dendl;
    return (wanted.find(m->get_type()) != wanted.end());
  }

  void add_wanted(int t) {
    dout(20) << __func__ << " type " << t << dendl;
    wanted.insert(t);
  }

  void rm_wanted(int t) {
    dout(20) << __func__ << " type " << t << dendl;
    wanted.erase(t);
  }

  void send_message(Message *m) {
    dout(15) << __func__ << " " << *m << dendl;
    monc.send_mon_message(m);
  }

  void wait() { msg->wait(); }
};

class MonMsgTest : public MonClientHelper,
                   public ::testing::Test
{
protected:
  MonMsgTest() :
    MonClientHelper(g_ceph_context) { }

public:
  void SetUp() override {
    reply_type = -1;
    if (reply_msg) {
      reply_msg->put();
      reply_msg = nullptr;
    }
    ASSERT_EQ(init(), 0);
  }

  void TearDown() override {
    shutdown();
    if (reply_msg) {
      reply_msg->put();
      reply_msg = nullptr;
    }
  }
};

TEST_F(MonMsgTest, MMonProbeTest)
{
  Message *m = new MMonProbe(get_monmap()->fsid,
			     MMonProbe::OP_PROBE, "b", false,
			     ceph_release());
  Message *r = send_wait_reply(m, MSG_MON_PROBE);
  ASSERT_NE(IS_ERR(r), 0);
  ASSERT_EQ(PTR_ERR(r), -ETIMEDOUT);
}

TEST_F(MonMsgTest, MRouteTest)
{
  Message *payload = new MGenericMessage(CEPH_MSG_SHUTDOWN);
  MRoute *m = new MRoute;
  m->msg = payload;
  Message *r = send_wait_reply(m, CEPH_MSG_SHUTDOWN);
  // we want an error
  ASSERT_NE(IS_ERR(r), 0);
  ASSERT_EQ(PTR_ERR(r), -ETIMEDOUT);
}

/* MMonScrub and MMonSync have other safeguards in place that prevent
 * us from actually receiving a reply even if the message is handled
 * by the monitor due to lack of cap checking.
 */
TEST_F(MonMsgTest, MMonJoin)
{
  Message *m = new MMonJoin(get_monmap()->fsid, string("client"),
                            msg->get_myaddrs());
  send_wait_reply(m, MSG_MON_PAXOS, 10.0);

  int r = monc.get_monmap();
  ASSERT_EQ(r, 0);
  ASSERT_FALSE(monc.monmap.contains("client"));
}

class MonAuthBypassTest : public MonMsgTest {
protected:
  boost::intrusive_ptr<CephContext> cct2;
  MonClientHelper* helper2 = nullptr;
  std::string entity;

  void setup_low_priv_client(const std::string& ent, const std::string& caps) {
    entity = ent;
    std::vector<std::string> cmd = {
      "{\"prefix\": \"auth get-or-create-key\", \"entity\": \"client." + entity + "\", \"caps\": " + caps + "}"
    };
    bufferlist inbl, outbl;
    string outs;
    C_SaferCond cond_cmd;
    monc.start_mon_command(std::move(cmd), std::move(inbl), &outbl, &outs, &cond_cmd);
    ASSERT_EQ(0, cond_cmd.wait());

    string key_str = outbl.to_str();
    key_str.erase(key_str.find_last_not_of(" \n\r\t") + 1);

    CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
    iparams.name.set(CEPH_ENTITY_TYPE_CLIENT, entity.c_str());
    cct2 = boost::intrusive_ptr<CephContext>{common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0), false};
    cct2->_conf.set_val("key", key_str);
    cct2->_conf.set_val("debug_ms", "1");
    cct2->_conf.set_val("debug_auth", "20");
    cct2->_conf.set_val("debug_monc", "20");
    cct2->_conf.set_val("log_to_file", "false");
    cct2->_conf.set_val("log_to_stderr", "true");
    cct2->_conf.set_val("err_to_stderr", "true");
    cct2->_conf.set_val("mon_host", g_ceph_context->_conf.get_val<std::string>("mon_host"));
    cct2->_conf.apply_changes(nullptr);

    cct2->_log->start();
    common_init_finish(cct2.get());

    helper2 = new MonClientHelper(cct2.get());
    ASSERT_EQ(0, helper2->init());
  }

  void teardown_low_priv_client() {
    if (helper2) {
      helper2->shutdown();
      delete helper2;
      helper2 = nullptr;
    }
    if (!entity.empty()) {
      std::vector<std::string> cmd = {
        "{\"prefix\": \"auth rm\", \"entity\": \"client." + entity + "\"}"
      };
      bufferlist inbl, outbl;
      string outs;
      C_SaferCond cond_cmd;
      monc.start_mon_command(std::move(cmd), std::move(inbl), &outbl, &outs, &cond_cmd);
      EXPECT_EQ(0, cond_cmd.wait());
      entity.clear();
    }
    if (cct2) {
      cct2.reset();
    }
  }

  void TearDown() override {
    teardown_low_priv_client();
    MonMsgTest::TearDown();
  }
};

TEST_F(MonAuthBypassTest, MMonSubscribeKV_AuthBypass)
{
  setup_low_priv_client("test_kv_sub", "[\"mon\", \"allow r, allow service osd r, allow service mds r\"]");

  std::vector<std::string> cmd = {
    "{\"prefix\": \"config-key set\", \"key\": \"test_secret\", \"val\": \"super_secret_value\"}"
  };
  bufferlist inbl, outbl;
  string outs;
  C_SaferCond cond_cmd2;
  monc.start_mon_command(std::move(cmd), std::move(inbl), &outbl, &outs, &cond_cmd2);
  ASSERT_EQ(0, cond_cmd2.wait());

  auto m = new MMonSubscribe();
  m->what["kv:"] = ceph_mon_subscribe_item();
  m->what["kv:"].start = 0;
  m->what["kv:"].flags = 0;

  auto dummy_msg = new MKVData();
  int msg_kv_data_type = dummy_msg->get_type();
  dummy_msg->put();

  Message *reply = helper2->send_wait_reply(m, msg_kv_data_type, 5.0);

  if (!IS_ERR(reply)) {
    auto kv_reply = static_cast<MKVData*>(reply);

    bool found_key = false;
    std::string found_val;

    for (const auto& pair : kv_reply->data) {
      if (pair.first.find("test_secret") != std::string::npos) {
        found_key = true;
        found_val = pair.second->to_str();
        break;
      }
    }

    if (found_key && found_val == "super_secret_value") {
      ADD_FAILURE() << "Vulnerability Explicitly Confirmed! Exfiltrated 'test_secret' with value: '"
                    << found_val << "' using only 'mon allow r' caps!";
    } else if (found_key) {
      ADD_FAILURE() << "Vulnerability present: Dumped key 'test_secret', but value was '"
                    << found_val << "'";
    } else {
      ADD_FAILURE() << "Vulnerability present (received MKVData payload), but 'test_secret' wasn't found in the "
                    << kv_reply->data.size() << " dumped keys.";
    }

    reply->put();
  } else {
    ASSERT_EQ(PTR_ERR(reply), -ETIMEDOUT);
  }

  cmd = {
    "{\"prefix\": \"config-key rm\", \"key\": \"test_secret\"}"
  };
  C_SaferCond cond_cmd4;
  outbl.clear();
  inbl.clear();
  monc.start_mon_command(std::move(cmd), std::move(inbl), &outbl, &outs, &cond_cmd4);
  ASSERT_EQ(0, cond_cmd4.wait());
}

TEST_F(MonAuthBypassTest, MMonSubscribeKV_Success)
{
  setup_low_priv_client("test_kv_success", "[\"mon\", \"allow r, allow service config-key r\"]");

  std::vector<std::string> cmd = {
    "{\"prefix\": \"config-key set\", \"key\": \"test_secret_success\", \"val\": \"super_secret_value_2\"}"
  };
  bufferlist inbl, outbl;
  string outs;
  C_SaferCond cond_cmd2;
  monc.start_mon_command(std::move(cmd), std::move(inbl), &outbl, &outs, &cond_cmd2);
  ASSERT_EQ(0, cond_cmd2.wait());

  auto m = new MMonSubscribe();
  m->what["kv:"] = ceph_mon_subscribe_item();
  m->what["kv:"].start = 0;
  m->what["kv:"].flags = 0;

  auto dummy_msg = new MKVData();
  int msg_kv_data_type = dummy_msg->get_type();
  dummy_msg->put();

  Message *reply = helper2->send_wait_reply(m, msg_kv_data_type, 5.0);

  if (!IS_ERR(reply)) {
    auto kv_reply = static_cast<MKVData*>(reply);

    bool found_key = false;
    std::string found_val;

    for (const auto& pair : kv_reply->data) {
      if (pair.first.find("test_secret_success") != std::string::npos) {
        found_key = true;
        found_val = pair.second->to_str();
        break;
      }
    }

    EXPECT_TRUE(found_key) << "Expected to find 'test_secret_success' but it was missing.";
    EXPECT_EQ(found_val, "super_secret_value_2");

    reply->put();
  } else {
    ADD_FAILURE() << "Expected to receive MKVData but got an error/timeout: " << PTR_ERR(reply);
  }

  cmd = {
    "{\"prefix\": \"config-key rm\", \"key\": \"test_secret_success\"}"
  };
  C_SaferCond cond_cmd4;
  outbl.clear();
  inbl.clear();
  monc.start_mon_command(std::move(cmd), std::move(inbl), &outbl, &outs, &cond_cmd4);
  ASSERT_EQ(0, cond_cmd4.wait());
}

TEST_F(MonAuthBypassTest, MMonSubscribeMDS_AuthBypass)
{
  setup_low_priv_client("test_mds_sub", "[\"mon\", \"allow service osd r, allow service mgr r\"]");

  auto m = new MMonSubscribe();
  m->what["mdsmap"] = ceph_mon_subscribe_item();
  m->what["mdsmap"].start = 0;
  m->what["mdsmap"].flags = 0;

  Message *reply = helper2->send_wait_reply(m, CEPH_MSG_MDS_MAP, 5.0);

  if (!IS_ERR(reply)) {
    reply->put();
    ADD_FAILURE() << "Vulnerability present: received MDSMap with insufficient caps!";
  } else {
    ASSERT_EQ(PTR_ERR(reply), -ETIMEDOUT);
  }
}

TEST_F(MonAuthBypassTest, MMonSubscribeOSD_AuthBypass)
{
  setup_low_priv_client("test_osd_sub", "[\"mon\", \"allow service mds r, allow service mgr r\"]");

  auto m = new MMonSubscribe();
  m->what["osdmap"] = ceph_mon_subscribe_item();
  m->what["osdmap"].start = 0;
  m->what["osdmap"].flags = 0;

  Message *reply = helper2->send_wait_reply(m, CEPH_MSG_OSD_MAP, 5.0);

  if (!IS_ERR(reply)) {
    reply->put();
    ADD_FAILURE() << "Vulnerability present: received OSDMap with only 'mon allow r' caps!";
  } else {
    ASSERT_EQ(PTR_ERR(reply), -ETIMEDOUT);
  }
}

TEST_F(MonAuthBypassTest, MMonSubscribeOSDPGCreates_AuthBypass)
{
  setup_low_priv_client("test_pg_sub", "[\"mon\", \"allow r, allow service mgr r\", \"osd\", \"allow r\"]");

  auto m = new MMonSubscribe();
  m->what["osd_pg_creates"] = ceph_mon_subscribe_item();
  m->what["osd_pg_creates"].start = 0;
  m->what["osd_pg_creates"].flags = 0;

  Message *reply = helper2->send_wait_reply(m, MSG_OSD_PG_CREATE2, 5.0);

  if (!IS_ERR(reply)) {
    reply->put();
    ADD_FAILURE() << "Vulnerability present: PG create sub bypassed cap checks!";
  } else {
    ASSERT_EQ(PTR_ERR(reply), -ETIMEDOUT);
  }
}

int main(int argc, char *argv[])
{
  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(nullptr, args,
			 CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf.apply_changes(nullptr);
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

