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
#include "common/ceph_argparse.h"
#include "common/version.h"
#include "common/dout.h"
#include "common/debug.h"
#include "common/Cond.h"
#include "common/Mutex.h"
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

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_
#undef dout_prefix
#define dout_prefix *_dout << "test-mon-msg "

class MonClientHelper : public Dispatcher
{
protected:
  CephContext *cct;
  Messenger *msg;
  MonClient monc;

  Mutex lock;

  set<int> wanted;

public:

  explicit MonClientHelper(CephContext *cct_)
    : Dispatcher(cct_),
      cct(cct_),
      msg(NULL),
      monc(cct_),
      lock("mon-msg-test::lock")
  { }


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
                            "test-mon-msg", 0, 0);
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

  virtual void handle_wanted(Message *m) { }

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
  int reply_type = 0;
  Message *reply_msg = nullptr;
  Mutex lock;
  Cond cond;

  MonMsgTest() :
    MonClientHelper(g_ceph_context),
    lock("lock") { }

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

  void handle_wanted(Message *m) override {
    lock.Lock();
    // caller will put() after they call us, so hold on to a ref
    m->get();
    reply_msg = m;
    cond.Signal();
    lock.Unlock();
  }

  Message *send_wait_reply(Message *m, int t, double timeout=30.0) {
    lock.Lock();
    reply_type = t;
    add_wanted(t);
    send_message(m);

    int err = 0;
    if (timeout > 0) {
      utime_t cond_timeout;
      cond_timeout.set_from_double(timeout);
      utime_t s = ceph_clock_now();
      err = cond.WaitInterval(lock, cond_timeout);
      utime_t e = ceph_clock_now();
      dout(20) << __func__ << " took " << (e-s) << " seconds" << dendl;
    } else {
      err = cond.Wait(lock);
    }
    rm_wanted(t);
    lock.Unlock();
    if (err > 0) {
      dout(20) << __func__ << " error: " << cpp_strerror(err) << dendl;
      return (Message*)((long)-err);
    }

    if (!reply_msg)
      dout(20) << __func__ << " reply_msg is nullptr" << dendl;
    else
      dout(20) << __func__ << " reply_msg " << *reply_msg << dendl;
    return reply_msg;
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

int main(int argc, char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(nullptr, args,
			 CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf.apply_changes(nullptr);
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

