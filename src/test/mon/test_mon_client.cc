// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "common/async/context_pool.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "mon/MonClient.h"

#include "gtest/gtest.h"

// No monitor listens on mon_host, so a ping that gets as far as
// connecting fails some other way than the key load's -ENOENT.
static int ping_without_key(const char *auth)
{
  boost::intrusive_ptr<CephContext> cct(
    new CephContext(CEPH_ENTITY_TYPE_CLIENT), false);
  auto& conf = cct->_conf;
  conf.set_val_or_die("mon_host", "v2:127.0.0.1:1");
  conf.set_val_or_die("auth_client_required", auth);
  conf.set_val_or_die("key", "");
  conf.set_val_or_die("keyfile", "");
  conf.set_val_or_die("keyring", "/nonexistent");
  conf.set_val_or_die("mon_client_ping_timeout", "1");
  conf.set_val_or_die("admin_socket", "");
  conf.set_val_or_die("log_file", "");
  conf.apply_changes(nullptr);
  common_init_finish(cct.get());

  ceph::async::io_context_pool poolctx(1);
  MonClient monc(cct.get(), poolctx);
  EXPECT_EQ(0, monc.build_initial_monmap());
  std::string reply;
  int r = monc.ping_monitor("a", &reply);
  poolctx.stop();
  return r;
}

TEST(MonClientPingNoKeyring, AuthNone) {
  ASSERT_NE(-ENOENT, ping_without_key("none"));
}

TEST(MonClientPingNoKeyring, Cephx) {
  ASSERT_EQ(-ENOENT, ping_without_key("cephx"));
}
