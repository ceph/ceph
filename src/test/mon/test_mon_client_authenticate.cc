// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "auth/DummyAuth.h"
#include "common/async/context_pool.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "mon/MonClient.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"

#include "gtest/gtest.h"

// A mon that authenticates clients and then ignores them: it never sends a
// monmap, so the session never completes.
struct SilentMon : public Dispatcher {
  explicit SilentMon(CephContext *cct) : Dispatcher(cct) {}
  bool ms_dispatch(Message *m) override {
    m->put();
    return true;
  }
  bool ms_handle_reset(Connection *con) override { return true; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override { return true; }
};

static boost::intrusive_ptr<CephContext> make_cct(
  uint32_t type, const std::string& mon_host = "")
{
  boost::intrusive_ptr<CephContext> cct(new CephContext(type), false);
  auto& conf = cct->_conf;
  conf.set_val_or_die("auth_cluster_required", "none");
  conf.set_val_or_die("auth_service_required", "none");
  conf.set_val_or_die("auth_client_required", "none");
  conf.set_val_or_die("admin_socket", "");
  conf.set_val_or_die("log_file", "");
  if (!mon_host.empty()) {
    conf.set_val_or_die("mon_host", mon_host);
  }
  conf.apply_changes(nullptr);
  common_init_finish(cct.get());
  return cct;
}

TEST(MonClientAuthenticate, SessionNeverCompletes) {
  auto mon_cct = make_cct(CEPH_ENTITY_TYPE_MON);
  DummyAuthClientServer mon_auth(mon_cct.get());
  mon_auth.auth_registry.refresh_config();
  SilentMon silent_mon(mon_cct.get());
  Messenger *mon = Messenger::create(mon_cct.get(), "async+posix",
                                     entity_name_t::MON(0), "mon", 0);
  mon->set_auth_client(&mon_auth);
  mon->set_auth_server(&mon_auth);
  mon->set_default_policy(Messenger::Policy::stateless_server(0));
  entity_addr_t addr;
  addr.parse("v2:127.0.0.1:0");
  ASSERT_EQ(0, mon->bind(addr));
  mon->add_dispatcher_head(&silent_mon);
  mon->start();

  auto cct = make_cct(
    CEPH_ENTITY_TYPE_CLIENT,
    "v2:127.0.0.1:" + std::to_string(mon->get_myaddrs().front().get_port()));
  ceph::async::io_context_pool poolctx(1);
  Messenger *msgr = Messenger::create_client_messenger(cct.get(), "monc");
  msgr->start();
  int r;
  {
    MonClient monc(cct.get(), poolctx);
    ASSERT_EQ(0, monc.build_initial_monmap());
    monc.set_messenger(msgr);
    ASSERT_EQ(0, monc.init());
    r = monc.authenticate(2);
    monc.shutdown();
  }
  msgr->shutdown();
  msgr->wait();
  delete msgr;
  mon->shutdown();
  mon->wait();
  delete mon;
  poolctx.stop();
  ASSERT_EQ(-ETIMEDOUT, r);
}
