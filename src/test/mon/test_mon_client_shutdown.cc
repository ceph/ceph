// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "common/async/context_pool.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"

#include "gtest/gtest.h"

// mon_host points at a plain listening socket, so a connection attempt by
// the MonClient shows up as a pending accept on it.
static bool renew_subs_connects(bool shut_down_first)
{
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  EXPECT_GE(fd, 0);
  sockaddr_in sa = {};
  sa.sin_family = AF_INET;
  sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  EXPECT_EQ(0, bind(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)));
  EXPECT_EQ(0, listen(fd, 8));
  socklen_t len = sizeof(sa);
  EXPECT_EQ(0, getsockname(fd, reinterpret_cast<sockaddr*>(&sa), &len));

  boost::intrusive_ptr<CephContext> cct(
    new CephContext(CEPH_ENTITY_TYPE_CLIENT), false);
  auto& conf = cct->_conf;
  conf.set_val_or_die("mon_host",
                      "v2:127.0.0.1:" + std::to_string(ntohs(sa.sin_port)));
  conf.set_val_or_die("auth_client_required", "none");
  conf.set_val_or_die("admin_socket", "");
  conf.set_val_or_die("log_file", "");
  conf.apply_changes(nullptr);
  common_init_finish(cct.get());

  ceph::async::io_context_pool poolctx(1);
  Messenger *msgr = Messenger::create_client_messenger(cct.get(), "monc");
  msgr->start();
  bool connected;
  {
    MonClient monc(cct.get(), poolctx);
    EXPECT_EQ(0, monc.build_initial_monmap());
    monc.set_messenger(msgr);
    EXPECT_EQ(0, monc.init());
    if (shut_down_first) {
      monc.shutdown();
    }
    monc.sub_want("osdmap", 1, 0);
    monc.renew_subs();
    pollfd pfd = {fd, POLLIN, 0};
    connected = poll(&pfd, 1, 2000) == 1;
    if (!shut_down_first) {
      monc.shutdown();
    }
  }
  msgr->shutdown();
  msgr->wait();
  delete msgr;
  poolctx.stop();
  close(fd);
  return connected;
}

TEST(MonClientShutdown, RenewSubsConnects) {
  ASSERT_TRUE(renew_subs_connects(false));
}

TEST(MonClientShutdown, RenewSubsAfterShutdownDoesNotConnect) {
  ASSERT_FALSE(renew_subs_connects(true));
}
