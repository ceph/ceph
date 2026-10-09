// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>

#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/scope_guard.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"

#include "gtest/gtest.h"

using namespace std::chrono;

// A socket that accepts connections but never speaks the messenger protocol,
// so a hunt started against it stays pending: authenticate() gives up on the
// timeout with the connection still in pending_cons, which is the state a
// retried authenticate() has to recover from.
class silent_mon {
  int fd = -1;
  int port = 0;
public:
  silent_mon() {
    fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ceph_assert(fd >= 0);
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ceph_assert(::bind(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0);
    ceph_assert(::listen(fd, 5) == 0);
    socklen_t len = sizeof(addr);
    ceph_assert(::getsockname(fd, (struct sockaddr *)&addr, &len) == 0);
    port = ntohs(addr.sin_port);
  }
  ~silent_mon() {
    if (fd >= 0)
      ::close(fd);
  }
  std::string get_addr() const {
    return "v1:127.0.0.1:" + std::to_string(port);
  }
};

TEST(MonClient, AuthenticateRetriedAfterTimeout) {
  ceph::async::io_context_pool poolctx(1);
  MonClient monc(g_ceph_context, poolctx);
  ASSERT_EQ(0, monc.build_initial_monmap());

  Messenger *msgr = Messenger::create_client_messenger(g_ceph_context,
						       "monclient-test");
  ASSERT_NE(nullptr, msgr);
  msgr->start();
  monc.set_messenger(msgr);
  monc.set_want_keys(CEPH_ENTITY_TYPE_MON);
  ASSERT_EQ(0, monc.init());
  auto shutdown = make_scope_guard([&] {
    monc.shutdown();
    msgr->shutdown();
    msgr->wait();
    delete msgr;
  });

  const double timeout = 0.5;
  ASSERT_GT(0, monc.authenticate(timeout));

  // the second call has to hunt the monitors again rather than hand back what
  // the first one recorded, so it can only return once it has waited too
  auto start = steady_clock::now();
  ASSERT_GT(0, monc.authenticate(timeout));
  duration<double> elapsed = steady_clock::now() - start;
  ASSERT_GE(elapsed.count(), timeout * 0.8);
}

int main(int argc, char **argv)
{
  // mon_host is a startup option, so the monitor the client hunts has to be
  // in place before the config is assembled
  silent_mon mon;
  std::string mon_host = "--mon-host=" + mon.get_addr();

  auto args = argv_to_vec(argc, argv);
  args.push_back(mon_host.c_str());
  args.push_back("--auth-client-required=none");
  auto cct = global_init(nullptr, args,
			 CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE|
			 CINIT_FLAG_NO_MON_CONFIG);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
