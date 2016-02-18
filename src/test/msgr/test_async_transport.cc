// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <thread>
#include <gtest/gtest.h>

#include "acconfig.h"
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"

#include "msg/async/Event.h"
#include "msg/async/GenericSocket.h"

#if GTEST_HAS_PARAM_TEST

class TransportTest : public ::testing::TestWithParam<const char*> {
 public:
  EventCenter *center;
  std::unique_ptr<NetworkStack> transport;
  string addr, port_addr;

  TransportTest() {}
  virtual void SetUp() {
    cerr << __func__ << " start set up " << GetParam() << std::endl;
    if (strncmp(GetParam(), "dpdk", 4)) {
      g_ceph_context->_conf->set_val("ms_dpdk_enable", "false");
      addr = "127.0.0.1:15000";
      port_addr = "127.0.0.1:15001";
    } else {
      g_ceph_context->_conf->set_val("ms_dpdk_enable", "true");
      g_ceph_context->_conf->set_val("ms_dpdk_host_ipv4_addr", "172.16.218.199", false, false);
      g_ceph_context->_conf->set_val("ms_dpdk_gateway_ipv4_addr", "172.16.218.2", false, false);
      g_ceph_context->_conf->set_val("ms_dpdk_netmask_ipv4_addr", "255.255.255.0", false, false);
      addr = "172.16.218.199:15000";
      port_addr = "172.16.218.199:15001";
    }
    g_ceph_context->_conf->apply_changes(nullptr);
    center = new EventCenter(g_ceph_context);
    center->init(1000);
    center->set_owner();
    transport = NetworkStack::create(g_ceph_context, GetParam(), center, 0);
    transport->initialize();
  }
  virtual void TearDown() {
    delete center;
    transport.reset();
  }
  string get_addr() const {
    return addr;
  }
  string get_ip_different_port() const {
    return port_addr;
  }
  string get_different_ip() const {
    return "10.0.123.100:4323";
  }
};

class C_poll : public EventCallback {
  EventCenter *center;
  std::atomic<bool> wakeuped;
  static const int sleepus = 500;
  Mutex *lock;

 public:
  C_poll(EventCenter *c, Mutex *l=nullptr): center(c), wakeuped(false), lock(l) {}
  void do_request(int r) {
    wakeuped = true;
  }
  bool poll(int milliseconds) {
    auto start = ceph::coarse_real_clock::now(g_ceph_context);
    while (!wakeuped) {
      if (lock)
        lock->Lock();
      center->process_events(sleepus);
      if (lock)
        lock->Unlock();
      usleep(sleepus);
      auto r = std::chrono::duration_cast<std::chrono::milliseconds>(
              ceph::coarse_real_clock::now(g_ceph_context) - start);
      if (r >= std::chrono::milliseconds(milliseconds))
        break;
    }
    return wakeuped;
  }
  void reset() {
    wakeuped = false;
  }
};

TEST_P(TransportTest, SimpleTest) {
  entity_addr_t bind_addr, cli_addr;
  ASSERT_EQ(bind_addr.parse(get_addr().c_str()), true);
  SocketOptions options;
  ServerSocket bind_socket;
  int r = transport->listen(bind_addr, options, &bind_socket);
  ASSERT_EQ(r, 0);
  ConnectedSocket cli_socket, srv_socket;
  r = transport->connect(bind_addr, options, &cli_socket);
  ASSERT_EQ(r, 0);

  {
    C_poll cb(center);
    center->create_file_event(bind_socket.fd(), EVENT_READABLE, &cb);
    ASSERT_EQ(cb.poll(500), true);
    center->delete_file_event(bind_socket.fd(), EVENT_READABLE);
  }

  r = bind_socket.accept(&srv_socket, &cli_addr);
  ASSERT_EQ(r, 0);
  ASSERT_TRUE(srv_socket.fd() > 0);

  {
    C_poll cb(center);
    center->create_file_event(cli_socket.fd(), EVENT_READABLE, &cb);
    r = cli_socket.is_connected();
    if (r == 0) {
      ASSERT_EQ(cb.poll(500), true);
      r = cli_socket.is_connected();
    }
    ASSERT_EQ(r, 1);
    center->delete_file_event(cli_socket.fd(), EVENT_READABLE);
  }

  struct msghdr msg;
  struct iovec msgvec[2];
  const char *message = "this is a new message";
  int len = strlen(message);
  memset(&msg, 0, sizeof(msg));
  msg.msg_iovlen = 1;
  msg.msg_iov = msgvec;
  msgvec[0].iov_base = (char*)message;
  msgvec[0].iov_len = len;
  r = cli_socket.sendmsg(msg, false);
  ASSERT_EQ(r, len);

  char buf[1024];
  C_poll cb(center);
  center->create_file_event(srv_socket.fd(), EVENT_READABLE, &cb);
  {
    r = srv_socket.read(buf, sizeof(buf));
    if (r == -EAGAIN) {
      ASSERT_EQ(cb.poll(500), true);
      r = srv_socket.read(buf, sizeof(buf));
    }
    ASSERT_EQ(r, len);
    ASSERT_EQ(0, memcmp(buf, message, len));
  }
  bind_socket.abort_accept();
  cli_socket.close();

  {
    cb.reset();
    ASSERT_EQ(cb.poll(500), true);
    r = srv_socket.read(buf, sizeof(buf));
    ASSERT_EQ(r, 0);
  }
  center->delete_file_event(srv_socket.fd(), EVENT_READABLE);

  srv_socket.close();
}

TEST_P(TransportTest, ConnectFailedTest) {
  entity_addr_t bind_addr, cli_addr;
  ASSERT_EQ(bind_addr.parse(get_addr().c_str()), true);
  ASSERT_EQ(cli_addr.parse(get_ip_different_port().c_str()), true);
  SocketOptions options;
  ServerSocket bind_socket;
  int r = transport->listen(bind_addr, options, &bind_socket);
  ASSERT_EQ(r, 0);

  ConnectedSocket cli_socket1, cli_socket2;
  r = transport->connect(cli_addr, options, &cli_socket1);
  ASSERT_EQ(r, 0);

  {
    C_poll cb(center);
    center->create_file_event(cli_socket1.fd(), EVENT_READABLE, &cb);
    r = cli_socket1.is_connected();
    if (r == 0) {
      ASSERT_EQ(cb.poll(500), true);
      r = cli_socket1.is_connected();
    }
    ASSERT_TRUE(r == -ECONNREFUSED || r == -ECONNRESET);
  }

  ASSERT_EQ(cli_addr.parse(get_different_ip().c_str()), true);
  r = transport->connect(cli_addr, options, &cli_socket2);
  ASSERT_EQ(r, 0);

  {
    C_poll cb(center);
    center->create_file_event(cli_socket2.fd(), EVENT_READABLE, &cb);
    r = cli_socket2.is_connected();
    if (r == 0) {
      ASSERT_EQ(cb.poll(500), false);
      r = cli_socket2.is_connected();
    }
    ASSERT_TRUE(r != 1);
  }
}

TEST_P(TransportTest, ComplexTest) {
  entity_addr_t bind_addr, cli_addr;
  ASSERT_EQ(bind_addr.parse(get_addr().c_str()), true);
  SocketOptions options;
  ServerSocket bind_socket;
  int r = transport->listen(bind_addr, options, &bind_socket);
  ASSERT_EQ(r, 0);
  ConnectedSocket cli_socket, srv_socket;
  r = transport->connect(bind_addr, options, &cli_socket);
  ASSERT_EQ(r, 0);

  {
    C_poll cb(center);
    center->create_file_event(bind_socket.fd(), EVENT_READABLE, &cb);
    ASSERT_EQ(cb.poll(500), true);
  }

  r = bind_socket.accept(&srv_socket, &cli_addr);
  ASSERT_EQ(r, 0);
  ASSERT_TRUE(srv_socket.fd() > 0);

  {
    C_poll cb(center);
    center->create_file_event(cli_socket.fd(), EVENT_READABLE, &cb);
    r = cli_socket.is_connected();
    if (r == 0) {
      ASSERT_EQ(cb.poll(500), true);
      r = cli_socket.is_connected();
    }
    ASSERT_EQ(r, 1);
  }

  const size_t message_size = 10240;
  string message(message_size, '!');
  for (size_t i = 0; i < message_size; i += 100)
    message[i] = ',';
  auto cli_fd = cli_socket.fd();
  bool done = false;
  size_t len = message_size * 100;
  Mutex lock("test_async_transport::lock");
  std::thread t([len, cli_fd](EventCenter *center, ConnectedSocket &cli_socket, const string &message, Mutex &lock, bool &done) {
    bool first = true;
   again:
    struct msghdr msg;
    struct iovec msgvec[100];
    memset(&msg, 0, sizeof(msg));
    msg.msg_iovlen = 100;
    msg.msg_iov = msgvec;
    for (size_t i = 0; i < msg.msg_iovlen; ++i) {
      msgvec[i].iov_base = (void*)message.data();
      msgvec[i].iov_len = message_size;
    }

    ASSERT_TRUE(center->get_owner());
    C_poll cb(center, &lock);
    center->create_file_event(cli_fd, EVENT_WRITABLE, &cb);
    int r = 0;
    size_t left = len;
    usleep(100);
    while (left > 0) {
      lock.Lock();
      r = cli_socket.sendmsg(msg, false);
      lock.Unlock();
      ASSERT_TRUE(r > 0 || r == -EAGAIN);
      if (r > 0)
        left -= r;
      while (r > 0) {
        if (msg.msg_iov[0].iov_len <= (size_t)r) {
          // drain this whole item
          r -= msg.msg_iov[0].iov_len;
          msg.msg_iov++;
          msg.msg_iovlen--;
        } else {
          msg.msg_iov[0].iov_base = (char *)msg.msg_iov[0].iov_base + r;
          msg.msg_iov[0].iov_len -= r;
          break;
        }
      }
      if (left == 0)
        break;
      cb.reset();
      ASSERT_EQ(cb.poll(500), true);
    }
    if (first) {
      first = false;
      goto again;
    }
    while (!done)
      usleep(100);
    center->delete_file_event(cli_fd, EVENT_WRITABLE);
  }, center, std::ref(cli_socket), std::ref(message), std::ref(lock), std::ref(done));

  char buf[1000];
  C_poll cb(center, &lock);
  center->create_file_event(srv_socket.fd(), EVENT_READABLE, &cb);
  string read_string;
  len *= 2;
  while (len > 0) {
    lock.Lock();
    r = srv_socket.read(buf, sizeof(buf));
    lock.Unlock();
    ASSERT_TRUE(r > 0 || r == -EAGAIN);
    if (r > 0) {
      read_string.append(buf, r);
      len -= r;
    }
    if (r == -EAGAIN) {
      cb.reset();
      ASSERT_EQ(cb.poll(500), true);
    }
  }
  center->delete_file_event(srv_socket.fd(), EVENT_READABLE);
  done = true;
  t.join();
  for (size_t i = 0; i < read_string.size(); i += message_size)
    ASSERT_EQ(memcmp(read_string.c_str()+i, message.c_str(), message_size), 0);

  bind_socket.abort_accept();
  srv_socket.close();
  cli_socket.close();
}

INSTANTIATE_TEST_CASE_P(
  NetworkStack,
  TransportTest,
  ::testing::Values(
#ifdef HAVE_DPDK
    "dpdk",
#endif
    "posix"
  )
);

#else

// Google Test may not support value-parameterized tests with some
// compilers. If we use conditional compilation to compile out all
// code referring to the gtest_main library, MSVC linker will not link
// that library at all and consequently complain about missing entry
// point defined in that library (fatal error LNK1561: entry point
// must be defined). This dummy test keeps gtest_main linked in.
TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}

#endif


int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make ceph_test_async_transport &&
 *    ./ceph_test_async_transport
 *
 * End:
 */
