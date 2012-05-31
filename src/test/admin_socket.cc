// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/Mutex.h"
#include "common/admin_socket.h"
#include "common/admin_socket_client.h"
#include "common/ceph_context.h"
#include "test/unit.h"

#include <stdint.h>
#include <string.h>
#include <string>
#include <sys/un.h>

class AdminSocketTest
{
public:
  AdminSocketTest(AdminSocket *asokc)
    : m_asokc(asokc)
  {
  }
  bool init(const std::string &uri) {
    return m_asokc->init(uri);
  }
  bool shutdown() {
    m_asokc->shutdown();
    return true;
  }
  AdminSocket *m_asokc;
};

TEST(AdminSocket, Teardown) {
  std::auto_ptr<AdminSocket>
      asokc(new AdminSocket(g_ceph_context));
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
}

TEST(AdminSocket, TeardownSetup) {
  std::auto_ptr<AdminSocket>
      asokc(new AdminSocket(g_ceph_context));
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_rand_socket_path()));
  ASSERT_EQ(true, asoct.shutdown());
}

TEST(AdminSocket, SendNoOp) {
  std::auto_ptr<AdminSocket>
      asokc(new AdminSocket(g_ceph_context));
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_rand_socket_path()));
  AdminSocketClient client(get_rand_socket_path());
  string version;
  ASSERT_EQ("", client.do_request("version", &version));
  ASSERT_EQ(CEPH_ADMIN_SOCK_VERSION, version);
  ASSERT_EQ(true, asoct.shutdown());
}

class MyTest : public AdminSocketHook {
  bool call(std::string command, bufferlist& result) {
    result.append("yes");
    return true;
  }
};

TEST(AdminSocket, RegisterCommand) {
  std::auto_ptr<AdminSocket>
      asokc(new AdminSocket(g_ceph_context));
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_rand_socket_path()));
  AdminSocketClient client(get_rand_socket_path());
  ASSERT_EQ(0, asoct.m_asokc->register_command("test", new MyTest(), ""));
  string result;
  ASSERT_EQ("", client.do_request("test", &result));
  ASSERT_EQ("yes", result);
  ASSERT_EQ(true, asoct.shutdown());
}

class MyTest2 : public AdminSocketHook {
  bool call(std::string command, bufferlist& result) {
    result.append("yessir");
    return true;
  }
};

TEST(AdminSocket, RegisterCommandPrefixes) {
  std::auto_ptr<AdminSocket>
      asokc(new AdminSocket(g_ceph_context));
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_rand_socket_path()));
  AdminSocketClient client(get_rand_socket_path());
  ASSERT_EQ(0, asoct.m_asokc->register_command("test", new MyTest(), ""));
  ASSERT_EQ(0, asoct.m_asokc->register_command("test command", new MyTest2(), ""));
  string result;
  ASSERT_EQ("", client.do_request("test", &result));
  ASSERT_EQ("yes", result);
  ASSERT_EQ("", client.do_request("test command", &result));
  ASSERT_EQ("yessir", result);
  ASSERT_EQ("", client.do_request("test command post", &result));
  ASSERT_EQ("yessir", result);
  ASSERT_EQ("", client.do_request("test command  post", &result));
  ASSERT_EQ("yessir", result);
  ASSERT_EQ("", client.do_request("test this thing", &result));
  ASSERT_EQ("yes", result);

  ASSERT_EQ("", client.do_request("test  command post", &result));
  ASSERT_EQ("yes", result);
  ASSERT_EQ("", client.do_request("test  this thing", &result));
  ASSERT_EQ("yes", result);
  ASSERT_EQ(true, asoct.shutdown());
}
