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

static char g_socket_path[sizeof(sockaddr_un::sun_path)] = { 0 };

static const char* get_socket_path()
{
  if (g_socket_path[0] == '\0') {
    const char *tdir = getenv("TMPDIR");
    if (tdir == NULL) {
      tdir = "/tmp";
    }
    snprintf(g_socket_path, sizeof(sockaddr_un::sun_path),
	     "%s/proflogger_test_socket.%ld.%ld",
	     tdir, (long int)getpid(), time(NULL));
  }
  return g_socket_path;
}

class AdminSocketTest
{
public:
  AdminSocketTest(AdminSocketConfigObs *asokc)
    : m_asokc(asokc)
  {
  }
  bool init(const std::string &uri) {
    if (m_asokc->m_thread != NULL) {
      return false;
    }
    return m_asokc->init(uri);
  }
  bool shutdown() {
    m_asokc->shutdown();
    return (m_asokc->m_thread == NULL);
  }
private:
  AdminSocketConfigObs *m_asokc;
};

TEST(AdminSocket, Teardown) {
  std::auto_ptr<AdminSocketConfigObs>
      asokc(new AdminSocketConfigObs(g_ceph_context));
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
}

TEST(AdminSocket, TeardownSetup) {
  std::auto_ptr<AdminSocketConfigObs>
      asokc(new AdminSocketConfigObs(g_ceph_context));
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_socket_path()));
  ASSERT_EQ(true, asoct.shutdown());
}

TEST(AdminSocket, SendNoOp) {
  std::auto_ptr<AdminSocketConfigObs>
      asokc(new AdminSocketConfigObs(g_ceph_context));
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_socket_path()));
  AdminSocketClient client(get_socket_path());
  ASSERT_EQ("", client.send_noop());
  ASSERT_EQ(true, asoct.shutdown());
}
