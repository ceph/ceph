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

#include "common/ProfLogger.h"
#include "common/ceph_context.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "test/unit.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <map>
#include <poll.h>
#include <sstream>
#include <stdint.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

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

class ProfLoggerCollectionTest
{
public:
  ProfLoggerCollectionTest(ProfLoggerCollection *coll)
    : m_coll(coll)
  {
  }
  bool init(const std::string &uri) {
    Mutex::Locker lock(m_coll->m_lock);
    if (m_coll->m_thread != NULL) {
      return false;
    }
    return m_coll->init(uri);
  }
  bool shutdown() {
    Mutex::Locker lock(m_coll->m_lock);
    m_coll->shutdown();
    return (m_coll->m_thread == NULL);
  }
private:
  ProfLoggerCollection *m_coll;
};

class Alarm
{
public:
  Alarm(int s) {
    alarm(s);
  }
  ~Alarm() {
    alarm(0);
  }
};

class ProfLoggerTestClient
{
public:
  ProfLoggerTestClient(const std::string &uri)
    : m_uri(uri)
  {
  }

  std::string get_message(std::string *message)
  {
    Alarm my_alarm(300);

    int socket_fd = socket(PF_UNIX, SOCK_STREAM, 0);
    if(socket_fd < 0) {
      int err = errno;
      ostringstream oss;
      oss << "socket(PF_UNIX, SOCK_STREAM, 0) failed: " << cpp_strerror(err);
      return oss.str();
    }

    struct sockaddr_un address;
    memset(&address, 0, sizeof(struct sockaddr_un));
    address.sun_family = AF_UNIX;
    snprintf(address.sun_path, sizeof(address.sun_path), m_uri.c_str());

    if (connect(socket_fd, (struct sockaddr *) &address, 
	  sizeof(struct sockaddr_un)) != 0) {
      int err = errno;
      ostringstream oss;
      oss << "connect(" << socket_fd << ") failed: " << cpp_strerror(err);
      close(socket_fd);
      return oss.str();
    }

    std::vector<uint8_t> vec(65536, 0);
    uint8_t *buffer = &vec[0];

    uint32_t message_size_raw;
    ssize_t res = safe_read_exact(socket_fd, &message_size_raw,
				  sizeof(message_size_raw));
    if (res < 0) {
      int err = res;
      ostringstream oss;
      oss << "safe_read(" << socket_fd << ") failed to read message size: "
  	  << cpp_strerror(err);
      close(socket_fd);
      return oss.str();
    }
    uint32_t message_size = ntohl(message_size_raw);
    res = safe_read_exact(socket_fd, buffer, message_size);
    if (res < 0) {
      int err = res;
      ostringstream oss;
      oss << "safe_read(" << socket_fd << ") failed: " << cpp_strerror(err);
      close(socket_fd);
      return oss.str();
    }

    //printf("MESSAGE FROM SERVER: %s\n", buffer);
    message->assign((const char*)buffer);
    close(socket_fd);
    return "";
  }

private:
  std::string m_uri;
};

TEST(ProfLogger, Teardown) {
  ProfLoggerCollectionTest plct(g_ceph_context->GetProfLoggerCollection());
  ASSERT_EQ(true, plct.shutdown());
}

TEST(ProfLogger, TeardownSetup) {
  ProfLoggerCollectionTest plct(g_ceph_context->GetProfLoggerCollection());
  ASSERT_EQ(true, plct.shutdown());
  ASSERT_EQ(true, plct.init(get_socket_path()));
  ASSERT_EQ(true, plct.shutdown());
}

TEST(ProfLogger, SimpleTest) {
  ProfLoggerCollectionTest plct(g_ceph_context->GetProfLoggerCollection());
  ASSERT_EQ(true, plct.shutdown());
  ASSERT_EQ(true, plct.init(get_socket_path()));
  ProfLoggerTestClient test_client(get_socket_path());
  std::string message;
  ASSERT_EQ("", test_client.get_message(&message));
  ASSERT_EQ("{}", message);
}

enum {
  FAKE_PROFLOGGER1_ELEMENT_FIRST = 200,
  FAKE_PROFLOGGER1_ELEMENT_1,
  FAKE_PROFLOGGER1_ELEMENT_2,
  FAKE_PROFLOGGER1_ELEMENT_3,
  FAKE_PROFLOGGER1_ELEMENT_LAST,
};

static ProfLogger* setup_fake_proflogger1(CephContext *cct)
{
  ProfLoggerBuilder bld(cct, "fake_proflogger_1",
	  FAKE_PROFLOGGER1_ELEMENT_FIRST, FAKE_PROFLOGGER1_ELEMENT_LAST);
  bld.add_u64(FAKE_PROFLOGGER1_ELEMENT_1, "element1");
  bld.add_fl(FAKE_PROFLOGGER1_ELEMENT_2, "element2");
  bld.add_fl_avg(FAKE_PROFLOGGER1_ELEMENT_3, "element3");
  return bld.create_proflogger();
}

TEST(ProfLogger, FakeProflogger1) {
  ProfLoggerCollection *coll = g_ceph_context->GetProfLoggerCollection();
  ProfLogger* fake_pf = setup_fake_proflogger1(g_ceph_context);
  coll->logger_add(fake_pf);
  ProfLoggerCollectionTest plct(coll);
  ASSERT_EQ(true, plct.shutdown());
  ASSERT_EQ(true, plct.init(get_socket_path()));
  ProfLoggerTestClient test_client(get_socket_path());
  std::string msg;
  ASSERT_EQ("", test_client.get_message(&msg));
  ASSERT_EQ("{'element1':0,'element2':0,'element3':{'count':0,'sum':0},}", msg);
  fake_pf->inc(FAKE_PROFLOGGER1_ELEMENT_1);
  fake_pf->fset(FAKE_PROFLOGGER1_ELEMENT_2, 0.5);
  fake_pf->finc(FAKE_PROFLOGGER1_ELEMENT_3, 100.0);
  ASSERT_EQ("", test_client.get_message(&msg));
  ASSERT_EQ("{'element1':1,'element2':0.5,'element3':{'count':1,'sum':100},}", msg);
  fake_pf->finc(FAKE_PROFLOGGER1_ELEMENT_3, 0.0);
  fake_pf->finc(FAKE_PROFLOGGER1_ELEMENT_3, 25.0);
  ASSERT_EQ("", test_client.get_message(&msg));
  ASSERT_EQ("{'element1':1,'element2':0.5,'element3':{'count':3,'sum':125},}", msg);
}
