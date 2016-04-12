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

#include <gtest/gtest.h>

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/admin_socket.h"
#include "common/admin_socket_client.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/global_context.h"

#include <stdint.h>
#include <string.h>
#include <string>
#include <sys/un.h>

class AdminSocketTest
{
public:
  explicit AdminSocketTest(AdminSocket *asokc)
    : m_asokc(asokc)
  {
  }
  bool init(const std::string &uri) {
    return m_asokc->init(uri);
  }
  string bind_and_listen(const std::string &sock_path, int *fd) {
    return m_asokc->bind_and_listen(sock_path, fd);
  }
  bool shutdown() {
    m_asokc->shutdown();
    return true;
  }
  AdminSocket *m_asokc;
};

TEST(AdminSocket, Teardown) {
  std::unique_ptr<AdminSocket>
      asokc(new AdminSocket(g_ceph_context));
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
}

TEST(AdminSocket, TeardownSetup) {
  std::unique_ptr<AdminSocket>
      asokc(new AdminSocket(g_ceph_context));
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_rand_socket_path()));
  ASSERT_EQ(true, asoct.shutdown());
}

TEST(AdminSocket, SendHelp) {
  std::unique_ptr<AdminSocket>
      asokc(new AdminSocket(g_ceph_context));
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_rand_socket_path()));
  AdminSocketClient client(get_rand_socket_path());

  {
    string help;
    ASSERT_EQ("", client.do_request("{\"prefix\":\"help\"}", &help));
    ASSERT_NE(string::npos, help.find("\"list available commands\""));
  }
  {
    string help;
    ASSERT_EQ("", client.do_request("{"
				    " \"prefix\":\"help\","
				    " \"format\":\"xml\","
				    "}", &help));
    ASSERT_NE(string::npos, help.find(">list available commands<"));
  }
  {
    string help;
    ASSERT_EQ("", client.do_request("{"
				    " \"prefix\":\"help\","
				    " \"format\":\"UNSUPPORTED\","
				    "}", &help));
    ASSERT_NE(string::npos, help.find("\"list available commands\""));
  }
  ASSERT_EQ(true, asoct.shutdown());
}

TEST(AdminSocket, SendNoOp) {
  std::unique_ptr<AdminSocket>
      asokc(new AdminSocket(g_ceph_context));
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_rand_socket_path()));
  AdminSocketClient client(get_rand_socket_path());
  string version;
  ASSERT_EQ("", client.do_request("{\"prefix\":\"0\"}", &version));
  ASSERT_EQ(CEPH_ADMIN_SOCK_VERSION, version);
  ASSERT_EQ(true, asoct.shutdown());
}

class MyTest : public AdminSocketHook {
  bool call(std::string command, cmdmap_t& cmdmap, std::string format, bufferlist& result) {
    std::vector<std::string> args;
    cmd_getval(g_ceph_context, cmdmap, "args", args);
    result.append(command);
    result.append("|");
    string resultstr;
    for (std::vector<std::string>::iterator it = args.begin();
	 it != args.end(); ++it) {
      if (it != args.begin())
	resultstr += ' ';
      resultstr += *it;
    }
    result.append(resultstr);
    return true;
  }
};

TEST(AdminSocket, RegisterCommand) {
  std::unique_ptr<AdminSocket>
      asokc(new AdminSocket(g_ceph_context));
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_rand_socket_path()));
  AdminSocketClient client(get_rand_socket_path());
  ASSERT_EQ(0, asoct.m_asokc->register_command("test", "test", new MyTest(), ""));
  string result;
  ASSERT_EQ("", client.do_request("{\"prefix\":\"test\"}", &result));
  ASSERT_EQ("test|", result);
  ASSERT_EQ(true, asoct.shutdown());
}

class MyTest2 : public AdminSocketHook {
  bool call(std::string command, cmdmap_t& cmdmap, std::string format, bufferlist& result) {
    std::vector<std::string> args;
    cmd_getval(g_ceph_context, cmdmap, "args", args);
    result.append(command);
    result.append("|");
    string resultstr;
    for (std::vector<std::string>::iterator it = args.begin();
	 it != args.end(); ++it) {
      if (it != args.begin())
	resultstr += ' ';
      resultstr += *it;
    }
    result.append(resultstr);
    return true;
  }
};

TEST(AdminSocket, RegisterCommandPrefixes) {
  std::unique_ptr<AdminSocket>
      asokc(new AdminSocket(g_ceph_context));
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_rand_socket_path()));
  AdminSocketClient client(get_rand_socket_path());
  ASSERT_EQ(0, asoct.m_asokc->register_command("test", "test name=args,type=CephString,n=N", new MyTest(), ""));
  ASSERT_EQ(0, asoct.m_asokc->register_command("test command", "test command name=args,type=CephString,n=N", new MyTest2(), ""));
  string result;
  ASSERT_EQ("", client.do_request("{\"prefix\":\"test\"}", &result));
  ASSERT_EQ("test|", result);
  ASSERT_EQ("", client.do_request("{\"prefix\":\"test command\"}", &result));
  ASSERT_EQ("test command|", result);
  ASSERT_EQ("", client.do_request("{\"prefix\":\"test command\",\"args\":[\"post\"]}", &result));
  ASSERT_EQ("test command|post", result);
  ASSERT_EQ("", client.do_request("{\"prefix\":\"test command\",\"args\":[\" post\"]}", &result));
  ASSERT_EQ("test command| post", result);
  ASSERT_EQ("", client.do_request("{\"prefix\":\"test\",\"args\":[\"this thing\"]}", &result));
  ASSERT_EQ("test|this thing", result);

  ASSERT_EQ("", client.do_request("{\"prefix\":\"test\",\"args\":[\" command post\"]}", &result));
  ASSERT_EQ("test| command post", result);
  ASSERT_EQ("", client.do_request("{\"prefix\":\"test\",\"args\":[\" this thing\"]}", &result));
  ASSERT_EQ("test| this thing", result);
  ASSERT_EQ(true, asoct.shutdown());
}

class BlockingHook : public AdminSocketHook {
public:
  Mutex _lock;
  Cond _cond;

  BlockingHook() : _lock("BlockingHook::_lock") {}

  bool call(std::string command, cmdmap_t& cmdmap, std::string format, bufferlist& result) {
    Mutex::Locker l(_lock);
    _cond.Wait(_lock);
    return true;
  }
};

TEST(AdminSocketClient, Ping) {
  string path = get_rand_socket_path();
  std::unique_ptr<AdminSocket>
      asokc(new AdminSocket(g_ceph_context));
  AdminSocketClient client(path);
  // no socket
  {
    bool ok;
    std::string result = client.ping(&ok);
    EXPECT_NE(std::string::npos, result.find("No such file or directory"));
    ASSERT_FALSE(ok);
  }
  // file exists but does not allow connections (no process, wrong type...)
  ASSERT_TRUE(::creat(path.c_str(), 0777));
  {
    bool ok;
    std::string result = client.ping(&ok);
#if defined(__APPLE__) || defined(__FreeBSD__)
    const char* errmsg = "Socket operation on non-socket";
#else
    const char* errmsg = "Connection refused";
#endif
    EXPECT_NE(std::string::npos, result.find(errmsg));
    ASSERT_FALSE(ok);
  }
  // a daemon is connected to the socket
  {
    AdminSocketTest asoct(asokc.get());
    ASSERT_TRUE(asoct.init(path));
    bool ok;
    std::string result = client.ping(&ok);
    EXPECT_EQ("", result);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(asoct.shutdown());
  }
  // hardcoded five seconds timeout prevents infinite blockage
  {
    AdminSocketTest asoct(asokc.get());
    BlockingHook *blocking = new BlockingHook();
    ASSERT_EQ(0, asoct.m_asokc->register_command("0", "0", blocking, ""));
    ASSERT_TRUE(asoct.init(path));
    bool ok;
    std::string result = client.ping(&ok);
    EXPECT_NE(std::string::npos, result.find("Resource temporarily unavailable"));
    ASSERT_FALSE(ok);
    {
      Mutex::Locker l(blocking->_lock);
      blocking->_cond.Signal();
    }
    ASSERT_TRUE(asoct.shutdown());
    delete blocking;
  }
}

TEST(AdminSocket, bind_and_listen) {
  string path = get_rand_socket_path();
  std::unique_ptr<AdminSocket>
      asokc(new AdminSocket(g_ceph_context));

  AdminSocketTest asoct(asokc.get());
  // successfull bind
  {
    int fd = 0;
    string message;
    message = asoct.bind_and_listen(path, &fd);
    ASSERT_NE(0, fd);
    ASSERT_EQ("", message);
    ASSERT_EQ(0, ::close(fd));
    ASSERT_EQ(0, ::unlink(path.c_str()));
  }
  // silently discard an existing file
  {
    int fd = 0;
    string message;
    ASSERT_TRUE(::creat(path.c_str(), 0777));
    message = asoct.bind_and_listen(path, &fd);
    ASSERT_NE(0, fd);
    ASSERT_EQ("", message);
    ASSERT_EQ(0, ::close(fd));
    ASSERT_EQ(0, ::unlink(path.c_str()));
  }
  // do not take over a live socket
  {
    ASSERT_TRUE(asoct.init(path));
    int fd = 0;
    string message;
    message = asoct.bind_and_listen(path, &fd);
    std::cout << "message: " << message << std::endl;
    EXPECT_NE(std::string::npos, message.find("File exists"));
    ASSERT_TRUE(asoct.shutdown());
  }
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  vector<const char*> def_args;
  global_init(&def_args, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd .. ;
 *   make unittest_admin_socket &&
 *    valgrind \
 *    --max-stackframe=20000000 --tool=memcheck \
 *   ./unittest_admin_socket --debug-asok 20 # --gtest_filter=AdminSocket*.*
 * "
 * End:
 */

