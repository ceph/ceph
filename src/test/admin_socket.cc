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

#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/admin_socket.h"
#include "common/admin_socket_client.h"
#include "common/ceph_argparse.h"
#include "json_spirit/json_spirit.h"
#include "gtest/gtest.h"
#include "fmt/format.h"

#include <stdint.h>
#include <string.h>
#include <string>
#include <sys/un.h>
#include <signal.h>

using namespace std;

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
  std::unique_ptr<AdminSocket> asokc = std::make_unique<AdminSocket>(g_ceph_context);
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
}

TEST(AdminSocket, TeardownSetup) {
  std::unique_ptr<AdminSocket> asokc = std::make_unique<AdminSocket>(g_ceph_context);
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_rand_socket_path()));
  ASSERT_EQ(true, asoct.shutdown());
}

TEST(AdminSocket, SendHelp) {
  std::unique_ptr<AdminSocket> asokc = std::make_unique<AdminSocket>(g_ceph_context);
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
  std::unique_ptr<AdminSocket> asokc = std::make_unique<AdminSocket>(g_ceph_context);
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_rand_socket_path()));
  AdminSocketClient client(get_rand_socket_path());
  string version;
  ASSERT_EQ("", client.do_request("{\"prefix\":\"0\"}", &version));
  ASSERT_EQ(CEPH_ADMIN_SOCK_VERSION, version);
  ASSERT_EQ(true, asoct.shutdown());
}

TEST(AdminSocket, SendTooLongRequest) {
  std::unique_ptr<AdminSocket> asokc = std::make_unique<AdminSocket>(g_ceph_context);
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_rand_socket_path()));
  AdminSocketClient client(get_rand_socket_path());
  string version;
  string request(16384, 'a');
  //if admin_socket cannot handle it, segfault will happened.
  ASSERT_NE("", client.do_request(request, &version));
  ASSERT_EQ(true, asoct.shutdown());
}

class MyTest : public AdminSocketHook {
  int call(std::string_view command, const cmdmap_t& cmdmap,
	   const bufferlist&,
	   Formatter *f,
	   std::ostream& ss,
	   bufferlist& result) override {
    std::vector<std::string> args;
    TOPNSPC::common::cmd_getval(cmdmap, "args", args);
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
    return 0;
  }
};

TEST(AdminSocket, RegisterCommand) {
  std::unique_ptr<AdminSocket> asokc = std::make_unique<AdminSocket>(g_ceph_context);
  std::unique_ptr<AdminSocketHook> my_test_asok = std::make_unique<MyTest>();
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_rand_socket_path()));
  AdminSocketClient client(get_rand_socket_path());
  ASSERT_EQ(0, asoct.m_asokc->register_command("test", my_test_asok.get(), ""));
  string result;
  ASSERT_EQ("", client.do_request("{\"prefix\":\"test\"}", &result));
  ASSERT_EQ("test|", result);
  ASSERT_EQ(true, asoct.shutdown());
}

class MyTest2 : public AdminSocketHook {
  int call(std::string_view command, const cmdmap_t& cmdmap,
	   const bufferlist&,
	   Formatter *f,
	   std::ostream& ss,
	   bufferlist& result) override {
    std::vector<std::string> args;
    TOPNSPC::common::cmd_getval(cmdmap, "args", args);
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
    ss << "error stream";
    return 0;
  }
};

TEST(AdminSocket, RegisterCommandPrefixes) {
  std::unique_ptr<AdminSocket> asokc = std::make_unique<AdminSocket>(g_ceph_context);
  std::unique_ptr<AdminSocketHook> my_test_asok = std::make_unique<MyTest>();
  std::unique_ptr<AdminSocketHook> my_test2_asok = std::make_unique<MyTest2>();
  AdminSocketTest asoct(asokc.get());
  ASSERT_EQ(true, asoct.shutdown());
  ASSERT_EQ(true, asoct.init(get_rand_socket_path()));
  AdminSocketClient client(get_rand_socket_path());
  ASSERT_EQ(0, asoct.m_asokc->register_command("test name=args,type=CephString,n=N", my_test_asok.get(), ""));
  ASSERT_EQ(0, asoct.m_asokc->register_command("test command name=args,type=CephString,n=N", my_test2_asok.get(), ""));
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
  ceph::mutex _lock = ceph::make_mutex("BlockingHook::_lock");
  ceph::condition_variable _cond;

  BlockingHook() = default;

  int call(std::string_view command, const cmdmap_t& cmdmap,
	   const bufferlist&,
	   Formatter *f,
	   std::ostream& ss,
	   bufferlist& result) override {
    std::unique_lock l{_lock};
    _cond.wait(l);
    return 0;
  }
};

TEST(AdminSocketClient, Ping) {
  string path = get_rand_socket_path();
  std::unique_ptr<AdminSocket> asokc = std::make_unique<AdminSocket>(g_ceph_context);
  AdminSocketClient client(path);
  // no socket
  {
    bool ok;
    std::string result = client.ping(&ok);
#ifndef _WIN32
// TODO: convert WSA errors.
    EXPECT_NE(std::string::npos, result.find("No such file or directory"));
#endif
    ASSERT_FALSE(ok);
  }
  // file exists but does not allow connections (no process, wrong type...)
  #ifdef _WIN32
  int fd = ::creat(path.c_str(), _S_IREAD | _S_IWRITE);
  #else
  int fd = ::creat(path.c_str(), 0777);
  #endif
  ASSERT_TRUE(fd);
  // On Windows, we won't be able to remove the file unless we close it
  // first.
  ASSERT_FALSE(::close(fd));
  {
    bool ok;
    std::string result = client.ping(&ok);
#ifndef _WIN32
#if defined(__APPLE__) || defined(__FreeBSD__)
    const char* errmsg = "Socket operation on non-socket";
#else
    const char* errmsg = "Connection refused";
#endif
    EXPECT_NE(std::string::npos, result.find(errmsg));
#endif /* _WIN32 */
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
    ASSERT_EQ(0, asoct.m_asokc->register_command("0", blocking, ""));
    ASSERT_TRUE(asoct.init(path));
    bool ok;
    std::string result = client.ping(&ok);
    #ifndef _WIN32
    EXPECT_NE(std::string::npos, result.find("Resource temporarily unavailable"));
    #endif
    ASSERT_FALSE(ok);
    {
      std::lock_guard l{blocking->_lock};
      blocking->_cond.notify_all();
    }
    ASSERT_TRUE(asoct.shutdown());
    delete blocking;
  }
}

TEST(AdminSocket, bind_and_listen) {
  string path = get_rand_socket_path();
  std::unique_ptr<AdminSocket> asokc = std::make_unique<AdminSocket>(g_ceph_context);

  AdminSocketTest asoct(asokc.get());
  // successfull bind
  {
    int fd = 0;
    string message;
    message = asoct.bind_and_listen(path, &fd);
    ASSERT_NE(0, fd);
    ASSERT_EQ("", message);
    ASSERT_EQ(0, ::compat_closesocket(fd));
    ASSERT_EQ(0, ::unlink(path.c_str()));
  }
  // silently discard an existing file
  {
    int fd = 0;
    string message;
    #ifdef _WIN32
    int fd2 = ::creat(path.c_str(), _S_IREAD | _S_IWRITE);
    #else
    int fd2 = ::creat(path.c_str(), 0777);
    #endif
    ASSERT_TRUE(fd2);
    // On Windows, we won't be able to remove the file unless we close it
    // first.
    ASSERT_FALSE(::close(fd2));
    message = asoct.bind_and_listen(path, &fd);
    ASSERT_NE(0, fd);
    ASSERT_EQ("", message);
    ASSERT_EQ(0, ::compat_closesocket(fd));
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

class AdminSocketRaise: public ::testing::Test 
{
public:
  struct TestSignal {
    int sig;
    const char * name;
    std::atomic<int> count;
  };

  static void SetUpTestSuite() {
    signal(sig1.sig, sighandler);
    signal(sig2.sig, sighandler);
  }
  static void TearDownTestSuite()
  {
    signal(sig1.sig, SIG_DFL);
    signal(sig2.sig, SIG_DFL);
  }
  void SetUp() override
  {
    std::string path = get_rand_socket_path();
    asock = std::make_unique<AdminSocket>(g_ceph_context);
    asock_client = std::make_unique<AdminSocketClient>(path);
    ASSERT_TRUE(asock->init(path));
    sig1.count = 0;
    sig2.count = 0;
  }
  void TearDown() override
  {
    AdminSocketTest(asock.get()).shutdown();
  }
protected:
  static TestSignal sig1;
  static TestSignal sig2;

  std::unique_ptr<AdminSocket> asock;
  std::unique_ptr<AdminSocketClient> asock_client;

  static void sighandler(int signal)
  {
    if (signal == sig1.sig) {
      sig1.count++;
    } else if (signal == sig2.sig) {
      sig2.count++;
    }

    // Windows resets the handler upon signal delivery
    // as apparently some linuxes do as well.
    // The below shouldn't hurt in any case.
    ::signal(signal, sighandler);
  }
  std::string send_raise(std::optional<std::string> arg, std::optional<double> after, bool cancel)
  {
    JSONFormatter f;
    f.open_object_section("");
    f.dump_string("prefix", "raise");
    if (arg) {
      f.dump_string("signal", *arg);
    }
    if (after) {
      f.dump_float("after", *after);
    }
    if (cancel) {
      f.dump_bool("cancel", true);
    }
    f.close_section();

    bufferlist command;
    f.flush(command);

    std::string response;

    asock_client->do_request(command.to_str(), &response);
    return response;
  }

  std::string send_raise_cancel(std::optional<std::string> arg = std::nullopt) {
    return send_raise(arg, std::nullopt, true);
  }

  std::string send_raise(std::string arg, std::optional<double> after = std::nullopt) {
    return send_raise(arg, after, false);
  }
};

AdminSocketRaise::TestSignal AdminSocketRaise::sig1 = { SIGINT, "INT", 0 };
AdminSocketRaise::TestSignal AdminSocketRaise::sig2 = { SIGTERM, "TERM", 0 };

TEST_F(AdminSocketRaise, List) {
  auto r = send_raise("-l");
  json_spirit::mValue v;
  ASSERT_TRUE(json_spirit::read(r, v));
  ASSERT_EQ(json_spirit::Value_type::obj_type, v.type());
  EXPECT_EQ(sig1.sig, v.get_obj()[sig1.name].get_int());
  EXPECT_EQ(sig2.sig, v.get_obj()[sig2.name].get_int());
}

TEST_F(AdminSocketRaise, ImmediateFormats) {
  std::string name1, name2;

  name1 = sig1.name;
  std::transform(name1.begin(), name1.end(), name1.begin(), [](int c) { return std::tolower(c); });
  name2 = fmt::format("-{}", sig2.name);
  std::transform(name2.begin(), name2.end(), name2.begin(), [](int c) { return std::tolower(c); });

  send_raise(fmt::format("-{}", sig1.sig));
  send_raise(name1);
  send_raise(name2);
  send_raise(fmt::format("{}", sig2.sig));
  EXPECT_EQ(2, sig1.count.load());
  EXPECT_EQ(2, sig2.count.load());
}

TEST_F(AdminSocketRaise, Async)
{
  using std::chrono::milliseconds;

#ifdef WIN32
  GTEST_SKIP() << "Windows doesn't support --after behavior";
#endif

  ASSERT_EQ("", send_raise(fmt::format("{}", sig1.sig)));
  ASSERT_EQ("", send_raise(sig2.name, 0.1));

  EXPECT_EQ(1, sig1.count.load());
  EXPECT_EQ(0, sig2.count.load());

  this_thread::sleep_for(milliseconds(150));

  EXPECT_EQ(1, sig1.count.load());
  EXPECT_EQ(1, sig2.count.load());
}

TEST_F(AdminSocketRaise, AsyncReschedule)
{
  using std::chrono::milliseconds;

#ifdef WIN32
  GTEST_SKIP() << "Windows doesn't support --after behavior";
#endif

  ASSERT_EQ("", send_raise(sig1.name, 0.1));
  ASSERT_EQ("", send_raise(sig2.name, 0.2));

  EXPECT_EQ(0, sig1.count.load());
  EXPECT_EQ(0, sig2.count.load());

  this_thread::sleep_for(milliseconds(150));

  // USR1 got overridden by the second async schedule
  EXPECT_EQ(0, sig1.count.load());
  EXPECT_EQ(0, sig2.count.load());

  this_thread::sleep_for(milliseconds(100));
  EXPECT_EQ(0, sig1.count.load());
  EXPECT_EQ(1, sig2.count.load());
}

TEST_F(AdminSocketRaise, AsyncCancel)
{
  using std::chrono::milliseconds;

#ifdef WIN32
  GTEST_SKIP() << "Windows doesn't support --after behavior";
#endif

  ASSERT_EQ("", send_raise(sig1.name, 0.1));

  EXPECT_EQ(0, sig1.count.load());
  EXPECT_EQ(0, sig2.count.load());

  ASSERT_EQ("", send_raise_cancel(sig2.name));

  this_thread::sleep_for(milliseconds(150));

  // cancel shouldn't have worked because the signals
  // didn't match
  EXPECT_EQ(1, sig1.count.load());

  ASSERT_EQ("", send_raise(sig2.name, 0.1));
  ASSERT_EQ("", send_raise_cancel(sig2.name));

  this_thread::sleep_for(milliseconds(150));

  // cancel must have worked
  EXPECT_EQ(0, sig2.count.load());

  ASSERT_EQ("", send_raise(sig1.name, 0.1));
  ASSERT_EQ("", send_raise_cancel());

  // cancel must have worked, the counter stays 1
  EXPECT_EQ(1, sig1.count.load());
}

TEST_F(AdminSocketRaise, StopCont)
{
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;
  using std::chrono::system_clock;

#ifdef WIN32
  GTEST_SKIP() << "Windows doesn't support SIGSTOP/SIGCONT and --after";
#endif

  auto then = system_clock::now();
  ASSERT_EQ("", send_raise("CONT", 0.2));
  ASSERT_EQ("", send_raise("STOP"));
  auto elapsed = system_clock::now() - then;
  // give it a 1% slack
  EXPECT_LE(milliseconds(198), duration_cast<milliseconds>(elapsed));
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

