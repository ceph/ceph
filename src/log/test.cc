#include <gtest/gtest.h>

#include "log/Log.h"
#include "common/Clock.h"
#include "common/PrebufferedStreambuf.h"
#include "include/coredumpctl.h"
#include "SubsystemMap.h"

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/dout.h"

using namespace ceph::logging;

TEST(Log, Simple)
{
  SubsystemMap subs;
  subs.set_log_level(0, 10);
  subs.set_gather_level(0, 10);

  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 1);

  subs.set_log_level(2, 20);
  subs.set_gather_level(2, 2);

  subs.set_log_level(3, 10);
  subs.set_gather_level(3, 3);

  Log log(&subs);
  log.start();
 
  log.set_log_file("/tmp/foo");
  log.reopen_log_file();

  log.set_stderr_level(5, -1);


  for (int i=0; i<100; i++) {
    int sys = i % 4;
    int l = 5 + (i%4);
    if (subs.should_gather(sys, l)) {
      Entry *e = log.create_entry(l, sys, "hello world");
      log.submit_entry(e);
    }
  }
  
  log.flush();

  log.dump_recent();

  log.stop();
}

TEST(Log, ReuseBad)
{
  SubsystemMap subs;
  subs.set_log_level(1, 1);
  subs.set_gather_level(1, 1);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/foo");
  log.reopen_log_file();

  const int l = 0;
  {
    auto e = log.create_entry(l, 1);
    auto& out = e->get_ostream();
    out << (std::streambuf*)nullptr;
    EXPECT_TRUE(out.bad()); // writing nullptr to a stream sets its badbit
    log.submit_entry(e);
  }
  {
    auto e = log.create_entry(l, 1);
    auto& out = e->get_ostream();
    EXPECT_FALSE(out.bad()); // should not see failures from previous log entry
    out << "hello world";
    log.submit_entry(e);
  }

  log.flush();
  log.stop();
}

int many = 10000;

TEST(Log, ManyNoGather)
{
  SubsystemMap subs;
  subs.set_log_level(1, 1);
  subs.set_gather_level(1, 1);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l))
      log.submit_entry(log.create_entry(l, 1));
  }
  log.flush();
  log.stop();
}


TEST(Log, ManyGatherLog)
{
  SubsystemMap subs;
  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 10);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l))
      log.submit_entry(log.create_entry(l, 1,
					"this is a long string asdf asdf asdf asdf asdf asdf asd fasd fasdf "));
  }
  log.flush();
  log.stop();
}

TEST(Log, ManyGatherLogStringAssign)
{
  SubsystemMap subs;
  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 10);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l)) {
      Entry *e = log.create_entry(l, 1);
      ostringstream oss;
      oss << "this i a long stream asdf asdf asdf asdf asdf asdf asdf asdf asdf as fd";
      e->set_str(oss.str());
      log.submit_entry(e);
    }
  }
  log.flush();
  log.stop();
}
TEST(Log, ManyGatherLogStringAssignWithReserve)
{
  SubsystemMap subs;
  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 10);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l)) {
      Entry *e = log.create_entry(l, 1);
      ostringstream oss;
      oss.str().reserve(80);
      oss << "this i a long stream asdf asdf asdf asdf asdf asdf asdf asdf asdf as fd";
      e->set_str(oss.str());
      log.submit_entry(e);
    }
  }
  log.flush();
  log.stop();
}

TEST(Log, ManyGatherLogPrebuf)
{
  SubsystemMap subs;
  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 10);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l)) {
      Entry *e = log.create_entry(l, 1);
      PrebufferedStreambuf psb(e->m_static_buf, sizeof(e->m_static_buf));
      ostream oss(&psb);
      oss << "this i a long stream asdf asdf asdf asdf asdf asdf asdf asdf asdf as fd";
      //e->m_str = oss.str();
      log.submit_entry(e);
    }
  }
  log.flush();
  log.stop();
}

TEST(Log, ManyGatherLogPrebufOverflow)
{
  SubsystemMap subs;
  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 10);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l)) {
      Entry *e = log.create_entry(l, 1);
      PrebufferedStreambuf psb(e->m_static_buf, sizeof(e->m_static_buf));
      ostream oss(&psb);
      oss << "this i a long stream asdf asdf asdf asdf asdf asdf asdf asdf asdf as fd"
          << std::string(sizeof(e->m_static_buf) * 2, '-') ;
      //e->m_str = oss.str();
      log.submit_entry(e);
    }
  }
  log.flush();
  log.stop();
}

TEST(Log, ManyGather)
{
  SubsystemMap subs;
  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 1);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l))
      log.submit_entry(log.create_entry(l, 1));
  }
  log.flush();
  log.stop();
}

void do_segv()
{
  SubsystemMap subs;
  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 1);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/big");
  log.reopen_log_file();

  log.inject_segv();
  Entry *e = log.create_entry(10, 1);
  {
    PrCtl unset_dumpable;
    log.submit_entry(e);  // this should segv
  }

  log.flush();
  log.stop();
}

TEST(Log, InternalSegv)
{
  ASSERT_DEATH(do_segv(), ".*");
}

TEST(Log, LargeLog)
{
  SubsystemMap subs;
  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 10);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/big");
  log.reopen_log_file();
  int l = 10;
  Entry *e = log.create_entry(l, 1);

  std::string msg(10000000, 0);
  e->set_str(msg);
  log.submit_entry(e);
  log.flush();
  log.stop();
}

// Make sure nothing bad happens when we switch

TEST(Log, TimeSwitch)
{
  SubsystemMap subs;
  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 10);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/time_switch_log");
  log.reopen_log_file();
  int l = 10;
  bool coarse = true;
  for (auto i = 0U; i < 300; ++i) {
    log.submit_entry(
      log.create_entry(l, 1, "SQUID THEFT! PUNISHABLE BY DEATH!"));
    if (i % 50)
      log.set_coarse_timestamps(coarse = !coarse);
  }
  log.flush();
  log.stop();
}

TEST(Log, TimeFormat)
{
  static constexpr auto buflen = 128u;
  char buf[buflen];
  ceph::logging::log_clock clock;
  {
    clock.coarsen();
    auto t = clock.now();
    ceph::logging::append_time(t, buf, buflen);
    auto c = std::strrchr(buf, '.');
    ASSERT_NE(c, nullptr);
    ASSERT_EQ(3u, strlen(c + 1));
  }
  {
    clock.refine();
    auto t = clock.now();
    ceph::logging::append_time(t, buf, buflen);
    auto c = std::strrchr(buf, '.');
    ASSERT_NE(c, nullptr);
    ASSERT_EQ(6u, std::strlen(c + 1));
  }
}

#define dout_subsys ceph_subsys_context

template <int depth, int x> struct do_log
{
  void log(CephContext* cct);
};

template <int x> struct do_log<12, x>
{
  void log(CephContext* cct);
};

template<int depth, int x> void do_log<depth,x>::log(CephContext* cct)
{
  ldout(cct, 20) << "Log depth=" << depth << " x=" << x << dendl;
  if (rand() % 2) {
    do_log<depth+1, x*2> log;
    log.log(cct);
  } else {
    do_log<depth+1, x*2+1> log;
    log.log(cct);
  }
}

std::string recursion(CephContext* cct)
{
  ldout(cct, 20) << "Preparing recursion string" << dendl;
  return "here-recursion";
}

template<int x> void do_log<12, x>::log(CephContext* cct)
{
  if ((rand() % 16) == 0) {
    ldout(cct, 20) << "End " << recursion(cct) << "x=" << x << dendl;
  } else {
    ldout(cct, 20) << "End x=" << x << dendl;
  }
}

TEST(Log, Speed_gather)
{
  do_log<0,0> start;
  g_ceph_context->_conf->subsys.set_gather_level(ceph_subsys_context, 30);
  g_ceph_context->_conf->subsys.set_log_level(ceph_subsys_context, 0);
  for (int i=0; i<100000;i++) {
    ldout(g_ceph_context, 20) << "Iteration " << i << dendl;
    start.log(g_ceph_context);
  }
}

TEST(Log, Speed_nogather)
{
  do_log<0,0> start;
  g_ceph_context->_conf->subsys.set_gather_level(ceph_subsys_context, 0);
  g_ceph_context->_conf->subsys.set_log_level(ceph_subsys_context, 0);
  for (int i=0; i<100000;i++) {
    ldout(g_ceph_context, 20) << "Iteration " << i << dendl;
    start.log(g_ceph_context);
  }
}


int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
