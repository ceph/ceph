#include <gtest/gtest.h>

#include "log/Log.h"
#include "common/Clock.h"
#include "include/coredumpctl.h"
#include "SubsystemMap.h"

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/dout.h"

#include <unistd.h>

#include <limits.h>

using namespace std;
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
 
  log.set_log_file("foo");
  log.reopen_log_file();

  log.set_stderr_level(5, -1);


  for (int i=0; i<100; i++) {
    int sys = i % 4;
    int l = 5 + (i%4);
    if (subs.should_gather(sys, l)) {
      MutableEntry e(l, sys);
      log.submit_entry(std::move(e));
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
  log.set_log_file("foo");
  log.reopen_log_file();

  const int l = 0;
  {
    MutableEntry e(l, 1);
    auto& out = e.get_ostream();
    out << (std::streambuf*)nullptr;
    EXPECT_TRUE(out.bad()); // writing nullptr to a stream sets its badbit
    log.submit_entry(std::move(e));
  }
  {
    MutableEntry e(l, 1);
    auto& out = e.get_ostream();
    EXPECT_FALSE(out.bad()); // should not see failures from previous log entry
    out << "hello world";
    log.submit_entry(std::move(e));
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
  log.set_log_file("big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l))
      log.submit_entry(MutableEntry(1, 0));
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
  log.set_log_file("big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l)) {
      MutableEntry e(l, 1);
      e.get_ostream() << "this is a long string asdf asdf asdf asdf asdf asdf asd fasd fasdf ";
      log.submit_entry(std::move(e));
    }
  }
  log.flush();
  log.stop();
}

TEST(Log, ManyGatherLogStackSpillover)
{
  SubsystemMap subs;
  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 10);
  Log log(&subs);
  log.start();
  log.set_log_file("big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l)) {
      MutableEntry e(l, 1);
      auto& s = e.get_ostream();
      s << "foo";
      s << std::string(sizeof(e) * 2, '-');
      log.submit_entry(std::move(e));
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
  log.set_log_file("big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l))
      log.submit_entry(MutableEntry(l, 1));
  }
  log.flush();
  log.stop();
}

static void readpipe(int fd, int verify)
{
  while (1) {
    /* Use larger buffer on receiver as Linux will allow pipes buffers to
     * exceed PIPE_BUF. We can't avoid tearing due to small read buffers from
     * the Ceph side.
     */

    char buf[65536] = "";
    int rc = read(fd, buf, (sizeof buf) - 1);
    if (rc == 0) {
      _exit(0);
    } else if (rc == -1) {
      _exit(1);
    } else if (rc > 0) {
      if (verify) {
        char* p = strrchr(buf, '\n');
        /* verify no torn writes */
        if (p == NULL) {
          _exit(2);
        } else if (p[1] != '\0') {
          std::ignore = write(2, buf, strlen(buf));
          _exit(3);
        }
      }
    } else _exit(100);
    usleep(500);
  }
}

TEST(Log, StderrPipeAtomic)
{
  int pfd[2] = {-1, -1};
  int rc = pipe(pfd);
  ASSERT_EQ(rc, 0);
  pid_t pid = fork();
  if (pid == 0) {
    close(pfd[1]);
    readpipe(pfd[0], 1);
  } else if (pid == (pid_t)-1) {
    ASSERT_EQ(0, 1);
  }
  close(pfd[0]);

  SubsystemMap subs;
  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 10);
  Log log(&subs);
  log.start();
  log.set_log_file("");
  log.reopen_log_file();
  log.set_stderr_fd(pfd[1]);
  log.set_stderr_level(1, 20);
  /* -128 for prefix space */
  for (int i = 0; i < PIPE_BUF-128; i++) {
    MutableEntry e(1, 1);
    auto& s = e.get_ostream();
    for (int j = 0; j < i; j++) {
      char c = 'a';
      c += (j % 26);
      s << c;
    }
    log.submit_entry(std::move(e));
  }
  log.flush();
  log.stop();
  close(pfd[1]);
  int status;
  pid_t waited = waitpid(pid, &status, 0);
  ASSERT_EQ(pid, waited);
  ASSERT_NE(WIFEXITED(status), 0);
  ASSERT_EQ(WEXITSTATUS(status), 0);
}

TEST(Log, StderrPipeBig)
{
  int pfd[2] = {-1, -1};
  int rc = pipe(pfd);
  ASSERT_EQ(rc, 0);
  pid_t pid = fork();
  if (pid == 0) {
    /* no verification as some reads will be torn due to size > PIPE_BUF */
    close(pfd[1]);
    readpipe(pfd[0], 0);
  } else if (pid == (pid_t)-1) {
    ASSERT_EQ(0, 1);
  }
  close(pfd[0]);

  SubsystemMap subs;
  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 10);
  Log log(&subs);
  log.start();
  log.set_log_file("");
  log.reopen_log_file();
  log.set_stderr_fd(pfd[1]);
  log.set_stderr_level(1, 20);
  /* -128 for prefix space */
  for (int i = 0; i < PIPE_BUF*2; i++) {
    MutableEntry e(1, 1);
    auto& s = e.get_ostream();
    for (int j = 0; j < i; j++) {
      char c = 'a';
      c += (j % 26);
      s << c;
    }
    log.submit_entry(std::move(e));
  }
  log.flush();
  log.stop();
  close(pfd[1]);
  int status;
  pid_t waited = waitpid(pid, &status, 0);
  ASSERT_EQ(pid, waited);
  ASSERT_NE(WIFEXITED(status), 0);
  ASSERT_EQ(WEXITSTATUS(status), 0);
}

void do_segv()
{
  SubsystemMap subs;
  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 1);
  Log log(&subs);
  log.start();
  log.set_log_file("big");
  log.reopen_log_file();

  log.inject_segv();
  MutableEntry e(10, 1);
  {
    PrCtl unset_dumpable;
    log.submit_entry(std::move(e));  // this should segv
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
  log.set_log_file("big");
  log.reopen_log_file();
  int l = 10;
  {
    MutableEntry e(l, 1);
    std::string msg(10000000, 'a');
    e.get_ostream() << msg;
    log.submit_entry(std::move(e));
  }
  log.flush();
  log.stop();
}

TEST(Log, LargeFromSmallLog)
{
  SubsystemMap subs;
  subs.set_log_level(1, 20);
  subs.set_gather_level(1, 10);
  Log log(&subs);
  log.start();
  log.set_log_file("big");
  log.reopen_log_file();
  int l = 10;
  {
    MutableEntry e(l, 1);
    for (int i = 0; i < 1000000; i++) {
      std::string msg(10, 'a');
      e.get_ostream() << msg;
    }
    log.submit_entry(std::move(e));
  }
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
  log.set_log_file("time_switch_log");
  log.reopen_log_file();
  int l = 10;
  bool coarse = true;
  for (auto i = 0U; i < 300; ++i) {
    MutableEntry e(l, 1);
    e.get_ostream() << "SQUID THEFT! PUNISHABLE BY DEATH!";
    log.submit_entry(std::move(e));
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
    ASSERT_EQ(8u, strlen(c + 1));
  }
  {
    clock.refine();
    auto t = clock.now();
    ceph::logging::append_time(t, buf, buflen);
    auto c = std::strrchr(buf, '.');
    ASSERT_NE(c, nullptr);
    ASSERT_EQ(11u, std::strlen(c + 1));
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

TEST(Log, GarbleRecovery)
{
  static const char* test_file="log_for_moment";

  Log* saved = g_ceph_context->_log;
  Log log(&g_ceph_context->_conf->subsys);
  log.start();
  unlink(test_file);
  log.set_log_file(test_file);
  log.reopen_log_file();
  g_ceph_context->_log = &log;

  std::string long_message(1000,'c');
  ldout(g_ceph_context, 0) << long_message << dendl;
  ldout(g_ceph_context, 0) << "Prologue" << (std::streambuf*)nullptr << long_message << dendl;
  ldout(g_ceph_context, 0) << "Epitaph" << long_message << dendl;

  g_ceph_context->_log = saved;
  log.flush();
  log.stop();
  struct stat file_status;
  ASSERT_EQ(stat(test_file, &file_status), 0);
  ASSERT_GT(file_status.st_size, 2000);
}

int main(int argc, char **argv)
{
  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
