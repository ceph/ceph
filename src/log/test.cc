#include <gtest/gtest.h>

#include "log/log.h"
#include "common/Clock.h"


using namespace ceph::log;

TEST(Log, Simple)
{
  SubsystemMap subs;
  subs.add(0, "none", 10, 10);
  subs.add(1, "foosys", 20, 1);
  subs.add(2, "bar", 20, 2);
  subs.add(3, "baz", 10, 3);

  Log log(&subs);
  log.start();
 
  log.set_log_file("/tmp/foo");
  log.reopen_log_file();

  log.set_stderr_level(5, -1);


  for (int i=0; i<100; i++) {
    int sys = i % 4;
    int l = 5 + (i%4);
    if (subs.should_gather(sys, l)) {
      Entry *e = new Entry(ceph_clock_now(NULL),
			   pthread_self(),
			   l,
			   sys,
			   "hello world");
      log.submit_entry(e);
    }
  }
  
  log.flush();

  log.dump_recent();

  log.stop();
}

int many = 10000000;

TEST(Log, ManyNoGather)
{
  SubsystemMap subs;
  subs.add(1, "foo", 1, 1);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l))
      log.submit_entry(new Entry(ceph_clock_now(NULL), pthread_self(), l, 1));
  }
  log.flush();
  log.stop();
}


TEST(Log, ManyGatherLog)
{
  SubsystemMap subs;
  subs.add(1, "foo", 20, 10);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l))
      log.submit_entry(new Entry(ceph_clock_now(NULL), pthread_self(), l, 1,
				 "this is a long string asdf asdf asdf asdf asdf asdf asd fasd fasdf "));
  }
  log.flush();
  log.stop();
}

TEST(Log, ManyGatherLogB)
{
  SubsystemMap subs;
  subs.add(1, "foo", 20, 10);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l)) {
      Entry *e = new Entry(ceph_clock_now(NULL), pthread_self(), l, 1);
      ostringstream oss;
      oss << "this i a long stream asdf asdf asdf asdf asdf asdf asdf asdf asdf as fd";
      e->m_str = oss.str();
      log.submit_entry(e);
    }
  }
  log.flush();
  log.stop();
}
TEST(Log, ManyGatherLogC)
{
  SubsystemMap subs;
  subs.add(1, "foo", 20, 10);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l)) {
      Entry *e = new Entry(ceph_clock_now(NULL), pthread_self(), l, 1);
      ostringstream oss;
      oss.str().reserve(80);
      oss << "this i a long stream asdf asdf asdf asdf asdf asdf asdf asdf asdf as fd";
      e->m_str = oss.str();
      log.submit_entry(e);
    }
  }
  log.flush();
  log.stop();
}

TEST(Log, ManyGather)
{
  SubsystemMap subs;
  subs.add(1, "foo", 20, 1);
  Log log(&subs);
  log.start();
  log.set_log_file("/tmp/big");
  log.reopen_log_file();
  for (int i=0; i<many; i++) {
    int l = 10;
    if (subs.should_gather(1, l))
      log.submit_entry(new Entry(ceph_clock_now(NULL), pthread_self(), l, 1));
  }
  log.flush();
  log.stop();
}
