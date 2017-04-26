#include "gtest/gtest.h"

#include "common/WorkQueue.h"
#include "common/ceph_argparse.h"
#include "test/unit.h"

TEST(WorkQueue, StartStop)
{
  ThreadPool tp(g_ceph_context, "foo", "tp_foo", 10, "");
  
  tp.start();
  tp.pause();
  tp.pause_new();
  tp.unpause();
  tp.unpause();
  tp.drain();
  tp.stop();
}

TEST(WorkQueue, Resize)
{
  ThreadPool tp(g_ceph_context, "bar", "tp_bar", 2, "osd_op_threads");
  
  tp.start();

  sleep(1);
  ASSERT_EQ(2, tp.get_num_threads());

  g_conf->set_val("osd op threads", "5");
  g_conf->apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(5, tp.get_num_threads());

  g_conf->set_val("osd op threads", "3");
  g_conf->apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(3, tp.get_num_threads());

  g_conf->set_val("osd op threads", "15");
  g_conf->apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(15, tp.get_num_threads());

  g_conf->set_val("osd op threads", "0");
  g_conf->apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(15, tp.get_num_threads());

  g_conf->set_val("osd op threads", "-1");
  g_conf->apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(15, tp.get_num_threads());

  sleep(1);
  tp.stop();
}
