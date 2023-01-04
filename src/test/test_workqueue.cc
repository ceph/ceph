#include "gtest/gtest.h"

#include "common/WorkQueue.h"
#include "common/ceph_argparse.h"

using namespace std;

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
  ThreadPool tp(g_ceph_context, "bar", "tp_bar", 2, "filestore_op_threads");
  
  tp.start();

  sleep(1);
  ASSERT_EQ(2, tp.get_num_threads());

  g_conf().set_val("filestore op threads", "5");
  g_conf().apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(5, tp.get_num_threads());

  g_conf().set_val("filestore op threads", "3");
  g_conf().apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(3, tp.get_num_threads());

  g_conf().set_val("filestore op threads", "0");
  g_conf().apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(0, tp.get_num_threads());

  g_conf().set_val("filestore op threads", "15");
  g_conf().apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(15, tp.get_num_threads());

  g_conf().set_val("filestore op threads", "-1");
  g_conf().apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(15, tp.get_num_threads());

  sleep(1);
  tp.stop();
}

class twq : public ThreadPool::WorkQueue<int> {
public:
    twq(time_t timeout, time_t suicide_timeout, ThreadPool *tp)
        : ThreadPool::WorkQueue<int>("test_wq", ceph::make_timespan(timeout), ceph::make_timespan(suicide_timeout), tp) {}

    bool _enqueue(int* item) override {
        return true;
    }
    void _dequeue(int* item) override {
        ceph_abort();
    }
    bool _empty() override {
        return true;
    }
    int *_dequeue() override {
        return nullptr;
    }
    void _process(int *osr, ThreadPool::TPHandle &handle) override {
    }
    void _process_finish(int *osr) override {
    }
    void _clear() override {
    }
};

TEST(WorkQueue, change_timeout){
    ThreadPool tp(g_ceph_context, "bar", "tp_bar", 2, "filestore_op_threads");
    tp.start();
    twq wq(2, 20, &tp);
    // check timeout and suicide
    ASSERT_EQ(ceph::make_timespan(2), wq.timeout_interval.load());
    ASSERT_EQ(ceph::make_timespan(20), wq.suicide_interval.load());

    // change the timeout and suicide and then check them
    wq.set_timeout(4);
    wq.set_suicide_timeout(40);
    ASSERT_EQ(ceph::make_timespan(4), wq.timeout_interval.load());
    ASSERT_EQ(ceph::make_timespan(40), wq.suicide_interval.load());
    tp.stop();
}
