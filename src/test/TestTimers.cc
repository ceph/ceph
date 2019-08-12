#include "common/ceph_argparse.h"
#include "common/ceph_mutex.h"
#include "common/Timer.h"
#include "global/global_init.h"
#include "include/Context.h"

#include <iostream>

/*
 * TestTimers
 *
 * Tests the timer classes
 */
#define MAX_TEST_CONTEXTS 5

class TestContext;

namespace
{
  int test_array[MAX_TEST_CONTEXTS];
  int array_idx;
  TestContext* test_contexts[MAX_TEST_CONTEXTS];

  ceph::mutex array_lock = ceph::make_mutex("test_timers_mutex");
}

class TestContext : public Context
{
public:
  explicit TestContext(int num_)
    : num(num_)
  {
  }

  void finish(int r) override
  {
    std::lock_guard locker{array_lock};
    cout << "TestContext " << num << std::endl;
    test_array[array_idx++] = num;
  }

  ~TestContext() override
  {
  }

protected:
  int num;
};

class StrictOrderTestContext : public TestContext
{
public:
  explicit StrictOrderTestContext (int num_)
    : TestContext(num_)
  {
  }

  void finish(int r) override
  {
    std::lock_guard locker{array_lock};
    cout << "StrictOrderTestContext " << num << std::endl;
    test_array[num] = num;
  }

  ~StrictOrderTestContext() override
  {
  }
};

static void print_status(const char *str, int ret)
{
  cout << str << ": ";
  cout << ((ret == 0) ? "SUCCESS" : "FAILURE");
  cout << std::endl;
}

template <typename T>
static int basic_timer_test(T &timer, ceph::mutex *lock)
{
  int ret = 0;
  memset(&test_array, 0, sizeof(test_array));
  array_idx = 0;
  memset(&test_contexts, 0, sizeof(test_contexts));

  cout << __PRETTY_FUNCTION__ << std::endl;

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    test_contexts[i] = new TestContext(i);
  }


  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    if (lock)
      lock->lock();
    auto t = ceph::real_clock::now() + std::chrono::seconds(2 * i);
    timer.add_event_at(t, test_contexts[i]);
    if (lock)
      lock->unlock();
  }

  bool done = false;
  do {
    sleep(1);
    std::lock_guard locker{array_lock};
    done = (array_idx == MAX_TEST_CONTEXTS);
  } while (!done);

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    if (test_array[i] != i) {
      ret = 1;
      cout << "error: expected test_array[" << i << "] = " << i
	   << "; got " << test_array[i] << " instead." << std::endl;
    }
  }

  return ret;
}

static int test_out_of_order_insertion(SafeTimer &timer, ceph::mutex *lock)
{
  int ret = 0;
  memset(&test_array, 0, sizeof(test_array));
  array_idx = 0;
  memset(&test_contexts, 0, sizeof(test_contexts));

  cout << __PRETTY_FUNCTION__ << std::endl;

  test_contexts[0] = new StrictOrderTestContext(0);
  test_contexts[1] = new StrictOrderTestContext(1);

  {
    auto t = ceph::real_clock::now() + 100s;
    std::lock_guard locker{*lock};
    timer.add_event_at(t, test_contexts[0]);
  }

  {
    auto t = ceph::real_clock::now() + 2s;
    std::lock_guard locker{*lock};
    timer.add_event_at(t, test_contexts[1]);
  }

  int secs = 0;
  for (; secs < 100 ; ++secs) {
    sleep(1);
    array_lock.lock();
    int a = test_array[1];
    array_lock.unlock();
    if (a == 1)
      break;
  }

  if (secs == 100) {
    ret = 1;
    cout << "error: expected test_array[" << 1 << "] = " << 1
	 << "; got " << test_array[1] << " instead." << std::endl;
  }

  return ret;
}

static int safe_timer_cancel_all_test(SafeTimer &safe_timer,
                                      ceph::mutex& safe_timer_lock)
{
  cout << __PRETTY_FUNCTION__ << std::endl;

  int ret = 0;
  memset(&test_array, 0, sizeof(test_array));
  array_idx = 0;
  memset(&test_contexts, 0, sizeof(test_contexts));

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    test_contexts[i] = new TestContext(i);
  }

  safe_timer_lock.lock();
  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    auto t = ceph::real_clock::now() + std::chrono::seconds(4 * i);
    safe_timer.add_event_at(t, test_contexts[i]);
  }
  safe_timer_lock.unlock();

  sleep(10);

  safe_timer_lock.lock();
  safe_timer.cancel_all_events();
  safe_timer_lock.unlock();

  for (int i = 0; i < array_idx; ++i) {
    if (test_array[i] != i) {
      ret = 1;
      cout << "error: expected test_array[" << i << "] = " << i
	   << "; got " << test_array[i] << " instead." << std::endl;
    }
  }

  return ret;
}

static int safe_timer_cancellation_test(SafeTimer &safe_timer,
                                        ceph::mutex& safe_timer_lock)
{
  cout << __PRETTY_FUNCTION__ << std::endl;

  int ret = 0;
  memset(&test_array, 0, sizeof(test_array));
  array_idx = 0;
  memset(&test_contexts, 0, sizeof(test_contexts));

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    test_contexts[i] = new StrictOrderTestContext(i);
  }

  safe_timer_lock.lock();
  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    auto t = ceph::real_clock::now() + std::chrono::seconds(4 * i);
    safe_timer.add_event_at(t, test_contexts[i]);
  }
  safe_timer_lock.unlock();

  // cancel the even-numbered events
  for (int i = 0; i < MAX_TEST_CONTEXTS; i += 2) {
    safe_timer_lock.lock();
    safe_timer.cancel_event(test_contexts[i]);
    safe_timer_lock.unlock();
  }

  sleep(20);

  safe_timer_lock.lock();
  safe_timer.cancel_all_events();
  safe_timer_lock.unlock();

  for (int i = 1; i < array_idx; i += 2) {
    if (test_array[i] != i) {
      ret = 1;
      cout << "error: expected test_array[" << i << "] = " << i
	   << "; got " << test_array[i] << " instead." << std::endl;
    }
  }

  return ret;
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  int ret;
  ceph::mutex safe_timer_lock = ceph::make_mutex("safe_timer_lock");
  SafeTimer safe_timer(g_ceph_context, safe_timer_lock);

  ret = basic_timer_test <SafeTimer>(safe_timer, &safe_timer_lock);
  if (ret)
    goto done;

  ret = safe_timer_cancel_all_test(safe_timer, safe_timer_lock);
  if (ret)
    goto done;

  ret = safe_timer_cancellation_test(safe_timer, safe_timer_lock);
  if (ret)
    goto done;

  ret = test_out_of_order_insertion(safe_timer, &safe_timer_lock);
  if (ret)
    goto done;

done:
  print_status(argv[0], ret);
  return ret;
}
