#include "common/Mutex.h"
#include "common/Timer.h"
#include "common/common_init.h"

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
  int array[MAX_TEST_CONTEXTS];
  int array_idx;
  TestContext* test_contexts[MAX_TEST_CONTEXTS];

  Mutex array_lock("test_timers_mutex");
}

class TestContext : public Context
{
public:
  TestContext(int num_)
    : num(num_)
  {
  }

  virtual void finish(int r)
  {
    array_lock.Lock();
    cout << "TestContext " << num << std::endl;
    array[array_idx++] = num;
    array_lock.Unlock();
  }

  virtual ~TestContext()
  {
  }

private:
  int num;
};

static void print_status(const char *str, int ret)
{
  cout << str << ": ";
  cout << ((ret == 0) ? "SUCCESS" : "FAILURE");
  cout << std::endl;
}

template <typename T>
static int basic_timer_test(T &timer, Mutex *lock)
{
  int ret = 0;
  memset(&array, 0, sizeof(array));
  array_idx = 0;
  memset(&test_contexts, 0, sizeof(test_contexts));

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    test_contexts[i] = new TestContext(i);
  }


  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    if (lock)
      lock->Lock();
    utime_t inc(2 * i, 0); 
    utime_t t = g_clock.now() + inc;
    timer.add_event_at(t, test_contexts[i]);
    if (lock)
      lock->Unlock();
  }

  bool done = false;
  do {
    sleep(1);
    array_lock.Lock();
    done = (array_idx == MAX_TEST_CONTEXTS);
    array_lock.Unlock();
  } while (!done);

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    if (array[i] != i) {
      ret = 1;
      cout << "error: expected array[" << i << "] = " << i
	   << "; got " << array[i] << " instead." << std::endl;
    }
  }

  return ret;
}

static int test_timers(void)
{
  int ret = 0;
  Timer timer;
  ret = basic_timer_test <Timer>(timer, NULL);
  if (ret)
    goto done;

done:
  print_status("test_timers", ret);
  return ret;
}

static int safe_timer_join_test(SafeTimer &safe_timer, Mutex& safe_timer_lock)
{
  int ret = 0;
  memset(&array, 0, sizeof(array));
  array_idx = 0;
  memset(&test_contexts, 0, sizeof(test_contexts));

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    test_contexts[i] = new TestContext(i);
  }

  safe_timer_lock.Lock();
  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    utime_t inc(4 * i, 0);
    utime_t t = g_clock.now() + inc;
    safe_timer.add_event_at(t, test_contexts[i]);
  }
  safe_timer_lock.Unlock();

  sleep(10);

  safe_timer_lock.Lock();
  safe_timer.cancel_all_events();
  safe_timer_lock.Unlock();
  safe_timer.shutdown();

  for (int i = 0; i < array_idx; ++i) {
    if (array[i] != i) {
      ret = 1;
      cout << "error: expected array[" << i << "] = " << i
	   << "; got " << array[i] << " instead." << std::endl;
    }
  }

  return ret;
}

static int test_safe_timers(void)
{
  int ret = 0;
  Mutex safe_timer_lock("safe_timer_lock");
  SafeTimer safe_timer(safe_timer_lock);

  ret = basic_timer_test <SafeTimer>(safe_timer, &safe_timer_lock);
  if (ret)
    goto done;

  ret = safe_timer_join_test(safe_timer, safe_timer_lock);
  if (ret)
    goto done;

done:
  print_status("test_safe_timers", ret);
  return ret;
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  ceph_set_default_id("admin");
  common_set_defaults(false);
  common_init(args, "ceph", true);

  int ret;
  ret = test_timers();
  if (ret)
    return ret;
  ret = test_safe_timers();
  if (ret)
    return ret;
  return 0;
}
