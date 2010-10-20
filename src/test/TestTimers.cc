#include "common/Mutex.h"
#include "common/Timer.h"

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
  int array_idx = 0;
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
    array[array_idx++] = num;
    array_lock.Unlock();
  }

  virtual ~TestContext()
  {
  }

private:
  int num;
};

int main(void)
{
  int ret = 0;
  memset(&array, 0, sizeof(array));
  memset(&test_contexts, 0, sizeof(test_contexts));

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    test_contexts[i] = new TestContext(i);
  }

  Timer my_timer;

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    utime_t inc(2 * i, 0);
    utime_t t = g_clock.now() + inc;
    my_timer.add_event_at(t, test_contexts[i]);
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

  cout << ((ret == 0) ? "SUCCESS" : "FAILURE");
  cout << std::endl;
  return ret;
}
