#include "gtest/gtest.h"

#include "common/simple_spin.h"

#include <future>

TEST(SimpleSpin, Test0)
{
  std::atomic_flag lock0 = ATOMIC_FLAG_INIT;
  simple_spin_lock(&lock0);
  simple_spin_unlock(&lock0);
}

static std::atomic_flag lock = ATOMIC_FLAG_INIT;
static uint32_t counter = 0;

static void* mythread(void *v)
{
  for (int j = 0; j < 1000000; ++j) {
    simple_spin_lock(&lock);
    counter++;
    simple_spin_unlock(&lock);
  }
  return NULL;
}

TEST(SimpleSpin, Test1)
{
  counter = 0;
  const auto n = 2000000U;

  int ret;
  pthread_t thread1;
  pthread_t thread2;
  ret = pthread_create(&thread1, NULL, mythread, NULL);
  ASSERT_EQ(0, ret);
  ret = pthread_create(&thread2, NULL, mythread, NULL);
  ASSERT_EQ(0, ret);
  ret = pthread_join(thread1, NULL);
  ASSERT_EQ(0, ret);
  ret = pthread_join(thread2, NULL);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(n, counter);


  // Should also work with pass-by-reference:
  // (Note that we don't care about cross-threading here as-such.)
  counter = 0;
  async(std::launch::async, []() {
        for(int i = 0; n != i; ++i) {
            simple_spin_lock(lock);
            counter++;
            simple_spin_unlock(lock);
        }
       });
  ASSERT_EQ(n, counter);
}

