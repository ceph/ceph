
#include <future>

#include "gtest/gtest.h"

#include "include/spinlock.h"

using ceph::spin_lock;
using ceph::spin_unlock;

static std::atomic_flag lock = ATOMIC_FLAG_INIT;
static int64_t counter = 0;

TEST(SimpleSpin, Test0)
{
  std::atomic_flag lock0 = ATOMIC_FLAG_INIT;
  spin_lock(&lock0);
  spin_unlock(&lock0);
}

static void* mythread(void *v)
{
  for (int j = 0; j < 1000000; ++j) {
    spin_lock(&lock);
    counter++;
    spin_unlock(&lock);
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
            spin_lock(lock);
            counter++;
            spin_unlock(lock);
        }
       });
  ASSERT_EQ(n, counter);
}

template <typename LockT>
int64_t check_lock_unlock(const int64_t n, int64_t& cntr, LockT& lock)
{
 auto do_lock_unlock = [&]() -> int64_t {
        int64_t i = 0;

        for(; n != i; ++i) {
           spin_lock(lock);
           cntr++;
           spin_unlock(lock);
        }

        return i;
      };

 auto fone   = async(std::launch::async, do_lock_unlock);
 auto ftwo   = async(std::launch::async, do_lock_unlock);
 auto fthree = async(std::launch::async, do_lock_unlock);

 auto one = fone.get();
 auto two = ftwo.get();
 auto three = fthree.get();

 // Google test doesn't like us using its macros out of individual tests, so:
 if(n != one || n != two || n != three)
  return 0;

 return one + two + three;
}

TEST(SimpleSpin, Test2)
{
 const auto n = 2000000U;

 // ceph::spinlock:
 {
 counter = 0;
 ceph::spinlock l;

 ASSERT_EQ(0, counter);
 auto result = check_lock_unlock(n, counter, l);
 ASSERT_NE(0, counter);
 ASSERT_EQ(counter, result);
 }
}

// ceph::spinlock should work with std::lock_guard<>:
TEST(SimpleSpin, spinlock_guard)
{
  const auto n = 2000000U;

  ceph::spinlock sl;
 
  counter = 0;
  async(std::launch::async, [&sl]() {
        for(int i = 0; n != i; ++i) {
            std::lock_guard<ceph::spinlock> g(sl);
            counter++;
        }
       });

  async(std::launch::async, [&sl]() {
        for(int i = 0; n != i; ++i) {
            std::lock_guard<ceph::spinlock> g(sl);
            counter++;
        }
       });

  ASSERT_EQ(2*n, counter);
}

