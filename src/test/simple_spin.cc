#include "gtest/gtest.h"

#include "common/simple_spin.h"

TEST(SimpleSpin, Test0)
{
  simple_spinlock_t lock0 = SIMPLE_SPINLOCK_INITIALIZER;
  simple_spin_lock(&lock0);
  simple_spin_unlock(&lock0);
}

static simple_spinlock_t lock = SIMPLE_SPINLOCK_INITIALIZER;
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
  int ret;
  pthread_t thread1;
  pthread_t thread2;
  ret = pthread_create(&thread1, NULL, mythread, NULL);
  ASSERT_EQ(ret, 0);
  ret = pthread_create(&thread2, NULL, mythread, NULL);
  ASSERT_EQ(ret, 0);
  ret = pthread_join(thread1, NULL);
  ASSERT_EQ(ret, 0);
  ret = pthread_join(thread2, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(counter, 2000000U);
}
