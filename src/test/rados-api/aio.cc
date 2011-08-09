#include "include/rados/librados.h"
#include "test/rados-api/test.h"

#include "gtest/gtest.h"
#include <errno.h>
#include <semaphore.h>
#include <string>

class AioTestData
{
public:
  AioTestData()
    : m_init(false),
      m_complete(false),
      m_safe(false)
  {
  }

  ~AioTestData()
  {
    if (m_init) {
      rados_ioctx_destroy(m_ioctx);
      destroy_one_pool(m_pool_name, &m_cluster);
      sem_destroy(&m_sem);
    }
  }

  int init()
  {
    int ret;
    if (sem_init(&m_sem, 0, 0)) {
      int err = errno;
      sem_destroy(&m_sem);
      return err;
    }
    std::string m_pool_name = get_temp_pool_name();
    ret = create_one_pool(m_pool_name, &m_cluster);
    if (ret) {
      sem_destroy(&m_sem);
      return ret;
    }
    ret = rados_ioctx_create(m_cluster, m_pool_name.c_str(), &m_ioctx);
    if (ret) {
      sem_destroy(&m_sem);
      destroy_one_pool(m_pool_name, &m_cluster);
      return ret;
    }
    m_init = true;
    return 0;
  }

  sem_t m_sem;
  rados_t m_cluster;
  rados_ioctx_t m_ioctx;
  rados_completion_t m_completion;
  std::string m_pool_name;
  bool m_init;
  bool m_complete;
  bool m_safe;
};

class TestAlarm
{
public:
  TestAlarm() {
    alarm(360);
  }
  ~TestAlarm() {
    alarm(0);
  }
};

void set_completion_complete(rados_completion_t cb, void *arg)
{
  AioTestData *test = (AioTestData*)arg;
  test->m_complete = true;
  sem_post(&test->m_sem);
}

void set_completion_safe(rados_completion_t cb, void *arg)
{
  AioTestData *test = (AioTestData*)arg;
  test->m_safe = true;
  sem_post(&test->m_sem);
}

TEST(LibRadosAio, SimpleWrite) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ(0, test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  TestAlarm alarm;
  sem_wait(&test_data.m_sem);
  sem_wait(&test_data.m_sem);
}
