#include "common/errno.h"
#include "include/rados/librados.h"
#include "test/rados-api/test.h"

#include "gtest/gtest.h"
#include <errno.h>
#include <semaphore.h>
#include <sstream>
#include <string>

using std::ostringstream;

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

  std::string init()
  {
    int ret;
    if (sem_init(&m_sem, 0, 0)) {
      int err = errno;
      sem_destroy(&m_sem);
      ostringstream oss;
      oss << "sem_init failed: " << cpp_strerror(err);
      return oss.str();
    }
    std::string m_pool_name = get_temp_pool_name();
    ret = create_one_pool(m_pool_name, &m_cluster);
    if (ret) {
      sem_destroy(&m_sem);
      ostringstream oss;
      oss << "create_one_pool(" << m_pool_name << ") failed: error " << ret;
      return oss.str();
    }
    ret = rados_ioctx_create(m_cluster, m_pool_name.c_str(), &m_ioctx);
    if (ret) {
      sem_destroy(&m_sem);
      destroy_one_pool(m_pool_name, &m_cluster);
      ostringstream oss;
      oss << "rados_ioctx_create failed: error " << ret;
      return oss.str();
    }
    m_init = true;
    return "";
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
  ASSERT_EQ("", test_data.init());
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

TEST(LibRadosAio, WaitForSafe) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  TestAlarm alarm;
  ASSERT_EQ(0, rados_aio_wait_for_safe(my_completion));
}

TEST(LibRadosAio, RoundTrip) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(&test_data.m_sem);
    sem_wait(&test_data.m_sem);
  }
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_aio_read(test_data.m_ioctx, "foo",
			      my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
}

TEST(LibRadosAio, RoundTripAppend) {
  AioTestData test_data;
  rados_completion_t my_completion, my_completion2, my_completion3;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_append(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf)));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion));
  }
  char buf2[128];
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_aio_append(test_data.m_ioctx, "foo",
			       my_completion2, buf2, sizeof(buf)));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  char buf3[sizeof(buf) + sizeof(buf2)];
  memset(buf3, 0, sizeof(buf3));
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion3));
  ASSERT_EQ(0, rados_aio_read(test_data.m_ioctx, "foo",
			      my_completion3, buf3, sizeof(buf3), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion3));
  }
  ASSERT_EQ(0, memcmp(buf3, buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(buf3 + sizeof(buf), buf2, sizeof(buf2)));
}
