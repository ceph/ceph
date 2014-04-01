#include "common/errno.h"
#include "include/rados/librados.h"
#include "test/librados/test.h"
#include "include/types.h"

#include "gtest/gtest.h"
#include <errno.h>
#include <semaphore.h>
#include <sstream>
#include <string>
#include <boost/scoped_ptr.hpp>
#include <utility>

using std::ostringstream;
using namespace librados;
using std::pair;

class AioTestData
{
public:
  AioTestData()
    : m_cluster(NULL),
      m_ioctx(NULL),
      m_init(false),
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
    m_pool_name = get_temp_pool_name();
    std::string err = create_one_pool(m_pool_name, &m_cluster);
    if (!err.empty()) {
      sem_destroy(&m_sem);
      ostringstream oss;
      oss << "create_one_pool(" << m_pool_name << ") failed: error " << err;
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
  std::string m_pool_name;
  bool m_init;
  bool m_complete;
  bool m_safe;
};

class AioTestDataPP
{
public:
  AioTestDataPP()
    : m_init(false),
      m_complete(false),
      m_safe(false)
  {
  }

  ~AioTestDataPP()
  {
    if (m_init) {
      m_ioctx.close();
      destroy_one_pool_pp(m_pool_name, m_cluster);
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
    m_pool_name = get_temp_pool_name();
    std::string err = create_one_pool_pp(m_pool_name, m_cluster);
    if (!err.empty()) {
      sem_destroy(&m_sem);
      ostringstream oss;
      oss << "create_one_pool(" << m_pool_name << ") failed: error " << err;
      return oss.str();
    }
    ret = m_cluster.ioctx_create(m_pool_name.c_str(), m_ioctx);
    if (ret) {
      sem_destroy(&m_sem);
      destroy_one_pool_pp(m_pool_name, m_cluster);
      ostringstream oss;
      oss << "rados_ioctx_create failed: error " << ret;
      return oss.str();
    }
    m_init = true;
    return "";
  }

  sem_t m_sem;
  Rados m_cluster;
  IoCtx m_ioctx;
  std::string m_pool_name;
  bool m_init;
  bool m_complete;
  bool m_safe;
};

void set_completion_complete(rados_completion_t cb, void *arg)
{
  AioTestData *test = static_cast<AioTestData*>(arg);
  test->m_complete = true;
  sem_post(&test->m_sem);
}

void set_completion_safe(rados_completion_t cb, void *arg)
{
  AioTestData *test = static_cast<AioTestData*>(arg);
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
  {
    TestAlarm alarm;
    sem_wait(&test_data.m_sem);
    sem_wait(&test_data.m_sem);
  }

  rados_ioctx_set_namespace(test_data.m_ioctx, "nspace");
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(&test_data.m_sem);
    sem_wait(&test_data.m_sem);
  }
  rados_aio_release(my_completion);
}

TEST(LibRadosAio, SimpleWritePP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo",
			       my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(&test_data.m_sem);
    sem_wait(&test_data.m_sem);
  }
  delete my_completion;
  }

  {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  test_data.m_ioctx.set_namespace("nspace");
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo",
			       my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(&test_data.m_sem);
    sem_wait(&test_data.m_sem);
  }
  delete my_completion;
  }
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
  rados_aio_release(my_completion);
}

TEST(LibRadosAio, WaitForSafePP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo",
			       my_completion, bl1, sizeof(buf), 0));
  TestAlarm alarm;
  ASSERT_EQ(0, my_completion->wait_for_safe());
  delete my_completion;
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
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAio, RoundTrip2) {
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
    ASSERT_EQ(0, rados_aio_wait_for_safe(my_completion2));
  }
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAio, RoundTripPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion,
					   bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(&test_data.m_sem);
    sem_wait(&test_data.m_sem);
  }
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo",
			      my_completion2, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAio, RoundTripPP2) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion,
					   bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(&test_data.m_sem);
    sem_wait(&test_data.m_sem);
  }
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo",
			      my_completion2, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_safe());
  }
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  delete my_completion;
  delete my_completion2;
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
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(my_completion3);
}

TEST(LibRadosAio, RoundTripAppendPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_append("foo", my_completion,
					    bl1, sizeof(buf)));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  char buf2[128];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_append("foo", my_completion2,
					    bl2, sizeof(buf2)));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  bufferlist bl3;
  AioCompletion *my_completion3 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion3, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo",
			      my_completion3, &bl3, 2 * sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion3->wait_for_complete());
  }
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(bl3.c_str() + sizeof(buf), buf2, sizeof(buf2)));
  delete my_completion;
  delete my_completion2;
  delete my_completion3;
}

TEST(LibRadosAio, IsComplete) {
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

    // Busy-wait until the AIO completes.
    // Normally we wouldn't do this, but we want to test rados_aio_is_complete.
    while (true) {
      int is_complete = rados_aio_is_complete(my_completion2);
      if (is_complete)
	break;
    }
  }
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAio, IsCompletePP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion,
					   bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(&test_data.m_sem);
    sem_wait(&test_data.m_sem);
  }
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion2,
					  &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;

    // Busy-wait until the AIO completes.
    // Normally we wouldn't do this, but we want to test rados_aio_is_complete.
    while (true) {
      int is_complete = my_completion2->is_complete();
      if (is_complete)
	break;
    }
  }
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAio, IsSafe) {
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

    // Busy-wait until the AIO completes.
    // Normally we wouldn't do this, but we want to test rados_aio_is_safe.
    while (true) {
      int is_safe = rados_aio_is_safe(my_completion);
      if (is_safe)
	break;
    }
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
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAio, IsSafePP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion,
					   bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;

    // Busy-wait until the AIO completes.
    // Normally we wouldn't do this, but we want to test rados_aio_is_safe.
    while (true) {
      int is_safe = my_completion->is_safe();
      if (is_safe)
	break;
    }
  }
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  bufferlist bl2;
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion2,
					  &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAio, ReturnValue) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0, sizeof(buf));
  ASSERT_EQ(0, rados_aio_read(test_data.m_ioctx, "nonexistent",
			       my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion));
  }
  ASSERT_EQ(-ENOENT, rados_aio_get_return_value(my_completion));
  rados_aio_release(my_completion);
}

TEST(LibRadosAio, ReturnValuePP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  bufferlist bl1;
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("nonexistent",
			       my_completion, &bl1, 128, 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(-ENOENT, my_completion->get_return_value());
  delete my_completion;
}

TEST(LibRadosAio, Flush) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xee, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_aio_flush(test_data.m_ioctx));
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
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAio, FlushPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char buf[128];
  memset(buf, 0xee, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion,
					   bl1, sizeof(buf), 0));
  ASSERT_EQ(0, test_data.m_ioctx.aio_flush());
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion2,
					  &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAio, FlushAsync) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion));
  rados_completion_t flush_completion;
  ASSERT_EQ(0, rados_aio_create_completion(NULL, NULL, NULL, &flush_completion));
  char buf[128];
  memset(buf, 0xee, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_aio_flush_async(test_data.m_ioctx, flush_completion));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(flush_completion));
    ASSERT_EQ(0, rados_aio_wait_for_safe(flush_completion));
  }
  ASSERT_EQ(1, rados_aio_is_complete(my_completion));
  ASSERT_EQ(1, rados_aio_is_safe(my_completion));
  ASSERT_EQ(1, rados_aio_is_complete(flush_completion));
  ASSERT_EQ(1, rados_aio_is_safe(flush_completion));
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
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(flush_completion);
}

TEST(LibRadosAio, FlushAsyncPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  AioCompletion *flush_completion =
      test_data.m_cluster.aio_create_completion(NULL, NULL, NULL);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char buf[128];
  memset(buf, 0xee, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion,
					   bl1, sizeof(buf), 0));
  ASSERT_EQ(0, test_data.m_ioctx.aio_flush_async(flush_completion));
  {
      TestAlarm alarm;
      ASSERT_EQ(0, flush_completion->wait_for_complete());
      ASSERT_EQ(0, flush_completion->wait_for_safe());
  }
  ASSERT_EQ(1, my_completion->is_complete());
  ASSERT_EQ(1, my_completion->is_safe());
  ASSERT_EQ(1, flush_completion->is_complete());
  ASSERT_EQ(1, flush_completion->is_safe());
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion2,
					  &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  delete my_completion;
  delete my_completion2;
  delete flush_completion;
}

TEST(LibRadosAio, RoundTripWriteFull) {
  AioTestData test_data;
  rados_completion_t my_completion, my_completion2, my_completion3;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion));
  }
  char buf2[64];
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_aio_write_full(test_data.m_ioctx, "foo",
			       my_completion2, buf2, sizeof(buf2)));
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
  ASSERT_EQ(0, memcmp(buf3, buf2, sizeof(buf2)));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(my_completion3);
}

TEST(LibRadosAio, RoundTripWriteFullPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion,
					   bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  char buf2[64];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write_full("foo", my_completion2, bl2));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  bufferlist bl3;
  AioCompletion *my_completion3 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion3, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion3,
					  &bl3, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion3->wait_for_complete());
  }
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf2, sizeof(buf2)));
  delete my_completion;
  delete my_completion2;
  delete my_completion3;
}


TEST(LibRadosAio, SimpleStat) {
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
  uint64_t psize;
  time_t pmtime;
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_aio_stat(test_data.m_ioctx, "foo",
			      my_completion2, &psize, &pmtime));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ(sizeof(buf), psize);
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAio, SimpleStatPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion,
					   bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(&test_data.m_sem);
    sem_wait(&test_data.m_sem);
  }
  uint64_t psize;
  time_t pmtime;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_stat("foo", my_completion2,
			  		&psize, &pmtime));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(sizeof(buf), psize);
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAio, SimpleStatNS) {
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
  rados_ioctx_set_namespace(test_data.m_ioctx, "nspace");
  char buf2[64];
  memset(buf2, 0xbb, sizeof(buf2));
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    sem_wait(&test_data.m_sem);
    sem_wait(&test_data.m_sem);
  }
  uint64_t psize;
  time_t pmtime;
  rados_completion_t my_completion2;
  rados_ioctx_set_namespace(test_data.m_ioctx, "");
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_aio_stat(test_data.m_ioctx, "foo",
			      my_completion2, &psize, &pmtime));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ(sizeof(buf), psize);

  rados_ioctx_set_namespace(test_data.m_ioctx, "nspace");
  rados_completion_t my_completion3;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion3));
  ASSERT_EQ(0, rados_aio_stat(test_data.m_ioctx, "foo",
			      my_completion3, &psize, &pmtime));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion3));
  }
  ASSERT_EQ(sizeof(buf2), psize);

  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(my_completion3);
}

TEST(LibRadosAio, SimpleStatPPNS) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion,
					   bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(&test_data.m_sem);
    sem_wait(&test_data.m_sem);
  }
  uint64_t psize;
  time_t pmtime;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_stat("foo", my_completion2,
			  		&psize, &pmtime));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(sizeof(buf), psize);
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAio, StatRemove) {
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
  uint64_t psize;
  time_t pmtime;
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_aio_stat(test_data.m_ioctx, "foo",
			      my_completion2, &psize, &pmtime));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ(sizeof(buf), psize);
  rados_completion_t my_completion3;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion3));
  ASSERT_EQ(0, rados_aio_remove(test_data.m_ioctx, "foo", my_completion3));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion3));
  }
  uint64_t psize2;
  time_t pmtime2;
  rados_completion_t my_completion4;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion4));
  ASSERT_EQ(0, rados_aio_stat(test_data.m_ioctx, "foo",
			      my_completion4, &psize2, &pmtime2));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion4));
  }
  ASSERT_EQ(-ENOENT, rados_aio_get_return_value(my_completion4));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(my_completion3);
  rados_aio_release(my_completion4);
}

TEST(LibRadosAio, StatRemovePP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion,
					   bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(&test_data.m_sem);
    sem_wait(&test_data.m_sem);
  }
  uint64_t psize;
  time_t pmtime;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_stat("foo", my_completion2,
			  		&psize, &pmtime));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(sizeof(buf), psize);
  uint64_t psize2;
  time_t pmtime2;
  AioCompletion *my_completion3 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion3, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_remove("foo", my_completion3));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion3->wait_for_complete());
  }

  AioCompletion *my_completion4 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion4, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_stat("foo", my_completion4,
			  		&psize2, &pmtime2));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion4->wait_for_complete());
  }
  ASSERT_EQ(-ENOENT, my_completion4->get_return_value());
  delete my_completion;
  delete my_completion2;
  delete my_completion3;
  delete my_completion4;
}

using std::string;
using std::map;
using std::set;

TEST(LibRadosAio, OmapPP) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string header_str = "baz";
  bufferptr bp(header_str.c_str(), header_str.size() + 1);
  bufferlist header_to_set;
  header_to_set.push_back(bp);
  map<string, bufferlist> to_set;
  {
    boost::scoped_ptr<AioCompletion> my_completion(cluster.aio_create_completion(0, 0, 0));
    ObjectWriteOperation op;
    to_set["foo"] = header_to_set;
    to_set["foo2"] = header_to_set;
    to_set["qfoo3"] = header_to_set;
    op.omap_set(to_set);

    op.omap_set_header(header_to_set);

    ioctx.aio_operate("test_obj", my_completion.get(), &op);
    {
      TestAlarm alarm;
      ASSERT_EQ(0, my_completion->wait_for_complete());
    }
  }

  {
    boost::scoped_ptr<AioCompletion> my_completion(cluster.aio_create_completion(0, 0, 0));
    ObjectReadOperation op;
    map<string, pair<bufferlist, int> > assertions;
    bufferlist val;
    val.append(string("bar"));
    assertions["foo"] = pair<bufferlist, int>(val, CEPH_OSD_CMPXATTR_OP_EQ);

    int r;
    op.omap_cmp(assertions, &r);

    ioctx.aio_operate("test_obj", my_completion.get(), &op, 0);
    {
      TestAlarm alarm;
      ASSERT_EQ(0, my_completion->wait_for_complete());
    }
    ASSERT_EQ(-ECANCELED, r);
  }

  {
    boost::scoped_ptr<AioCompletion> my_completion(cluster.aio_create_completion(0, 0, 0));
    ObjectReadOperation op;

    set<string> set_got;
    map<string, bufferlist> map_got;

    set<string> to_get;
    map<string, bufferlist> got3;

    map<string, bufferlist> got4;

    bufferlist header;

    op.omap_get_keys("", 1, &set_got, 0);
    op.omap_get_vals("foo", 1, &map_got, 0);

    to_get.insert("foo");
    to_get.insert("qfoo3");
    op.omap_get_vals_by_keys(to_get, &got3, 0);

    op.omap_get_header(&header, 0);

    op.omap_get_vals("foo2", "q", 1, &got4, 0);

    ioctx.aio_operate("test_obj", my_completion.get(), &op, 0);
    {
      TestAlarm alarm;
      ASSERT_EQ(0, my_completion->wait_for_complete());
    }

    ASSERT_EQ(header.length(), header_to_set.length());
    ASSERT_EQ(set_got.size(), (unsigned)1);
    ASSERT_EQ(*set_got.begin(), "foo");
    ASSERT_EQ(map_got.size(), (unsigned)1);
    ASSERT_EQ(map_got.begin()->first, "foo2");
    ASSERT_EQ(got3.size(), (unsigned)2);
    ASSERT_EQ(got3.begin()->first, "foo");
    ASSERT_EQ(got3.rbegin()->first, "qfoo3");
    ASSERT_EQ(got4.size(), (unsigned)1);
    ASSERT_EQ(got4.begin()->first, "qfoo3");
  }

  {
    boost::scoped_ptr<AioCompletion> my_completion(cluster.aio_create_completion(0, 0, 0));
    ObjectWriteOperation op;
    set<string> to_remove;
    to_remove.insert("foo2");
    op.omap_rm_keys(to_remove);
    ioctx.aio_operate("test_obj", my_completion.get(), &op);
    {
      TestAlarm alarm;
      ASSERT_EQ(0, my_completion->wait_for_complete());
    }
  }

  {
    boost::scoped_ptr<AioCompletion> my_completion(cluster.aio_create_completion(0, 0, 0));
    ObjectReadOperation op;

    set<string> set_got;
    op.omap_get_keys("", -1, &set_got, 0);
    ioctx.aio_operate("test_obj", my_completion.get(), &op, 0);
    {
      TestAlarm alarm;
      ASSERT_EQ(0, my_completion->wait_for_complete());
    }
    ASSERT_EQ(set_got.size(), (unsigned)2);
  }

  {
    boost::scoped_ptr<AioCompletion> my_completion(cluster.aio_create_completion(0, 0, 0));
    ObjectWriteOperation op;
    op.omap_clear();
    ioctx.aio_operate("test_obj", my_completion.get(), &op);
    {
      TestAlarm alarm;
      ASSERT_EQ(0, my_completion->wait_for_complete());
    }
  }

  {
    boost::scoped_ptr<AioCompletion> my_completion(cluster.aio_create_completion(0, 0, 0));
    ObjectReadOperation op;

    set<string> set_got;
    op.omap_get_keys("", -1, &set_got, 0);
    ioctx.aio_operate("test_obj", my_completion.get(), &op, 0);
    {
      TestAlarm alarm;
      ASSERT_EQ(0, my_completion->wait_for_complete());
    }
    ASSERT_EQ(set_got.size(), (unsigned)0);
  }

  ioctx.remove("test_obj");
  destroy_one_pool_pp(pool_name, cluster);
}
