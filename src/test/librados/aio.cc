#include "common/errno.h"
#include "include/rados/librados.h"
#include "test/librados/test.h"
#include "include/types.h"
#include "include/stringify.h"

#include "gtest/gtest.h"
#include <errno.h>
#include <fcntl.h>
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
      sem_close(m_sem);
    }
  }

  std::string init()
  {
    int ret;
    if (SEM_FAILED == (m_sem = sem_open("/test_aio_sem", O_CREAT, 0644, 0))) {
      int err = errno;
      ostringstream oss;
      oss << "sem_open failed: " << cpp_strerror(err);
      return oss.str();
    }
    m_pool_name = get_temp_pool_name();
    std::string err = create_one_pool(m_pool_name, &m_cluster);
    if (!err.empty()) {
      sem_close(m_sem);
      ostringstream oss;
      oss << "create_one_pool(" << m_pool_name << ") failed: error " << err;
      return oss.str();
    }
    ret = rados_ioctx_create(m_cluster, m_pool_name.c_str(), &m_ioctx);
    if (ret) {
      sem_close(m_sem);
      destroy_one_pool(m_pool_name, &m_cluster);
      ostringstream oss;
      oss << "rados_ioctx_create failed: error " << ret;
      return oss.str();
    }
    m_init = true;
    return "";
  }

  sem_t *m_sem;
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
      sem_close(m_sem);
    }
  }

  std::string init()
  {
    int ret;
    if (SEM_FAILED == (m_sem = sem_open("/test_aio_sem", O_CREAT, 0644, 0))) {
      int err = errno;
      ostringstream oss;
      oss << "sem_open failed: " << cpp_strerror(err);
      return oss.str();
    }
    m_pool_name = get_temp_pool_name();
    std::string err = create_one_pool_pp(m_pool_name, m_cluster);
    if (!err.empty()) {
      sem_close(m_sem);
      ostringstream oss;
      oss << "create_one_pool(" << m_pool_name << ") failed: error " << err;
      return oss.str();
    }
    ret = m_cluster.ioctx_create(m_pool_name.c_str(), m_ioctx);
    if (ret) {
      sem_close(m_sem);
      destroy_one_pool_pp(m_pool_name, m_cluster);
      ostringstream oss;
      oss << "rados_ioctx_create failed: error " << ret;
      return oss.str();
    }
    m_init = true;
    return "";
  }

  sem_t *m_sem;
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
  sem_post(test->m_sem);
}

void set_completion_safe(rados_completion_t cb, void *arg)
{
  AioTestData *test = static_cast<AioTestData*>(arg);
  test->m_safe = true;
  sem_post(test->m_sem);
}

void set_completion_completePP(rados_completion_t cb, void *arg)
{
  AioTestDataPP *test = static_cast<AioTestDataPP*>(arg);
  test->m_complete = true;
  sem_post(test->m_sem);
}

void set_completion_safePP(rados_completion_t cb, void *arg)
{
  AioTestDataPP *test = static_cast<AioTestDataPP*>(arg);
  test->m_safe = true;
  sem_post(test->m_sem);
}

TEST(LibRadosAio, TooBig) {
  AioTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(-E2BIG, rados_aio_write(test_data.m_ioctx, "foo",
                                    my_completion, buf, UINT_MAX, 0));
  ASSERT_EQ(-E2BIG, rados_aio_write_full(test_data.m_ioctx, "foo",
                                         my_completion, buf, UINT_MAX));
  ASSERT_EQ(-E2BIG, rados_aio_append(test_data.m_ioctx, "foo",
                                     my_completion, buf, UINT_MAX));
  rados_aio_release(my_completion);
}

TEST(LibRadosAio, TooBigPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());

  bufferlist bl;
  AioCompletion *aio_completion = test_data.m_cluster.aio_create_completion(
                                                                            (void*)&test_data, NULL, NULL);
  ASSERT_EQ(-E2BIG, test_data.m_ioctx.aio_write("foo", aio_completion, bl, UINT_MAX, 0));
  ASSERT_EQ(-E2BIG, test_data.m_ioctx.aio_append("foo", aio_completion, bl, UINT_MAX));
  // ioctx.aio_write_full no way to overflow bl.length()
  delete aio_completion;
}

TEST(LibRadosAio, PoolQuotaPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  string p = get_temp_pool_name();
  ASSERT_EQ(0, test_data.m_cluster.pool_create(p.c_str()));
  IoCtx ioctx;
  ASSERT_EQ(0, test_data.m_cluster.ioctx_create(p.c_str(), ioctx));

  bufferlist inbl;
  ASSERT_EQ(0, test_data.m_cluster.mon_command(
      "{\"prefix\": \"osd pool set-quota\", \"pool\": \"" + p +
      "\", \"field\": \"max_bytes\", \"val\": \"4096\"}",
      inbl, NULL, NULL));

  bufferlist bl;
  bufferptr z(4096);
  bl.append(z);
  int n;
  for (n = 0; n < 1024; ++n) {
    ObjectWriteOperation op;
    op.write_full(bl);
    librados::AioCompletion *completion =
      test_data.m_cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
	"foo" + stringify(n), completion, &op,
	librados::OPERATION_FULL_TRY));
    completion->wait_for_safe();
    int r = completion->get_return_value();
    completion->release();
    if (r == -EDQUOT)
      break;
    ASSERT_EQ(0, r);
    sleep(1);
  }
  ASSERT_LT(n, 1024);

  // make sure we block without FULL_TRY
  {
    ObjectWriteOperation op;
    op.write_full(bl);
    librados::AioCompletion *completion =
      test_data.m_cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("bar", completion, &op, 0));
    sleep(5);
    ASSERT_FALSE(completion->is_safe());
    completion->release();
  }

  ioctx.close();
  ASSERT_EQ(0, test_data.m_cluster.pool_delete(p.c_str()));
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));

  rados_ioctx_set_namespace(test_data.m_ioctx, "nspace");
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion2, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
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
	  (void*)&test_data, set_completion_completePP, set_completion_safePP);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo",
			       my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  delete my_completion;
  }

  {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  test_data.m_ioctx.set_namespace("nspace");
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completePP, set_completion_safePP);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo",
			       my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
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
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  rados_aio_release(my_completion);
}

TEST(LibRadosAio, WaitForSafePP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completePP, set_completion_safePP);
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
  ASSERT_EQ(0, my_completion->get_return_value());
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  char buf2[256];
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
  ASSERT_EQ((int)sizeof(buf), rados_aio_get_return_value(my_completion2));
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
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
  ASSERT_EQ((int)sizeof(buf), rados_aio_get_return_value(my_completion2));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAio, RoundTripPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completePP, set_completion_safePP);
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completePP, set_completion_safePP);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo",
			      my_completion2, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), bl2.length());
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAio, RoundTripPP2) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completePP, set_completion_safePP);
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completePP, set_completion_safePP);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo",
			      my_completion2, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_safe());
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), bl2.length());
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  delete my_completion;
  delete my_completion2;
}

//using ObjectWriteOperation/ObjectReadOperation with iohint
TEST(LibRadosAio, RoundTripPP3)
{
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  boost::scoped_ptr<AioCompletion> my_completion1(cluster.aio_create_completion(0, 0, 0));
  ObjectWriteOperation op;
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf);

  op.write(0, bl);
  op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ioctx.aio_operate("test_obj", my_completion1.get(), &op);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion1->wait_for_complete());
  }
  EXPECT_EQ(0, my_completion1->get_return_value());

  boost::scoped_ptr<AioCompletion> my_completion2(cluster.aio_create_completion(0, 0, 0));
  bl.clear();
  ObjectReadOperation op1;
  op1.read(0, sizeof(buf), &bl, NULL);
  op1.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED|LIBRADOS_OP_FLAG_FADVISE_RANDOM);
  ioctx.aio_operate("test_obj", my_completion2.get(), &op1, 0);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  EXPECT_EQ(0, my_completion2->get_return_value());
  ASSERT_EQ(0, memcmp(buf, bl.c_str(), sizeof(buf)));
  ioctx.remove("test_obj");
  destroy_one_pool_pp(pool_name, cluster);
}

TEST(LibRadosAio, RoundTripSparseReadPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completePP, set_completion_safePP);
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  std::map<uint64_t, uint64_t> extents;
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completePP, set_completion_safePP);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_sparse_read("foo",
			      my_completion2, &extents, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());
  assert_eq_sparse(bl1, extents, bl2);
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
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  char buf2[128];
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_aio_append(test_data.m_ioctx, "foo",
			       my_completion2, buf2, sizeof(buf2)));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));
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
  ASSERT_EQ((int)sizeof(buf3), rados_aio_get_return_value(my_completion3));
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
  ASSERT_EQ(0, my_completion->get_return_value());
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
  ASSERT_EQ(0, my_completion2->get_return_value());
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
  ASSERT_EQ((int)(sizeof(buf) * 2), my_completion3->get_return_value());
  ASSERT_EQ(sizeof(buf) * 2, bl3.length());
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
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
  ASSERT_EQ((int)sizeof(buf), rados_aio_get_return_value(my_completion2));
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion2,
					  &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;

    // Busy-wait until the AIO completes.
    // Normally we wouldn't do this, but we want to test is_complete.
    while (true) {
      int is_complete = my_completion2->is_complete();
      if (is_complete)
	break;
    }
  }
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), bl2.length());
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
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
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
  ASSERT_EQ((int)sizeof(buf), rados_aio_get_return_value(my_completion2));
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
  ASSERT_EQ(0, my_completion->get_return_value());
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
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), bl2.length());
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
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
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
  ASSERT_EQ((int)sizeof(buf2), rados_aio_get_return_value(my_completion2));
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
  ASSERT_EQ(0, my_completion->get_return_value());
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
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), bl2.length());
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
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
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
  ASSERT_EQ((int)sizeof(buf2), rados_aio_get_return_value(my_completion2));
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
  ASSERT_EQ(0, my_completion->get_return_value());
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
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), bl2.length());
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
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
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
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));
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
  ASSERT_EQ((int)sizeof(buf2), rados_aio_get_return_value(my_completion3));
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
  ASSERT_EQ(0, my_completion->get_return_value());
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
  ASSERT_EQ(0, my_completion2->get_return_value());
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
  ASSERT_EQ((int)sizeof(buf2), my_completion3->get_return_value());
  ASSERT_EQ(sizeof(buf2), bl3.length());
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf2, sizeof(buf2)));
  delete my_completion;
  delete my_completion2;
  delete my_completion3;
}

//using ObjectWriteOperation/ObjectReadOperation with iohint
TEST(LibRadosAio, RoundTripWriteFullPP2)
{
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  boost::scoped_ptr<AioCompletion> my_completion1(cluster.aio_create_completion(0, 0, 0));
  ObjectWriteOperation op;
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf);

  op.write_full(bl);
  op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ioctx.aio_operate("test_obj", my_completion1.get(), &op);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion1->wait_for_complete());
  }
  EXPECT_EQ(0, my_completion1->get_return_value());

  boost::scoped_ptr<AioCompletion> my_completion2(cluster.aio_create_completion(0, 0, 0));
  bl.clear();
  ObjectReadOperation op1;
  op1.read(0, sizeof(buf), &bl, NULL);
  op1.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED|LIBRADOS_OP_FLAG_FADVISE_RANDOM);
  ioctx.aio_operate("test_obj", my_completion2.get(), &op1, 0);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  EXPECT_EQ(0, my_completion2->get_return_value());
  ASSERT_EQ(0, memcmp(buf, bl.c_str(), sizeof(buf)));

  ioctx.remove("test_obj");
  destroy_one_pool_pp(pool_name, cluster);
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
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
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
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
  ASSERT_EQ(0, my_completion2->get_return_value());
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  rados_ioctx_set_namespace(test_data.m_ioctx, "nspace");
  char buf2[64];
  memset(buf2, 0xbb, sizeof(buf2));
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
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
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));
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
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion3));
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
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
  ASSERT_EQ(0, my_completion2->get_return_value());
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
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
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));
  ASSERT_EQ(sizeof(buf), psize);
  rados_completion_t my_completion3;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion3));
  ASSERT_EQ(0, rados_aio_remove(test_data.m_ioctx, "foo", my_completion3));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion3));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion3));
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
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
  ASSERT_EQ(0, my_completion2->get_return_value());
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
  ASSERT_EQ(0, my_completion3->get_return_value());

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
    EXPECT_EQ(0, my_completion->get_return_value());
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
    EXPECT_EQ(-ECANCELED, my_completion->get_return_value());
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
    EXPECT_EQ(0, my_completion->get_return_value());

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
    EXPECT_EQ(0, my_completion->get_return_value());
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
    EXPECT_EQ(0, my_completion->get_return_value());
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
    EXPECT_EQ(0, my_completion->get_return_value());
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
    EXPECT_EQ(0, my_completion->get_return_value());
    ASSERT_EQ(set_got.size(), (unsigned)0);
  }

  // omap_clear clears header *and* keys
  {
    boost::scoped_ptr<AioCompletion> my_completion(cluster.aio_create_completion(0, 0, 0));
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("some data");
    map<string,bufferlist> to_set;
    to_set["foo"] = bl;
    to_set["foo2"] = bl;
    to_set["qfoo3"] = bl;
    op.omap_set(to_set);
    op.omap_set_header(bl);
    ioctx.aio_operate("foo3", my_completion.get(), &op);
    {
      TestAlarm alarm;
      ASSERT_EQ(0, my_completion->wait_for_complete());
    }
    EXPECT_EQ(0, my_completion->get_return_value());
  }
  {
    boost::scoped_ptr<AioCompletion> my_completion(cluster.aio_create_completion(0, 0, 0));
    ObjectWriteOperation op;
    op.omap_clear();
    ioctx.aio_operate("foo3", my_completion.get(), &op);
    {
      TestAlarm alarm;
      ASSERT_EQ(0, my_completion->wait_for_complete());
    }
    EXPECT_EQ(0, my_completion->get_return_value());
  }
  {
    boost::scoped_ptr<AioCompletion> my_completion(cluster.aio_create_completion(0, 0, 0));
    ObjectReadOperation op;
    set<string> set_got;
    bufferlist hdr;
    op.omap_get_keys("", -1, &set_got, 0);
    op.omap_get_header(&hdr, NULL);
    ioctx.aio_operate("foo3", my_completion.get(), &op, 0);
    {
      TestAlarm alarm;
      ASSERT_EQ(0, my_completion->wait_for_complete());
    }
    EXPECT_EQ(0, my_completion->get_return_value());
    ASSERT_EQ(set_got.size(), (unsigned)0);
    ASSERT_EQ(hdr.length(), 0u);
  }

  ioctx.remove("test_obj");
  destroy_one_pool_pp(pool_name, cluster);
}

TEST(LibRadosAio, MultiWrite) {
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
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));

  char buf2[64];
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion2));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion2, buf2, sizeof(buf2), sizeof(buf)));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));

  char buf3[(sizeof(buf) + sizeof(buf2)) * 3];
  memset(buf3, 0, sizeof(buf3));
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_complete, set_completion_safe, &my_completion3));
  ASSERT_EQ(0, rados_aio_read(test_data.m_ioctx, "foo",
			      my_completion3, buf3, sizeof(buf3), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion3));
  }
  ASSERT_EQ((int)(sizeof(buf) + sizeof(buf2)), rados_aio_get_return_value(my_completion3));
  ASSERT_EQ(0, memcmp(buf3, buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(buf3 + sizeof(buf), buf2, sizeof(buf2)));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(my_completion3);
}

TEST(LibRadosAio, MultiWritePP) {
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
  ASSERT_EQ(0, my_completion->get_return_value());

  char buf2[64];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion2,
					   bl2, sizeof(buf2), sizeof(buf)));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());

  bufferlist bl3;
  AioCompletion *my_completion3 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_complete, set_completion_safe);
  ASSERT_NE(my_completion3, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion3,
					  &bl3, (sizeof(buf) + sizeof(buf2) * 3), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion3->wait_for_complete());
  }
  ASSERT_EQ((int)(sizeof(buf) + sizeof(buf2)), my_completion3->get_return_value());
  ASSERT_EQ(sizeof(buf) + sizeof(buf2), bl3.length());
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(bl3.c_str() + sizeof(buf), buf2, sizeof(buf2)));
  delete my_completion;
  delete my_completion2;
  delete my_completion3;
}

// EC test cases
class AioTestDataEC
{
public:
  AioTestDataEC()
    : m_cluster(NULL),
      m_ioctx(NULL),
      m_init(false),
      m_complete(false),
      m_safe(false)
  {
  }

  ~AioTestDataEC()
  {
    if (m_init) {
      rados_ioctx_destroy(m_ioctx);
      destroy_one_ec_pool(m_pool_name, &m_cluster);
      sem_close(m_sem);
    }
  }

  std::string init()
  {
    int ret;
    if (SEM_FAILED == (m_sem = sem_open("/test_aio_sem", O_CREAT, 0644, 0))) {
      int err = errno;
      ostringstream oss;
      oss << "sem_open failed: " << cpp_strerror(err);
      return oss.str();
    }
    m_pool_name = get_temp_pool_name();
    std::string err = create_one_ec_pool(m_pool_name, &m_cluster);
    if (!err.empty()) {
      sem_close(m_sem);
      ostringstream oss;
      oss << "create_one_ec_pool(" << m_pool_name << ") failed: error " << err;
      return oss.str();
    }
    ret = rados_ioctx_create(m_cluster, m_pool_name.c_str(), &m_ioctx);
    if (ret) {
      sem_close(m_sem);
      destroy_one_ec_pool(m_pool_name, &m_cluster);
      ostringstream oss;
      oss << "rados_ioctx_create failed: error " << ret;
      return oss.str();
    }
    m_init = true;
    return "";
  }

  sem_t *m_sem;
  rados_t m_cluster;
  rados_ioctx_t m_ioctx;
  std::string m_pool_name;
  bool m_init;
  bool m_complete;
  bool m_safe;
};

class AioTestDataECPP
{
public:
  AioTestDataECPP()
    : m_init(false),
      m_complete(false),
      m_safe(false)
  {
  }

  ~AioTestDataECPP()
  {
    if (m_init) {
      m_ioctx.close();
      destroy_one_ec_pool_pp(m_pool_name, m_cluster);
      sem_close(m_sem);
    }
  }

  std::string init()
  {
    int ret;
    if (SEM_FAILED == (m_sem = sem_open("/test_aio_sem", O_CREAT, 0644, 0))) {
      int err = errno;
      ostringstream oss;
      oss << "sem_open failed: " << cpp_strerror(err);
      return oss.str();
    }
    m_pool_name = get_temp_pool_name();
    std::string err = create_one_ec_pool_pp(m_pool_name, m_cluster);
    if (!err.empty()) {
      sem_close(m_sem);
      ostringstream oss;
      oss << "create_one_ec_pool(" << m_pool_name << ") failed: error " << err;
      return oss.str();
    }
    ret = m_cluster.ioctx_create(m_pool_name.c_str(), m_ioctx);
    if (ret) {
      sem_close(m_sem);
      destroy_one_ec_pool_pp(m_pool_name, m_cluster);
      ostringstream oss;
      oss << "rados_ioctx_create failed: error " << ret;
      return oss.str();
    }
    m_init = true;
    return "";
  }

  sem_t *m_sem;
  Rados m_cluster;
  IoCtx m_ioctx;
  std::string m_pool_name;
  bool m_init;
  bool m_complete;
  bool m_safe;
};

void set_completion_completeEC(rados_completion_t cb, void *arg)
{
  AioTestDataEC *test = static_cast<AioTestDataEC*>(arg);
  test->m_complete = true;
  sem_post(test->m_sem);
}

void set_completion_safeEC(rados_completion_t cb, void *arg)
{
  AioTestDataEC *test = static_cast<AioTestDataEC*>(arg);
  test->m_safe = true;
  sem_post(test->m_sem);
}

void set_completion_completeECPP(rados_completion_t cb, void *arg)
{
  AioTestDataECPP *test = static_cast<AioTestDataECPP*>(arg);
  test->m_complete = true;
  sem_post(test->m_sem);
}

void set_completion_safeECPP(rados_completion_t cb, void *arg)
{
  AioTestDataECPP *test = static_cast<AioTestDataECPP*>(arg);
  test->m_safe = true;
  sem_post(test->m_sem);
}

TEST(LibRadosAioEC, SimpleWrite) {
  AioTestDataEC test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));

  rados_ioctx_set_namespace(test_data.m_ioctx, "nspace");
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion2));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion2, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAioEC, SimpleWritePP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeECPP, set_completion_safeECPP);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo",
			       my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  delete my_completion;
  }

  {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  test_data.m_ioctx.set_namespace("nspace");
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeECPP, set_completion_safeECPP);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo",
			       my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  delete my_completion;
  }
}

TEST(LibRadosAioEC, WaitForSafe) {
  AioTestDataEC test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  TestAlarm alarm;
  ASSERT_EQ(0, rados_aio_wait_for_safe(my_completion));
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  rados_aio_release(my_completion);
}

TEST(LibRadosAioEC, WaitForSafePP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeECPP, set_completion_safeECPP);
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
  ASSERT_EQ(0, my_completion->get_return_value());
  delete my_completion;
}

TEST(LibRadosAioEC, RoundTrip) {
  AioTestDataEC test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  char buf2[256];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion2));
  ASSERT_EQ(0, rados_aio_read(test_data.m_ioctx, "foo",
			      my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ((int)sizeof(buf), rados_aio_get_return_value(my_completion2));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAioEC, RoundTrip2) {
  AioTestDataEC test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion2));
  ASSERT_EQ(0, rados_aio_read(test_data.m_ioctx, "foo",
			      my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ((int)sizeof(buf), rados_aio_get_return_value(my_completion2));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAioEC, RoundTripPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeECPP, set_completion_safeECPP);
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeECPP, set_completion_safeECPP);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo",
			      my_completion2, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), bl2.length());
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAioEC, RoundTripPP2) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeECPP, set_completion_safeECPP);
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeECPP, set_completion_safeECPP);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo",
			      my_completion2, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_safe());
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), bl2.length());
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  delete my_completion;
  delete my_completion2;
}

//using ObjectWriteOperation/ObjectReadOperation with iohint
TEST(LibRadosAioEC, RoundTripPP3)
{
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  boost::scoped_ptr<AioCompletion> my_completion1(cluster.aio_create_completion(0, 0, 0));
  ObjectWriteOperation op;
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf);

  op.write(0, bl);
  op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ioctx.aio_operate("test_obj", my_completion1.get(), &op);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion1->wait_for_complete());
  }
  EXPECT_EQ(0, my_completion1->get_return_value());

  boost::scoped_ptr<AioCompletion> my_completion2(cluster.aio_create_completion(0, 0, 0));
  bl.clear();
  ObjectReadOperation op1;
  op1.read(0, sizeof(buf), &bl, NULL);
  op1.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED|LIBRADOS_OP_FLAG_FADVISE_RANDOM);
  ioctx.aio_operate("test_obj", my_completion2.get(), &op1, 0);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  EXPECT_EQ(0, my_completion2->get_return_value());
  ASSERT_EQ(0, memcmp(buf, bl.c_str(), sizeof(buf)));

  ioctx.remove("test_obj");
  destroy_one_pool_pp(pool_name, cluster);
}

TEST(LibRadosAioEC, RoundTripSparseReadPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeECPP, set_completion_safeECPP);
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());

  map<uint64_t, uint64_t> extents;
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeECPP, set_completion_safeECPP);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_sparse_read("foo",
			      my_completion2, &extents, &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());
  assert_eq_sparse(bl1, extents, bl2);
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAioEC, RoundTripAppend) {
  AioTestDataEC test_data;
  rados_completion_t my_completion, my_completion2, my_completion3, my_completion4;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
  int requires;
  ASSERT_EQ(0, rados_ioctx_pool_requires_alignment2(test_data.m_ioctx, &requires));
  ASSERT_NE(0, requires);
  uint64_t alignment;
  ASSERT_EQ(0, rados_ioctx_pool_required_alignment2(test_data.m_ioctx, &alignment));
  ASSERT_NE(0U, alignment);

  int bsize = alignment;
  char *buf = (char *)new char[bsize];
  memset(buf, 0xcc, bsize);
  ASSERT_EQ(0, rados_aio_append(test_data.m_ioctx, "foo",
			       my_completion, buf, bsize));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));

  int hbsize = bsize / 2;
  char *buf2 = (char *)new char[hbsize];
  memset(buf2, 0xdd, hbsize);
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion2));
  ASSERT_EQ(0, rados_aio_append(test_data.m_ioctx, "foo",
			       my_completion2, buf2, hbsize));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));

  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion3));
  ASSERT_EQ(0, rados_aio_append(test_data.m_ioctx, "foo",
			       my_completion3, buf2, hbsize));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion3));
  }
  EXPECT_EQ(-EOPNOTSUPP, rados_aio_get_return_value(my_completion3));

  int tbsize = bsize + hbsize;
  char *buf3 = (char *)new char[tbsize];
  memset(buf3, 0, tbsize);
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion4));
  ASSERT_EQ(0, rados_aio_read(test_data.m_ioctx, "foo",
			      my_completion4, buf3, bsize * 3, 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion4));
  }
  ASSERT_EQ(tbsize, rados_aio_get_return_value(my_completion4));
  ASSERT_EQ(0, memcmp(buf3, buf, bsize));
  ASSERT_EQ(0, memcmp(buf3 + bsize, buf2, hbsize));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(my_completion3);
  rados_aio_release(my_completion4);
  delete[] buf;
  delete[] buf2;
  delete[] buf3;
}

TEST(LibRadosAioEC, RoundTripAppendPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  bool requires;
  ASSERT_EQ(0, test_data.m_ioctx.pool_requires_alignment2(&requires));
  ASSERT_TRUE(requires);
  uint64_t alignment;
  ASSERT_EQ(0, test_data.m_ioctx.pool_required_alignment2(&alignment));
  ASSERT_NE((unsigned)0, alignment);
  int bsize = alignment;
  char *buf = (char *)new char[bsize];
  memset(buf, 0xcc, bsize);
  bufferlist bl1;
  bl1.append(buf, bsize);
  ASSERT_EQ(0, test_data.m_ioctx.aio_append("foo", my_completion,
					    bl1, bsize));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());

  int hbsize = bsize / 2;
  char *buf2 = (char *)new char[hbsize];
  memset(buf2, 0xdd, hbsize);
  bufferlist bl2;
  bl2.append(buf2, hbsize);
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_append("foo", my_completion2,
					    bl2, hbsize));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());

  AioCompletion *my_completion3 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  ASSERT_NE(my_completion3, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_append("foo", my_completion3,
					    bl2, hbsize));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion3->wait_for_complete());
  }
  EXPECT_EQ(-EOPNOTSUPP, my_completion3->get_return_value());

  bufferlist bl3;
  AioCompletion *my_completion4 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  ASSERT_NE(my_completion4, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo",
			      my_completion4, &bl3, bsize * 3, 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion4->wait_for_complete());
  }
  int tbsize = bsize + hbsize;
  ASSERT_EQ(tbsize, my_completion4->get_return_value());
  ASSERT_EQ((unsigned)tbsize, bl3.length());
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, bsize));
  ASSERT_EQ(0, memcmp(bl3.c_str() + bsize, buf2, hbsize));
  delete my_completion;
  delete my_completion2;
  delete my_completion3;
  delete[] buf;
  delete[] buf2;
}

TEST(LibRadosAioEC, IsComplete) {
  AioTestDataEC test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion2));
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
  ASSERT_EQ((int)sizeof(buf), rados_aio_get_return_value(my_completion2));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAioEC, IsCompletePP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion2,
					  &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;

    // Busy-wait until the AIO completes.
    // Normally we wouldn't do this, but we want to test is_complete.
    while (true) {
      int is_complete = my_completion2->is_complete();
      if (is_complete)
	break;
    }
  }
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), bl2.length());
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAioEC, IsSafe) {
  AioTestDataEC test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
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
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion2));
  ASSERT_EQ(0, rados_aio_read(test_data.m_ioctx, "foo",
			      my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ((int)sizeof(buf), rados_aio_get_return_value(my_completion2));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAioEC, IsSafePP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
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
  ASSERT_EQ(0, my_completion->get_return_value());
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  bufferlist bl2;
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion2,
					  &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), bl2.length());
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAioEC, ReturnValue) {
  AioTestDataEC test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
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

TEST(LibRadosAioEC, ReturnValuePP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
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

TEST(LibRadosAioEC, Flush) {
  AioTestDataEC test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
  char buf[128];
  memset(buf, 0xee, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_aio_flush(test_data.m_ioctx));
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion2));
  ASSERT_EQ(0, rados_aio_read(test_data.m_ioctx, "foo",
			      my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ((int)sizeof(buf2), rados_aio_get_return_value(my_completion2));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAioEC, FlushPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char buf[128];
  memset(buf, 0xee, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion,
					   bl1, sizeof(buf), 0));
  ASSERT_EQ(0, test_data.m_ioctx.aio_flush());
  ASSERT_EQ(0, my_completion->get_return_value());
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion2,
					  &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), bl2.length());
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAioEC, FlushAsync) {
  AioTestDataEC test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
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
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion2));
  ASSERT_EQ(0, rados_aio_read(test_data.m_ioctx, "foo",
			      my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ((int)sizeof(buf2), rados_aio_get_return_value(my_completion2));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(flush_completion);
}

TEST(LibRadosAioEC, FlushAsyncPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
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
  ASSERT_EQ(0, my_completion->get_return_value());
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion2,
					  &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), bl2.length());
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
  delete my_completion;
  delete my_completion2;
  delete flush_completion;
}

TEST(LibRadosAioEC, RoundTripWriteFull) {
  AioTestDataEC test_data;
  rados_completion_t my_completion, my_completion2, my_completion3;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  char buf2[64];
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion2));
  ASSERT_EQ(0, rados_aio_write_full(test_data.m_ioctx, "foo",
			       my_completion2, buf2, sizeof(buf2)));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));
  char buf3[sizeof(buf) + sizeof(buf2)];
  memset(buf3, 0, sizeof(buf3));
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion3));
  ASSERT_EQ(0, rados_aio_read(test_data.m_ioctx, "foo",
			      my_completion3, buf3, sizeof(buf3), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion3));
  }
  ASSERT_EQ((int)sizeof(buf2), rados_aio_get_return_value(my_completion3));
  ASSERT_EQ(0, memcmp(buf3, buf2, sizeof(buf2)));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(my_completion3);
}

TEST(LibRadosAioEC, RoundTripWriteFullPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
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
  ASSERT_EQ(0, my_completion->get_return_value());
  char buf2[64];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write_full("foo", my_completion2, bl2));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());
  bufferlist bl3;
  AioCompletion *my_completion3 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  ASSERT_NE(my_completion3, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion3,
					  &bl3, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion3->wait_for_complete());
  }
  ASSERT_EQ((int)sizeof(buf2), my_completion3->get_return_value());
  ASSERT_EQ(sizeof(buf2), bl3.length());
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf2, sizeof(buf2)));
  delete my_completion;
  delete my_completion2;
  delete my_completion3;
}

//using ObjectWriteOperation/ObjectReadOperation with iohint
TEST(LibRadosAioEC, RoundTripWriteFullPP2)
{
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  boost::scoped_ptr<AioCompletion> my_completion1(cluster.aio_create_completion(0, 0, 0));
  ObjectWriteOperation op;
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf);

  op.write_full(bl);
  op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
  ioctx.aio_operate("test_obj", my_completion1.get(), &op);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion1->wait_for_complete());
  }
  EXPECT_EQ(0, my_completion1->get_return_value());

  boost::scoped_ptr<AioCompletion> my_completion2(cluster.aio_create_completion(0, 0, 0));
  bl.clear();
  ObjectReadOperation op1;
  op1.read(0, sizeof(buf), &bl, NULL);
  op1.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_NOCACHE|LIBRADOS_OP_FLAG_FADVISE_RANDOM);
  ioctx.aio_operate("test_obj", my_completion2.get(), &op1, 0);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  EXPECT_EQ(0, my_completion2->get_return_value());
  ASSERT_EQ(0, memcmp(buf, bl.c_str(), sizeof(buf)));

  ioctx.remove("test_obj");
  destroy_one_pool_pp(pool_name, cluster);
}

TEST(LibRadosAioEC, SimpleStat) {
  AioTestDataEC test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  uint64_t psize;
  time_t pmtime;
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion2));
  ASSERT_EQ(0, rados_aio_stat(test_data.m_ioctx, "foo",
			      my_completion2, &psize, &pmtime));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));
  ASSERT_EQ(sizeof(buf), psize);
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAioEC, SimpleStatPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  uint64_t psize;
  time_t pmtime;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_stat("foo", my_completion2,
			  		&psize, &pmtime));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), psize);
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAioEC, SimpleStatNS) {
  AioTestDataEC test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  rados_ioctx_set_namespace(test_data.m_ioctx, "nspace");
  char buf2[64];
  memset(buf2, 0xbb, sizeof(buf2));
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  uint64_t psize;
  time_t pmtime;
  rados_completion_t my_completion2;
  rados_ioctx_set_namespace(test_data.m_ioctx, "");
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion2));
  ASSERT_EQ(0, rados_aio_stat(test_data.m_ioctx, "foo",
			      my_completion2, &psize, &pmtime));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));
  ASSERT_EQ(sizeof(buf), psize);

  rados_ioctx_set_namespace(test_data.m_ioctx, "nspace");
  rados_completion_t my_completion3;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion3));
  ASSERT_EQ(0, rados_aio_stat(test_data.m_ioctx, "foo",
			      my_completion3, &psize, &pmtime));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion3));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion3));
  ASSERT_EQ(sizeof(buf2), psize);

  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(my_completion3);
}

TEST(LibRadosAioEC, SimpleStatPPNS) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  uint64_t psize;
  time_t pmtime;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_stat("foo", my_completion2,
			  		&psize, &pmtime));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), psize);
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAioEC, StatRemove) {
  AioTestDataEC test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  uint64_t psize;
  time_t pmtime;
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion2));
  ASSERT_EQ(0, rados_aio_stat(test_data.m_ioctx, "foo",
			      my_completion2, &psize, &pmtime));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));
  ASSERT_EQ(sizeof(buf), psize);
  rados_completion_t my_completion3;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion3));
  ASSERT_EQ(0, rados_aio_remove(test_data.m_ioctx, "foo", my_completion3));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion3));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion3));
  uint64_t psize2;
  time_t pmtime2;
  rados_completion_t my_completion4;
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion4));
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

TEST(LibRadosAioEC, StatRemovePP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
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
    sem_wait(test_data.m_sem);
    sem_wait(test_data.m_sem);
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  uint64_t psize;
  time_t pmtime;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_stat("foo", my_completion2,
			  		&psize, &pmtime));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), psize);
  uint64_t psize2;
  time_t pmtime2;
  AioCompletion *my_completion3 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  ASSERT_NE(my_completion3, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_remove("foo", my_completion3));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion3->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion3->get_return_value());

  AioCompletion *my_completion4 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
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

TEST(LibRadosAioEC, OmapPP) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, cluster));
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
    EXPECT_EQ(-EOPNOTSUPP, my_completion->get_return_value());
  }
  ioctx.remove("test_obj");
  destroy_one_pool_pp(pool_name, cluster);
}

TEST(LibRadosAioEC, MultiWrite) {
  AioTestDataEC test_data;
  rados_completion_t my_completion, my_completion2, my_completion3;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));

  char buf2[64];
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion2));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
			       my_completion2, buf2, sizeof(buf2), sizeof(buf)));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ(-EOPNOTSUPP, rados_aio_get_return_value(my_completion2));

  char buf3[(sizeof(buf) + sizeof(buf2)) * 3];
  memset(buf3, 0, sizeof(buf3));
  ASSERT_EQ(0, rados_aio_create_completion((void*)&test_data,
	      set_completion_completeEC, set_completion_safeEC, &my_completion3));
  ASSERT_EQ(0, rados_aio_read(test_data.m_ioctx, "foo",
			      my_completion3, buf3, sizeof(buf3), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion3));
  }
  ASSERT_EQ((int)sizeof(buf), rados_aio_get_return_value(my_completion3));
  ASSERT_EQ(0, memcmp(buf3, buf, sizeof(buf)));
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
  rados_aio_release(my_completion3);
}

TEST(LibRadosAioEC, MultiWritePP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
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
  ASSERT_EQ(0, my_completion->get_return_value());

  char buf2[64];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion2,
					   bl2, sizeof(buf2), sizeof(buf)));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(-EOPNOTSUPP, my_completion2->get_return_value());

  bufferlist bl3;
  AioCompletion *my_completion3 = test_data.m_cluster.aio_create_completion(
	  (void*)&test_data, set_completion_completeEC, set_completion_safeEC);
  ASSERT_NE(my_completion3, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion3,
					  &bl3, (sizeof(buf) + sizeof(buf2) * 3), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion3->wait_for_complete());
  }
  ASSERT_EQ((int)sizeof(buf), my_completion3->get_return_value());
  ASSERT_EQ(sizeof(buf), bl3.length());
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, sizeof(buf)));
  delete my_completion;
  delete my_completion2;
  delete my_completion3;
}
