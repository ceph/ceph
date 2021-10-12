#include <errno.h>
#include <fcntl.h>
#include <string>
#include <sstream>
#include <utility>
#include <boost/scoped_ptr.hpp>

#include "include/err.h"
#include "include/rados/librados.h"
#include "include/types.h"
#include "include/stringify.h"
#include "include/scope_guard.h"
#include "osdc/QosProfileMgr.h"

#include "common/errno.h"

#include "gtest/gtest.h"

#include "test.h"

using std::ostringstream;

class AioQosTestData
{
public:
  AioQosTestData()
    : m_cluster(NULL),
      m_ioctx(NULL),
      m_init(false),
      m_qos_profile(NULL)
  {
  }

  ~AioQosTestData()
  {
    if (m_init) {
      rados_qos_profile_release(m_qos_profile);
      rados_ioctx_destroy(m_ioctx);
      destroy_one_pool(m_pool_name, &m_cluster);
    }
  }

  std::string init()
  {
    int ret;
    m_pool_name = get_temp_pool_name();
    std::string err = create_one_pool(m_pool_name, &m_cluster);
    if (!err.empty()) {
      ostringstream oss;
      oss << "create_one_pool(" << m_pool_name << ") failed: error " << err;
      return oss.str();
    }
    ret = rados_ioctx_create(m_cluster, m_pool_name.c_str(), &m_ioctx);
    if (ret) {
      destroy_one_pool(m_pool_name, &m_cluster);
      ostringstream oss;
      oss << "rados_ioctx_create failed: error " << ret;
      return oss.str();
    }
    m_init = true;
    return "";
  }

  int create_qos_profile(uint64_t res, uint64_t wgt, uint64_t lim)
  {
    return rados_qos_profile_create(res, wgt, lim, &m_qos_profile);
  }

  void set_qos_profile()
  {
    rados_ioctx_set_qos_profile(m_ioctx, m_qos_profile);
  }

  uint64_t get_qos_profile_id()
  {
    return rados_qos_profile_get_id(m_qos_profile);
  }

  client_qos_params_t& get_profile_qos_params()
  {
    auto profile_ref = (osdc::qos_profile_ref) m_qos_profile;
    return (*profile_ref)->qos_params();
  }

  rados_t m_cluster;
  rados_ioctx_t m_ioctx;
  std::string m_pool_name;
  bool m_init;
  rados_qos_profile_t m_qos_profile;
};

TEST(LibRadosAioQos, SimpleWrite) {
  AioQosTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, test_data.create_qos_profile(1, 1, 50));
  test_data.set_qos_profile();
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr,
            nullptr, &my_completion));
  auto sg = make_scope_guard([&] { rados_aio_release(my_completion); });
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
            my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  ASSERT_GE(test_data.get_qos_profile_id(), 1u);
  auto params = test_data.get_profile_qos_params();
  ASSERT_EQ(1, params.reservation);
  ASSERT_EQ(1, params.weight);
  ASSERT_EQ(50, params.limit);

  rados_ioctx_set_namespace(test_data.m_ioctx, "nspace");
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr,
            nullptr, &my_completion2));
  auto sg2 = make_scope_guard([&] { rados_aio_release(my_completion2); });
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
            my_completion2, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));
  ASSERT_GE(test_data.get_qos_profile_id(), 1u);
  params = test_data.get_profile_qos_params();
  ASSERT_EQ(1, params.reservation);
  ASSERT_EQ(1, params.weight);
  ASSERT_EQ(50, params.limit);
}

TEST(LibRadosAioQos, WaitForSafe) {
  AioQosTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, test_data.create_qos_profile(1, 1, 50));
  test_data.set_qos_profile();
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr,
            nullptr, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
            my_completion, buf, sizeof(buf), 0));
  TestAlarm alarm;
  ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion));
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  ASSERT_GE(test_data.get_qos_profile_id(), 1u);
  auto params = test_data.get_profile_qos_params();
  ASSERT_EQ(1, params.reservation);
  ASSERT_EQ(1, params.weight);
  ASSERT_EQ(50, params.limit);
  rados_aio_release(my_completion);
}

TEST(LibRadosAioQos, RoundTrip) {
  AioQosTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, test_data.create_qos_profile(1, 1, 50));
  test_data.set_qos_profile();
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr,
            nullptr, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
            my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  char buf2[256];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr,
            nullptr, &my_completion2));
  ASSERT_EQ(0, rados_aio_read(test_data.m_ioctx, "foo",
            my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ((int)sizeof(buf), rados_aio_get_return_value(my_completion2));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  ASSERT_GE(test_data.get_qos_profile_id(), 1u);
  auto params = test_data.get_profile_qos_params();
  ASSERT_EQ(1, params.reservation);
  ASSERT_EQ(1, params.weight);
  ASSERT_EQ(50, params.limit);
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAioQos, RoundTrip2) {
  AioQosTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, test_data.create_qos_profile(1, 1, 50));
  test_data.set_qos_profile();
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr,
            nullptr, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_aio_write(test_data.m_ioctx, "foo",
            my_completion, buf, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr,
            nullptr, &my_completion2));
  ASSERT_EQ(0, rados_aio_read(test_data.m_ioctx, "foo",
            my_completion2, buf2, sizeof(buf2), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ((int)sizeof(buf), rados_aio_get_return_value(my_completion2));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  // Verify QoS params
  ASSERT_GE(test_data.get_qos_profile_id(), 1u);
  auto params = test_data.get_profile_qos_params();
  ASSERT_EQ(1, params.reservation);
  ASSERT_EQ(1, params.weight);
  ASSERT_EQ(50, params.limit);
  rados_aio_release(my_completion);
  rados_aio_release(my_completion2);
}

TEST(LibRadosAioQos, RoundTrip3) {
  AioQosTestData test_data;
  rados_completion_t my_completion;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, test_data.create_qos_profile(1, 1, 100));
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr,
            nullptr, &my_completion));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));

  rados_write_op_t op1 = rados_create_write_op();
  // Set the QoS params on the write op
  rados_write_op_set_qos_profile(op1, test_data.m_qos_profile);
  rados_write_op_write(op1, buf, sizeof(buf), 0);
  rados_write_op_set_alloc_hint2(op1, 0, 0, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ASSERT_EQ(0, rados_aio_write_op_operate(op1, test_data.m_ioctx, my_completion,
            "foo", NULL, 0));
  rados_release_write_op(op1);

  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion));
  }

  ASSERT_EQ(0, rados_aio_get_return_value(my_completion));
  // Verify QoS params
  ASSERT_GE(test_data.get_qos_profile_id(), 1u);
  auto params = test_data.get_profile_qos_params();
  ASSERT_EQ(1, params.reservation);
  ASSERT_EQ(1, params.weight);
  ASSERT_EQ(100, params.limit);
  rados_aio_release(my_completion);

  char buf2[128];
  memset(buf2, 0, sizeof(buf2));
  rados_completion_t my_completion2;
  ASSERT_EQ(0, rados_aio_create_completion2(nullptr,
            nullptr, &my_completion2));

  rados_read_op_t op2 = rados_create_read_op();
  rados_read_op_read(op2, 0, sizeof(buf2), buf2, NULL, NULL);
  rados_read_op_set_flags(op2, LIBRADOS_OP_FLAG_FADVISE_NOCACHE |
                          LIBRADOS_OP_FLAG_FADVISE_RANDOM);
  ceph_le32 init_value(-1);
  ceph_le32 checksum[2];
  rados_read_op_checksum(op2, LIBRADOS_CHECKSUM_TYPE_CRC32C,
                         reinterpret_cast<char *>(&init_value),
                         sizeof(init_value), 0, 0, 0,
                         reinterpret_cast<char *>(&checksum),
                         sizeof(checksum), NULL);
  ASSERT_EQ(0, rados_aio_read_op_operate(op2, test_data.m_ioctx, my_completion2,
            "foo", 0));
  rados_release_read_op(op2);

  {
    TestAlarm alarm;
    ASSERT_EQ(0, rados_aio_wait_for_complete(my_completion2));
  }
  ASSERT_EQ(0, rados_aio_get_return_value(my_completion2));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  rados_aio_release(my_completion2);

  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(1U, checksum[0]);
  ASSERT_EQ(bl.crc32c(-1), checksum[1]);
}
