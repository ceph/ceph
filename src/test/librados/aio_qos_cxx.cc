#include <errno.h>
#include <fcntl.h>
#include <sstream>
#include <string>
#include <utility>
#include <boost/scoped_ptr.hpp>

#include "gtest/gtest.h"

#include "common/errno.h"
#include "include/err.h"
#include "include/rados/librados.hpp"
#include "include/types.h"
#include "include/stringify.h"
#include "include/scope_guard.h"
#include "common/ceph_mutex.h"
#include "osdc/QosProfileMgr.h"

#include "test_cxx.h"

using namespace std;
using namespace librados;

class AioQosTestDataPP
{
public:
  AioQosTestDataPP()
    : m_init(false)
  {
  }

  ~AioQosTestDataPP()
  {
    if (m_init) {
      m_cluster.qos_profile_release(m_qos_profile);
      m_ioctx.close();
      destroy_one_pool_pp(m_pool_name, m_cluster);
    }
  }

  std::string init()
  {
      return init({});
  }

  std::string init(const std::map<std::string, std::string> &config)
  {
    int ret;

    m_pool_name = get_temp_pool_name();
    std::string err = create_one_pool_pp(m_pool_name, m_cluster, config);
    if (!err.empty()) {
      ostringstream oss;
      oss << "create_one_pool(" << m_pool_name << ") failed: error " << err;
      return oss.str();
    }
    ret = m_cluster.ioctx_create(m_pool_name.c_str(), m_ioctx);
    if (ret) {
      destroy_one_pool_pp(m_pool_name, m_cluster);
      ostringstream oss;
      oss << "rados_ioctx_create failed: error " << ret;
      return oss.str();
    }
    m_init = true;

    return "";
  }

  void create_qos_profile(uint64_t res, uint64_t wgt, uint64_t lim)
  {
    m_qos_profile = m_cluster.qos_profile_create(res, wgt, lim);
    m_ioctx.set_qos_profile(m_qos_profile);
  }

  uint64_t get_qos_profile_id()
  {
    return m_cluster.qos_profile_get_id(m_qos_profile);
  }

  client_qos_params_t& get_profile_qos_params()
  {
    auto profile_ref = (osdc::qos_profile_ref) m_qos_profile;
    return (*profile_ref)->qos_params();
  }

  Rados m_cluster;
  IoCtx m_ioctx;
  std::string m_pool_name;
  bool m_init;
  rados_qos_profile_t m_qos_profile;
};

TEST(LibRadosAioQos, SimpleWriteQosPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  {
  AioQosTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  test_data.create_qos_profile(1, 1, 10);
  auto my_completion = std::unique_ptr<AioCompletion>{Rados::aio_create_completion()};
  ASSERT_TRUE(my_completion);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion.get(),
                                           bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  ASSERT_GE(test_data.get_qos_profile_id(), 1u);
  auto params = test_data.get_profile_qos_params();
  ASSERT_EQ(1, params.reservation);
  ASSERT_EQ(1, params.weight);
  ASSERT_EQ(10, params.limit);
  }

  {
  AioQosTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  test_data.create_qos_profile(1, 1, 10);
  test_data.m_ioctx.set_namespace("nspace");
  auto my_completion = std::unique_ptr<AioCompletion>{Rados::aio_create_completion()};
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion.get(),
                                           bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  ASSERT_GE(test_data.get_qos_profile_id(), 1u);
  auto params = test_data.get_profile_qos_params();
  ASSERT_EQ(1, params.reservation);
  ASSERT_EQ(1, params.weight);
  ASSERT_EQ(10, params.limit);
  }
}

TEST(LibRadosAioQos, WaitForSafeQosPP) {
  AioQosTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  test_data.create_qos_profile(1, 1, 10);
  auto my_completion = std::unique_ptr<AioCompletion>{Rados::aio_create_completion()};
  ASSERT_TRUE(my_completion);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion.get(),
                                           bl1, sizeof(buf), 0));
  TestAlarm alarm;
  ASSERT_EQ(0, my_completion->wait_for_complete());
  ASSERT_EQ(0, my_completion->get_return_value());

  ASSERT_GE(test_data.get_qos_profile_id(), 1u);
  auto params = test_data.get_profile_qos_params();
  ASSERT_EQ(1, params.reservation);
  ASSERT_EQ(1, params.weight);
  ASSERT_EQ(10, params.limit);
}

TEST(LibRadosAioQos, RoundTripQosPP) {
  AioQosTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  test_data.create_qos_profile(1, 1, 10);
  auto my_completion = std::unique_ptr<AioCompletion>{Rados::aio_create_completion()};
  ASSERT_TRUE(my_completion);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion.get(),
                                           bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  bufferlist bl2;
  auto my_completion2 = std::unique_ptr<AioCompletion>{Rados::aio_create_completion()};
  ASSERT_TRUE(my_completion2);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion2.get(),
                                          &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), bl2.length());
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));

  // Verify QoS params
  ASSERT_GE(test_data.get_qos_profile_id(), 1u);
  auto params = test_data.get_profile_qos_params();
  ASSERT_EQ(1, params.reservation);
  ASSERT_EQ(1, params.weight);
  ASSERT_EQ(10, params.limit);
}

TEST(LibRadosAioQos, RoundTripQosPP2) {
  AioQosTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  test_data.create_qos_profile(1, 1, 10);
  auto my_completion = std::unique_ptr<AioCompletion>{Rados::aio_create_completion()};
  ASSERT_TRUE(my_completion);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion.get(),
                                           bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
   }
  ASSERT_EQ(0, my_completion->get_return_value());
  bufferlist bl2;
  auto my_completion2 = std::unique_ptr<AioCompletion>{Rados::aio_create_completion()};
  ASSERT_TRUE(my_completion2);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion2.get(),
                                          &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(sizeof(buf), bl2.length());
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));

  // Verify QoS params
  ASSERT_GE(test_data.get_qos_profile_id(), 1u);
  auto params = test_data.get_profile_qos_params();
  ASSERT_EQ(1, params.reservation);
  ASSERT_EQ(1, params.weight);
  ASSERT_EQ(10, params.limit);
}

//using ObjectWriteOperation/ObjectReadOperation with iohint
TEST(LibRadosAioQos, RoundTripQosPP3)
{
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  rados_qos_profile_t qos_profile = cluster.qos_profile_create(1, 1, 100);

  auto my_completion1 = std::unique_ptr<AioCompletion>{Rados::aio_create_completion()};
  ObjectWriteOperation op;
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  op.write(0, bl);
  // Set the QoS profile on the write op
  op.set_qos_profile(qos_profile);
  op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ioctx.aio_operate("test_obj", my_completion1.get(), &op);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion1->wait_for_complete());
  }
  EXPECT_EQ(0, my_completion1->get_return_value());

  // Verify QoS params
  ASSERT_GE(cluster.qos_profile_get_id(qos_profile), 1u);
  auto profile_ref = (osdc::qos_profile_ref) qos_profile;
  auto params = (*profile_ref)->qos_params();
  ASSERT_EQ(1, params.reservation);
  ASSERT_EQ(1, params.weight);
  ASSERT_EQ(100, params.limit);

  auto my_completion2 = std::unique_ptr<AioCompletion>{Rados::aio_create_completion()};
  bl.clear();
  ObjectReadOperation op1;
  op1.read(0, sizeof(buf), &bl, NULL);
  op1.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED|LIBRADOS_OP_FLAG_FADVISE_RANDOM);
  bufferlist init_value_bl;
  encode(static_cast<int32_t>(-1), init_value_bl);
  bufferlist csum_bl;
  op1.checksum(LIBRADOS_CHECKSUM_TYPE_CRC32C, init_value_bl,
	       0, 0, 0, &csum_bl, nullptr);
  ioctx.aio_operate("test_obj", my_completion2.get(), &op1, 0);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  EXPECT_EQ(0, my_completion2->get_return_value());
  ASSERT_EQ(0, memcmp(buf, bl.c_str(), sizeof(buf)));

  ASSERT_EQ(8U, csum_bl.length());
  auto csum_bl_it = csum_bl.cbegin();
  uint32_t csum_count;
  uint32_t csum;
  decode(csum_count, csum_bl_it);
  ASSERT_EQ(1U, csum_count);
  decode(csum, csum_bl_it);
  ASSERT_EQ(bl.crc32c(-1), csum);
  ioctx.remove("test_obj");
  ASSERT_EQ(0, cluster.qos_profile_release(qos_profile));
  destroy_one_pool_pp(pool_name, cluster);
}
