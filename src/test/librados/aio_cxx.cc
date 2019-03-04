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

#include "test_cxx.h"

using namespace librados;
using std::pair;
using std::ostringstream;

class AioTestDataPP
{
public:
  AioTestDataPP()
    : m_init(false)
  {
  }

  ~AioTestDataPP()
  {
    if (m_init) {
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

  Rados m_cluster;
  IoCtx m_ioctx;
  std::string m_pool_name;
  bool m_init;
};

TEST(LibRadosAio, TooBigPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());

  bufferlist bl;
  AioCompletion *aio_completion = test_data.m_cluster.aio_create_completion(nullptr, NULL, NULL);
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
  ioctx.application_enable("rados", true);

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

  // make sure we have latest map that marked the pool full
  test_data.m_cluster.wait_for_latest_osdmap();

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

TEST(LibRadosAio, SimpleWritePP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo",
			       my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  delete my_completion;
  }

  {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  test_data.m_ioctx.set_namespace("nspace");
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo",
			       my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  delete my_completion;
  }
}

TEST(LibRadosAio, WaitForSafePP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAio, RoundTripPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
  bl.append(buf, sizeof(buf));

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
  destroy_one_pool_pp(pool_name, cluster);
}

TEST(LibRadosAio, RoundTripSparseReadPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
  std::map<uint64_t, uint64_t> extents;
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAioPP, ReadIntoBufferlist) {

  // here we test reading into a non-empty bufferlist referencing existing
  // buffers

  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

  bufferlist bl2;
  char buf2[sizeof(buf)];
  memset(buf2, 0xbb, sizeof(buf2));
  bl2.append(buffer::create_static(sizeof(buf2), buf2));
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
    nullptr, nullptr, nullptr);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion2,
					  &bl2, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ((int)sizeof(buf), my_completion2->get_return_value());
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAioPP, XattrsRoundTripPP) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, test_data.m_ioctx.append("foo", bl1, sizeof(buf)));
  bufferlist bl2;
  // async getxattr
  boost::scoped_ptr<AioCompletion> my_completion
    (test_data.m_cluster.aio_create_completion
     (nullptr, nullptr, nullptr));
  ASSERT_EQ(0, test_data.m_ioctx.aio_getxattr("foo", my_completion.get(), attr1, bl2));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(-ENODATA, my_completion->get_return_value());
  // append
  bufferlist bl3;
  bl3.append(attr1_buf, sizeof(attr1_buf));
  // async setxattr
  AioTestDataPP test_data2;
  ASSERT_EQ("", test_data2.init());
  boost::scoped_ptr<AioCompletion> my_completion2
    (test_data.m_cluster.aio_create_completion
     (nullptr, nullptr, nullptr));
  ASSERT_EQ(0, test_data.m_ioctx.aio_setxattr("foo", my_completion2.get(), attr1, bl3));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());
  // async getxattr
  bufferlist bl4;
  AioTestDataPP test_data3;
  ASSERT_EQ("", test_data3.init());
  boost::scoped_ptr<AioCompletion> my_completion3
    (test_data.m_cluster.aio_create_completion
     (nullptr, nullptr, nullptr));
  ASSERT_EQ(0, test_data.m_ioctx.aio_getxattr("foo", my_completion3.get(), attr1, bl4));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion3->wait_for_complete());
  }
  ASSERT_EQ((int)sizeof(attr1_buf), my_completion3->get_return_value());
  // check content of attribute
  ASSERT_EQ(0, memcmp(bl4.c_str(), attr1_buf, sizeof(attr1_buf)));
}

TEST(LibRadosAioPP, RmXattrPP) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, test_data.m_ioctx.append("foo", bl1, sizeof(buf)));
  // async setxattr
  bufferlist bl2;
  bl2.append(attr1_buf, sizeof(attr1_buf));
  boost::scoped_ptr<AioCompletion> my_completion
    (test_data.m_cluster.aio_create_completion
     (nullptr, nullptr, nullptr));
  ASSERT_EQ(0, test_data.m_ioctx.aio_setxattr("foo", my_completion.get(), attr1, bl2));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  // async rmxattr
  AioTestDataPP test_data2;
  ASSERT_EQ("", test_data2.init());
  boost::scoped_ptr<AioCompletion> my_completion2
    (test_data.m_cluster.aio_create_completion
     (nullptr, nullptr, nullptr));
  ASSERT_EQ(0, test_data.m_ioctx.aio_rmxattr("foo", my_completion2.get(), attr1));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());
  // async getxattr
  AioTestDataPP test_data3;
  ASSERT_EQ("", test_data3.init());
  boost::scoped_ptr<AioCompletion> my_completion3
    (test_data.m_cluster.aio_create_completion
     (nullptr, nullptr, nullptr));
  bufferlist bl3;
  ASSERT_EQ(0, test_data.m_ioctx.aio_getxattr("foo", my_completion3.get(), attr1, bl3));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion3->wait_for_complete());
  }
  ASSERT_EQ(-ENODATA, my_completion3->get_return_value());
  // Test rmxattr on a removed object
  char buf2[128];
  char attr2[] = "attr2";
  char attr2_buf[] = "foo bar baz";
  memset(buf2, 0xbb, sizeof(buf2));
  bufferlist bl21;
  bl21.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.write("foo_rmxattr", bl21, sizeof(buf2), 0));
  bufferlist bl22;
  bl22.append(attr2_buf, sizeof(attr2_buf));
  // async setxattr
  AioTestDataPP test_data4;
  ASSERT_EQ("", test_data4.init());
  boost::scoped_ptr<AioCompletion> my_completion4
    (test_data.m_cluster.aio_create_completion
     (nullptr, nullptr, nullptr));
  ASSERT_EQ(0, test_data.m_ioctx.aio_setxattr("foo_rmxattr", my_completion4.get(), attr2, bl22));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion4->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion4->get_return_value());
  // remove object
  ASSERT_EQ(0, test_data.m_ioctx.remove("foo_rmxattr"));
  // async rmxattr on non existing object
  AioTestDataPP test_data5;
  ASSERT_EQ("", test_data5.init());
  boost::scoped_ptr<AioCompletion> my_completion5
    (test_data.m_cluster.aio_create_completion
     (nullptr, nullptr, nullptr));
  ASSERT_EQ(0, test_data.m_ioctx.aio_rmxattr("foo_rmxattr", my_completion5.get(), attr2));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion5->wait_for_complete());
  }
  ASSERT_EQ(-ENOENT, my_completion5->get_return_value());
}

TEST(LibRadosIoPP, XattrListPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  // create an object with 2 attributes
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  char attr2[] = "attr2";
  char attr2_buf[256];
  for (size_t j = 0; j < sizeof(attr2_buf); ++j) {
    attr2_buf[j] = j % 0xff;
  }
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, test_data.m_ioctx.append("foo", bl1, sizeof(buf)));
  bufferlist bl2;
  bl2.append(attr1_buf, sizeof(attr1_buf));
  ASSERT_EQ(0, test_data.m_ioctx.setxattr("foo", attr1, bl2));
  bufferlist bl3;
  bl3.append(attr2_buf, sizeof(attr2_buf));
  ASSERT_EQ(0, test_data.m_ioctx.setxattr("foo", attr2, bl3));
  // call async version of getxattrs
  boost::scoped_ptr<AioCompletion> my_completion
    (test_data.m_cluster.aio_create_completion
     (nullptr, nullptr, nullptr));
  std::map<std::string, bufferlist> attrset;
  ASSERT_EQ(0, test_data.m_ioctx.aio_getxattrs("foo", my_completion.get(), attrset));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  for (std::map<std::string, bufferlist>::iterator i = attrset.begin();
       i != attrset.end(); ++i) {
    if (i->first == string(attr1)) {
      ASSERT_EQ(0, memcmp(i->second.c_str(), attr1_buf, sizeof(attr1_buf)));
    }
    else if (i->first == string(attr2)) {
      ASSERT_EQ(0, memcmp(i->second.c_str(), attr2_buf, sizeof(attr2_buf)));
    }
    else {
      ASSERT_EQ(0, 1);
    }
  }
}

TEST(LibRadosAio, IsCompletePP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAio, IsSafePP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAio, ReturnValuePP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAio, FlushPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAio, FlushAsyncPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAio, RoundTripWriteFullPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write_full("foo", my_completion2, bl2));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());
  bufferlist bl3;
  AioCompletion *my_completion3 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAio, RoundTripWriteSamePP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char full[128];
  memset(full, 0xcc, sizeof(full));
  bufferlist bl1;
  bl1.append(full, sizeof(full));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion,
					   bl1, sizeof(full), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  /* write the same buf four times */
  char buf[32];
  size_t ws_write_len = sizeof(full);
  memset(buf, 0xdd, sizeof(buf));
  bufferlist bl2;
  bl2.append(buf, sizeof(buf));
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_writesame("foo", my_completion2, bl2,
					       ws_write_len, 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());
  bufferlist bl3;
  AioCompletion *my_completion3 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
  ASSERT_NE(my_completion3, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_read("foo", my_completion3,
					  &bl3, sizeof(full), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion3->wait_for_complete());
  }
  ASSERT_EQ((int)sizeof(full), my_completion3->get_return_value());
  ASSERT_EQ(sizeof(full), bl3.length());
  for (char *cmp = bl3.c_str(); cmp < bl3.c_str() + bl3.length();
							cmp += sizeof(buf)) {
    ASSERT_EQ(0, memcmp(cmp, buf, sizeof(buf)));
  }
  delete my_completion;
  delete my_completion2;
  delete my_completion3;
}

TEST(LibRadosAio, RoundTripWriteSamePP2)
{
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  boost::scoped_ptr<AioCompletion>
			wr_cmpl(cluster.aio_create_completion(0, 0, 0));
  ObjectWriteOperation op;
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  op.writesame(0, sizeof(buf) * 4, bl);
  op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ioctx.aio_operate("test_obj", wr_cmpl.get(), &op);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, wr_cmpl->wait_for_complete());
  }
  EXPECT_EQ(0, wr_cmpl->get_return_value());

  boost::scoped_ptr<AioCompletion>
			rd_cmpl(cluster.aio_create_completion(0, 0, 0));
  char *cmp;
  char full[sizeof(buf) * 4];
  memset(full, 0, sizeof(full));
  bufferlist fl;
  fl.append(full, sizeof(full));
  ObjectReadOperation op1;
  op1.read(0, sizeof(full), &fl, NULL);
  op1.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ioctx.aio_operate("test_obj", rd_cmpl.get(), &op1, 0);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rd_cmpl->wait_for_complete());
  }
  EXPECT_EQ(0, rd_cmpl->get_return_value());
  for (cmp = fl.c_str(); cmp < fl.c_str() + fl.length(); cmp += sizeof(buf)) {
    ASSERT_EQ(0, memcmp(cmp, buf, sizeof(buf)));
  }

  ioctx.remove("test_obj");
  destroy_one_pool_pp(pool_name, cluster);
}

TEST(LibRadosAio, SimpleStatPPNS) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
  uint64_t psize;
  time_t pmtime;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAio, SimpleStatPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
  uint64_t psize;
  time_t pmtime;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAio, StatRemovePP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
  uint64_t psize;
  time_t pmtime;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
  ASSERT_NE(my_completion3, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_remove("foo", my_completion3));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion3->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion3->get_return_value());

  AioCompletion *my_completion4 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAio, ExecuteClassPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
  ASSERT_NE(my_completion2, my_completion_null);
  bufferlist in, out;
  ASSERT_EQ(0, test_data.m_ioctx.aio_exec("foo", my_completion2,
					  "hello", "say_hello", in, &out));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());
  ASSERT_EQ(std::string("Hello, world!"), std::string(out.c_str(), out.length()));
  delete my_completion;
  delete my_completion2;
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

    op.omap_get_keys2("", 1, &set_got, nullptr, 0);
    op.omap_get_vals2("foo", 1, &map_got, nullptr, 0);

    to_get.insert("foo");
    to_get.insert("qfoo3");
    op.omap_get_vals_by_keys(to_get, &got3, 0);

    op.omap_get_header(&header, 0);

    op.omap_get_vals2("foo2", "q", 1, &got4, nullptr, 0);

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
    op.omap_get_keys2("", -1, &set_got, nullptr, 0);
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
    op.omap_get_keys2("", -1, &set_got, nullptr, 0);
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
    op.omap_get_keys2("", -1, &set_got, nullptr, 0);
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

TEST(LibRadosAio, MultiWritePP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAio, AioUnlockPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, test_data.m_ioctx.lock_exclusive("foo", "TestLock", "Cookie", "", NULL, 0));
  boost::scoped_ptr<AioCompletion> my_completion
    (test_data.m_cluster.aio_create_completion
     (nullptr, nullptr, nullptr));
  ASSERT_EQ(0, test_data.m_ioctx.aio_unlock("foo", "TestLock", "Cookie", my_completion.get()));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  bufferlist bl2;
  ASSERT_EQ(0, test_data.m_ioctx.lock_exclusive("foo", "TestLock", "Cookie", "", NULL, 0));
}

class AioTestDataECPP
{
public:
  AioTestDataECPP()
    : m_init(false)
  {}

  ~AioTestDataECPP()
  {
    if (m_init) {
      m_ioctx.close();
      destroy_one_ec_pool_pp(m_pool_name, m_cluster);
    }
  }

  std::string init()
  {
    int ret;
    m_pool_name = get_temp_pool_name();
    std::string err = create_one_ec_pool_pp(m_pool_name, m_cluster);
    if (!err.empty()) {
      ostringstream oss;
      oss << "create_one_ec_pool(" << m_pool_name << ") failed: error " << err;
      return oss.str();
    }
    ret = m_cluster.ioctx_create(m_pool_name.c_str(), m_ioctx);
    if (ret) {
      destroy_one_ec_pool_pp(m_pool_name, m_cluster);
      ostringstream oss;
      oss << "rados_ioctx_create failed: error " << ret;
      return oss.str();
    }
    m_init = true;
    return "";
  }

  Rados m_cluster;
  IoCtx m_ioctx;
  std::string m_pool_name;
  bool m_init;
};

// EC test cases
TEST(LibRadosAioEC, SimpleWritePP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo",
			       my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  delete my_completion;
  }

  {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  test_data.m_ioctx.set_namespace("nspace");
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo",
			       my_completion, bl1, sizeof(buf), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  delete my_completion;
  }
}

TEST(LibRadosAioEC, WaitForSafePP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAioEC, RoundTripPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAio, RoundTripAppendPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAioPP, RemoveTestPP) {
  char buf[128];
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  ASSERT_EQ(0, test_data.m_ioctx.append("foo", bl1, sizeof(buf)));
  boost::scoped_ptr<AioCompletion> my_completion
    (test_data.m_cluster.aio_create_completion
     (nullptr, nullptr, nullptr));
  ASSERT_EQ(0, test_data.m_ioctx.aio_remove("foo", my_completion.get()));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());
  bufferlist bl2;
  ASSERT_EQ(-ENOENT, test_data.m_ioctx.read("foo", bl2, sizeof(buf), 0));
}

TEST(LibRadosAioEC, RoundTripSparseReadPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

  map<uint64_t, uint64_t> extents;
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAioEC, RoundTripAppendPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_append("foo", my_completion2,
					    bl2, hbsize));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());

  AioCompletion *my_completion3 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAioEC, IsCompletePP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
  bufferlist bl2;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
TEST(LibRadosAioEC, IsSafePP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAioEC, ReturnValuePP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAioEC, FlushPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAioEC, FlushAsyncPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAioEC, RoundTripWriteFullPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
  ASSERT_NE(my_completion2, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_write_full("foo", my_completion2, bl2));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());
  bufferlist bl3;
  AioCompletion *my_completion3 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAioEC, SimpleStatPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
  uint64_t psize;
  time_t pmtime;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAioEC, SimpleStatPPNS) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
  uint64_t psize;
  time_t pmtime;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAioEC, StatRemovePP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
  uint64_t psize;
  time_t pmtime;
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
  ASSERT_NE(my_completion3, my_completion_null);
  ASSERT_EQ(0, test_data.m_ioctx.aio_remove("foo", my_completion3));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion3->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion3->get_return_value());

  AioCompletion *my_completion4 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAioEC, ExecuteClassPP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
  ASSERT_NE(my_completion2, my_completion_null);
  bufferlist in, out;
  ASSERT_EQ(0, test_data.m_ioctx.aio_exec("foo", my_completion2,
					  "hello", "say_hello", in, &out));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());
  ASSERT_EQ(std::string("Hello, world!"), std::string(out.c_str(), out.length()));
  delete my_completion;
  delete my_completion2;
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

TEST(LibRadosAioEC, MultiWritePP) {
  AioTestDataECPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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
	  nullptr, nullptr, nullptr);
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

TEST(LibRadosAio, RacingRemovePP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init({{"objecter_retry_writes_after_first_reply", "true"}}));
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
        nullptr, nullptr, nullptr);
  ASSERT_NE(my_completion, nullptr);
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
        nullptr, nullptr, nullptr);
  ASSERT_NE(my_completion2, nullptr);
  ASSERT_EQ(0, test_data.m_ioctx.aio_remove("foo", my_completion2));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion,
                                         bl, sizeof(buf), 0));
  {
    TestAlarm alarm;
    my_completion2->wait_for_complete();
    my_completion->wait_for_complete();
  }
  ASSERT_EQ(-ENOENT, my_completion2->get_return_value());
  ASSERT_EQ(0, my_completion->get_return_value());
  ASSERT_EQ(0, test_data.m_ioctx.stat("foo", nullptr, nullptr));
  delete my_completion;
  delete my_completion2;
}

TEST(LibRadosAio, RoundTripCmpExtPP) {
  AioTestDataPP test_data;
  ASSERT_EQ("", test_data.init());
  AioCompletion *my_completion = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);
  char full[128];
  memset(full, 0xcc, sizeof(full));
  bufferlist bl1;
  bl1.append(full, sizeof(full));
  ASSERT_EQ(0, test_data.m_ioctx.aio_write("foo", my_completion,
					   bl1, sizeof(full), 0));
  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion->get_return_value());

  /* compare with match */
  bufferlist cbl;
  cbl.append(full, sizeof(full));
  AioCompletion *my_completion2 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
  ASSERT_EQ(0, test_data.m_ioctx.aio_cmpext("foo", my_completion2, 0, cbl));

  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion2->wait_for_complete());
  }
  ASSERT_EQ(0, my_completion2->get_return_value());

  /* compare with mismatch */
  memset(full, 0xdd, sizeof(full));
  cbl.clear();
  cbl.append(full, sizeof(full));
  AioCompletion *my_completion3 = test_data.m_cluster.aio_create_completion(
	  nullptr, nullptr, nullptr);
  ASSERT_EQ(0, test_data.m_ioctx.aio_cmpext("foo", my_completion3, 0, cbl));

  {
    TestAlarm alarm;
    ASSERT_EQ(0, my_completion3->wait_for_complete());
  }
  ASSERT_EQ(-MAX_ERRNO, my_completion3->get_return_value());

  delete my_completion;
  delete my_completion2;
  delete my_completion3;
}

TEST(LibRadosAio, RoundTripCmpExtPP2)
{
  int ret;
  char buf[128];
  char miscmp_buf[128];
  bufferlist cbl;
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  boost::scoped_ptr<AioCompletion>
			wr_cmpl(cluster.aio_create_completion(0, 0, 0));
  ObjectWriteOperation wr_op;
  memset(buf, 0xcc, sizeof(buf));
  memset(miscmp_buf, 0xdd, sizeof(miscmp_buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  wr_op.write_full(bl);
  wr_op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ioctx.aio_operate("test_obj", wr_cmpl.get(), &wr_op);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, wr_cmpl->wait_for_complete());
  }
  EXPECT_EQ(0, wr_cmpl->get_return_value());

  /* cmpext as write op. first match then mismatch */
  boost::scoped_ptr<AioCompletion>
			wr_cmpext_cmpl(cluster.aio_create_completion(0, 0, 0));
  cbl.append(buf, sizeof(buf));
  ret = 0;

  wr_op.cmpext(0, cbl, &ret);
  wr_op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ioctx.aio_operate("test_obj", wr_cmpext_cmpl.get(), &wr_op);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, wr_cmpext_cmpl->wait_for_complete());
  }
  EXPECT_EQ(0, wr_cmpext_cmpl->get_return_value());
  EXPECT_EQ(0, ret);

  boost::scoped_ptr<AioCompletion>
			wr_cmpext_cmpl2(cluster.aio_create_completion(0, 0, 0));
  cbl.clear();
  cbl.append(miscmp_buf, sizeof(miscmp_buf));
  ret = 0;

  wr_op.cmpext(0, cbl, &ret);
  wr_op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ioctx.aio_operate("test_obj", wr_cmpext_cmpl2.get(), &wr_op);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, wr_cmpext_cmpl2->wait_for_complete());
  }
  EXPECT_EQ(-MAX_ERRNO, wr_cmpext_cmpl2->get_return_value());
  EXPECT_EQ(-MAX_ERRNO, ret);

  /* cmpext as read op */
  boost::scoped_ptr<AioCompletion>
			rd_cmpext_cmpl(cluster.aio_create_completion(0, 0, 0));
  ObjectReadOperation rd_op;
  cbl.clear();
  cbl.append(buf, sizeof(buf));
  ret = 0;
  rd_op.cmpext(0, cbl, &ret);
  rd_op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ioctx.aio_operate("test_obj", rd_cmpext_cmpl.get(), &rd_op, 0);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rd_cmpext_cmpl->wait_for_complete());
  }
  EXPECT_EQ(0, rd_cmpext_cmpl->get_return_value());
  EXPECT_EQ(0, ret);

  boost::scoped_ptr<AioCompletion>
			rd_cmpext_cmpl2(cluster.aio_create_completion(0, 0, 0));
  cbl.clear();
  cbl.append(miscmp_buf, sizeof(miscmp_buf));
  ret = 0;

  rd_op.cmpext(0, cbl, &ret);
  rd_op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ioctx.aio_operate("test_obj", rd_cmpext_cmpl2.get(), &rd_op, 0);
  {
    TestAlarm alarm;
    ASSERT_EQ(0, rd_cmpext_cmpl2->wait_for_complete());
  }
  EXPECT_EQ(-MAX_ERRNO, rd_cmpext_cmpl2->get_return_value());
  EXPECT_EQ(-MAX_ERRNO, ret);

  ioctx.remove("test_obj");
  destroy_one_pool_pp(pool_name, cluster);
}
