// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"

#include "rgw/rgw_sal_wrapper.h"
#include "rgw/rgw_sal.h"
#include "rgw/rgw_sal_config.h"
#include "rgw/rgw_bucket.h"
#include "rgw/rgw_zone.h"
#include "common/ceph_argparse.h"
#include "common/dout.h"
#include "common/async/context_pool.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "common/async/yield_context.h"

#include <boost/asio/io_context.hpp>
#include <atomic>
#include <cstddef>
#include <cstring>
#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#define dout_subsys ceph_subsys_rgw

// ------------------
// Global test state
// ------------------
namespace {

struct TestEnv {
  rgw::sal::Driver* driver = nullptr;
  const DoutPrefixProvider* dpp = nullptr;
  std::unique_ptr<rgw::sal::ConfigStore> cfgstore;
  std::string bucket_name;
  std::string backend;  // "rados", "dbstore", "posix", etc.
};

TestEnv* g_env = nullptr;

}

// ============
// Null safety
// ============

TEST(NullSafety, NullDriverReturnsError) {
  CRgwBucket b{"bucket", nullptr};
  CRgwObject o{"key", nullptr};
  CRgwBuffer buf{nullptr, 0};
  CRgwObjectMeta meta{};
  CRgwListResult result{};

  // core ops
  EXPECT_LT(rgw_put_object(nullptr, nullptr, nullptr, &b, &o, &buf, nullptr, nullptr), 0);
  EXPECT_LT(rgw_get_object(nullptr, nullptr, nullptr, &b, &o, 0, 0, &buf), 0);
  EXPECT_LT(rgw_delete_object(nullptr, nullptr, nullptr, &b, &o), 0);
  EXPECT_LT(rgw_head_object(nullptr, nullptr, nullptr, &b, &o, &meta), 0);
  EXPECT_LT(rgw_list_objects(nullptr, nullptr, nullptr, &b,
             nullptr, nullptr, nullptr, 100, &result), 0);

  // conditional put
  int canceled = 0;
  EXPECT_LT(rgw_put_object_conditional(nullptr, nullptr, nullptr,
             &b, &o, &buf, nullptr, nullptr, &canceled, nullptr, nullptr), 0);

  // copy
  CRgwBucket db{"dst", nullptr};
  CRgwObject d_o{"dkey", nullptr};
  EXPECT_LT(rgw_copy_object(nullptr, nullptr, nullptr, &b, &o, &db, &d_o), 0);
  EXPECT_LT(rgw_copy_object_conditional(nullptr, nullptr, nullptr,
             &b, &o, &db, &d_o, nullptr, nullptr), 0);

  // bulk delete
  const char* keys[] = {"k1"};
  EXPECT_LT(rgw_delete_objects(nullptr, nullptr, nullptr, &b, keys, 1), 0);

  // multipart
  char* upload_id = nullptr;
  EXPECT_LT(rgw_multipart_init(nullptr, nullptr, nullptr, &b, &o, nullptr, &upload_id), 0);

  char* etag = nullptr;
  uint8_t data[] = {0};
  EXPECT_LT(rgw_multipart_put_part(nullptr, nullptr, nullptr,
             &b, &o, "fake-id", 1, data, 1, &etag), 0);

  const char* etags[] = {"etag1"};
  EXPECT_LT(rgw_multipart_complete(nullptr, nullptr, nullptr,
             &b, &o, "fake-id", etags, 1), 0);

  EXPECT_LT(rgw_multipart_abort(nullptr, nullptr, nullptr,
             &b, &o, "fake-id"), 0);
}

TEST(NullSafety, NullBucketAndKeyReturnsError) {
  auto* driver = reinterpret_cast<CRgwDriver*>(0x1234);
  CRgwBucket b{"bucket", nullptr};
  CRgwObject o{"key", nullptr};
  CRgwBuffer buf{nullptr, 0};

  // null bucket
  EXPECT_LT(rgw_put_object(driver, nullptr, nullptr, nullptr, &o, &buf, nullptr, nullptr), 0);
  EXPECT_LT(rgw_get_object(driver, nullptr, nullptr, nullptr, &o, 0, 0, &buf), 0);
  EXPECT_LT(rgw_delete_object(driver, nullptr, nullptr, nullptr, &o), 0);
  EXPECT_LT(rgw_head_object(driver, nullptr, nullptr, nullptr, &o, nullptr), 0);

  // null object key
  EXPECT_LT(rgw_put_object(driver, nullptr, nullptr, &b, nullptr, &buf, nullptr, nullptr), 0);
  EXPECT_LT(rgw_get_object(driver, nullptr, nullptr, &b, nullptr, 0, 0, &buf), 0);
  EXPECT_LT(rgw_delete_object(driver, nullptr, nullptr, &b, nullptr), 0);

  // null output pointers
  EXPECT_LT(rgw_get_object(driver, nullptr, nullptr, &b, &o, 0, 0, nullptr), 0);
  EXPECT_LT(rgw_head_object(driver, nullptr, nullptr, &b, &o, nullptr), 0);
  EXPECT_LT(rgw_list_objects(driver, nullptr, nullptr, &b,
             nullptr, nullptr, nullptr, 100, nullptr), 0);

  // multipart null output
  EXPECT_LT(rgw_multipart_init(driver, nullptr, nullptr, &b, &o, nullptr, nullptr), 0);
  EXPECT_LT(rgw_multipart_put_part(driver, nullptr, nullptr,
             &b, &o, "id", 1, nullptr, 0, nullptr), 0);
}



TEST(NullSafety, FreeNullPointers) {
  // free functions should handle null gracefully
  rgw_free_buffer(nullptr);
  rgw_free_object_meta(nullptr);
  rgw_free_list_result(nullptr);
}

TEST(Utility, WrapperVersion) {
  const char* ver = rgw_sal_wrapper_version();
  ASSERT_NE(ver, nullptr);
  EXPECT_GT(strlen(ver), 0u);

  // verify the runtime version matches the header defines
  std::string expected = std::to_string(RGW_SAL_WRAPPER_VERSION_MAJOR) + "."
                       + std::to_string(RGW_SAL_WRAPPER_VERSION_MINOR);
  EXPECT_EQ(expected, std::string(ver))
      << "sal_wrapper version mismatch: header says " << expected
      << " but runtime returns " << ver;
}

// ===========================================================================
// Functional tests — require live driver + bucket
// ===========================================================================

class SALWrapperTest : public ::testing::Test {
protected:
  CRgwDriver* driver() {
    return reinterpret_cast<CRgwDriver*>(g_env->driver);
  }
  const CRgwDoutPrefix* dpp() {
    return reinterpret_cast<const CRgwDoutPrefix*>(g_env->dpp);
  }

  void SetUp() override {
    if (!g_env || !g_env->driver) {
      GTEST_SKIP() << "SAL driver not initialized";
    }
    bucket_ = {g_env->bucket_name.c_str(), nullptr};
  }

  int put(const char* key, const uint8_t* data, size_t len) {
    CRgwObject obj{key, nullptr};
    CRgwBuffer buf{const_cast<uint8_t*>(data), len};
    return rgw_put_object(driver(), dpp(), nullptr, &bucket_, &obj, &buf, nullptr, nullptr);
  }

  int put_str(const char* key, const std::string& data) {
    return put(key, reinterpret_cast<const uint8_t*>(data.data()), data.size());
  }

  int get(const char* key, CRgwBuffer* buf,
          uint64_t offset = 0, uint64_t length = UINT64_MAX) {
    CRgwObject obj{key, nullptr};
    return rgw_get_object(driver(), dpp(), nullptr, &bucket_, &obj,
                          offset, length, buf);
  }

  int del(const char* key) {
    CRgwObject obj{key, nullptr};
    return rgw_delete_object(driver(), dpp(), nullptr, &bucket_, &obj);
  }

  int head(const char* key, CRgwObjectMeta* meta) {
    CRgwObject obj{key, nullptr};
    return rgw_head_object(driver(), dpp(), nullptr, &bucket_, &obj, meta);
  }

  CRgwBucket bucket_{};
  std::vector<std::string> created_keys_;

  void TearDown() override {
    if (!g_env || !g_env->driver) return;
    for (auto& k : created_keys_) {
      del(k.c_str());
    }
  }

  int put_tracked(const char* key, const uint8_t* data, size_t len) {
    int ret = put(key, data, len);
    if (ret == 0) created_keys_.emplace_back(key);
    return ret;
  }
  int put_str_tracked(const char* key, const std::string& data) {
    return put_tracked(key,
                       reinterpret_cast<const uint8_t*>(data.data()),
                       data.size());
  }
};

// --------------- Null buffer ---------------

TEST_F(SALWrapperTest, OutputBufferUntouchedOnFailure) {
  CRgwBuffer buf{nullptr, 0};
  CRgwObject obj{"test/no-such-key-ever", nullptr};
  EXPECT_EQ(-ENOENT, rgw_get_object(driver(), dpp(), nullptr,
             &bucket_, &obj, 0, 0, &buf));
  EXPECT_EQ(buf.data, nullptr);
  EXPECT_EQ(buf.len, 0u);
}

TEST_F(SALWrapperTest, NullBufferPut) {
  CRgwObject obj{"test/null-buf", nullptr};

  // null buffer pointer is treated as a zero-byte put (valid)
  EXPECT_EQ(0, rgw_put_object(driver(), dpp(), nullptr, &bucket_, &obj, nullptr, nullptr, nullptr));
  created_keys_.emplace_back("test/null-buf");

  // buffer with null data but non-zero length is invalid
  CRgwBuffer bad_buf{nullptr, 100};
  EXPECT_LT(rgw_put_object(driver(), dpp(), nullptr, &bucket_, &obj, &bad_buf, nullptr, nullptr), 0);
}

// --------------- Put / Get ---------------

TEST_F(SALWrapperTest, PutGetRoundTrip) {
  std::string data = "hello, SAL wrapper!";
  ASSERT_EQ(0, put_str_tracked("test/put-get", data));

  CRgwBuffer buf{};
  ASSERT_EQ(0, get("test/put-get", &buf));
  ASSERT_EQ(data.size(), buf.len);
  EXPECT_EQ(0, memcmp(data.data(), buf.data, buf.len));
  rgw_free_buffer(&buf);
}

TEST_F(SALWrapperTest, PutGetBinaryData) {
  std::vector<uint8_t> data(256);
  std::iota(data.begin(), data.end(), 0);
  ASSERT_EQ(0, put_tracked("test/binary", data.data(), data.size()));

  CRgwBuffer buf{};
  ASSERT_EQ(0, get("test/binary", &buf));
  ASSERT_EQ(data.size(), buf.len);
  EXPECT_EQ(0, memcmp(data.data(), buf.data, buf.len));
  rgw_free_buffer(&buf);
}

// known fail: dbstore, posix — backend does not support multi-chunk reads
TEST_F(SALWrapperTest, PutGetLargeObject) {
  size_t test_size = 1024 * 1024;
  std::vector<uint8_t> data(test_size);
  std::iota(data.begin(), data.end(), 0);
  ASSERT_EQ(0, put_tracked("test/large-obj", data.data(), data.size()));

  CRgwBuffer buf{};
  ASSERT_EQ(0, get("test/large-obj", &buf));
  ASSERT_EQ(data.size(), buf.len);
  EXPECT_EQ(0, memcmp(data.data(), buf.data, buf.len));
  rgw_free_buffer(&buf);
}

TEST_F(SALWrapperTest, PutZeroLength) {
  ASSERT_EQ(0, put_tracked("test/empty-obj", nullptr, 0));

  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/empty-obj", &meta));
  EXPECT_EQ(0u, meta.size);
  rgw_free_object_meta(&meta);
}

TEST_F(SALWrapperTest, PutOverwrite) {
  std::string data1 = "first version";
  std::string data2 = "second version, different length";
  ASSERT_EQ(0, put_str_tracked("test/overwrite", data1));
  ASSERT_EQ(0, put_str("test/overwrite", data2));

  CRgwBuffer buf{};
  ASSERT_EQ(0, get("test/overwrite", &buf));
  ASSERT_EQ(data2.size(), buf.len);
  EXPECT_EQ(0, memcmp(data2.data(), buf.data, buf.len));
  rgw_free_buffer(&buf);
}

TEST_F(SALWrapperTest, PutGetMultipleObjects) {
  const int count = 10;
  for (int i = 0; i < count; i++) {
    std::string key = "test/multi/obj-" + std::to_string(i);
    std::string val = "value-" + std::to_string(i);
    ASSERT_EQ(0, put_str_tracked(key.c_str(), val));
  }

  for (int i = 0; i < count; i++) {
    std::string key = "test/multi/obj-" + std::to_string(i);
    std::string expected = "value-" + std::to_string(i);
    CRgwBuffer buf{};
    ASSERT_EQ(0, get(key.c_str(), &buf));
    ASSERT_EQ(expected.size(), buf.len);
    EXPECT_EQ(0, memcmp(expected.data(), buf.data, buf.len));
    rgw_free_buffer(&buf);
  }
}


// --------------- Put/Get with Attributes ---------------

TEST_F(SALWrapperTest, PutObjectWithAttributes) {
  std::string data = "object with attributes";
  CRgwObject obj{"test/put-with-attrs", nullptr};
  CRgwBuffer buf{reinterpret_cast<uint8_t*>(const_cast<char*>(data.data())), data.size()};
  
  // Set attributes
  CRgwObjectMeta attrs{};
  attrs.content_type = const_cast<char*>("text/plain");
  attrs.cache_control = const_cast<char*>("max-age=3600");
  attrs.content_encoding = const_cast<char*>("gzip");
  attrs.metadata = const_cast<char*>("{\"user-key\":\"user-value\",\"x-custom\":\"test\"}");
  
  char* etag = nullptr;
  ASSERT_EQ(0, rgw_put_object(driver(), dpp(), nullptr, &bucket_, &obj, &buf, &attrs, &etag));
  ASSERT_NE(etag, nullptr);
  created_keys_.emplace_back("test/put-with-attrs");
  
  // Verify attributes were set
  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/put-with-attrs", &meta));
  
  ASSERT_NE(meta.content_type, nullptr);
  EXPECT_STREQ("text/plain", meta.content_type);
  
  ASSERT_NE(meta.cache_control, nullptr);
  EXPECT_STREQ("max-age=3600", meta.cache_control);
  
  ASSERT_NE(meta.content_encoding, nullptr);
  EXPECT_STREQ("gzip", meta.content_encoding);
  
  ASSERT_NE(meta.metadata, nullptr);
  std::string metadata_str(meta.metadata);
  EXPECT_TRUE(metadata_str.find("user-key") != std::string::npos);
  EXPECT_TRUE(metadata_str.find("user-value") != std::string::npos);
  
  rgw_free_object_meta(&meta);
  free(etag);
}

TEST_F(SALWrapperTest, PutObjectWithContentType) {
  std::string data = "JSON content";
  CRgwObject obj{"test/put-json", nullptr};
  CRgwBuffer buf{reinterpret_cast<uint8_t*>(const_cast<char*>(data.data())), data.size()};
  
  CRgwObjectMeta attrs{};
  attrs.content_type = const_cast<char*>("application/json");
  
  char* etag = nullptr;
  ASSERT_EQ(0, rgw_put_object(driver(), dpp(), nullptr, &bucket_, &obj, &buf, &attrs, &etag));
  created_keys_.emplace_back("test/put-json");
  
  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/put-json", &meta));
  ASSERT_NE(meta.content_type, nullptr);
  EXPECT_STREQ("application/json", meta.content_type);
  
  rgw_free_object_meta(&meta);
  free(etag);
}

TEST_F(SALWrapperTest, PutObjectWithCustomMetadata) {
  std::string data = "data with custom metadata";
  CRgwObject obj{"test/put-custom-meta", nullptr};
  CRgwBuffer buf{reinterpret_cast<uint8_t*>(const_cast<char*>(data.data())), data.size()};
  
  CRgwObjectMeta attrs{};
  attrs.metadata = const_cast<char*>("{\"x-amz-meta-author\":\"test-user\",\"x-amz-meta-version\":\"1.0\"}");
  
  char* etag = nullptr;
  ASSERT_EQ(0, rgw_put_object(driver(), dpp(), nullptr, &bucket_, &obj, &buf, &attrs, &etag));
  created_keys_.emplace_back("test/put-custom-meta");
  
  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/put-custom-meta", &meta));
  ASSERT_NE(meta.metadata, nullptr);
  
  std::string metadata_str(meta.metadata);
  EXPECT_TRUE(metadata_str.find("x-amz-meta-author") != std::string::npos);
  EXPECT_TRUE(metadata_str.find("test-user") != std::string::npos);
  EXPECT_TRUE(metadata_str.find("x-amz-meta-version") != std::string::npos);
  EXPECT_TRUE(metadata_str.find("1.0") != std::string::npos);
  
  rgw_free_object_meta(&meta);
  free(etag);
}

TEST_F(SALWrapperTest, PutObjectAttributesOverwrite) {
  std::string data = "original data";
  CRgwObject obj{"test/attrs-overwrite", nullptr};
  CRgwBuffer buf{reinterpret_cast<uint8_t*>(const_cast<char*>(data.data())), data.size()};
  
  // First put with initial attributes
  CRgwObjectMeta attrs1{};
  attrs1.content_type = const_cast<char*>("text/plain");
  attrs1.cache_control = const_cast<char*>("max-age=1800");
  
  char* etag1 = nullptr;
  ASSERT_EQ(0, rgw_put_object(driver(), dpp(), nullptr, &bucket_, &obj, &buf, &attrs1, &etag1));
  created_keys_.emplace_back("test/attrs-overwrite");
  
  // Second put with different attributes
  std::string data2 = "updated data";
  CRgwBuffer buf2{reinterpret_cast<uint8_t*>(const_cast<char*>(data2.data())), data2.size()};
  CRgwObjectMeta attrs2{};
  attrs2.content_type = const_cast<char*>("application/json");
  attrs2.cache_control = const_cast<char*>("max-age=7200");
  
  char* etag2 = nullptr;
  ASSERT_EQ(0, rgw_put_object(driver(), dpp(), nullptr, &bucket_, &obj, &buf2, &attrs2, &etag2));
  
  // Verify new attributes replaced old ones
  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/attrs-overwrite", &meta));
  
  ASSERT_NE(meta.content_type, nullptr);
  EXPECT_STREQ("application/json", meta.content_type);
  
  ASSERT_NE(meta.cache_control, nullptr);
  EXPECT_STREQ("max-age=7200", meta.cache_control);
  
  rgw_free_object_meta(&meta);
  free(etag1);
  free(etag2);
}

TEST_F(SALWrapperTest, GetObjectPreservesAttributes) {
  std::string data = "data to retrieve";
  CRgwObject obj{"test/get-with-attrs", nullptr};
  CRgwBuffer put_buf{reinterpret_cast<uint8_t*>(const_cast<char*>(data.data())), data.size()};
  
  CRgwObjectMeta attrs{};
  attrs.content_type = const_cast<char*>("text/html");
  attrs.content_encoding = const_cast<char*>("deflate");
  
  char* etag = nullptr;
  ASSERT_EQ(0, rgw_put_object(driver(), dpp(), nullptr, &bucket_, &obj, &put_buf, &attrs, &etag));
  created_keys_.emplace_back("test/get-with-attrs");
  
  // Get the object
  CRgwBuffer get_buf{};
  ASSERT_EQ(0, get("test/get-with-attrs", &get_buf));
  ASSERT_EQ(data.size(), get_buf.len);
  EXPECT_EQ(0, memcmp(data.data(), get_buf.data, get_buf.len));
  
  // Verify attributes are still present via head
  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/get-with-attrs", &meta));
  
  ASSERT_NE(meta.content_type, nullptr);
  EXPECT_STREQ("text/html", meta.content_type);
  
  ASSERT_NE(meta.content_encoding, nullptr);
  EXPECT_STREQ("deflate", meta.content_encoding);
  
  rgw_free_buffer(&get_buf);
  rgw_free_object_meta(&meta);
  free(etag);
}

TEST_F(SALWrapperTest, PutObjectWithNullAttributes) {
  std::string data = "data without attributes";
  CRgwObject obj{"test/put-null-attrs", nullptr};
  CRgwBuffer buf{reinterpret_cast<uint8_t*>(const_cast<char*>(data.data())), data.size()};
  
  // Put with null attributes pointer (should succeed)
  char* etag = nullptr;
  ASSERT_EQ(0, rgw_put_object(driver(), dpp(), nullptr, &bucket_, &obj, &buf, nullptr, &etag));
  created_keys_.emplace_back("test/put-null-attrs");
  
  // Verify object exists and has default/no attributes
  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/put-null-attrs", &meta));
  EXPECT_EQ(data.size(), meta.size);
  
  rgw_free_object_meta(&meta);
  free(etag);
}


// --------------- Get edge cases ---------------

TEST_F(SALWrapperTest, GetNonExistent) {
  CRgwBuffer buf{};
  EXPECT_EQ(-ENOENT, get("test/no-such-key-ever", &buf));
}

TEST_F(SALWrapperTest, RangeRead) {
  std::vector<uint8_t> data(512);
  std::iota(data.begin(), data.end(), 0);
  ASSERT_EQ(0, put_tracked("test/range-read", data.data(), data.size()));

  CRgwBuffer buf{};
  ASSERT_EQ(0, get("test/range-read", &buf, 100, 200));
  ASSERT_EQ(200u, buf.len);
  EXPECT_EQ(0, memcmp(data.data() + 100, buf.data, buf.len));
  rgw_free_buffer(&buf);
}

TEST_F(SALWrapperTest, RangeReadFromStart) {
  std::string data = "range-read-from-start-test-data";
  ASSERT_EQ(0, put_str_tracked("test/range-start", data));

  CRgwBuffer buf{};
  ASSERT_EQ(0, get("test/range-start", &buf, 0, 10));
  ASSERT_EQ(10u, buf.len);
  EXPECT_EQ(0, memcmp(data.data(), buf.data, 10));
  rgw_free_buffer(&buf);
}

TEST_F(SALWrapperTest, RangeReadToEnd) {
  std::string data = "read-to-end-data-here";
  ASSERT_EQ(0, put_str_tracked("test/range-end", data));

  CRgwBuffer buf{};
  ASSERT_EQ(0, get("test/range-end", &buf, 5, UINT64_MAX));
  ASSERT_EQ(data.size() - 5, buf.len);
  EXPECT_EQ(0, memcmp(data.data() + 5, buf.data, buf.len));
  rgw_free_buffer(&buf);
}

TEST_F(SALWrapperTest, RangeReadBeyondObjectSize) {
  std::string data = "short-data";
  ASSERT_EQ(0, put_str_tracked("test/range-beyond", data));

  CRgwBuffer buf{};
  EXPECT_EQ(-ERANGE, get("test/range-beyond", &buf, data.size(), 10));
  EXPECT_EQ(nullptr, buf.data);
  EXPECT_EQ(0u, buf.len);

  EXPECT_EQ(-ERANGE, get("test/range-beyond", &buf, data.size() + 100, 10));
  EXPECT_EQ(nullptr, buf.data);
  EXPECT_EQ(0u, buf.len);
}

// --------------- Conditional get ---------------

TEST_F(SALWrapperTest, ConditionalGetIfMatch) {
  std::string data = "conditional get data";
  ASSERT_EQ(0, put_str_tracked("test/cond-get", data));

  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/cond-get", &meta));
  ASSERT_NE(meta.etag, nullptr);
  std::string etag = meta.etag;
  rgw_free_object_meta(&meta);

  // if_match with correct etag — should succeed
  CRgwBuffer buf{};
  CRgwObject obj{"test/cond-get", nullptr};
  ASSERT_EQ(0, rgw_get_object_conditional(driver(), dpp(), nullptr,
             &bucket_, &obj, 0, UINT64_MAX,
             etag.c_str(), nullptr, nullptr, nullptr, &buf));
  ASSERT_EQ(data.size(), buf.len);
  EXPECT_EQ(0, memcmp(data.data(), buf.data, buf.len));
  rgw_free_buffer(&buf);

  // if_match with wrong etag — should fail with precondition
  CRgwBuffer buf2{};
  EXPECT_EQ(-ERR_PRECONDITION_FAILED,
            rgw_get_object_conditional(driver(), dpp(), nullptr,
             &bucket_, &obj, 0, UINT64_MAX,
             "wrong-etag", nullptr, nullptr, nullptr, &buf2));
}

TEST_F(SALWrapperTest, ConditionalGetIfNoneMatch) {
  std::string data = "none-match data";
  ASSERT_EQ(0, put_str_tracked("test/cond-get-nm", data));

  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/cond-get-nm", &meta));
  ASSERT_NE(meta.etag, nullptr);
  std::string etag = meta.etag;
  rgw_free_object_meta(&meta);

  CRgwObject obj{"test/cond-get-nm", nullptr};

  // if_nomatch with matching etag — should fail with not modified
  CRgwBuffer buf{};
  EXPECT_EQ(-ERR_NOT_MODIFIED,
            rgw_get_object_conditional(driver(), dpp(), nullptr,
             &bucket_, &obj, 0, UINT64_MAX,
             nullptr, etag.c_str(), nullptr, nullptr, &buf));

  // if_nomatch with different etag — should succeed
  CRgwBuffer buf2{};
  ASSERT_EQ(0, rgw_get_object_conditional(driver(), dpp(), nullptr,
             &bucket_, &obj, 0, UINT64_MAX,
             nullptr, "different-etag", nullptr, nullptr, &buf2));
  ASSERT_EQ(data.size(), buf2.len);
  rgw_free_buffer(&buf2);
}

TEST_F(SALWrapperTest, ConditionalGetIfModifiedSince) {
  std::string data = "modified since data";
  ASSERT_EQ(0, put_str_tracked("test/cond-get-mod", data));

  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/cond-get-mod", &meta));
  int64_t mtime = meta.last_modified;
  rgw_free_object_meta(&meta);
  ASSERT_GT(mtime, 0);

  CRgwObject obj{"test/cond-get-mod", nullptr};

  // if_modified_since far in the past — object is newer, should succeed
  int64_t past = mtime - 3600;
  CRgwBuffer buf{};
  ASSERT_EQ(0, rgw_get_object_conditional(driver(), dpp(), nullptr,
             &bucket_, &obj, 0, UINT64_MAX,
             nullptr, nullptr, &past, nullptr, &buf));
  ASSERT_EQ(data.size(), buf.len);
  rgw_free_buffer(&buf);

  // if_modified_since in the future — object is older, should fail
  int64_t future = mtime + 3600;
  CRgwBuffer buf2{};
  EXPECT_EQ(-ERR_NOT_MODIFIED,
            rgw_get_object_conditional(driver(), dpp(), nullptr,
             &bucket_, &obj, 0, UINT64_MAX,
             nullptr, nullptr, &future, nullptr, &buf2));
}

TEST_F(SALWrapperTest, ConditionalGetIfUnmodifiedSince) {
  std::string data = "unmodified since data";
  ASSERT_EQ(0, put_str_tracked("test/cond-get-unmod", data));

  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/cond-get-unmod", &meta));
  int64_t mtime = meta.last_modified;
  rgw_free_object_meta(&meta);
  ASSERT_GT(mtime, 0);

  CRgwObject obj{"test/cond-get-unmod", nullptr};

  // if_unmodified_since in the future — object is older, should succeed
  int64_t future = mtime + 3600;
  CRgwBuffer buf{};
  ASSERT_EQ(0, rgw_get_object_conditional(driver(), dpp(), nullptr,
             &bucket_, &obj, 0, UINT64_MAX,
             nullptr, nullptr, nullptr, &future, &buf));
  ASSERT_EQ(data.size(), buf.len);
  rgw_free_buffer(&buf);

  // if_unmodified_since far in the past — object is newer, should fail
  int64_t past = mtime - 3600;
  CRgwBuffer buf2{};
  EXPECT_EQ(-ERR_PRECONDITION_FAILED,
            rgw_get_object_conditional(driver(), dpp(), nullptr,
             &bucket_, &obj, 0, UINT64_MAX,
             nullptr, nullptr, nullptr, &past, &buf2));
}

// --------------- Head / Metadata ---------------

TEST_F(SALWrapperTest, HeadObject) {
  std::string data = "metadata test data";
  ASSERT_EQ(0, put_str_tracked("test/head-obj", data));

  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/head-obj", &meta));
  EXPECT_EQ(data.size(), meta.size);
  EXPECT_NE(meta.etag, nullptr);
  if (meta.etag) {
    EXPECT_GT(strlen(meta.etag), 0u);
  }
  EXPECT_GT(meta.last_modified, 0);
  rgw_free_object_meta(&meta);
}

TEST_F(SALWrapperTest, HeadNonExistent) {
  CRgwObjectMeta meta{};
  EXPECT_EQ(-ENOENT, head("test/head-nonexistent", &meta));
  rgw_free_object_meta(&meta);
}

TEST_F(SALWrapperTest, EtagChangesOnOverwrite) {
  ASSERT_EQ(0, put_str_tracked("test/etag-change", "version1"));

  CRgwObjectMeta meta1{};
  ASSERT_EQ(0, head("test/etag-change", &meta1));

  ASSERT_EQ(0, put_str("test/etag-change", "version2-different"));

  CRgwObjectMeta meta2{};
  ASSERT_EQ(0, head("test/etag-change", &meta2));

  ASSERT_NE(meta1.etag, nullptr);
  ASSERT_NE(meta2.etag, nullptr);
  EXPECT_STRNE(meta1.etag, meta2.etag);

  rgw_free_object_meta(&meta1);
  rgw_free_object_meta(&meta2);
}

TEST_F(SALWrapperTest, HeadReflectsCorrectSize) {
  std::vector<size_t> sizes = {1, 100, 512, 999};
  for (size_t sz : sizes) {
    std::string key = "test/size-" + std::to_string(sz);
    std::vector<uint8_t> data(sz, 'x');
    ASSERT_EQ(0, put_tracked(key.c_str(), data.data(), data.size()));

    CRgwObjectMeta meta{};
    ASSERT_EQ(0, head(key.c_str(), &meta));
    EXPECT_EQ(sz, meta.size) << "size mismatch for " << key;
    rgw_free_object_meta(&meta);
  }
}

// --------------- Delete ---------------

TEST_F(SALWrapperTest, DeleteObject) {
  std::string data = "to-be-deleted";
  ASSERT_EQ(0, put_str("test/delete-me", data));

  ASSERT_EQ(0, del("test/delete-me"));

  CRgwObjectMeta meta{};
  EXPECT_EQ(-ENOENT, head("test/delete-me", &meta));
  rgw_free_object_meta(&meta);
}

TEST_F(SALWrapperTest, DeleteNonExistent) {
  EXPECT_EQ(0, del("test/already-gone"));
}

TEST_F(SALWrapperTest, DeleteThenPut) {
  std::string data1 = "original";
  ASSERT_EQ(0, put_str("test/delete-reput", data1));
  ASSERT_EQ(0, del("test/delete-reput"));

  std::string data2 = "rewritten after delete";
  ASSERT_EQ(0, put_str_tracked("test/delete-reput", data2));

  CRgwBuffer buf{};
  ASSERT_EQ(0, get("test/delete-reput", &buf));
  ASSERT_EQ(data2.size(), buf.len);
  EXPECT_EQ(0, memcmp(data2.data(), buf.data, buf.len));
  rgw_free_buffer(&buf);
}

TEST_F(SALWrapperTest, BulkDelete) {
  const int count = 5;
  std::vector<std::string> keys;
  std::vector<const char*> key_ptrs;
  for (int i = 0; i < count; i++) {
    keys.push_back("test/bulk-del/obj-" + std::to_string(i));
    ASSERT_EQ(0, put_str(keys.back().c_str(), "data"));
  }
  for (auto& k : keys) key_ptrs.push_back(k.c_str());

  ASSERT_EQ(0, rgw_delete_objects(driver(), dpp(), nullptr, &bucket_,
             key_ptrs.data(), key_ptrs.size()));

  for (auto& k : keys) {
    CRgwObjectMeta meta{};
    EXPECT_EQ(-ENOENT, head(k.c_str(), &meta));
    rgw_free_object_meta(&meta);
  }
}

// --------------- List ---------------

TEST_F(SALWrapperTest, ListObjects) {
  // write each object with a distinct size so the size reported by list is
  // meaningful to verify (not all identical).
  std::map<std::string, uint64_t> expected;  // key -> size
  for (int i = 0; i < 5; i++) {
    std::string key = "test/list/obj-" + std::to_string(i);
    std::string data(i + 1, 'x');
    ASSERT_EQ(0, put_str_tracked(key.c_str(), data));
    expected[key] = data.size();
  }

  CRgwListResult result{};
  ASSERT_EQ(0, rgw_list_objects(driver(), dpp(), nullptr, &bucket_,
             "test/list/", nullptr, nullptr, 100, &result));
  EXPECT_GE(result.count, 5u);

  size_t matched = 0;
  for (size_t i = 0; i < result.count; i++) {
    ASSERT_NE(result.entries[i].key, nullptr);
    EXPECT_GT(strlen(result.entries[i].key), 0u);

    auto it = expected.find(result.entries[i].key);
    if (it == expected.end()) continue;  // ignore entries from other tests
    matched++;

    // size must match exactly what we wrote
    EXPECT_EQ(it->second, result.entries[i].size)
        << "size mismatch for " << result.entries[i].key;

    // mtime must be populated with a plausible (non-zero) value
    EXPECT_GT(result.entries[i].last_modified, 0)
        << "missing mtime for " << result.entries[i].key;

    // The list entry carries no etag, so cross-check via head that the object
    // has a valid, non-empty etag and that head agrees with the list on
    // size and mtime.
    CRgwObjectMeta meta{};
    ASSERT_EQ(0, head(result.entries[i].key, &meta));
    EXPECT_EQ(it->second, meta.size);
    EXPECT_EQ(result.entries[i].size, meta.size);
    EXPECT_EQ(result.entries[i].last_modified, meta.last_modified);
    ASSERT_NE(meta.etag, nullptr);
    EXPECT_GT(strlen(meta.etag), 0u);
    rgw_free_object_meta(&meta);
  }
  // every object we created must appear in the listing
  EXPECT_EQ(expected.size(), matched);

  rgw_free_list_result(&result);
}

TEST_F(SALWrapperTest, ListWithDelimiter) {
  ASSERT_EQ(0, put_str_tracked("test/delim/a/1", "d"));
  ASSERT_EQ(0, put_str_tracked("test/delim/a/2", "d"));
  ASSERT_EQ(0, put_str_tracked("test/delim/b/1", "d"));

  CRgwListResult result{};
  ASSERT_EQ(0, rgw_list_objects(driver(), dpp(), nullptr, &bucket_,
             "test/delim/", "/", nullptr, 100, &result));
  EXPECT_GE(result.count, 2u);
  rgw_free_list_result(&result);
}

TEST_F(SALWrapperTest, ListPagination) {
  for (int i = 0; i < 10; i++) {
    std::string key = "test/page/obj-" + std::to_string(i);
    ASSERT_EQ(0, put_str_tracked(key.c_str(), "data"));
  }

  size_t total = 0;
  std::string marker_str;
  bool truncated = true;
  int pages = 0;

  while (truncated && pages < 20) {
    CRgwListResult result{};
    ASSERT_EQ(0, rgw_list_objects(driver(), dpp(), nullptr, &bucket_,
               "test/page/", nullptr,
               marker_str.empty() ? nullptr : marker_str.c_str(),
               3, &result));
    total += result.count;
    truncated = result.is_truncated != 0;
    if (result.next_marker) {
      marker_str = result.next_marker;
    }
    rgw_free_list_result(&result);
    pages++;
  }
  EXPECT_GE(total, 10u);
  EXPECT_FALSE(truncated);
}

TEST_F(SALWrapperTest, ListEmpty) {
  CRgwListResult result{};
  ASSERT_EQ(0, rgw_list_objects(driver(), dpp(), nullptr, &bucket_,
             "no-such-prefix-xyz/", nullptr, nullptr, 100, &result));
  EXPECT_EQ(0u, result.count);
  EXPECT_EQ(0, result.is_truncated);
  rgw_free_list_result(&result);
}

TEST_F(SALWrapperTest, ListReflectsDeletedObjects) {
  for (int i = 0; i < 3; i++) {
    std::string key = "test/list-del/obj-" + std::to_string(i);
    ASSERT_EQ(0, put_str(key.c_str(), "data"));
  }

  ASSERT_EQ(0, del("test/list-del/obj-1"));

  CRgwListResult result{};
  ASSERT_EQ(0, rgw_list_objects(driver(), dpp(), nullptr, &bucket_,
             "test/list-del/", nullptr, nullptr, 100, &result));

  bool found_0 = false, found_1 = false, found_2 = false;
  for (size_t i = 0; i < result.count; i++) {
    std::string k = result.entries[i].key;
    if (k == "test/list-del/obj-0") found_0 = true;
    if (k == "test/list-del/obj-1") found_1 = true;
    if (k == "test/list-del/obj-2") found_2 = true;
  }
  EXPECT_TRUE(found_0);
  EXPECT_FALSE(found_1);
  EXPECT_TRUE(found_2);

  rgw_free_list_result(&result);
  del("test/list-del/obj-0");
  del("test/list-del/obj-2");
}

// --------------- Copy ---------------

// known fail: dbstore — copy_object is a no-op stub
TEST_F(SALWrapperTest, CopyObject) {
  std::string data = "copy me";
  ASSERT_EQ(0, put_str_tracked("test/copy-src", data));

  CRgwObject src{"test/copy-src", nullptr};
  CRgwObject dst{"test/copy-dst", nullptr};
  ASSERT_EQ(0, rgw_copy_object(driver(), dpp(), nullptr,
                                &bucket_, &src, &bucket_, &dst));
  created_keys_.emplace_back("test/copy-dst");

  CRgwBuffer buf{};
  ASSERT_EQ(0, get("test/copy-dst", &buf));
  ASSERT_EQ(data.size(), buf.len);
  EXPECT_EQ(0, memcmp(data.data(), buf.data, buf.len));
  rgw_free_buffer(&buf);
}

// known fail: dbstore — copy_object is a no-op stub
TEST_F(SALWrapperTest, CopyPreservesData) {
  std::vector<uint8_t> data(512);
  std::iota(data.begin(), data.end(), 0);
  ASSERT_EQ(0, put_tracked("test/copy-bin-src", data.data(), data.size()));

  CRgwObject src{"test/copy-bin-src", nullptr};
  CRgwObject dst{"test/copy-bin-dst", nullptr};
  ASSERT_EQ(0, rgw_copy_object(driver(), dpp(), nullptr,
                                &bucket_, &src, &bucket_, &dst));
  created_keys_.emplace_back("test/copy-bin-dst");

  CRgwBuffer buf{};
  ASSERT_EQ(0, get("test/copy-bin-dst", &buf));
  ASSERT_EQ(data.size(), buf.len);
  EXPECT_EQ(0, memcmp(data.data(), buf.data, buf.len));
  rgw_free_buffer(&buf);
}

// known fail: dbstore — copy_object is a no-op stub
TEST_F(SALWrapperTest, CopyNonExistent) {
  CRgwObject src{"test/no-such-copy-src", nullptr};
  CRgwObject dst{"test/copy-dst-fail", nullptr};
  EXPECT_LT(rgw_copy_object(driver(), dpp(), nullptr,
                              &bucket_, &src, &bucket_, &dst), 0);
}

// --------------- Conditional put ---------------

// known fail: dbstore — ignores conditionals; posix — inverted if_nomatch logic
TEST_F(SALWrapperTest, ConditionalPutIfNomatch) {
  std::string data = "conditional data";
  ASSERT_EQ(0, put_str_tracked("test/cond-put", data));

  // put with if_nomatch="*" should be canceled since object already exists
  std::string new_data = "should not overwrite";
  CRgwObject obj{"test/cond-put", nullptr};
  CRgwBuffer buf{reinterpret_cast<uint8_t*>(new_data.data()),
                 new_data.size()};
  int canceled = 0;
  int ret = rgw_put_object_conditional(driver(), dpp(), nullptr,
                                       &bucket_, &obj, &buf,
                                       nullptr, "*", &canceled, nullptr, nullptr);
  ASSERT_EQ(0, ret);
  EXPECT_EQ(1, canceled);

  // original data should be preserved
  CRgwBuffer get_buf{};
  ASSERT_EQ(0, get("test/cond-put", &get_buf));
  ASSERT_EQ(data.size(), get_buf.len);
  EXPECT_EQ(0, memcmp(data.data(), get_buf.data, get_buf.len));
  rgw_free_buffer(&get_buf);
}

// --------------- MaxChunkSize ---------------

TEST_F(SALWrapperTest, MaxChunkSize) {
  uint64_t chunk = rgw_get_max_chunk_size(driver());
  EXPECT_GT(chunk, 0u);
}

// --------------- Key naming edge cases ---------------

TEST_F(SALWrapperTest, SpecialCharacterKeys) {
  std::string data = "special";
  const char* keys[] = {
    "test/special/key with spaces",
    "test/special/key+plus",
    "test/special/key=equals",
    "test/special/deep/nested/path/obj",
  };

  for (auto key : keys) {
    ASSERT_EQ(0, put_str_tracked(key, data)) << "put failed for: " << key;

    CRgwBuffer buf{};
    ASSERT_EQ(0, get(key, &buf)) << "get failed for: " << key;
    ASSERT_EQ(data.size(), buf.len) << "size mismatch for: " << key;
    EXPECT_EQ(0, memcmp(data.data(), buf.data, buf.len));
    rgw_free_buffer(&buf);
  }
}

// --------------- Conditional put — if_match ---------------

// known fail: dbstore, posix — do not enforce conditional write preconditions
TEST_F(SALWrapperTest, ConditionalPutIfMatchSucceeds) {
  std::string data = "original";
  ASSERT_EQ(0, put_str_tracked("test/cond-match", data));

  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/cond-match", &meta));
  ASSERT_NE(meta.etag, nullptr);
  std::string etag = meta.etag;
  rgw_free_object_meta(&meta);

  // put with if_match=current_etag should succeed (not canceled)
  std::string new_data = "updated via if_match";
  CRgwObject obj{"test/cond-match", nullptr};
  CRgwBuffer buf{reinterpret_cast<uint8_t*>(new_data.data()), new_data.size()};
  int canceled = 0;
  ASSERT_EQ(0, rgw_put_object_conditional(driver(), dpp(), nullptr,
                                          &bucket_, &obj, &buf,
                                          etag.c_str(), nullptr, &canceled, nullptr, nullptr));
  EXPECT_EQ(0, canceled);

  CRgwBuffer get_buf{};
  ASSERT_EQ(0, get("test/cond-match", &get_buf));
  ASSERT_EQ(new_data.size(), get_buf.len);
  EXPECT_EQ(0, memcmp(new_data.data(), get_buf.data, get_buf.len));
  rgw_free_buffer(&get_buf);
}

// known fail: dbstore, posix — do not enforce conditional write preconditions
TEST_F(SALWrapperTest, ConditionalPutIfMatchWrongEtag) {
  std::string data = "should stay";
  ASSERT_EQ(0, put_str_tracked("test/cond-match-fail", data));

  // put with if_match=wrong_etag should be canceled
  std::string new_data = "should not appear";
  CRgwObject obj{"test/cond-match-fail", nullptr};
  CRgwBuffer buf{reinterpret_cast<uint8_t*>(new_data.data()), new_data.size()};
  int canceled = 0;
  ASSERT_EQ(0, rgw_put_object_conditional(driver(), dpp(), nullptr,
                                          &bucket_, &obj, &buf,
                                          "bogus-etag", nullptr, &canceled, nullptr, nullptr));
  EXPECT_EQ(1, canceled);

  CRgwBuffer get_buf{};
  ASSERT_EQ(0, get("test/cond-match-fail", &get_buf));
  ASSERT_EQ(data.size(), get_buf.len);
  EXPECT_EQ(0, memcmp(data.data(), get_buf.data, get_buf.len));
  rgw_free_buffer(&get_buf);
}

// --------------- Conditional copy ---------------

// known fail: dbstore — copy_object is a no-op stub
TEST_F(SALWrapperTest, ConditionalCopyIfNomatch) {
  std::string data = "conditional copy source";
  ASSERT_EQ(0, put_str_tracked("test/ccopy-src", data));

  // copy to a new destination with if_nomatch="*" — should succeed
  CRgwObject src{"test/ccopy-src", nullptr};
  CRgwObject dst{"test/ccopy-dst-new", nullptr};
  ASSERT_EQ(0, rgw_copy_object_conditional(driver(), dpp(), nullptr,
                                           &bucket_, &src, &bucket_, &dst,
                                           nullptr, "*"));
  created_keys_.emplace_back("test/ccopy-dst-new");

  CRgwBuffer buf{};
  ASSERT_EQ(0, get("test/ccopy-dst-new", &buf));
  ASSERT_EQ(data.size(), buf.len);
  EXPECT_EQ(0, memcmp(data.data(), buf.data, buf.len));
  rgw_free_buffer(&buf);
}

// known fail: dbstore — copy_object is a no-op stub
TEST_F(SALWrapperTest, ConditionalCopyIfNomatchExists) {
  ASSERT_EQ(0, put_str_tracked("test/ccopy-src2", "source data"));
  ASSERT_EQ(0, put_str_tracked("test/ccopy-dst-exists", "existing data"));

  // copy with if_nomatch="*" to an existing destination — should fail
  CRgwObject src{"test/ccopy-src2", nullptr};
  CRgwObject dst{"test/ccopy-dst-exists", nullptr};
  int ret = rgw_copy_object_conditional(driver(), dpp(), nullptr,
                                        &bucket_, &src, &bucket_, &dst,
                                        nullptr, "*");
  EXPECT_LT(ret, 0);

  // existing destination data should be unchanged
  CRgwBuffer buf{};
  ASSERT_EQ(0, get("test/ccopy-dst-exists", &buf));
  std::string expected = "existing data";
  ASSERT_EQ(expected.size(), buf.len);
  EXPECT_EQ(0, memcmp(expected.data(), buf.data, buf.len));
  rgw_free_buffer(&buf);
}

// --------------- Multipart upload ---------------

// known fail: dbstore — does not support multi-chunk reads needed to verify multipart
TEST_F(SALWrapperTest, MultipartBasic) {
  CRgwObject obj{"test/multipart-obj", nullptr};
  char* upload_id = nullptr;

  int ret = rgw_multipart_init(driver(), dpp(), nullptr, &bucket_, &obj, nullptr, &upload_id);
  ASSERT_EQ(0, ret);
  ASSERT_NE(upload_id, nullptr);
  EXPECT_GT(strlen(upload_id), 0u);

  // part 1: 5MB (minimum for non-last parts)
  const size_t part_size = 5 * 1024 * 1024;
  std::vector<uint8_t> part1_data(part_size, 'A');
  // part 2: small last part (no minimum)
  std::vector<uint8_t> part2_data(1024, 'B');

  char* etag1 = nullptr;
  ret = rgw_multipart_put_part(driver(), dpp(), nullptr, &bucket_, &obj,
                               upload_id, 1,
                               part1_data.data(), part1_data.size(), &etag1);
  ASSERT_EQ(0, ret);
  ASSERT_NE(etag1, nullptr);

  char* etag2 = nullptr;
  ret = rgw_multipart_put_part(driver(), dpp(), nullptr, &bucket_, &obj,
                               upload_id, 2,
                               part2_data.data(), part2_data.size(), &etag2);
  ASSERT_EQ(0, ret);
  ASSERT_NE(etag2, nullptr);

  const char* etags[] = {etag1, etag2};
  ret = rgw_multipart_complete(driver(), dpp(), nullptr, &bucket_, &obj,
                               upload_id, etags, 2);
  ASSERT_EQ(0, ret);
  created_keys_.emplace_back("test/multipart-obj");

  // verify the assembled object is readable and has correct total size
  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/multipart-obj", &meta));
  EXPECT_EQ(part1_data.size() + part2_data.size(), meta.size);
  rgw_free_object_meta(&meta);

  free(upload_id);
  free(etag1);
  free(etag2);
}

TEST_F(SALWrapperTest, MultipartAbort) {
  CRgwObject obj{"test/multipart-abort", nullptr};
  char* upload_id = nullptr;

  ASSERT_EQ(0, rgw_multipart_init(driver(), dpp(), nullptr,
                                  &bucket_, &obj, nullptr, &upload_id));
  ASSERT_NE(upload_id, nullptr);

  // upload one part
  std::string part_data = "part that will be aborted";
  char* etag = nullptr;
  ASSERT_EQ(0, rgw_multipart_put_part(driver(), dpp(), nullptr, &bucket_, &obj,
                                      upload_id, 1,
                                      reinterpret_cast<const uint8_t*>(part_data.data()),
                                      part_data.size(), &etag));

  // abort the upload
  ASSERT_EQ(0, rgw_multipart_abort(driver(), dpp(), nullptr,
                                   &bucket_, &obj, upload_id));

  // the final object should NOT exist
  CRgwObjectMeta meta{};
  EXPECT_EQ(-ENOENT, head("test/multipart-abort", &meta));
  rgw_free_object_meta(&meta);

  free(upload_id);
  free(etag);
}

TEST_F(SALWrapperTest, MultipartInvalidUploadId) {
  CRgwObject obj{"test/multipart-bad-id", nullptr};
  std::string data = "data";
  char* etag = nullptr;

  int ret = rgw_multipart_put_part(driver(), dpp(), nullptr, &bucket_, &obj,
                                   "nonexistent-upload-id", 1,
                                   reinterpret_cast<const uint8_t*>(data.data()),
                                   data.size(), &etag);
  EXPECT_LT(ret, 0);

  const char* etags[] = {"fake"};
  ret = rgw_multipart_complete(driver(), dpp(), nullptr, &bucket_, &obj,
                               "nonexistent-upload-id", etags, 1);
  EXPECT_LT(ret, 0);
}

TEST_F(SALWrapperTest, MultipartSinglePart) {
  CRgwObject obj{"test/multipart-single", nullptr};
  char* upload_id = nullptr;

  ASSERT_EQ(0, rgw_multipart_init(driver(), dpp(), nullptr,
                                  &bucket_, &obj, nullptr, &upload_id));
  ASSERT_NE(upload_id, nullptr);

  std::string data = "single part upload content";
  char* etag = nullptr;
  ASSERT_EQ(0, rgw_multipart_put_part(driver(), dpp(), nullptr, &bucket_, &obj,
                                      upload_id, 1,
                                      reinterpret_cast<const uint8_t*>(data.data()),
                                      data.size(), &etag));
  ASSERT_NE(etag, nullptr);

  const char* etags[] = {etag};
  ASSERT_EQ(0, rgw_multipart_complete(driver(), dpp(), nullptr, &bucket_, &obj,
                                      upload_id, etags, 1));
  created_keys_.emplace_back("test/multipart-single");

  CRgwBuffer buf{};
  ASSERT_EQ(0, get("test/multipart-single", &buf));
  EXPECT_GT(buf.len, 0u);
  rgw_free_buffer(&buf);

  free(upload_id);
  free(etag);
}


// Test that multipart complete handles concurrent operations safely with locking
TEST_F(SALWrapperTest, MultipartCompleteConcurrentSafety) {
  CRgwObject obj{"test/multipart-concurrent", nullptr};
  
  char* upload_id = nullptr;
  int ret = rgw_multipart_init(driver(), dpp(), nullptr, &bucket_, &obj, 
                               nullptr, &upload_id);
  ASSERT_EQ(0, ret);
  ASSERT_NE(upload_id, nullptr);

  // Upload a single part
  std::string data = "multipart data for concurrent test";
  char* etag = nullptr;
  ret = rgw_multipart_put_part(driver(), dpp(), nullptr, &bucket_, &obj,
                               upload_id, 1,
                               reinterpret_cast<const uint8_t*>(data.data()),
                               data.size(), &etag);
  ASSERT_EQ(0, ret);
  ASSERT_NE(etag, nullptr);

  // Spawn multiple threads to complete the same upload concurrently
  const int num_threads = 5;
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};
  std::atomic<int> error_count{0};
  const char* etags[] = {etag};
  
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back([&]() {
      int result = rgw_multipart_complete(driver(), dpp(), nullptr, &bucket_, &obj,
                                         upload_id, etags, 1);
      if (result == 0) {
        success_count++;
      } else {
        error_count++;
      }
    });
  }
  
  // Wait for all threads to complete
  for (auto& t : threads) {
    t.join();
  }
  
  // Only one thread should succeed, others should fail gracefully
  EXPECT_EQ(1, success_count.load()) << "Expected exactly one successful completion";
  EXPECT_EQ(num_threads - 1, error_count.load()) << "Expected other threads to fail";
  
  created_keys_.emplace_back("test/multipart-concurrent");

  // Verify the object was created successfully
  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/multipart-concurrent", &meta));
  EXPECT_EQ(data.size(), meta.size);
  
  rgw_free_object_meta(&meta);
  free(upload_id);
  free(etag);
}

// Test that multipart complete is idempotent (can be called multiple times)
TEST_F(SALWrapperTest, MultipartCompleteIdempotent) {
  CRgwObject obj{"test/multipart-idempotent", nullptr};
  char* upload_id = nullptr;

  ASSERT_EQ(0, rgw_multipart_init(driver(), dpp(), nullptr,
                                  &bucket_, &obj, nullptr, &upload_id));
  ASSERT_NE(upload_id, nullptr);

  // Upload one part
  std::string data = "idempotent test data";
  char* etag = nullptr;
  ASSERT_EQ(0, rgw_multipart_put_part(driver(), dpp(), nullptr, &bucket_, &obj,
                                      upload_id, 1,
                                      reinterpret_cast<const uint8_t*>(data.data()),
                                      data.size(), &etag));
  ASSERT_NE(etag, nullptr);

  // Complete the upload
  const char* etags[] = {etag};
  ASSERT_EQ(0, rgw_multipart_complete(driver(), dpp(), nullptr, &bucket_, &obj,
                                      upload_id, etags, 1));
  created_keys_.emplace_back("test/multipart-idempotent");

  // Try to complete again with the same upload_id - should fail gracefully
  // The lock mechanism should prevent racing completions
  int ret = rgw_multipart_complete(driver(), dpp(), nullptr, &bucket_, &obj,
                                   upload_id, etags, 1);
  // Expect either success (idempotent) or specific error indicating upload doesn't exist
  EXPECT_TRUE(ret == -ERR_NO_SUCH_UPLOAD);

  // Verify the object still exists and is correct
  CRgwBuffer buf{};
  ASSERT_EQ(0, get("test/multipart-idempotent", &buf));
  ASSERT_EQ(data.size(), buf.len);
  EXPECT_EQ(0, memcmp(data.data(), buf.data, buf.len));
  rgw_free_buffer(&buf);

  free(upload_id);
  free(etag);
}

// Test multipart with custom metadata
TEST_F(SALWrapperTest, MultipartWithCustomMetadata) {
  CRgwObject obj{"test/multipart-custom-meta", nullptr};
  
  // Create attributes with custom metadata (JSON format)
  CRgwObjectMeta init_attrs{};
  init_attrs.content_type = const_cast<char*>("text/plain");
  init_attrs.metadata = const_cast<char*>("{\"user-key\":\"user-value\",\"x-custom\":\"test\"}");
  
  char* upload_id = nullptr;
  int ret = rgw_multipart_init(driver(), dpp(), nullptr, &bucket_, &obj, 
                               &init_attrs, &upload_id);
  ASSERT_EQ(0, ret);
  ASSERT_NE(upload_id, nullptr);

  // Upload a part
  std::string data = "data with custom metadata";
  char* etag = nullptr;
  ret = rgw_multipart_put_part(driver(), dpp(), nullptr, &bucket_, &obj,
                               upload_id, 1,
                               reinterpret_cast<const uint8_t*>(data.data()),
                               data.size(), &etag);
  ASSERT_EQ(0, ret);
  ASSERT_NE(etag, nullptr);

  // Complete the upload
  const char* etags[] = {etag};
  ret = rgw_multipart_complete(driver(), dpp(), nullptr, &bucket_, &obj,
                               upload_id, etags, 1);
  ASSERT_EQ(0, ret);
  created_keys_.emplace_back("test/multipart-custom-meta");

  // Verify metadata was preserved
  CRgwObjectMeta meta{};
  ASSERT_EQ(0, head("test/multipart-custom-meta", &meta));
  
  ASSERT_NE(meta.content_type, nullptr);
  EXPECT_STREQ("text/plain", meta.content_type);
  
  // Custom metadata should be preserved
  ASSERT_NE(meta.metadata, nullptr);
  std::string metadata_str(meta.metadata);
  EXPECT_TRUE(metadata_str.find("user-key") != std::string::npos);
  EXPECT_TRUE(metadata_str.find("user-value") != std::string::npos);
  
  rgw_free_object_meta(&meta);
  free(upload_id);
  free(etag);
}


// ===========================================================================
// main() — bootstrap SAL driver
// ===========================================================================

int main(int argc, char** argv) {
  // ceph.conf is found via -c flag, $CEPH_CONF, or default paths.
  // When running under teuthology, the default path is used.
  auto args = argv_to_vec(argc, const_cast<const char**>(argv));

  auto cct = rgw_global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                             CODE_ENVIRONMENT_UTILITY, 0);

  // region -> zonegroup conversion (must happen before common_init_finish)
  if (!g_conf()->rgw_region.empty() && g_conf()->rgw_zonegroup.empty()) {
    g_conf().set_val_or_die("rgw_zonegroup", g_conf()->rgw_region.c_str());
  }

  common_init_finish(g_ceph_context);

  static TestEnv env;
  g_env = &env;

  static NoDoutPrefix nodpp(g_ceph_context, ceph_subsys_rgw);
  env.dpp = &nodpp;

  // io_context_pool provides worker threads needed by RADOS async ops
  static ceph::async::io_context_pool context_pool{
    cct->_conf->rgw_thread_pool_size};

  // backend is read from ceph.conf via rgw_backend_store
  DriverManager::Config cfg = DriverManager::get_config(true, g_ceph_context);
  env.backend = cfg.store_name;
  std::cerr << "INFO: using backend '" << cfg.store_name << "'" << std::endl;

  // create config store
  auto config_store_type = g_conf().get_val<std::string>("rgw_config_store");
  env.cfgstore = DriverManager::create_config_store(env.dpp, config_store_type);
  if (!env.cfgstore) {
    std::cerr << "ERROR: failed to create config store" << std::endl;
    return 1;
  }

  // load site config
  rgw::SiteConfig site;
  int r = site.load(env.dpp, null_yield, env.cfgstore.get());
  if (r < 0) {
    std::cerr << "ERROR: failed to load site config (r=" << r << ")" << std::endl;
    return 1;
  }

  // initialize SAL driver with all background threads disabled
  env.driver = DriverManager::get_storage(env.dpp,
                                          g_ceph_context,
                                          cfg,
                                          context_pool,
                                          site,
                                          false,  // use_gc_thread
                                          false,  // use_lc_thread
                                          false,  // use_restore_thread
                                          false,  // quota_threads
                                          false,  // run_sync_thread
                                          false,  // run_reshard_thread
                                          false,  // run_notification_thread
                                          false,  // run_bucket_logging_thread
                                          false,  // background_tasks
                                          null_yield,
                                          env.cfgstore.get(),
                                          false); // use_cache
  if (!env.driver) {
    std::cerr << "ERROR: failed to initialize SAL driver" << std::endl;
    return 1;
  }

  // create test bucket via SAL API with proper zonegroup + placement
  env.bucket_name = "sal-wrapper-test-" + std::to_string(getpid());
  {
    rgw_bucket b;
    b.name = env.bucket_name;

    std::unique_ptr<rgw::sal::Bucket> bucket;
    r = env.driver->load_bucket(env.dpp, b, &bucket, null_yield);

    if (bucket) {
      rgw::sal::Bucket::CreateParams params;
      rgw_user uid{"", "sal-wrapper-test-user"};
      params.owner = uid;
      params.zonegroup_id = site.get_zonegroup().get_id();
      params.placement_rule = site.get_zonegroup().default_placement;
      params.zone_placement = rgw::find_zone_placement(
          env.dpp, site.get_zone_params(), params.placement_rule);

      r = bucket->create(env.dpp, params, null_yield);
      if (r < 0 && r != -EEXIST) {
        std::cerr << "ERROR: failed to create test bucket (r=" << r << ")" << std::endl;
        env.driver->shutdown();
        DriverManager::close_storage(env.driver);
        return 1;
      }
    } else {
      std::cerr << "ERROR: load_bucket returned no bucket object" << std::endl;
      env.driver->shutdown();
      DriverManager::close_storage(env.driver);
      return 1;
    }
  }

  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();

  // cleanup: delete test bucket and all objects in it
  if (env.driver) {
    rgw_bucket b;
    b.name = env.bucket_name;
    std::unique_ptr<rgw::sal::Bucket> bucket;
    if (env.driver->load_bucket(env.dpp, b, &bucket, null_yield) == 0 && bucket) {
      bucket->remove(env.dpp, true, null_yield);
    }
    env.driver->shutdown();
    DriverManager::close_storage(env.driver);
    env.driver = nullptr;
  }

  return ret;
}
