// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Tests for RGWRandomAccessFile and RGWOutputStream against a live SAL
 * driver.
 *
 * Like ceph_test_rgw_sal_wrapper, this brings up the SAL driver from
 * ceph.conf (-c, $CEPH_CONF or the default paths) and works on a bucket of
 * its own, so it needs a running cluster but no radosgw process:
 *
 *   ceph_test_rgw_random_access_file -c ceph.conf
 *
 * The parquet tests are the demo: a large parquet file is written into an
 * object by the Arrow parquet writer over RGWOutputStream, then read back
 * with the Arrow parquet reader over RGWRandomAccessFile.
 */

#include "gtest/gtest.h"

#include "rgw/rgw_output_stream.h"
#include "rgw/rgw_random_access_file.h"
#include "rgw/rgw_sal.h"
#include "rgw/rgw_sal_config.h"
#include "rgw/rgw_zone.h"
#include "common/ceph_argparse.h"
#include "common/ceph_crypto.h"
#include "common/dout.h"
#include "common/async/context_pool.h"
#include "common/async/yield_context.h"
#include "global/global_init.h"
#include "global/global_context.h"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/util/io_util.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#define dout_subsys ceph_subsys_rgw

namespace {

struct TestEnv {
  rgw::sal::Driver* driver = nullptr;
  const DoutPrefixProvider* dpp = nullptr;
  std::unique_ptr<rgw::sal::ConfigStore> cfgstore;
  std::string bucket_name;
  std::string backend;
};

TestEnv* g_env = nullptr;

// deterministic non-repeating content, cheap to generate and to check
std::vector<uint8_t> make_pattern(size_t len, uint32_t seed)
{
  std::vector<uint8_t> data(len);
  std::minstd_rand rng(seed);
  for (size_t i = 0; i < len; ++i) {
    data[i] = static_cast<uint8_t>(rng());
  }
  return data;
}

std::string md5_hex(const uint8_t* data, size_t len)
{
  unsigned char digest[CEPH_CRYPTO_MD5_DIGESTSIZE];
  ceph::crypto::MD5 hash;
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  hash.Update(data, len);
  hash.Final(digest);
  std::string etag;
  buf_to_hex(digest, std::back_inserter(etag));
  return etag;
}

} // anonymous namespace

class RandomAccessFileTest : public ::testing::Test {
protected:
  static constexpr size_t kWriteChunk = 4 << 20;

  void SetUp() override {
    if (!g_env || !g_env->driver) {
      GTEST_SKIP() << "SAL driver not initialized";
    }
    bucket_id_.name = g_env->bucket_name;
    ASSERT_EQ(0, driver()->load_bucket(dpp(), bucket_id_, &bucket_, null_yield));
    ASSERT_NE(nullptr, bucket_);
  }

  void TearDown() override {
    if (!g_env || !g_env->driver || !bucket_) return;
    for (const auto& key : created_keys_) {
      auto obj = bucket_->get_object(rgw_obj_key(key));
      obj->delete_object(dpp(), null_yield, rgw::sal::FLAG_LOG_OP, nullptr,
                         nullptr);
    }
  }

  rgw::sal::Driver* driver() { return g_env->driver; }
  const DoutPrefixProvider* dpp() { return g_env->dpp; }

  // Write an object through the SAL atomic writer, streaming the data in
  // kWriteChunk pieces the way a request body would arrive.  Returns the
  // etag on success.
  //
  // This is deliberately independent of RGWOutputStream: the reader tests
  // fill their objects with it so that a reader bug cannot be masked by a
  // matching writer bug, and the stream tests use it as the reference for
  // the etag and the stored bytes.
  int put(const std::string& key, const uint8_t* data, size_t len,
          rgw::sal::Attrs extra_attrs = {}, std::string* etag_out = nullptr) {
    auto obj = bucket_->get_object(rgw_obj_key(key));
    const ACLOwner owner = bucket_->get_acl().get_owner();
    const rgw_placement_rule& placement = bucket_->get_placement_rule();
    const std::string unique_tag =
        driver()->zone_unique_id(driver()->get_new_req_id());

    auto writer = driver()->get_atomic_writer(dpp(), null_yield, obj.get(),
                                              owner, &placement, 0, unique_tag);
    if (!writer) return -ENOMEM;
    int r = writer->prepare(null_yield);
    if (r < 0) return r;

    for (size_t ofs = 0; ofs < len; ofs += kWriteChunk) {
      const size_t n = std::min(kWriteChunk, len - ofs);
      bufferlist bl;
      bl.append(reinterpret_cast<const char*>(data + ofs), n);
      r = writer->process(std::move(bl), ofs);
      if (r < 0) return r;
    }
    r = writer->process(bufferlist(), len);  // flush
    if (r < 0) return r;

    const std::string etag = md5_hex(data, len);
    rgw::sal::Attrs attrs = std::move(extra_attrs);
    bufferlist etag_bl;
    etag_bl.append(etag);
    attrs[RGW_ATTR_ETAG] = std::move(etag_bl);

    ceph::real_time mtime = ceph::real_clock::now();
    const req_context rctx{dpp(), null_yield, nullptr};
    r = writer->complete(len, etag, &mtime, mtime, attrs, std::nullopt,
                         ceph::real_time(), nullptr, nullptr, nullptr, nullptr,
                         nullptr, rctx, 0);
    if (r < 0) return r;

    created_keys_.push_back(key);
    if (etag_out) *etag_out = etag;
    return 0;
  }

  int put(const std::string& key, const std::vector<uint8_t>& data,
          rgw::sal::Attrs extra_attrs = {}, std::string* etag_out = nullptr) {
    return put(key, data.data(), data.size(), std::move(extra_attrs), etag_out);
  }

  arrow::Result<std::shared_ptr<RGWRandomAccessFile>> open(const std::string& key) {
    return RGWRandomAccessFile::Open(dpp(), null_yield, bucket_.get(),
                                     rgw_obj_key(key));
  }

  // opens key for writing through RGWOutputStream; the key is cleaned up at
  // TearDown whether or not the stream gets closed
  arrow::Result<std::shared_ptr<RGWOutputStream>> open_out(
      const std::string& key, rgw::sal::Attrs attrs = {},
      std::string unique_tag = {},
      int64_t chunk_size = RGWOutputStream::kDefaultChunkSize) {
    created_keys_.push_back(key);
    return RGWOutputStream::Open(dpp(), null_yield, driver(), bucket_.get(),
                                 rgw_obj_key(key), std::move(attrs),
                                 std::move(unique_tag), chunk_size);
  }

  // reads the whole object back through RGWRandomAccessFile
  arrow::Result<std::vector<uint8_t>> read_all(const std::string& key) {
    ARROW_ASSIGN_OR_RAISE(auto file, open(key));
    ARROW_ASSIGN_OR_RAISE(const int64_t size, file->GetSize());
    std::vector<uint8_t> out(size);
    if (size > 0) {
      ARROW_ASSIGN_OR_RAISE(const int64_t n, file->ReadAt(0, size, out.data()));
      if (n != size) {
        return arrow::Status::IOError("short read: ", n, " of ", size);
      }
    }
    return out;
  }

  // writes table as one parquet file into key through RGWOutputStream and
  // returns the number of bytes written
  arrow::Result<int64_t> write_parquet(const std::string& key,
                                       const arrow::Table& table,
                                       int64_t row_group_size) {
    ARROW_ASSIGN_OR_RAISE(auto sink, open_out(key));
    auto props = parquet::WriterProperties::Builder()
                     .compression(parquet::Compression::UNCOMPRESSED)
                     ->build();
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
        table, arrow::default_memory_pool(), sink, row_group_size, props));
    ARROW_RETURN_NOT_OK(sink->Close());
    return sink->size();
  }

  // compares file[position, position + nbytes) against the expected content
  void expect_range(RGWRandomAccessFile& file, const std::vector<uint8_t>& expected,
                    int64_t position, int64_t nbytes) {
    const int64_t avail = std::max<int64_t>(0, expected.size() - position);
    const int64_t want = std::min(nbytes, avail);

    std::vector<uint8_t> out(nbytes + 1, 0xee);  // canary past the end
    auto n = file.ReadAt(position, nbytes, out.data());
    ASSERT_TRUE(n.ok()) << n.status().ToString();
    ASSERT_EQ(want, *n) << "ReadAt(" << position << ", " << nbytes << ")";
    if (want > 0) {
      EXPECT_EQ(0, memcmp(expected.data() + position, out.data(), want))
          << "content mismatch at " << position;
    }
    EXPECT_EQ(0xee, out[nbytes]);

    auto buf = file.ReadAt(position, nbytes);
    ASSERT_TRUE(buf.ok()) << buf.status().ToString();
    ASSERT_EQ(want, (*buf)->size());
    if (want > 0) {
      EXPECT_EQ(0, memcmp(expected.data() + position, (*buf)->data(), want));
    }
  }

  rgw_bucket bucket_id_;
  std::unique_ptr<rgw::sal::Bucket> bucket_;  // loaded once per test
  std::vector<std::string> created_keys_;
};

// --------------- open ---------------

TEST_F(RandomAccessFileTest, OpenNullArguments) {
  EXPECT_TRUE(RGWRandomAccessFile::Open(dpp(), null_yield, nullptr,
                                        rgw_obj_key("raf/x")).status().IsInvalid());
  EXPECT_TRUE(RGWRandomAccessFile::Open(dpp(), null_yield,
                                        std::unique_ptr<rgw::sal::Object>())
                  .status().IsInvalid());
}

// A missing object is an IOError with an ENOENT errno detail, the way
// Arrow's own filesystems report a missing path.
TEST_F(RandomAccessFileTest, OpenMissingObject) {
  auto file = open("raf/no-such-object");
  ASSERT_FALSE(file.ok());
  EXPECT_TRUE(file.status().IsIOError()) << file.status().ToString();
  EXPECT_EQ(ENOENT, arrow::internal::ErrnoFromStatus(file.status()))
      << file.status().ToString();
  EXPECT_NE(std::string::npos, file.status().ToString().find("No such file"))
      << file.status().ToString();
}

TEST_F(RandomAccessFileTest, OpenFromObject) {
  const auto data = make_pattern(100, 1);
  ASSERT_EQ(0, put("raf/open-from-object", data));

  auto obj = bucket_->get_object(rgw_obj_key("raf/open-from-object"));

  auto file = RGWRandomAccessFile::Open(dpp(), null_yield, std::move(obj));
  ASSERT_TRUE(file.ok()) << file.status().ToString();
  EXPECT_EQ(g_env->bucket_name + "/raf/open-from-object", (*file)->name());
  expect_range(**file, data, 0, 100);
}

// --------------- ReadAt ---------------

TEST_F(RandomAccessFileTest, ReadAtSmallObject) {
  const auto data = make_pattern(1000, 2);
  ASSERT_EQ(0, put("raf/small", data));

  auto res = open("raf/small");
  ASSERT_TRUE(res.ok()) << res.status().ToString();
  auto& file = **res;

  auto size = file.GetSize();
  ASSERT_TRUE(size.ok());
  EXPECT_EQ(1000, *size);
  EXPECT_FALSE(file.closed());
  EXPECT_FALSE(file.supports_zero_copy());

  expect_range(file, data, 0, 1000);     // whole object
  expect_range(file, data, 0, 1);        // first byte
  expect_range(file, data, 999, 1);      // last byte
  expect_range(file, data, 10, 5);       // middle
  expect_range(file, data, 0, 5000);     // more than the object: truncated
  expect_range(file, data, 997, 10);     // crossing the end: truncated
  expect_range(file, data, 1000, 10);    // at the end: nothing
  expect_range(file, data, 4000, 10);    // past the end: nothing
  expect_range(file, data, 500, 0);      // zero length

  uint8_t byte;
  EXPECT_TRUE(file.ReadAt(-1, 1, &byte).status().IsInvalid());
  EXPECT_TRUE(file.ReadAt(0, -1, &byte).status().IsInvalid());
  EXPECT_TRUE(file.ReadAt(-1, 1).status().IsInvalid());
  EXPECT_TRUE(file.ReadAt(0, 1, nullptr).status().IsInvalid());
}

TEST_F(RandomAccessFileTest, ReadAtEmptyObject) {
  ASSERT_EQ(0, put("raf/empty", std::vector<uint8_t>{}));

  auto res = open("raf/empty");
  ASSERT_TRUE(res.ok()) << res.status().ToString();
  auto& file = **res;

  auto size = file.GetSize();
  ASSERT_TRUE(size.ok());
  EXPECT_EQ(0, *size);
  expect_range(file, {}, 0, 10);
  expect_range(file, {}, 5, 10);

  auto buf = file.Read(10);
  ASSERT_TRUE(buf.ok());
  EXPECT_EQ(0, (*buf)->size());
}

// A 12 MiB + 4097 byte object spans several rados stripes (4 MiB each by
// default) and several rgw_get_obj_max_req_size chunks, so reads that
// straddle those boundaries exercise the chunk reassembly in the callback.
TEST_F(RandomAccessFileTest, ReadAtLargeObjectAcrossChunks) {
  constexpr int64_t mib = 1 << 20;
  const auto data = make_pattern(12 * mib + 4097, 3);
  ASSERT_EQ(0, put("raf/large", data));

  auto res = open("raf/large");
  ASSERT_TRUE(res.ok()) << res.status().ToString();
  auto& file = **res;

  auto size = file.GetSize();
  ASSERT_TRUE(size.ok());
  EXPECT_EQ(static_cast<int64_t>(data.size()), *size);

  expect_range(file, data, 4 * mib - 100, 200);          // one stripe boundary
  expect_range(file, data, 4 * mib - 1, 2);              // two bytes across it
  expect_range(file, data, 3 * mib, 6 * mib);            // two boundaries
  expect_range(file, data, 0, 4 * mib);                  // exactly one stripe
  expect_range(file, data, 4 * mib, 4 * mib);            // exactly the second
  expect_range(file, data, 12 * mib, 4097);              // the tail stripe
  expect_range(file, data, 12 * mib + 4000, 1000);       // truncated tail
  expect_range(file, data, 0, data.size());              // everything
  expect_range(file, data, data.size(), 10);             // at the end
}

// ReadAt() is position-independent, so concurrent callers must each get the
// range they asked for (the parquet reader does this with use_threads).
TEST_F(RandomAccessFileTest, ReadAtConcurrent) {
  constexpr int64_t mib = 1 << 20;
  const auto data = make_pattern(9 * mib, 4);
  ASSERT_EQ(0, put("raf/concurrent", data));

  auto res = open("raf/concurrent");
  ASSERT_TRUE(res.ok()) << res.status().ToString();
  auto file = *res;

  constexpr int nthreads = 8;
  std::vector<std::thread> threads;
  std::vector<std::string> errors(nthreads);
  for (int t = 0; t < nthreads; ++t) {
    threads.emplace_back([&, t] {
      std::minstd_rand rng(100 + t);
      for (int i = 0; i < 20; ++i) {
        const int64_t position = rng() % data.size();
        const int64_t nbytes = 1 + rng() % (3 * mib);
        auto buf = file->ReadAt(position, nbytes);
        if (!buf.ok()) {
          errors[t] = buf.status().ToString();
          return;
        }
        const int64_t want =
            std::min<int64_t>(nbytes, data.size() - position);
        if ((*buf)->size() != want ||
            memcmp(data.data() + position, (*buf)->data(), want) != 0) {
          errors[t] = "mismatch at " + std::to_string(position);
          return;
        }
      }
    });
  }
  for (auto& t : threads) t.join();
  for (int t = 0; t < nthreads; ++t) {
    EXPECT_TRUE(errors[t].empty()) << "thread " << t << ": " << errors[t];
  }
}

// --------------- sequential stream API ---------------

TEST_F(RandomAccessFileTest, ReadSeekTellPeek) {
  const auto data = make_pattern(1000, 5);
  ASSERT_EQ(0, put("raf/stream", data));

  auto res = open("raf/stream");
  ASSERT_TRUE(res.ok()) << res.status().ToString();
  auto& file = **res;

  auto tell = file.Tell();
  ASSERT_TRUE(tell.ok());
  EXPECT_EQ(0, *tell);

  // Read(nbytes, out) advances the position
  std::vector<uint8_t> out(100);
  auto n = file.Read(100, out.data());
  ASSERT_TRUE(n.ok()) << n.status().ToString();
  EXPECT_EQ(100, *n);
  EXPECT_EQ(0, memcmp(data.data(), out.data(), 100));
  EXPECT_EQ(100, *file.Tell());

  // Read(nbytes) continues from there
  auto buf = file.Read(50);
  ASSERT_TRUE(buf.ok()) << buf.status().ToString();
  ASSERT_EQ(50, (*buf)->size());
  EXPECT_EQ(0, memcmp(data.data() + 100, (*buf)->data(), 50));
  EXPECT_EQ(150, *file.Tell());

  // Peek does not move the position
  ASSERT_TRUE(file.Seek(500).ok());
  auto peek = file.Peek(10);
  ASSERT_TRUE(peek.ok()) << peek.status().ToString();
  ASSERT_EQ(10u, peek->size());
  EXPECT_EQ(0, memcmp(data.data() + 500, peek->data(), 10));
  EXPECT_EQ(500, *file.Tell());

  // ReadAt does not move it either
  ASSERT_TRUE(file.ReadAt(0, 10, out.data()).ok());
  EXPECT_EQ(500, *file.Tell());

  // reading across the end truncates, and leaves the position at the end
  buf = file.Read(1000);
  ASSERT_TRUE(buf.ok());
  ASSERT_EQ(500, (*buf)->size());
  EXPECT_EQ(0, memcmp(data.data() + 500, (*buf)->data(), 500));
  EXPECT_EQ(1000, *file.Tell());

  n = file.Read(10, out.data());
  ASSERT_TRUE(n.ok());
  EXPECT_EQ(0, *n);
  peek = file.Peek(10);
  ASSERT_TRUE(peek.ok());
  EXPECT_EQ(0u, peek->size());

  // seeking to the end is fine, past it or before the start is not
  EXPECT_TRUE(file.Seek(1000).ok());
  EXPECT_TRUE(file.Seek(1001).IsInvalid());
  EXPECT_TRUE(file.Seek(-1).IsInvalid());
  EXPECT_EQ(1000, *file.Tell());
  EXPECT_TRUE(file.Peek(-1).status().IsInvalid());
}

TEST_F(RandomAccessFileTest, Close) {
  const auto data = make_pattern(10, 6);
  ASSERT_EQ(0, put("raf/close", data));

  auto res = open("raf/close");
  ASSERT_TRUE(res.ok()) << res.status().ToString();
  auto& file = **res;

  EXPECT_FALSE(file.closed());
  EXPECT_TRUE(file.Close().ok());
  EXPECT_TRUE(file.closed());
  EXPECT_TRUE(file.Close().ok());  // idempotent

  uint8_t byte;
  EXPECT_TRUE(file.GetSize().status().IsInvalid());
  EXPECT_TRUE(file.Tell().status().IsInvalid());
  EXPECT_TRUE(file.Seek(0).IsInvalid());
  EXPECT_TRUE(file.ReadAt(0, 1, &byte).status().IsInvalid());
  EXPECT_TRUE(file.ReadAt(0, 1).status().IsInvalid());
  EXPECT_TRUE(file.Read(1, &byte).status().IsInvalid());
  EXPECT_TRUE(file.Read(1).status().IsInvalid());
  EXPECT_TRUE(file.Peek(1).status().IsInvalid());
  EXPECT_TRUE(file.ReadMetadata().status().IsInvalid());
}

// --------------- metadata ---------------

TEST_F(RandomAccessFileTest, ReadMetadata) {
  const auto data = make_pattern(4321, 7);
  rgw::sal::Attrs attrs;
  attrs[RGW_ATTR_CONTENT_TYPE].append("application/vnd.apache.parquet");
  attrs[RGW_ATTR_META_PREFIX "purpose"].append("test");
  std::string etag;
  ASSERT_EQ(0, put("raf/metadata", data, std::move(attrs), &etag));

  auto res = open("raf/metadata");
  ASSERT_TRUE(res.ok()) << res.status().ToString();

  auto md = (*res)->ReadMetadata();
  ASSERT_TRUE(md.ok()) << md.status().ToString();
  ASSERT_NE(nullptr, *md);
  EXPECT_EQ("4321", (*md)->Get("Content-Length").ValueOr(""));
  EXPECT_EQ(etag, (*md)->Get("ETag").ValueOr(""));
  EXPECT_EQ("application/vnd.apache.parquet",
            (*md)->Get("Content-Type").ValueOr(""));
  EXPECT_EQ("test", (*md)->Get("x-amz-meta-purpose").ValueOr(""));
  EXPECT_TRUE((*md)->Contains("Last-Modified"));
}

// --------------- output stream ---------------

TEST_F(RandomAccessFileTest, OutputStreamNullArguments) {
  EXPECT_TRUE(RGWOutputStream::Open(dpp(), null_yield, nullptr, bucket_.get(),
                                    rgw_obj_key("raf/x")).status().IsInvalid());
  EXPECT_TRUE(RGWOutputStream::Open(dpp(), null_yield, driver(), nullptr,
                                    rgw_obj_key("raf/x")).status().IsInvalid());
  EXPECT_TRUE(RGWOutputStream::Open(dpp(), null_yield, driver(), nullptr)
                  .status().IsInvalid());
  EXPECT_TRUE(open_out("raf/x", {}, {}, 0).status().IsInvalid());
}

// Several small writes, then the object is read back through the file class
// and its metadata matches what was written.
TEST_F(RandomAccessFileTest, OutputStreamSmallObject) {
  const auto data = make_pattern(100'000, 21);
  rgw::sal::Attrs attrs;
  attrs[RGW_ATTR_CONTENT_TYPE].append("application/octet-stream");
  attrs[RGW_ATTR_META_PREFIX "origin"].append("output-stream");

  auto sink = open_out("raf/out-small", std::move(attrs));
  ASSERT_TRUE(sink.ok()) << sink.status().ToString();
  EXPECT_FALSE((*sink)->closed());
  EXPECT_EQ(arrow::io::FileMode::WRITE, (*sink)->mode());
  EXPECT_EQ(0, *(*sink)->Tell());

  // mixed write sizes, including an empty one and the Buffer overload
  size_t ofs = 0;
  const size_t sizes[] = {1, 0, 4095, 4096, 4097, 30'000, 1};
  for (size_t n : sizes) {
    ASSERT_TRUE((*sink)->Write(data.data() + ofs, n).ok());
    ofs += n;
    EXPECT_EQ(static_cast<int64_t>(ofs), *(*sink)->Tell());
  }
  auto rest = arrow::Buffer::Wrap(data.data() + ofs, data.size() - ofs);
  ASSERT_TRUE((*sink)->Write(rest).ok());
  EXPECT_EQ(static_cast<int64_t>(data.size()), *(*sink)->Tell());

  // invalid writes are rejected without affecting the position
  EXPECT_TRUE((*sink)->Write(data.data(), -1).IsInvalid());
  EXPECT_TRUE((*sink)->Write(nullptr, 1).IsInvalid());
  EXPECT_EQ(static_cast<int64_t>(data.size()), *(*sink)->Tell());

  // nothing is visible until the stream is closed
  EXPECT_TRUE(open("raf/out-small").status().IsIOError());

  EXPECT_EQ(static_cast<int64_t>(data.size()), (*sink)->size());
  EXPECT_TRUE((*sink)->etag().empty());  // not known before Close()

  auto st = (*sink)->Close();
  ASSERT_TRUE(st.ok()) << st.ToString();
  EXPECT_TRUE((*sink)->closed());
  EXPECT_EQ(md5_hex(data.data(), data.size()), (*sink)->etag());
  // size() outlives Close(), Tell() does not
  EXPECT_EQ(static_cast<int64_t>(data.size()), (*sink)->size());
  EXPECT_TRUE((*sink)->Tell().status().IsInvalid());

  auto back = read_all("raf/out-small");
  ASSERT_TRUE(back.ok()) << back.status().ToString();
  EXPECT_EQ(data, *back);

  auto file = open("raf/out-small");
  ASSERT_TRUE(file.ok());
  auto md = (*file)->ReadMetadata();
  ASSERT_TRUE(md.ok());
  EXPECT_EQ((*sink)->etag(), (*md)->Get("ETag").ValueOr(""));
  EXPECT_EQ("application/octet-stream", (*md)->Get("Content-Type").ValueOr(""));
  EXPECT_EQ("output-stream", (*md)->Get("x-amz-meta-origin").ValueOr(""));
  EXPECT_EQ(std::to_string(data.size()),
            (*md)->Get("Content-Length").ValueOr(""));
}

TEST_F(RandomAccessFileTest, OutputStreamEmptyObject) {
  auto sink = open_out("raf/out-empty");
  ASSERT_TRUE(sink.ok()) << sink.status().ToString();
  auto st = (*sink)->Close();
  ASSERT_TRUE(st.ok()) << st.ToString();
  EXPECT_EQ(0, (*sink)->size());
  EXPECT_EQ("d41d8cd98f00b204e9800998ecf8427e", (*sink)->etag());

  auto file = open("raf/out-empty");
  ASSERT_TRUE(file.ok()) << file.status().ToString();
  EXPECT_EQ(0, *(*file)->GetSize());
}

// Writes that straddle the stream's chunk size and the rados stripe size, in
// pieces that never line up with either.
TEST_F(RandomAccessFileTest, OutputStreamLargeObjectAcrossChunks) {
  const size_t total = (12 << 20) + 4097;
  const auto data = make_pattern(total, 22);

  // a chunk size below the stripe size: the writer sees many partial stripes
  auto sink = open_out("raf/out-large", {}, {}, (1 << 20) + 1);
  ASSERT_TRUE(sink.ok()) << sink.status().ToString();

  const size_t piece = (3 << 20) - 7;
  for (size_t ofs = 0; ofs < total; ofs += piece) {
    const size_t n = std::min(piece, total - ofs);
    ASSERT_TRUE((*sink)->Write(data.data() + ofs, n).ok());
  }
  EXPECT_EQ(static_cast<int64_t>(total), *(*sink)->Tell());
  auto st = (*sink)->Close();
  ASSERT_TRUE(st.ok()) << st.ToString();
  EXPECT_EQ(md5_hex(data.data(), data.size()), (*sink)->etag());

  auto file = open("raf/out-large");
  ASSERT_TRUE(file.ok()) << file.status().ToString();
  EXPECT_EQ(static_cast<int64_t>(total), *(*file)->GetSize());
  expect_range(**file, data, 0, 4096);
  expect_range(**file, data, (4 << 20) - 10, 20);          // rados stripe edge
  expect_range(**file, data, (1 << 20) - 5, 10);           // chunk edge
  expect_range(**file, data, total - 4097, 4097);
  auto back = read_all("raf/out-large");
  ASSERT_TRUE(back.ok()) << back.status().ToString();
  EXPECT_EQ(data, *back);
}

// Flush pushes buffered data to the writer early without completing the
// object; Close is idempotent and every operation fails afterwards.
TEST_F(RandomAccessFileTest, OutputStreamFlushAndClose) {
  const auto data = make_pattern(200'000, 23);
  auto sink = open_out("raf/out-flush");
  ASSERT_TRUE(sink.ok()) << sink.status().ToString();

  ASSERT_TRUE((*sink)->Write(data.data(), 100'000).ok());
  ASSERT_TRUE((*sink)->Flush().ok());
  EXPECT_EQ(100'000, *(*sink)->Tell());
  EXPECT_TRUE(open("raf/out-flush").status().IsIOError());  // not visible yet
  ASSERT_TRUE((*sink)->Flush().ok());  // nothing pending: no-op
  ASSERT_TRUE((*sink)->Write(data.data() + 100'000, 100'000).ok());

  ASSERT_TRUE((*sink)->Close().ok());
  EXPECT_TRUE((*sink)->Close().ok());
  EXPECT_TRUE((*sink)->closed());
  EXPECT_TRUE((*sink)->Write(data.data(), 1).IsInvalid());
  EXPECT_TRUE((*sink)->Flush().IsInvalid());
  EXPECT_TRUE((*sink)->Tell().status().IsInvalid());
  EXPECT_TRUE((*sink)->Abort().ok());  // harmless once closed

  auto back = read_all("raf/out-flush");
  ASSERT_TRUE(back.ok()) << back.status().ToString();
  EXPECT_EQ(data, *back);
}

// Abort, or dropping the stream without Close, leaves no object behind.
TEST_F(RandomAccessFileTest, OutputStreamAbortDiscardsObject) {
  const auto data = make_pattern(5 << 20, 24);  // more than one chunk
  {
    auto sink = open_out("raf/out-abort");
    ASSERT_TRUE(sink.ok()) << sink.status().ToString();
    ASSERT_TRUE((*sink)->Write(data.data(), data.size()).ok());
    ASSERT_TRUE((*sink)->Abort().ok());
    EXPECT_TRUE((*sink)->closed());
    EXPECT_TRUE((*sink)->Write(data.data(), 1).IsInvalid());
    EXPECT_TRUE((*sink)->Close().ok());  // no-op after Abort
  }
  EXPECT_TRUE(open("raf/out-abort").status().IsIOError());

  {
    auto sink = open_out("raf/out-dropped");
    ASSERT_TRUE(sink.ok()) << sink.status().ToString();
    ASSERT_TRUE((*sink)->Write(data.data(), data.size()).ok());
    // destroyed without Close()
  }
  EXPECT_TRUE(open("raf/out-dropped").status().IsIOError());
}

// The unique tag given to Open() is the write's operation tag and ends up
// as the object's ID tag; without one, the stream generates a tag.
TEST_F(RandomAccessFileTest, OutputStreamUniqueTag) {
  const auto data = make_pattern(1000, 27);
  auto id_tag = [&](const std::string& key) -> std::string {
    auto obj = bucket_->get_object(rgw_obj_key(key));
    if (obj->load_obj_state(dpp(), null_yield) < 0 || !obj->exists()) {
      return "<missing>";
    }
    auto it = obj->get_attrs().find(RGW_ATTR_ID_TAG);
    if (it == obj->get_attrs().end()) return "<no id tag>";
    return std::string(it->second.c_str());  // stored NUL-terminated
  };

  const std::string tag = "random-access-file-test-tag-" + std::to_string(getpid());
  auto sink = open_out("raf/out-tagged", {}, tag);
  ASSERT_TRUE(sink.ok()) << sink.status().ToString();
  EXPECT_EQ(tag, (*sink)->unique_tag());
  ASSERT_TRUE((*sink)->Write(data.data(), data.size()).ok());
  ASSERT_TRUE((*sink)->Close().ok());
  EXPECT_EQ(tag, id_tag("raf/out-tagged"));

  auto untagged = open_out("raf/out-untagged");
  ASSERT_TRUE(untagged.ok()) << untagged.status().ToString();
  EXPECT_FALSE((*untagged)->unique_tag().empty());
  EXPECT_NE(tag, (*untagged)->unique_tag());
  ASSERT_TRUE((*untagged)->Write(data.data(), data.size()).ok());
  ASSERT_TRUE((*untagged)->Close().ok());
  EXPECT_EQ((*untagged)->unique_tag(), id_tag("raf/out-untagged"));
}

// Writing the same key again replaces the object atomically.
TEST_F(RandomAccessFileTest, OutputStreamOverwrite) {
  const auto first = make_pattern(3 << 20, 25);
  const auto second = make_pattern(1000, 26);
  for (const auto* data : {&first, &second}) {
    auto sink = open_out("raf/out-overwrite");
    ASSERT_TRUE(sink.ok()) << sink.status().ToString();
    ASSERT_TRUE((*sink)->Write(data->data(), data->size()).ok());
    ASSERT_TRUE((*sink)->Close().ok());
  }
  auto back = read_all("raf/out-overwrite");
  ASSERT_TRUE(back.ok()) << back.status().ToString();
  EXPECT_EQ(second, *back);
}

// --------------- parquet ---------------

namespace {

// id (int64), value (double), label (utf8): nrows rows of easily checked data
arrow::Result<std::shared_ptr<arrow::Table>> make_table(int64_t nrows)
{
  arrow::Int64Builder ids;
  arrow::DoubleBuilder values;
  arrow::StringBuilder labels;
  ARROW_RETURN_NOT_OK(ids.Reserve(nrows));
  ARROW_RETURN_NOT_OK(values.Reserve(nrows));
  ARROW_RETURN_NOT_OK(labels.Reserve(nrows));
  for (int64_t i = 0; i < nrows; ++i) {
    ids.UnsafeAppend(i);
    values.UnsafeAppend(static_cast<double>(i) * 0.5);
    ARROW_RETURN_NOT_OK(labels.Append("row-" + std::to_string(i % 1000)));
  }
  std::shared_ptr<arrow::Array> id_arr, value_arr, label_arr;
  ARROW_RETURN_NOT_OK(ids.Finish(&id_arr));
  ARROW_RETURN_NOT_OK(values.Finish(&value_arr));
  ARROW_RETURN_NOT_OK(labels.Finish(&label_arr));

  auto schema = arrow::schema({arrow::field("id", arrow::int64()),
                               arrow::field("value", arrow::float64()),
                               arrow::field("label", arrow::utf8())});
  return arrow::Table::Make(schema, {id_arr, value_arr, label_arr});
}

// encodes the table in memory the same way write_parquet() does, to compare
// against what went through RGWOutputStream
arrow::Result<std::shared_ptr<arrow::Buffer>> table_to_parquet(
    const arrow::Table& table, int64_t row_group_size)
{
  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::BufferOutputStream::Create());
  auto props = parquet::WriterProperties::Builder()
                   .compression(parquet::Compression::UNCOMPRESSED)
                   ->build();
  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
      table, arrow::default_memory_pool(), sink, row_group_size, props));
  return sink->Finish();
}

} // anonymous namespace

// The parquet writer's output through RGWOutputStream is byte for byte what
// it produces in memory.
TEST_F(RandomAccessFileTest, ParquetWriteIsByteExact) {
  constexpr int64_t nrows = 300'000;
  constexpr int64_t row_group_size = 100'000;

  auto table = make_table(nrows);
  ASSERT_TRUE(table.ok()) << table.status().ToString();
  auto expected = table_to_parquet(**table, row_group_size);
  ASSERT_TRUE(expected.ok()) << expected.status().ToString();

  auto size = write_parquet("raf/exact.parquet", **table, row_group_size);
  ASSERT_TRUE(size.ok()) << size.status().ToString();
  EXPECT_EQ((*expected)->size(), *size);

  auto back = read_all("raf/exact.parquet");
  ASSERT_TRUE(back.ok()) << back.status().ToString();
  ASSERT_EQ((*expected)->size(), static_cast<int64_t>(back->size()));
  EXPECT_EQ(0, memcmp((*expected)->data(), back->data(), back->size()));
}

// Streams a parquet file of tens of MiB (many row groups, several rados
// stripes) into an object through RGWOutputStream, then reads it back
// through the parquet reader over RGWRandomAccessFile.
TEST_F(RandomAccessFileTest, ParquetLargeFileRoundTrip) {
  constexpr int64_t nrows = 2'000'000;
  constexpr int64_t row_group_size = 100'000;

  auto table = make_table(nrows);
  ASSERT_TRUE(table.ok()) << table.status().ToString();
  auto size = write_parquet("raf/large.parquet", **table, row_group_size);
  ASSERT_TRUE(size.ok()) << size.status().ToString();
  ASSERT_GT(*size, 32 << 20) << "file too small for the demo";

  auto file = open("raf/large.parquet");
  ASSERT_TRUE(file.ok()) << file.status().ToString();

  // single threaded, no pre-buffering: every column chunk is a ReadAt()
  parquet::ArrowReaderProperties props(/*use_threads=*/false);
  props.set_pre_buffer(false);
  parquet::arrow::FileReaderBuilder builder;
  ASSERT_TRUE(builder.Open(*file).ok());
  builder.properties(props);
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ASSERT_TRUE(builder.Build(&reader).ok());

  EXPECT_EQ(nrows / row_group_size, reader->num_row_groups());

  std::shared_ptr<arrow::Table> out;
  auto st = reader->ReadTable(&out);
  ASSERT_TRUE(st.ok()) << st.ToString();
  ASSERT_EQ(nrows, out->num_rows());
  EXPECT_TRUE((*table)->Equals(*out)) << "table content differs after round trip";
}

// The parquet reader's threaded, pre-buffered mode issues ReadAt() for many
// column chunks at once from Arrow's I/O pool.
TEST_F(RandomAccessFileTest, ParquetThreadedPreBufferedRead) {
  constexpr int64_t nrows = 500'000;
  constexpr int64_t row_group_size = 50'000;

  auto table = make_table(nrows);
  ASSERT_TRUE(table.ok()) << table.status().ToString();
  auto size = write_parquet("raf/threaded.parquet", **table, row_group_size);
  ASSERT_TRUE(size.ok()) << size.status().ToString();

  auto file = open("raf/threaded.parquet");
  ASSERT_TRUE(file.ok()) << file.status().ToString();

  parquet::ArrowReaderProperties props(/*use_threads=*/true);
  props.set_pre_buffer(true);
  parquet::arrow::FileReaderBuilder builder;
  ASSERT_TRUE(builder.Open(*file).ok());
  builder.properties(props);
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ASSERT_TRUE(builder.Build(&reader).ok());

  std::shared_ptr<arrow::Table> out;
  auto st = reader->ReadTable(&out);
  ASSERT_TRUE(st.ok()) << st.ToString();
  ASSERT_EQ(nrows, out->num_rows());
  EXPECT_TRUE((*table)->Equals(*out));
}

// Reading a projection of row groups and columns only touches the matching
// byte ranges of the object.
TEST_F(RandomAccessFileTest, ParquetRowGroupAndColumnProjection) {
  constexpr int64_t nrows = 300'000;
  constexpr int64_t row_group_size = 100'000;

  auto table = make_table(nrows);
  ASSERT_TRUE(table.ok()) << table.status().ToString();
  auto size = write_parquet("raf/projection.parquet", **table, row_group_size);
  ASSERT_TRUE(size.ok()) << size.status().ToString();

  auto file = open("raf/projection.parquet");
  ASSERT_TRUE(file.ok()) << file.status().ToString();

  auto reader = parquet::arrow::OpenFile(*file, arrow::default_memory_pool());
  ASSERT_TRUE(reader.ok()) << reader.status().ToString();
  ASSERT_EQ(3, (*reader)->num_row_groups());

  // second row group, columns id and label
  std::shared_ptr<arrow::Table> out;
  auto st = (*reader)->ReadRowGroup(1, {0, 2}, &out);
  ASSERT_TRUE(st.ok()) << st.ToString();
  ASSERT_EQ(row_group_size, out->num_rows());
  ASSERT_EQ(2, out->num_columns());

  auto expected = (*table)->Slice(row_group_size, row_group_size)
                      ->SelectColumns({0, 2});
  ASSERT_TRUE(expected.ok());
  EXPECT_TRUE((*expected)->Equals(*out));

  // the parquet footer and schema are intact
  std::shared_ptr<arrow::Schema> schema;
  ASSERT_TRUE((*reader)->GetSchema(&schema).ok());
  EXPECT_TRUE(schema->Equals(*(*table)->schema()));
}

// Opening a non-parquet object through the parquet reader must fail cleanly
// rather than crash or hang.
TEST_F(RandomAccessFileTest, ParquetRejectsNonParquetObject) {
  const auto data = make_pattern(64 << 10, 8);
  ASSERT_EQ(0, put("raf/not-parquet", data));

  auto file = open("raf/not-parquet");
  ASSERT_TRUE(file.ok()) << file.status().ToString();

  auto reader = parquet::arrow::OpenFile(*file, arrow::default_memory_pool());
  EXPECT_FALSE(reader.ok());
}

// ===========================================================================
// main() -- bootstrap the SAL driver, as ceph_test_rgw_sal_wrapper does
// ===========================================================================

int main(int argc, char** argv) {
  auto args = argv_to_vec(argc, const_cast<const char**>(argv));

  auto cct = rgw_global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                             CODE_ENVIRONMENT_UTILITY, 0);

  if (!g_conf()->rgw_region.empty() && g_conf()->rgw_zonegroup.empty()) {
    g_conf().set_val_or_die("rgw_zonegroup", g_conf()->rgw_region.c_str());
  }

  common_init_finish(g_ceph_context);

  static TestEnv env;
  g_env = &env;

  static NoDoutPrefix nodpp(g_ceph_context, ceph_subsys_rgw);
  env.dpp = &nodpp;

  static ceph::async::io_context_pool context_pool{
    cct->_conf->rgw_thread_pool_size};

  DriverManager::Config cfg = DriverManager::get_config(true, g_ceph_context);
  env.backend = cfg.store_name;
  std::cerr << "INFO: using backend '" << cfg.store_name << "'" << std::endl;

  auto config_store_type = g_conf().get_val<std::string>("rgw_config_store");
  env.cfgstore = DriverManager::create_config_store(env.dpp, config_store_type);
  if (!env.cfgstore) {
    std::cerr << "ERROR: failed to create config store" << std::endl;
    return 1;
  }

  rgw::SiteConfig site;
  int r = site.load(env.dpp, null_yield, env.cfgstore.get());
  if (r < 0) {
    std::cerr << "ERROR: failed to load site config (r=" << r << ")" << std::endl;
    return 1;
  }

  // all background threads off; the tests drive the driver synchronously
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

  auto shutdown = [&] {
    env.driver->shutdown();
    DriverManager::close_storage(env.driver);
    env.driver = nullptr;
  };

  env.bucket_name = "random-access-file-test-" + std::to_string(getpid());
  {
    rgw_bucket b;
    b.name = env.bucket_name;

    std::unique_ptr<rgw::sal::Bucket> bucket;
    r = env.driver->load_bucket(env.dpp, b, &bucket, null_yield);
    if (!bucket) {
      std::cerr << "ERROR: load_bucket returned no bucket object" << std::endl;
      shutdown();
      return 1;
    }

    rgw::sal::Bucket::CreateParams params;
    params.owner = rgw_user{"", "random-access-file-test-user"};
    params.zonegroup_id = site.get_zonegroup().get_id();
    params.placement_rule = site.get_zonegroup().default_placement;
    params.zone_placement = rgw::find_zone_placement(
        env.dpp, site.get_zone_params(), params.placement_rule);

    r = bucket->create(env.dpp, params, null_yield);
    if (r < 0 && r != -EEXIST) {
      std::cerr << "ERROR: failed to create test bucket (r=" << r << ")" << std::endl;
      shutdown();
      return 1;
    }
  }

  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();

  {
    rgw_bucket b;
    b.name = env.bucket_name;
    std::unique_ptr<rgw::sal::Bucket> bucket;
    if (env.driver->load_bucket(env.dpp, b, &bucket, null_yield) == 0 && bucket) {
      bucket->remove(env.dpp, true, null_yield);
    }
  }
  shutdown();

  return ret;
}
