// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * How a SAL filter passes S3-over-RDMA reads and writes on to the store
 * below it: the out-of-band params of a read, which RGWGetObj sets after
 * prepare(), and the pulled writes of an OSD-direct PUT.
 */

#include "rgw_sal_filter.h"
#ifdef WITH_RADOSGW_D4N
#include "driver/d4n/rgw_sal_d4n.h"
#endif
#include "include/rados/librados.hpp"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace {

using rgw::sal::FilterObject;
using rgw::sal::FilterWriter;
using rgw::sal::PulledStripe;

/// the store below the filter: records the out-of-band params iterate()
/// sees and answers through their out-pointers, as RadosReadOp does
struct MockReadOp : rgw::sal::Object::ReadOp {
  std::string token;
  uint64_t* bytes = nullptr;
  std::optional<uint64_t>* crc64 = nullptr;
  std::optional<uint32_t>* crc32c = nullptr;
  int iterated = 0;

  int prepare(optional_yield y, const DoutPrefixProvider* dpp) override {
    return 0;
  }
  int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y,
           const DoutPrefixProvider* dpp) override {
    return 0;
  }
  int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
              RGWGetDataCB* cb, optional_yield y) override {
    ++iterated;
    token = params.rdma_token;
    bytes = params.rdma_bytes;
    crc64 = params.rdma_crc64;
    crc32c = params.rdma_crc32c;
    if (params.rdma_token.empty()) {
      return 0;
    }
    params.rdma_submitted = true;
    if (params.rdma_bytes) {
      *params.rdma_bytes = end - ofs + 1;
    }
    if (params.rdma_crc64) {
      *params.rdma_crc64 = 0x0123456789abcdefull;
    }
    if (params.rdma_crc32c) {
      *params.rdma_crc32c = 0x89abcdefu;
    }
    return 0;
  }
  int get_attr(const DoutPrefixProvider* dpp, const char* name,
               bufferlist& dest, optional_yield y) override {
    return -ENODATA;
  }
};

/// the writer below the filter: a store that can pull, or not
struct MockWriter : rgw::sal::Writer {
  bool can_pull = true;
  bool pulled = false;
  uint64_t len = 0;
  std::string token;
  uint32_t want = 0;

  int prepare(optional_yield y) override { return 0; }
  int process(bufferlist&& data, uint64_t offset) override { return 0; }
  int complete(size_t accounted_size, const std::string& etag,
               ceph::real_time *mtime, ceph::real_time set_mtime,
               std::map<std::string, bufferlist>& attrs,
               const std::optional<rgw::cksum::Cksum>& cksum,
               ceph::real_time delete_at,
               const char *if_match, const char *if_nomatch,
               const std::string *user_data,
               rgw_zone_set *zones_trace, bool *canceled,
               const req_context& rctx, uint32_t flags) override {
    return 0;
  }
  bool set_pulled() override {
    pulled = can_pull;
    return can_pull;
  }
  int process_pulled(uint64_t l, const std::string& t, uint32_t w,
                     std::vector<PulledStripe>* stripes) override {
    len = l;
    token = t;
    want = w;
    PulledStripe s;
    s.len = l;
    s.bytes = l;
    s.crc64 = 0x0123456789abcdefull;
    s.crc64_valid = true;
    stripes->push_back(s);
    return 0;
  }
};

constexpr const char* TOKEN = "7f0000000000:100000:1a2b:0:4d2:0:0";

} // anonymous namespace

// RGWGetObj sets the token and the out-pointers after prepare() copied the
// params into the store's ReadOp, and the CRC pointers are what ask the
// OSDs for CRCs: all of them must reach the store
TEST(FilterReadOp, IterateForwardsRdmaParams)
{
  auto mock = std::make_unique<MockReadOp>();
  MockReadOp* m = mock.get();
  FilterObject::FilterReadOp op(std::move(mock));
  ASSERT_EQ(0, op.prepare(null_yield, nullptr));

  uint64_t bytes = 0;
  std::optional<uint64_t> crc64;
  std::optional<uint32_t> crc32c;
  op.params.rdma_token = TOKEN;
  op.params.rdma_bytes = &bytes;
  op.params.rdma_crc64 = &crc64;
  op.params.rdma_crc32c = &crc32c;

  ASSERT_EQ(0, op.iterate(nullptr, 0, 4095, nullptr, null_yield));
  EXPECT_EQ(TOKEN, m->token);
  EXPECT_EQ(&bytes, m->bytes);
  EXPECT_EQ(&crc64, m->crc64);
  EXPECT_EQ(&crc32c, m->crc32c);
  EXPECT_EQ(4096u, bytes);
  ASSERT_TRUE(crc64);
  EXPECT_EQ(0x0123456789abcdefull, *crc64);
  ASSERT_TRUE(crc32c);
  EXPECT_EQ(0x89abcdefu, *crc32c);
  EXPECT_TRUE(op.params.rdma_submitted);
}

// a GET asking for one CRC only leaves the other pointer null, and the
// fallback after an -EOPNOTSUPP clears them all and iterates again: the
// store must see that, not the pointers of the earlier pass
TEST(FilterReadOp, IterateForwardsClearedRdmaParams)
{
  auto mock = std::make_unique<MockReadOp>();
  MockReadOp* m = mock.get();
  FilterObject::FilterReadOp op(std::move(mock));
  ASSERT_EQ(0, op.prepare(null_yield, nullptr));

  uint64_t bytes = 0;
  std::optional<uint32_t> crc32c;
  op.params.rdma_token = TOKEN;
  op.params.rdma_bytes = &bytes;
  op.params.rdma_crc32c = &crc32c;
  ASSERT_EQ(0, op.iterate(nullptr, 0, 4095, nullptr, null_yield));
  EXPECT_EQ(nullptr, m->crc64);
  EXPECT_EQ(&crc32c, m->crc32c);

  op.params.rdma_token.clear();
  op.params.rdma_bytes = nullptr;
  op.params.rdma_crc64 = nullptr;
  op.params.rdma_crc32c = nullptr;
  ASSERT_EQ(0, op.iterate(nullptr, 0, 4095, nullptr, null_yield));
  EXPECT_EQ(2, m->iterated);
  EXPECT_TRUE(m->token.empty());
  EXPECT_EQ(nullptr, m->bytes);
  EXPECT_EQ(nullptr, m->crc64);
  EXPECT_EQ(nullptr, m->crc32c);
}

// the filter can pull exactly when the store below it can
TEST(FilterWriter, SetPulledForwards)
{
  {
    auto mock = std::make_unique<MockWriter>();
    MockWriter* m = mock.get();
    FilterWriter w(std::move(mock), nullptr);
    EXPECT_TRUE(w.set_pulled());
    EXPECT_TRUE(m->pulled);
  }
  {
    auto mock = std::make_unique<MockWriter>();
    mock->can_pull = false;
    FilterWriter w(std::move(mock), nullptr);
    EXPECT_FALSE(w.set_pulled());
  }
}

TEST(FilterWriter, ProcessPulledForwards)
{
  auto mock = std::make_unique<MockWriter>();
  MockWriter* m = mock.get();
  FilterWriter w(std::move(mock), nullptr);
  ASSERT_TRUE(w.set_pulled());

  const uint32_t want = librados::ObjectOperation::RDMA_DELIVERY_WANT_CRC64;
  std::vector<PulledStripe> stripes;
  ASSERT_EQ(0, w.process_pulled(1 << 20, TOKEN, want, &stripes));
  EXPECT_EQ(uint64_t(1 << 20), m->len);
  EXPECT_EQ(TOKEN, m->token);
  EXPECT_EQ(want, m->want);
  ASSERT_EQ(1u, stripes.size());
  EXPECT_EQ(uint64_t(1 << 20), stripes[0].bytes);
  EXPECT_TRUE(stripes[0].crc64_valid);
}

#ifdef WITH_RADOSGW_D4N
// D4N caches what it writes, so it must see the bytes, even when the store
// below it could pull them
TEST(D4NFilterWriter, DeclinesPulled)
{
  auto mock = std::make_unique<MockWriter>();
  MockWriter* m = mock.get();
  rgw::sal::D4NFilterWriter w(std::move(mock), nullptr, nullptr, nullptr,
                              null_yield);
  EXPECT_FALSE(w.set_pulled());
  EXPECT_FALSE(m->pulled);
  std::vector<PulledStripe> stripes;
  EXPECT_EQ(-EOPNOTSUPP, w.process_pulled(4096, TOKEN, 0, &stripes));
  EXPECT_EQ(0u, m->len);
  EXPECT_TRUE(stripes.empty());
}
#endif
