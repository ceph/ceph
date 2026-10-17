//#include "common/config.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"

#include "gtest/gtest.h"

TEST(Librados, CreateShutdown) {
  rados_t cluster;
  int err;
  err = rados_create(&cluster, "someid");
  EXPECT_EQ(err, 0);

  rados_shutdown(cluster);
}

namespace {
using result_t = librados::ObjectOperation::rdma_delivery_result;

/// a result as an earlier use of it left it
result_t used_result()
{
  result_t r;
  r.bytes = 42;
  r.crc64 = 0x1234;
  r.crc32c = 0x5678;
  r.flags = librados::ObjectOperation::rdma_delivery_result::DECLINED |
    librados::ObjectOperation::rdma_delivery_result::LANDED;
  r.ranges.push_back({0, 42, 0x1234, 0x5678});
  return r;
}

void expect_reset(const result_t& r)
{
  EXPECT_EQ(0u, r.bytes);
  EXPECT_EQ(0u, r.crc64);
  EXPECT_EQ(0u, r.crc32c);
  EXPECT_EQ(0u, r.flags);
  EXPECT_TRUE(r.ranges.empty());
}
} // anonymous namespace

// Only the reply fills in an rdma delivery result. Setting up the op
// resets it, so that an op that gets no reply, as one that timed out or
// was cancelled, does not leave the flags of an earlier use, which could
// read as declined or landed.
TEST(LibradosRdmaDelivery, SetUpResetsTheResult) {
  const std::string token = "t";
  {
    result_t r = used_result();
    librados::ObjectReadOperation op;
    op.read(0, 4096, nullptr, nullptr);
    op.set_rdma_delivery(token, 0, 0, &r);
    expect_reset(r);
  }
  {
    result_t r = used_result();
    librados::ObjectWriteOperation op;
    op.write_full_pulled(4096, token, 0, 0, &r);
    expect_reset(r);
  }
  {
    result_t r = used_result();
    librados::ObjectWriteOperation op;
    op.write_pulled(4096, 4096, token, 0, 0, &r);
    expect_reset(r);
  }
  // and a NULL result is still taken
  librados::ObjectReadOperation op;
  op.read(0, 4096, nullptr, nullptr);
  op.set_rdma_delivery(token, 0, 0, nullptr);
}

// A request for delivery with no read before it asks for nothing: it
// adds no op, and does not abort the caller.
TEST(LibradosRdmaDelivery, NoOpRequestsNothing) {
  result_t r = used_result();
  librados::ObjectReadOperation op;
  op.set_rdma_delivery("t", 0, 0, &r);
  op.set_rdma_delivery("t", 0, 0, nullptr);
  EXPECT_EQ(0u, op.size());
  expect_reset(r);
  // the operation is as good as one that never asked
  op.read(0, 4096, nullptr, nullptr);
  op.set_rdma_delivery("t", 0, 0, &r);
  EXPECT_EQ(1u, op.size());
}
