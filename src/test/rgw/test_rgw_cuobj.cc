// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <algorithm>
#include <atomic>
#include <barrier>
#include <cerrno>
#include <limits>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <cuobjserver.h>
#include <gtest/gtest.h>

#include "global/global_context.h"
#include "rgw_cuobj.h"

namespace {
std::vector<std::string> invalid_hex_fields()
{
  return {"", "xyz", "10xyz", "+1", "-1", " 10", "10 ", "10\n", "0x10",
          std::string("10\0f", 4), std::string(512, 'f')};
}

std::string size_token(const std::string& size)
{
  return "1234:" + size + ":1:0:1:0:0";
}

class CuObjInvalidSize : public ::testing::TestWithParam<std::string> {};

TEST_P(CuObjInvalidSize, ReturnsZeroWithoutThrowing)
{
  EXPECT_EQ(RGWCuObjServer::parse_rdma_descriptor_size(size_token(GetParam())), 0);
}

INSTANTIATE_TEST_SUITE_P(MalformedHex, CuObjInvalidSize,
                        ::testing::ValuesIn(invalid_hex_fields()));

TEST(CuObjDescriptorSize, AcceptsHexadecimalSizes)
{
  for (const auto& [field, expected] :
       std::vector<std::pair<std::string, size_t>>{
         {"0", 0}, {"1", 1}, {"10", 16}, {"800000", 8 * 1024 * 1024},
         {"abcdef", 0xabcdef}, {"ABCDEF", 0xabcdef}, {"00010", 16}}) {
    SCOPED_TRACE(field);
    EXPECT_EQ(RGWCuObjServer::parse_rdma_descriptor_size(size_token(field)),
              expected);
  }
  std::ostringstream max_hex;
  max_hex << std::hex << std::numeric_limits<size_t>::max();
  EXPECT_EQ(RGWCuObjServer::parse_rdma_descriptor_size(size_token(max_hex.str())),
            std::numeric_limits<size_t>::max());
}

TEST(CuObjDescriptorSize, RejectsOverflowAndMissingSeparators)
{
  const auto overflow = "1" + std::string(2 * sizeof(size_t), '0');
  for (const std::string& token : {std::string{}, std::string{"1234"},
                                  std::string{"1234:10"}, size_token(overflow)}) {
    SCOPED_TRACE(token);
    EXPECT_EQ(RGWCuObjServer::parse_rdma_descriptor_size(token), 0);
  }
}
} // anonymous namespace

// Construct only pool metadata. The production parser, acquisition, release,
// and transfer helpers run without constructing a cuObjServer or registering memory.
class RGWCuObjServerTest : public ::testing::Test {
protected:
  std::unique_ptr<RGWCuObjServer> server;

  void SetUp() override {
    server.reset(new RGWCuObjServer);
    server->m_cct = g_ceph_context;
  }
  void TearDown() override {
    RGWCuObjServer::tls_channel_valid = false;
    RGWCuObjServer::tls_channel_id = 0;
    server.reset();
  }
  void set_pool(const std::vector<size_t>& sizes) {
    server->m_buf_count = sizes.size();
    server->m_buffer_pool =
        std::make_unique<RGWCuObjServer::RDMABufEntry[]>(sizes.size());
    for (size_t i = 0; i < sizes.size(); ++i) {
      entry(i).size = sizes[i];
    }
  }
  RGWCuObjServer::RDMABufEntry& entry(size_t index) {
    return server->m_buffer_pool[index];
  }

  ssize_t transfer(const std::string& token, bool write) {
    // A cached channel and a zero-length transfer exercise address validation
    // without calling the SDK, even if a malformed address is wrongly accepted.
    RGWCuObjServer::tls_channel_valid = true;
    RGWCuObjServer::RDMABufEntry buffer;
    if (write) {
      return server->rdma_write_to_client("test", &buffer, 0, 0, token);
    }
    return server->rdma_read_from_client("test", &buffer, 0, 0, token);
  }
};

class CuObjInvalidAddress : public RGWCuObjServerTest,
                           public ::testing::WithParamInterface<std::string> {};

TEST_P(CuObjInvalidAddress, BothTransferDirectionsReturnInvalidArgument)
{
  const auto token = GetParam() + ":10:1:0:1:0:0";
  EXPECT_EQ(transfer(token, false), -EINVAL);
  EXPECT_EQ(transfer(token, true), -EINVAL);
}

INSTANTIATE_TEST_SUITE_P(MalformedHex, CuObjInvalidAddress,
                        ::testing::ValuesIn(invalid_hex_fields()));

TEST_F(RGWCuObjServerTest, AcceptsHexadecimalAddresses)
{
  for (const std::string field : {"0", "1", "1234", "abcdef", "ABCDEF",
                                 "00010", "ffffffffffffffff"}) {
    SCOPED_TRACE(field);
    const auto token = field + ":10:1:0:1:0:0";
    EXPECT_EQ(transfer(token, false), 0);
    EXPECT_EQ(transfer(token, true), 0);
  }
}

TEST_F(RGWCuObjServerTest, RejectsAddressOverflowAndMissingSeparator)
{
  for (const std::string token : {"", "1234", "10000000000000000:10:1:0:1:0:0"}) {
    SCOPED_TRACE(token);
    EXPECT_EQ(transfer(token, false), -EINVAL);
    EXPECT_EQ(transfer(token, true), -EINVAL);
  }
}

TEST_F(RGWCuObjServerTest, EmptyPoolHasNoBuffers)
{
  EXPECT_EQ(server->acquire_buffer(1), nullptr);
  server->release_buffer(nullptr);
}

TEST_F(RGWCuObjServerTest, SingleBufferIsExclusiveAndReusable)
{
  set_pool({16});
  auto* buffer = server->acquire_buffer(16);
  ASSERT_EQ(buffer, &entry(0));
  EXPECT_TRUE(buffer->in_use.load());
  EXPECT_EQ(server->acquire_buffer(1), nullptr);
  server->release_buffer(buffer);
  EXPECT_FALSE(buffer->in_use.load());
  EXPECT_EQ(server->acquire_buffer(16), buffer);
  server->release_buffer(buffer);
  EXPECT_EQ(server->acquire_buffer(17), nullptr);
}

TEST_F(RGWCuObjServerTest, SkipsUndersizedBuffers)
{
  set_pool({4, 8, 16, 32});
  auto* first = server->acquire_buffer(16);
  auto* second = server->acquire_buffer(16);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_NE(first, second);
  EXPECT_GE(first->size, 16);
  EXPECT_GE(second->size, 16);
  EXPECT_EQ(server->acquire_buffer(16), nullptr);
  EXPECT_FALSE(entry(0).in_use.load());
  EXPECT_FALSE(entry(1).in_use.load());
  server->release_buffer(first);
  server->release_buffer(second);
}

TEST_F(RGWCuObjServerTest, FindsTheOnlyFreeBufferAcrossTheWholePool)
{
  set_pool(std::vector<size_t>(7, 16));
  for (size_t free_index = 0; free_index < 7; ++free_index) {
    SCOPED_TRACE(free_index);
    for (size_t i = 0; i < 7; ++i) {
      entry(i).in_use.store(i != free_index);
    }
    // The result must be the same for every randomly chosen starting point.
    for (unsigned attempt = 0; attempt < 32; ++attempt) {
      auto* buffer = server->acquire_buffer(16);
      ASSERT_EQ(buffer, &entry(free_index));
      server->release_buffer(buffer);
    }
  }
}

class CuObjConcurrentPool : public RGWCuObjServerTest,
                           public ::testing::WithParamInterface<size_t> {};

TEST_P(CuObjConcurrentPool, ClaimsAllAvailableBuffersWithoutDoubleOwnership)
{
  constexpr size_t threads = 16;
  constexpr size_t rounds = 64;
  const size_t count = GetParam();
  set_pool(std::vector<size_t>(count, 16));
  std::vector<std::atomic<unsigned>> owners(count);
  std::vector<std::atomic<unsigned>> successes(rounds);
  std::atomic<unsigned> violations{0};
  std::barrier phase(threads);
  std::vector<std::thread> workers;
  for (size_t thread = 0; thread < threads; ++thread) {
    workers.emplace_back([&, thread] {
      for (size_t round = 0; round < rounds; ++round) {
        phase.arrive_and_wait();
        auto* buffer = server->acquire_buffer(16);
        size_t index = 0;
        if (buffer) {
          index = buffer - &entry(0);
          if (owners[index].exchange(thread + 1) != 0) {
            ++violations;
          }
          ++successes[round];
        }
        // Keep successful claims until every thread has finished acquiring.
        phase.arrive_and_wait();
        if (buffer) {
          owners[index].store(0);
          server->release_buffer(buffer);
        }
        phase.arrive_and_wait();
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }
  EXPECT_EQ(violations.load(), 0);
  for (const auto& successful : successes) {
    EXPECT_EQ(successful.load(), std::min(count, threads));
  }
  for (size_t i = 0; i < count; ++i) {
    EXPECT_FALSE(entry(i).in_use.load());
  }
}

INSTANTIATE_TEST_SUITE_P(PoolSizes, CuObjConcurrentPool,
                        ::testing::Values(1, 7, 16, 33));
