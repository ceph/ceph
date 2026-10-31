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

#include "common/oob_arena.h"
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

// Construct only the server's metadata, with an arena to stage in and a
// channel cached for the thread. The production parser, acquisition,
// release, and transfer helpers run without constructing a cuObjServer
// or registering memory.
//
// Changed since the pool of fixed-size buffers that these tests were
// written for: the gateway now stages each transfer in a buffer of its
// out-of-band arena (common/oob_arena.h), a buddy allocator that hands
// out powers of two from Arena::MIN to Arena::MAX, and a thread takes a
// buffer only when it has a cuObject channel. The pool tests below keep
// their intent (a buffer has one owner, is reused once released, and is
// at least the size asked for) with the arena's sizes.
class RGWCuObjServerTest : public ::testing::Test {
protected:
  using Arena = ceph::oob::Arena;
  std::unique_ptr<RGWCuObjServer> server;
  std::shared_ptr<Arena> arena;

  /// give the calling thread a cuObject channel, which acquire_buffer()
  /// asks for
  static void set_channel(uint16_t id) {
    RGWCuObjServer::tls_channel_valid = true;
    RGWCuObjServer::tls_channel_id = id;
  }

  void SetUp() override {
    server.reset(new RGWCuObjServer);
    server->m_cct = g_ceph_context;
    set_channel(0);
  }
  void TearDown() override {
    RGWCuObjServer::tls_channel_valid = false;
    RGWCuObjServer::tls_channel_id = 0;
    server.reset();
    arena.reset();
  }
  /// an arena of blocks blocks of Arena::MAX bytes to stage in
  void set_arena(uint64_t blocks) {
    std::string err;
    arena = Arena::create(blocks * Arena::MAX, &err);
    ASSERT_TRUE(arena) << err;
    server->m_arena = arena.get();
  }

  ssize_t transfer(const std::string& token, bool write) {
    // A cached channel and a zero-length transfer exercise address validation
    // without calling the SDK, even if a malformed address is wrongly accepted.
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

TEST_F(RGWCuObjServerTest, FullArenaHasNoBuffers)
{
  // was EmptyPoolHasNoBuffers: an arena always has a block, so it is the
  // arena whose block is taken that has none to give
  set_arena(1);
  auto* whole = server->acquire_buffer(Arena::MAX);
  ASSERT_NE(whole, nullptr);
  EXPECT_EQ(server->acquire_buffer(1), nullptr);
  server->release_buffer(whole);
  server->release_buffer(nullptr);
}

TEST_F(RGWCuObjServerTest, SingleBufferIsExclusiveAndReusable)
{
  set_arena(1);
  auto* buffer = server->acquire_buffer(Arena::MAX);
  ASSERT_NE(buffer, nullptr);
  EXPECT_EQ(buffer->ptr, arena->base());
  EXPECT_EQ(buffer->size, Arena::MAX);
  EXPECT_EQ(server->acquire_buffer(1), nullptr);
  server->release_buffer(buffer);
  buffer = server->acquire_buffer(Arena::MAX);
  ASSERT_NE(buffer, nullptr);
  EXPECT_EQ(buffer->ptr, arena->base());
  server->release_buffer(buffer);
  // nothing larger than a block
  EXPECT_EQ(server->acquire_buffer(Arena::MAX + 1), nullptr);
  EXPECT_EQ(0u, arena->stats().in_use);
}

TEST_F(RGWCuObjServerTest, BuffersAreAtLeastTheSizeAskedFor)
{
  // was SkipsUndersizedBuffers: the arena has no buffers of fixed sizes
  // to skip, and rounds each one up to a power of two of at least MIN
  set_arena(1);
  auto* small = server->acquire_buffer(16);
  auto* odd = server->acquire_buffer(3 * Arena::MIN);
  ASSERT_NE(small, nullptr);
  ASSERT_NE(odd, nullptr);
  EXPECT_EQ(small->size, Arena::MIN);
  EXPECT_EQ(odd->size, 4 * Arena::MIN);
  // apart from each other, inside the arena
  const auto* a = static_cast<const char*>(small->ptr);
  const auto* b = static_cast<const char*>(odd->ptr);
  EXPECT_TRUE(a + small->size <= b || b + odd->size <= a);
  EXPECT_LE(b + odd->size, arena->base() + arena->size());
  server->release_buffer(small);
  server->release_buffer(odd);
  EXPECT_EQ(0u, arena->stats().in_use);
}

TEST_F(RGWCuObjServerTest, FindsTheOnlyFreeBufferAcrossTheWholeArena)
{
  // was ...AcrossTheWholePool: every MIN buffer of a block taken but one
  set_arena(1);
  constexpr size_t count = Arena::MAX / Arena::MIN;
  std::vector<RGWCuObjServer::RDMABufEntry*> buffers;
  for (size_t i = 0; i < count; ++i) {
    buffers.push_back(server->acquire_buffer(Arena::MIN));
    ASSERT_NE(buffers.back(), nullptr);
  }
  EXPECT_EQ(server->acquire_buffer(Arena::MIN), nullptr);
  for (size_t free_index : {size_t(0), count / 2, count - 1}) {
    SCOPED_TRACE(free_index);
    void* const free_ptr = buffers[free_index]->ptr;
    server->release_buffer(buffers[free_index]);
    for (unsigned attempt = 0; attempt < 4; ++attempt) {
      auto* buffer = server->acquire_buffer(Arena::MIN);
      ASSERT_NE(buffer, nullptr);
      EXPECT_EQ(buffer->ptr, free_ptr);
      if (attempt < 3) {
        server->release_buffer(buffer);
      } else {
        buffers[free_index] = buffer;
      }
    }
  }
  for (auto* buffer : buffers) {
    server->release_buffer(buffer);
  }
  EXPECT_EQ(0u, arena->stats().in_use);
}

class CuObjConcurrentArena : public RGWCuObjServerTest,
                            public ::testing::WithParamInterface<size_t> {};

TEST_P(CuObjConcurrentArena, ClaimsAllAvailableBuffersWithoutDoubleOwnership)
{
  // was CuObjConcurrentPool: count buffers that fill one block of the
  // arena between them, so that count of them can be held at once
  constexpr size_t threads = 16;
  constexpr size_t rounds = 64;
  const size_t count = GetParam();
  const uint64_t size = Arena::MAX / count;
  set_arena(1);
  std::vector<std::atomic<unsigned>> owners(count);
  std::vector<std::atomic<unsigned>> successes(rounds);
  std::atomic<unsigned> violations{0};
  std::barrier phase(threads);
  std::vector<std::thread> workers;
  for (size_t thread = 0; thread < threads; ++thread) {
    workers.emplace_back([&, thread] {
      // every thread of the gateway has a channel of its own
      set_channel(thread);
      for (size_t round = 0; round < rounds; ++round) {
        phase.arrive_and_wait();
        auto* buffer = server->acquire_buffer(size);
        size_t index = 0;
        if (buffer) {
          index = (static_cast<char*>(buffer->ptr) - arena->base()) / size;
          if (buffer->size != size || index >= count ||
              owners[index].exchange(thread + 1) != 0) {
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
  EXPECT_EQ(0u, arena->stats().in_use);
}

// the arena's buffers are powers of two: 1, 4, 16 and 64 of them fill a
// block, as 1, 7, 16 and 33 buffers made up the pools before
INSTANTIATE_TEST_SUITE_P(BufferCounts, CuObjConcurrentArena,
                        ::testing::Values(1, 4, 16, 64));
