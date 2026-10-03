// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <array>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <cuobjserver.h>
#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "common/Formatter.h"
#include "global/global_context.h"
#include "rgw_client_io.h"
#include "rgw_cuobj.h"
#include "rgw_process_env.h"
#include "rgw_rest_s3.h"
#include "driver/rados/rgw_sal_rados.h"

namespace {
struct FakeRdma {
  std::array<char, 16> buffer{};
  RGWCuObjServer::RDMABufEntry entry;
  bool available = true;
  size_t requested_size = 0;
  size_t writes = 0;
  size_t releases = 0;
  std::optional<ssize_t> result;
  std::string payload;
  std::vector<std::string> events;

  void reset() {
    buffer.fill('!');
    entry.ptr = buffer.data();
    entry.size = buffer.size();
    entry.in_use = false;
    available = true;
    requested_size = writes = releases = 0;
    result.reset();
    payload.clear();
    events.clear();
  }
} rdma;

class RecordingClient : public rgw::io::RestfulClient {
public:
  RGWEnv env;
  std::vector<int> statuses;
  std::map<std::string, std::string> headers;
  std::string body;
  uint64_t content_length = 0;
  int init_env(CephContext* cct) override { env.init(cct); return 0; }
  RGWEnv& get_env() noexcept override { return env; }
  size_t complete_request() override { return 0; }
  size_t send_100_continue() override { return 0; }
  size_t send_status(int status, const char*) override {
    statuses.push_back(status);
    rdma.events.push_back("http_status");
    return 0;
  }
  size_t send_header(const std::string_view& name,
                     const std::string_view& value) override {
    headers.emplace(name, value);
    return 0;
  }
  size_t send_content_length(uint64_t len) override {
    content_length = len;
    return 0;
  }
  size_t complete_header() override {
    rdma.events.push_back("http_complete");
    return 0;
  }
  size_t recv_body(char*, size_t) override { return 0; }
  size_t send_body(const char* data, size_t len) override {
    body.append(data, len);
    return len;
  }
  void flush() override {}
};

class TestGet : public RGWGetObj_ObjStore_S3 {
public:
  void setup(uint64_t size, bool use_rdma = true, bool data = true) {
    total_len = size;
    get_data = data;
    rdma_active = use_rdma;
    rdma_descriptor = "test descriptor";
    op_ret = 0;
  }
  void fail(int error) {
    op_ret = error;
    send_response_data_error(null_yield);
  }
  void range(uint64_t first, uint64_t last) {
    partial_content = true;
    range_str = "bytes=2-5";
    start = first;
    end = last;
  }
};

class CuObjGet : public ::testing::Test {
protected:
  RGWProcessEnv penv;
  RecordingClient client;
  RGWRestfulIO io{g_ceph_context, &client};
  std::unique_ptr<req_state> state;
  std::unique_ptr<TestGet> op;

  void SetUp() override {
    rdma.reset();
    ASSERT_EQ(0, RGWCuObjServer::init(g_ceph_context));
    client.init(g_ceph_context);
    client.env.set("REQUEST_METHOD", "GET");
    state = std::make_unique<req_state>(g_ceph_context, penv, &client.env, 0);
    state->cio = &io;
    state->formatter = ceph::Formatter::create("xml");
    state->format = RGWFormat::XML;
    state->prot_flags = RGW_REST_S3;
    state->object = std::make_unique<rgw::sal::RadosObject>(nullptr, rgw_obj_key("test"));
    state->obj_size = 4;
    op = std::make_unique<TestGet>();
    op->init(nullptr, state.get(), nullptr);
    op->setup(4);
  }
  void TearDown() override {
    op.reset();
    RGWCuObjServer::shutdown();
  }
  int data(std::string_view value, off_t offset = 0, off_t length = -1) {
    bufferlist bl;
    // Deliberately use separate segments to exercise bufferlist offsets.
    for (char c : value) {
      bl.append(buffer::copy(&c, 1));
    }
    return op->send_response_data(bl, offset, length < 0 ? value.size() - offset : length);
  }
  void expect_error(int status) {
    ASSERT_EQ(client.statuses, std::vector<int>({status}));
    EXPECT_EQ(client.headers.count("x-amz-rdma-reply"), 0);
    EXPECT_EQ(client.headers.count("x-amz-rdma-bytes-transferred"), 0);
  }
};

TEST_F(CuObjGet, SuccessWaitsForOperationAndRdmaCompletion)
{
  ASSERT_EQ(0, data("xab", 1));
  ASSERT_EQ(0, data(""));
  ASSERT_EQ(0, data("cdy", 0, 2));
  ASSERT_EQ(0, data(""));
  EXPECT_TRUE(client.statuses.empty());
  EXPECT_EQ(rdma.writes, 0);
  op->complete();
  EXPECT_EQ(rdma.payload, "abcd");
  EXPECT_EQ(rdma.events, std::vector<std::string>({"rdma_complete", "http_status", "http_complete"}));
  EXPECT_EQ(client.statuses, std::vector<int>({200}));
  EXPECT_EQ(client.headers.at("x-amz-rdma-bytes-transferred"), "4");
  EXPECT_EQ(client.content_length, 0);
  EXPECT_TRUE(client.body.empty());
  EXPECT_EQ(state->rdma_bytes_transferred, 4);
  op->complete();
  EXPECT_EQ(rdma.writes, 1);
  EXPECT_EQ(rdma.releases, 1);
  EXPECT_EQ(client.statuses.size(), 1);
}

TEST_F(CuObjGet, CompletesWithoutEofCallback)
{
  ASSERT_EQ(0, data("abcd"));
  op->complete();
  EXPECT_EQ(rdma.payload, "abcd");
  EXPECT_EQ(client.statuses, std::vector<int>({200}));
}

TEST_F(CuObjGet, RangeAllocatesOnlyResponseLength)
{
  state->obj_size = 1024;
  op->range(2, 5);
  ASSERT_EQ(0, data("abcd"));
  op->complete();
  EXPECT_EQ(rdma.requested_size, 4);
  EXPECT_EQ(client.statuses, std::vector<int>({206}));
  EXPECT_EQ(client.headers.at("Content-Range"), "bytes 2-5/1024");
  EXPECT_EQ(client.headers.at("x-amz-rdma-bytes-transferred"), "4");
}

TEST_F(CuObjGet, EmptyObjectNeedsNoBuffer)
{
  op->setup(0);
  ASSERT_EQ(0, data(""));
  EXPECT_TRUE(client.statuses.empty());
  op->complete();
  EXPECT_EQ(rdma.writes, 0);
  EXPECT_EQ(client.statuses, std::vector<int>({200}));
  EXPECT_EQ(client.headers.at("x-amz-rdma-bytes-transferred"), "0");
}

TEST_F(CuObjGet, BufferExhaustionReturnsServiceUnavailable)
{
  rdma.available = false;
  ASSERT_EQ(-EBUSY, data("abcd"));
  EXPECT_TRUE(client.statuses.empty());
  op->fail(-EBUSY);
  op->complete();
  expect_error(503);
  EXPECT_EQ(rdma.writes, 0);
}

TEST_F(CuObjGet, RdmaFailureReturnsErrorBeforeSuccess)
{
  rdma.result = -EIO;
  ASSERT_EQ(0, data("abcd"));
  op->complete();
  expect_error(500);
  EXPECT_EQ(rdma.releases, 1);
  EXPECT_EQ(state->rdma_bytes_transferred, 0);
}

TEST_F(CuObjGet, InvalidAddressReturnsInvalidArgument)
{
  rdma.result = -EINVAL;
  ASSERT_EQ(0, data("abcd"));
  op->complete();
  expect_error(400);
  EXPECT_NE(client.body.find("<Code>InvalidArgument</Code>"), std::string::npos);
  EXPECT_EQ(rdma.releases, 1);
  EXPECT_EQ(state->rdma_bytes_transferred, 0);
}

TEST_F(CuObjGet, OversizedResponseReturnsServiceUnavailable)
{
  state->obj_size = rdma.buffer.size() + 1;
  op->setup(state->obj_size);
  ASSERT_EQ(-EBUSY, data("abcd"));
  EXPECT_TRUE(client.statuses.empty());
  op->fail(-EBUSY);
  expect_error(503);
  EXPECT_EQ(rdma.requested_size, rdma.buffer.size() + 1);
  EXPECT_EQ(rdma.writes, 0);
  EXPECT_EQ(rdma.releases, 0);
}

class CuObjGetWriteCount : public CuObjGet,
                         public ::testing::WithParamInterface<ssize_t> {};

TEST_P(CuObjGetWriteCount, InvalidCompletionReturnsError)
{
  rdma.result = GetParam();
  ASSERT_EQ(0, data("abcd"));
  op->complete();
  expect_error(500);
  EXPECT_EQ(rdma.releases, 1);
  EXPECT_EQ(state->rdma_bytes_transferred, GetParam());
}

INSTANTIATE_TEST_SUITE_P(CompletionCounts, CuObjGetWriteCount,
                        ::testing::Values(0, 1, 2, 3, 5));

TEST_F(CuObjGet, TruncatedReadReturnsErrorWithoutRdma)
{
  ASSERT_EQ(0, data("ab"));
  op->complete();
  expect_error(500);
  EXPECT_EQ(rdma.writes, 0);
  op.reset();
  EXPECT_EQ(rdma.releases, 1);
}

TEST_F(CuObjGet, OversizedDataDoesNotOverrunBuffer)
{
  ASSERT_EQ(-EIO, data("abcde"));
  op->fail(-EIO);
  expect_error(500);
  EXPECT_EQ(rdma.buffer.front(), '!');
  EXPECT_EQ(rdma.writes, 0);
}

TEST_F(CuObjGet, FilterFailureAfterDataReturnsErrorWithoutRdma)
{
  ASSERT_EQ(0, data("abcd"));
  op->fail(-EIO);
  op->complete();
  expect_error(500);
  EXPECT_EQ(rdma.writes, 0);
  op.reset();
  EXPECT_EQ(rdma.releases, 1);
}

TEST_F(CuObjGet, HeadPreservesContentLengthWithoutRdma)
{
  op->setup(4, true, false);
  state->op = OP_HEAD;
  ASSERT_EQ(0, data(""));
  op->complete();
  EXPECT_EQ(client.statuses, std::vector<int>({200}));
  EXPECT_EQ(client.content_length, 4);
  EXPECT_TRUE(client.body.empty());
  EXPECT_EQ(client.headers.count("x-amz-rdma-reply"), 0);
  EXPECT_EQ(client.headers.count("x-amz-rdma-bytes-transferred"), 0);
  EXPECT_EQ(rdma.writes, 0);
}

TEST_F(CuObjGet, HttpGetStillStreams)
{
  op->setup(4, false);
  ASSERT_EQ(0, data("ab"));
  EXPECT_EQ(client.statuses, std::vector<int>({200}));
  EXPECT_EQ(client.body, "ab");
  ASSERT_EQ(0, data("cd"));
  op->complete();
  EXPECT_EQ(client.body, "abcd");
  EXPECT_EQ(client.content_length, 4);
  EXPECT_EQ(client.headers.count("x-amz-rdma-reply"), 0);
  EXPECT_EQ(rdma.writes, 0);
}
} // anonymous namespace

// Link-time fake: exercise the real S3 response code without initializing RDMA
// hardware. These definitions replace rgw_cuobj.cc in this test executable.
std::unique_ptr<RGWCuObjServer> RGWCuObjServer::s_instance;
RGWCuObjServer::~RGWCuObjServer() = default;
int RGWCuObjServer::init(CephContext*) {
  s_instance.reset(new RGWCuObjServer);
  return 0;
}
void RGWCuObjServer::shutdown() { s_instance.reset(); }
RGWCuObjServer* RGWCuObjServer::get_instance() { return s_instance.get(); }
bool RGWCuObjServer::is_available() const { return true; }
RGWCuObjServer::RDMABufEntry* RGWCuObjServer::acquire_buffer(size_t size) {
  rdma.requested_size = size;
  if (!rdma.available || size > rdma.entry.size || rdma.entry.in_use.exchange(true)) {
    return nullptr;
  }
  return &rdma.entry;
}
void RGWCuObjServer::release_buffer(RDMABufEntry* entry) {
  EXPECT_TRUE(entry->in_use.exchange(false));
  ++rdma.releases;
}
ssize_t RGWCuObjServer::rdma_write_to_client(const std::string&, RDMABufEntry* entry,
                                           uint64_t, size_t size, const std::string&) {
  EXPECT_TRUE(rdma.events.empty());
  ++rdma.writes;
  const ssize_t result = rdma.result.value_or(size);
  if (result > 0) {
    rdma.payload.assign(static_cast<char*>(entry->ptr), result);
  }
  rdma.events.push_back("rdma_complete");
  return result;
}
ssize_t RGWCuObjServer::rdma_read_from_client(const std::string&, RDMABufEntry*,
                                            uint64_t, size_t, const std::string&) {
  ADD_FAILURE() << "GET must not read client memory";
  return -EIO;
}
size_t RGWCuObjServer::parse_rdma_descriptor_size(const std::string&) { return 0; }
