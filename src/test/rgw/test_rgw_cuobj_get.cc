// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <algorithm>
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

/*
 * A gateway-staged GET (cuObject) from the request to the response.
 *
 * Changed since these tests were written: RGWGetObj::execute() stages
 * the data that the store reads, writes it into the client's window
 * (rdma_write_staged()), and sends the response itself, where
 * send_response_data() did that on a last call from complete(). So each
 * test runs execute() over an object whose reads come from a fake store,
 * instead of feeding send_response_data() and calling complete(). And
 * the staging buffer is reserved before the read: a GET that finds none,
 * or none large enough, goes over HTTP with x-amz-rdma-reply: 501, where
 * it failed with 503.
 */

namespace {

// a cuObject descriptor (raddr:rsize:rkey:lid:qp:has_gid:gid) of a 1 MiB
// window
constexpr const char* TOKEN = "7f0000000000:100000:1a2b:0:4d2:0:0";

struct FakeRdma {
  std::array<char, 16> buffer{};
  RGWCuObjServer::RDMABufEntry entry;
  bool in_use = false;
  bool available = true;
  size_t requested_size = 0;
  size_t acquisitions = 0;
  size_t writes = 0;
  size_t releases = 0;
  std::optional<ssize_t> result;
  std::string payload;
  std::vector<std::string> events;

  void reset() {
    buffer.fill('!');
    entry.ptr = buffer.data();
    entry.size = buffer.size();
    in_use = false;
    available = true;
    requested_size = acquisitions = writes = releases = 0;
    result.reset();
    payload.clear();
    events.clear();
  }
} rdma;

/// what the store hands the GET: each chunk's bytes from ofs, then error
struct FakeReads {
  struct chunk {
    std::string data;
    off_t ofs = 0;
    off_t len = -1;
  };
  std::vector<chunk> chunks;
  int error = 0;
  unsigned iterations = 0;

  void reset() {
    chunks.clear();
    error = 0;
    iterations = 0;
  }
} reads;

class FakeReadOp : public rgw::sal::Object::ReadOp {
public:
  int prepare(optional_yield, const DoutPrefixProvider*) override {
    return 0;
  }
  int read(int64_t, int64_t, bufferlist&, optional_yield,
           const DoutPrefixProvider*) override {
    return -EOPNOTSUPP;
  }
  int iterate(const DoutPrefixProvider*, int64_t, int64_t,
              RGWGetDataCB* cb, optional_yield) override {
    ++reads.iterations;
    // staged: the gateway reads the data, and no OSD gets the token
    EXPECT_TRUE(params.rdma_token.empty());
    for (const auto& c : reads.chunks) {
      bufferlist bl;
      // Deliberately use separate segments to exercise bufferlist offsets.
      for (char ch : c.data) {
        bl.append(buffer::copy(&ch, 1));
      }
      const off_t len = c.len < 0 ? off_t(c.data.size()) - c.ofs : c.len;
      if (int r = cb->handle_data(bl, c.ofs, len); r < 0) {
        return r;
      }
    }
    return reads.error;
  }
  int get_attr(const DoutPrefixProvider*, const char*, bufferlist&,
               optional_yield) override {
    return -ENODATA;
  }
};

class FakeObject : public rgw::sal::RadosObject {
public:
  FakeObject() : RadosObject(nullptr, rgw_obj_key("test")) {}
  std::unique_ptr<ReadOp> get_read_op() override {
    return std::make_unique<FakeReadOp>();
  }
};

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

class TestGet : public RGWGetObj_ObjStore_S3 {};

class CuObjGet : public ::testing::Test {
protected:
  RGWProcessEnv penv;
  RecordingClient client;
  RGWRestfulIO io{g_ceph_context, &client};
  std::unique_ptr<req_state> state;
  std::unique_ptr<TestGet> op;

  void SetUp() override {
    rdma.reset();
    reads.reset();
    ASSERT_EQ(0, RGWCuObjServer::init(g_ceph_context, nullptr));
    client.init(g_ceph_context);
    client.env.set("REQUEST_METHOD", "GET");
    client.env.set("HTTP_X_AMZ_RDMA_TOKEN", TOKEN);
    state = std::make_unique<req_state>(g_ceph_context, penv, &client.env, 0);
    state->cio = &io;
    state->formatter = ceph::Formatter::create("xml");
    state->format = RGWFormat::XML;
    state->prot_flags = RGW_REST_S3;
    state->op = OP_GET;
    state->object = std::make_unique<FakeObject>();
    set_size(4);
    op = std::make_unique<TestGet>();
    op->init(nullptr, state.get(), nullptr);
    op->set_get_data(true);
  }
  void TearDown() override {
    op.reset();
    RGWCuObjServer::shutdown();
  }
  void set_size(uint64_t size) {
    state->object->set_obj_size(size);
  }
  void data(std::string value, off_t offset = 0, off_t length = -1) {
    reads.chunks.push_back({std::move(value), offset, length});
  }
  void get() {
    op->execute(null_yield);
  }
  void expect_error(int status) {
    ASSERT_EQ(client.statuses, std::vector<int>({status}));
    EXPECT_EQ(client.headers.count("x-amz-rdma-reply"), 0);
    EXPECT_EQ(client.headers.count("x-amz-rdma-bytes-transferred"), 0);
  }
};

TEST_F(CuObjGet, SuccessWaitsForOperationAndRdmaCompletion)
{
  data("xab", 1);
  data("");
  data("cdy", 0, 2);
  data("");
  get();
  EXPECT_EQ(rdma.payload, "abcd");
  EXPECT_EQ(rdma.events, std::vector<std::string>({"rdma_complete", "http_status", "http_complete"}));
  EXPECT_EQ(client.statuses, std::vector<int>({200}));
  EXPECT_EQ(client.headers.at("x-amz-rdma-reply"), "200");
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
  data("abcd");
  get();
  EXPECT_EQ(rdma.payload, "abcd");
  EXPECT_EQ(client.statuses, std::vector<int>({200}));
}

TEST_F(CuObjGet, RangeAllocatesOnlyResponseLength)
{
  set_size(1024);
  client.env.set("HTTP_RANGE", "bytes=2-5");
  data("abcd");
  get();
  EXPECT_EQ(rdma.requested_size, 4);
  EXPECT_EQ(client.statuses, std::vector<int>({206}));
  EXPECT_EQ(client.headers.at("x-amz-rdma-reply"), "206");
  EXPECT_EQ(client.headers.at("Content-Range"), "bytes 2-5/1024");
  EXPECT_EQ(client.headers.at("x-amz-rdma-bytes-transferred"), "4");
}

TEST_F(CuObjGet, EmptyObjectNeedsNoWrite)
{
  // was EmptyObjectNeedsNoBuffer: the buffer is now reserved before the
  // read, for an empty object too, and given back with the op
  set_size(0);
  get();
  EXPECT_EQ(rdma.writes, 0);
  EXPECT_EQ(reads.iterations, 0);
  EXPECT_EQ(client.statuses, std::vector<int>({200}));
  EXPECT_EQ(client.headers.at("x-amz-rdma-reply"), "200");
  EXPECT_EQ(client.headers.at("x-amz-rdma-bytes-transferred"), "0");
  op.reset();
  EXPECT_EQ(rdma.acquisitions, rdma.releases);
}

TEST_F(CuObjGet, BufferExhaustionServesTheBodyOverHttp)
{
  // was BufferExhaustionReturnsServiceUnavailable: without a buffer the
  // GET now goes over HTTP, and x-amz-rdma-reply: 501 tells the client so
  rdma.available = false;
  data("abcd");
  get();
  EXPECT_EQ(client.statuses, std::vector<int>({200}));
  EXPECT_EQ(client.headers.at("x-amz-rdma-reply"), "501");
  EXPECT_EQ(client.headers.count("x-amz-rdma-bytes-transferred"), 0);
  EXPECT_EQ(client.content_length, 4);
  EXPECT_EQ(client.body, "abcd");
  EXPECT_EQ(rdma.acquisitions, 1);
  EXPECT_EQ(rdma.writes, 0);
  EXPECT_EQ(state->rdma_bytes_transferred, 0);
}

TEST_F(CuObjGet, RdmaFailureReturnsErrorBeforeSuccess)
{
  rdma.result = -EIO;
  data("abcd");
  get();
  expect_error(500);
  EXPECT_EQ(rdma.releases, 1);
  EXPECT_EQ(state->rdma_bytes_transferred, 0);
}

TEST_F(CuObjGet, InvalidAddressReturnsInvalidArgument)
{
  rdma.result = -EINVAL;
  data("abcd");
  get();
  expect_error(400);
  EXPECT_NE(client.body.find("<Code>InvalidArgument</Code>"), std::string::npos);
  EXPECT_EQ(rdma.releases, 1);
  EXPECT_EQ(state->rdma_bytes_transferred, 0);
}

TEST_F(CuObjGet, OversizedResponseServesTheBodyOverHttp)
{
  // was OversizedResponseReturnsServiceUnavailable: a response that no
  // buffer holds now goes over HTTP too, with x-amz-rdma-reply: 501
  const std::string body(rdma.buffer.size() + 1, 'z');
  set_size(body.size());
  data(body);
  get();
  EXPECT_EQ(client.statuses, std::vector<int>({200}));
  EXPECT_EQ(client.headers.at("x-amz-rdma-reply"), "501");
  EXPECT_EQ(client.body, body);
  EXPECT_EQ(rdma.requested_size, rdma.buffer.size() + 1);
  EXPECT_EQ(rdma.writes, 0);
  EXPECT_EQ(rdma.releases, 0);
}

class CuObjGetWriteCount : public CuObjGet,
                         public ::testing::WithParamInterface<ssize_t> {};

TEST_P(CuObjGetWriteCount, InvalidCompletionReturnsError)
{
  rdma.result = GetParam();
  data("abcd");
  get();
  expect_error(500);
  EXPECT_EQ(rdma.releases, 1);
  // what reached the client's window is charged, a short write too, but
  // no more than the 4 bytes the write held
  EXPECT_EQ(state->rdma_bytes_transferred, std::min<ssize_t>(GetParam(), 4));
}

INSTANTIATE_TEST_SUITE_P(CompletionCounts, CuObjGetWriteCount,
                        ::testing::Values(0, 1, 2, 3, 5));

TEST_F(CuObjGet, TruncatedReadReturnsErrorWithoutRdma)
{
  data("ab");
  get();
  expect_error(500);
  EXPECT_EQ(rdma.writes, 0);
  op.reset();
  EXPECT_EQ(rdma.releases, 1);
}

TEST_F(CuObjGet, OversizedDataDoesNotOverrunBuffer)
{
  data("abcde");
  get();
  expect_error(500);
  EXPECT_EQ(rdma.buffer.front(), '!');
  EXPECT_EQ(rdma.writes, 0);
}

TEST_F(CuObjGet, FilterFailureAfterDataReturnsErrorWithoutRdma)
{
  // the store fails after it handed over the data
  data("abcd");
  reads.error = -EIO;
  get();
  expect_error(500);
  EXPECT_EQ(rdma.writes, 0);
  op.reset();
  EXPECT_EQ(rdma.releases, 1);
}

TEST_F(CuObjGet, HeadPreservesContentLengthWithoutRdma)
{
  op->set_get_data(false);
  state->op = OP_HEAD;
  client.env.set("REQUEST_METHOD", "HEAD");
  get();
  EXPECT_EQ(client.statuses, std::vector<int>({200}));
  EXPECT_EQ(client.content_length, 4);
  EXPECT_TRUE(client.body.empty());
  EXPECT_EQ(client.headers.count("x-amz-rdma-reply"), 0);
  EXPECT_EQ(client.headers.count("x-amz-rdma-bytes-transferred"), 0);
  EXPECT_EQ(rdma.writes, 0);
  EXPECT_EQ(rdma.acquisitions, 0);
}

TEST_F(CuObjGet, HttpGetStillStreams)
{
  client.env.remove("HTTP_X_AMZ_RDMA_TOKEN");
  data("ab");
  data("cd");
  get();
  EXPECT_EQ(client.statuses, std::vector<int>({200}));
  EXPECT_EQ(client.body, "abcd");
  EXPECT_EQ(client.content_length, 4);
  EXPECT_EQ(client.headers.count("x-amz-rdma-reply"), 0);
  EXPECT_EQ(rdma.writes, 0);
  EXPECT_EQ(rdma.acquisitions, 0);
}
} // anonymous namespace

// Link-time fake: exercise the real S3 GET without initializing RDMA
// hardware. These definitions replace rgw_cuobj.cc in this test executable.
std::atomic<RGWCuObjServer*> RGWCuObjServer::s_instance{nullptr};
RGWCuObjServer::~RGWCuObjServer() = default;
int RGWCuObjServer::init(CephContext* cct, ceph::oob::Arena*) {
  auto* srv = new RGWCuObjServer;
  srv->m_cct = cct;
  s_instance.store(srv);
  return 0;
}
void RGWCuObjServer::shutdown() { delete s_instance.exchange(nullptr); }
RGWCuObjServer* RGWCuObjServer::get_instance() { return s_instance.load(); }
bool RGWCuObjServer::is_available() { return true; }
RGWCuObjServer::RDMABufEntry* RGWCuObjServer::acquire_buffer(size_t size) {
  ++rdma.acquisitions;
  rdma.requested_size = size;
  if (!rdma.available || size > rdma.entry.size || rdma.in_use) {
    return nullptr;
  }
  rdma.in_use = true;
  return &rdma.entry;
}
void RGWCuObjServer::release_buffer(RDMABufEntry* entry) {
  EXPECT_EQ(entry, &rdma.entry);
  EXPECT_TRUE(rdma.in_use);
  rdma.in_use = false;
  ++rdma.releases;
}
ssize_t RGWCuObjServer::rdma_write_to_client(const std::string&, RDMABufEntry* entry,
                                           uint64_t, size_t size, const std::string&) {
  EXPECT_TRUE(rdma.events.empty());
  ++rdma.writes;
  const ssize_t result = rdma.result.value_or(size);
  if (result > 0) {
    rdma.payload.assign(static_cast<char*>(entry->ptr),
                        std::min<size_t>(result, entry->size));
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
