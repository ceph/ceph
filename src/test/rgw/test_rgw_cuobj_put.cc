// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <array>
#include <cerrno>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <cuobjserver.h>
#include <gtest/gtest.h>

#include "common/Formatter.h"
#include "global/global_context.h"
#include "rgw_auth.h"
#include "rgw_client_io.h"
#include "rgw_cuobj.h"
#include "rgw_process_env.h"
#include "rgw_rest_s3.h"
#include "rgw_sal_filter.h"
#include "rgw_tracer.h"
#include "rgw_zone.h"
#include "driver/rados/rgw_sal_rados.h"

namespace {
struct FakeRdma {
  std::array<char, 16> data{'a', 'b', 'c', 'd'};
  RGWCuObjServer::RDMABufEntry buffer;
  size_t descriptor_size = 4;
  ssize_t result = 4;
  bool available = true;
  int process_error = 0;
  int complete_error = 0;
  unsigned acquisitions = 0;
  unsigned reads = 0;
  unsigned releases = 0;
  unsigned prepares = 0;
  unsigned processes = 0;
  unsigned completes = 0;
  std::string stored;
  std::vector<std::string> events;

  void reset() {
    descriptor_size = result = 4;
    available = true;
    process_error = complete_error = 0;
    acquisitions = reads = releases = prepares = processes = completes = 0;
    stored.clear();
    events.clear();
    buffer.ptr = data.data();
    buffer.size = data.size();
    buffer.in_use = false;
  }
} rdma;

class FakeWriter : public rgw::sal::Writer {
public:
  int prepare(optional_yield) override { ++rdma.prepares; return 0; }
  int process(bufferlist&& data, uint64_t offset) override {
    ++rdma.processes;
    EXPECT_EQ(offset, rdma.stored.size());
    if (rdma.process_error) {
      return rdma.process_error;
    }
    rdma.stored += data.to_str();
    return 0;
  }
  int complete(size_t size, const std::string&, ceph::real_time*, ceph::real_time,
               std::map<std::string, bufferlist>&,
               const std::optional<rgw::cksum::Cksum>&, ceph::real_time,
               const char*, const char*, const std::string*, rgw_zone_set*,
               bool*, const req_context&, uint32_t) override {
    EXPECT_EQ(size, rdma.stored.size());
    ++rdma.completes;
    rdma.events.push_back("storage_complete");
    return rdma.complete_error;
  }
};

class FakeNotification : public rgw::sal::Notification {
public:
  int publish_reserve(const DoutPrefixProvider*, RGWObjTags*) override { return 0; }
  int publish_commit(const DoutPrefixProvider*, uint64_t, const ceph::real_time&,
                     const std::string&, const std::string&) override { return 0; }
};

class FakeDriver : public rgw::sal::FilterDriver {
public:
  FakeDriver() : FilterDriver(nullptr) {}
  CephContext* ctx() override { return g_ceph_context; }
  std::unique_ptr<rgw::sal::Notification> get_notification(
      rgw::sal::Object*, rgw::sal::Object*, req_state*, rgw::notify::EventType,
      optional_yield, const std::string*) override {
    return std::make_unique<FakeNotification>();
  }
  std::unique_ptr<rgw::sal::Writer> get_atomic_writer(
      const DoutPrefixProvider*, optional_yield, rgw::sal::Object*,
      const ACLOwner&, const rgw_placement_rule*, uint64_t,
      const std::string&) override {
    return std::make_unique<FakeWriter>();
  }
  const std::string& get_compression_type(const rgw_placement_rule&) override {
    static const std::string none = "none";
    return none;
  }
  int get_sync_policy_handler(const DoutPrefixProvider*,
                              std::optional<rgw_zone_id>, std::optional<rgw_bucket>,
                              RGWBucketSyncPolicyHandlerRef*, optional_yield) override {
    return 0;
  }
};

class FakeBucket : public rgw::sal::RadosBucket {
public:
  FakeBucket() : RadosBucket(nullptr, rgw_bucket("", "test-bucket")) {}
  int check_quota(const DoutPrefixProvider*, RGWQuota&, uint64_t,
                  optional_yield, bool) override { return 0; }
};

class FakeObject : public rgw::sal::RadosObject {
public:
  explicit FakeObject(rgw::sal::Bucket* bucket)
    : RadosObject(nullptr, rgw_obj_key("test-object"), bucket) {}
  int swift_versioning_copy(const ACLOwner&, const rgw_user&,
                            const DoutPrefixProvider*, optional_yield) override {
    return 0;
  }
};

class RecordingClient : public rgw::io::RestfulClient {
public:
  RGWEnv env;
  std::vector<int> statuses;
  std::map<std::string, std::string> headers;
  std::string body;
  int init_env(CephContext* cct) override { env.init(cct); return 0; }
  RGWEnv& get_env() noexcept override { return env; }
  size_t complete_request() override { return 0; }
  size_t send_100_continue() override { return 0; }
  size_t send_status(int status, const char*) override {
    statuses.push_back(status);
    rdma.events.push_back("http_status");
    return 0;
  }
  size_t send_header(const std::string_view& key, const std::string_view& value) override {
    headers.emplace(key, value);
    return 0;
  }
  size_t send_content_length(uint64_t) override { return 0; }
  size_t complete_header() override { return 0; }
  size_t recv_body(char*, size_t) override {
    ADD_FAILURE() << "RDMA PUT must not read the HTTP body";
    return 0;
  }
  size_t send_body(const char* data, size_t size) override {
    body.append(data, size);
    return size;
  }
  void flush() override {}
};

class TestPut : public RGWPutObj_ObjStore_S3 {
public:
  int get_encrypt_filter(std::unique_ptr<rgw::sal::DataProcessor>*,
                         rgw::sal::DataProcessor*) override { return 0; }
};

class CuObjPut : public ::testing::Test {
protected:
  FakeDriver driver;
  std::unique_ptr<rgw::SiteConfig> site = rgw::SiteConfig::make_fake();
  RGWProcessEnv penv;
  RecordingClient client;
  RGWRestfulIO io{g_ceph_context, &client};
  std::unique_ptr<req_state> state;
  std::unique_ptr<TestPut> op;

  void SetUp() override {
    rdma.reset();
    ASSERT_EQ(0, RGWCuObjServer::init(g_ceph_context));
    penv.driver = &driver;
    penv.site = site.get();
    ASSERT_EQ(0, client.init(g_ceph_context));
    client.env.set("REQUEST_METHOD", "PUT");
    client.env.set("HTTP_X_AMZ_RDMA_TOKEN", "1234:4:1:0:1:0:0");
    state = std::make_unique<req_state>(g_ceph_context, penv, &client.env, 0);
    state->cio = &io;
    state->formatter = ceph::Formatter::create("xml");
    state->format = RGWFormat::XML;
    state->prot_flags = RGW_REST_S3;
    state->op = OP_PUT;
    state->op_type = RGW_OP_PUT_OBJ;
    state->length = "4";
    state->content_length = 4;
    state->bucket_exists = true;
    state->bucket = std::make_unique<FakeBucket>();
    state->object = std::make_unique<FakeObject>(state->bucket.get());
    state->user = std::make_unique<rgw::sal::RadosUser>(nullptr, rgw_user("test"));
    state->auth.identity = std::make_unique<rgw::auth::LocalApplier>(
        g_ceph_context, state->user->clone(), std::nullopt,
        std::vector<rgw::IAM::Policy>{}, rgw::auth::LocalApplier::NO_SUBUSER,
        std::nullopt, rgw::auth::LocalApplier::NO_ACCESS_KEY);
    state->owner.id = rgw_user("test");
    state->owner.display_name = "test";
    state->bucket->get_info().owner = state->owner.id;
    state->trace = tracing::rgw::tracer.start_trace("put-test", false);
    op = std::make_unique<TestPut>();
    op->init(&driver, state.get(), nullptr);
    ASSERT_EQ(0, op->get_params(null_yield));
  }
  void TearDown() override {
    op.reset();
    state.reset();
    RGWCuObjServer::shutdown();
  }
  void expect_error(int error, int status, const std::string& code) {
    EXPECT_EQ(op->get_ret(), error);
    EXPECT_TRUE(client.statuses.empty());
    op->complete();
    EXPECT_EQ(client.statuses, std::vector<int>({status}));
    EXPECT_NE(client.body.find("<Code>" + code + "</Code>"), std::string::npos)
        << client.body;
    EXPECT_EQ(client.headers.count("x-amz-rdma-reply"), 0);
    EXPECT_EQ(client.headers.count("x-amz-rdma-bytes-transferred"), 0);
    EXPECT_FALSE(rdma.buffer.in_use.load());
  }
};

class CuObjPutReadFailure : public CuObjPut,
                          public ::testing::WithParamInterface<ssize_t> {};

TEST_P(CuObjPutReadFailure, RejectsInvalidCompletionsBeforeStorage)
{
  rdma.result = GetParam();
  op->execute(null_yield);
  const bool invalid_argument = rdma.result == -EINVAL;
  // RGW maps -EIO to its generic HTTP 500 / UnknownError response.
  expect_error(invalid_argument ? -EINVAL : -EIO, invalid_argument ? 400 : 500,
               invalid_argument ? "InvalidArgument" : "UnknownError");
  EXPECT_EQ(rdma.prepares, 1);
  EXPECT_EQ(rdma.reads, 1);
  EXPECT_EQ(rdma.releases, 1);
  EXPECT_EQ(rdma.processes, 0);
  EXPECT_EQ(rdma.completes, 0);
  EXPECT_EQ(state->obj_size, 0);
  EXPECT_EQ(state->rdma_bytes_transferred, 0);
}

INSTANTIATE_TEST_SUITE_P(CompletionCounts, CuObjPutReadFailure,
                        ::testing::Values(0, 1, 2, 3, 5, -EINVAL, -EIO, -EAGAIN));

TEST_F(CuObjPut, CompleteReadCommitsThePayloadBeforeSuccess)
{
  op->execute(null_yield);
  ASSERT_EQ(op->get_ret(), 0);
  EXPECT_TRUE(client.statuses.empty());
  EXPECT_EQ(rdma.reads, 1);
  EXPECT_EQ(rdma.releases, 1);
  EXPECT_EQ(rdma.processes, 2); // data followed by the filter flush
  EXPECT_EQ(rdma.completes, 1);
  EXPECT_EQ(rdma.stored, "abcd");
  EXPECT_EQ(state->obj_size, 4);
  EXPECT_EQ(state->rdma_bytes_transferred, 4);
  EXPECT_FALSE(rdma.buffer.in_use.load());
  op->complete();
  EXPECT_EQ(client.statuses, std::vector<int>({200}));
  EXPECT_EQ(client.headers.at("x-amz-rdma-reply"), "200");
  EXPECT_EQ(client.headers.at("x-amz-rdma-bytes-transferred"), "4");
  EXPECT_EQ(rdma.events, std::vector<std::string>(
      {"rdma_complete", "storage_complete", "http_status"}));
}

TEST_F(CuObjPut, InvalidDescriptorSizeReturnsInvalidArgument)
{
  rdma.descriptor_size = 0;
  op->execute(null_yield);
  expect_error(-EINVAL, 400, "InvalidArgument");
  EXPECT_EQ(rdma.acquisitions, 0);
  EXPECT_EQ(rdma.reads, 0);
  EXPECT_EQ(rdma.releases, 0);
  EXPECT_EQ(rdma.processes, 0);
  EXPECT_EQ(rdma.completes, 0);
}

TEST_F(CuObjPut, BufferExhaustionReturnsServiceUnavailable)
{
  rdma.available = false;
  op->execute(null_yield);
  expect_error(-ERR_SERVICE_UNAVAILABLE, 503, "ServiceUnavailable");
  EXPECT_EQ(rdma.acquisitions, 1);
  EXPECT_EQ(rdma.reads, 0);
  EXPECT_EQ(rdma.releases, 0);
  EXPECT_EQ(rdma.processes, 0);
  EXPECT_EQ(rdma.completes, 0);
}

TEST_F(CuObjPut, OversizedPayloadReturnsServiceUnavailable)
{
  rdma.descriptor_size = rdma.buffer.size + 1;
  op->execute(null_yield);
  expect_error(-ERR_SERVICE_UNAVAILABLE, 503, "ServiceUnavailable");
  EXPECT_EQ(rdma.reads, 0);
  EXPECT_EQ(rdma.releases, 0);
  EXPECT_EQ(rdma.processes, 0);
  EXPECT_EQ(rdma.completes, 0);
}

TEST_F(CuObjPut, FilterFailureReleasesBufferWithoutCommitting)
{
  rdma.process_error = -EIO;
  op->execute(null_yield);
  expect_error(-EIO, 500, "UnknownError");
  EXPECT_EQ(rdma.reads, 1);
  EXPECT_EQ(rdma.releases, 1);
  EXPECT_EQ(rdma.processes, 1);
  EXPECT_EQ(rdma.completes, 0);
}

TEST_F(CuObjPut, StorageFailureDoesNotReturnRdmaSuccess)
{
  rdma.complete_error = -EIO;
  op->execute(null_yield);
  expect_error(-EIO, 500, "UnknownError");
  EXPECT_EQ(rdma.reads, 1);
  EXPECT_EQ(rdma.releases, 1);
  EXPECT_EQ(rdma.completes, 1);
}
} // anonymous namespace

// Link-time fake: the real PUT operation runs with a controlled RDMA result.
// Descriptor parsing itself is covered against production code in test_rgw_cuobj.cc.
std::unique_ptr<RGWCuObjServer> RGWCuObjServer::s_instance;
RGWCuObjServer::~RGWCuObjServer() = default;
int RGWCuObjServer::init(CephContext* cct) {
  s_instance.reset(new RGWCuObjServer);
  s_instance->m_cct = cct;
  return 0;
}
void RGWCuObjServer::shutdown() { s_instance.reset(); }
RGWCuObjServer* RGWCuObjServer::get_instance() { return s_instance.get(); }
bool RGWCuObjServer::is_available() const { return true; }
RGWCuObjServer::RDMABufEntry* RGWCuObjServer::acquire_buffer(size_t size) {
  ++rdma.acquisitions;
  if (!rdma.available || size > rdma.buffer.size || rdma.buffer.in_use.exchange(true)) {
    return nullptr;
  }
  return &rdma.buffer;
}
void RGWCuObjServer::release_buffer(RDMABufEntry* buffer) {
  EXPECT_TRUE(buffer->in_use.exchange(false));
  ++rdma.releases;
}
size_t RGWCuObjServer::parse_rdma_descriptor_size(const std::string&) {
  return rdma.descriptor_size;
}
ssize_t RGWCuObjServer::rdma_read_from_client(const std::string&, RDMABufEntry* buffer,
                                            uint64_t offset, size_t size,
                                            const std::string&) {
  EXPECT_EQ(buffer, &rdma.buffer);
  EXPECT_EQ(offset, 0);
  EXPECT_EQ(size, rdma.descriptor_size);
  ++rdma.reads;
  rdma.events.push_back("rdma_complete");
  return rdma.result;
}
ssize_t RGWCuObjServer::rdma_write_to_client(const std::string&, RDMABufEntry*,
                                           uint64_t, size_t, const std::string&) {
  ADD_FAILURE() << "PUT must not write client memory";
  return -EIO;
}
