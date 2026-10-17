// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * How RGWGetObj picks the out-of-band mode of a GET with
 * x-amz-rdma-token, and how it leaves it when the store streams the
 * data instead, without a store behind it. And the checksum headers of
 * an out-of-band transfer. And what RGWPutObj leaves behind of a PUT
 * without a body that it declines, against fakes of the store.
 */

#include "rgw_auth.h"
#include "rgw_client_io.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_process_env.h"
#include "rgw_rdma_cksum.h"
#include "rgw_rdma_fence.h"
#include "rgw_perf_counters.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_swift.h"
#include "rgw_sal_filter.h"
#include "rgw_tracer.h"
#include "rgw_zone.h"
#include "driver/rados/rgw_sal_rados.h"
#ifdef WITH_RADOSGW_CUOBJ
#include "rgw_cuobj.h"
#endif

#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "global/global_context.h"

// rgw_op.cc
std::optional<rgw::cksum::Cksum> stored_full_crc(
  const rgw::sal::Attrs& attrs, const std::optional<int>& part_num,
  const std::optional<int>& parts_count);
bool rdma_crc_unchecked(const DoutPrefixProvider* dpp, rgw::cksum::Type type);

namespace {

// a cuObject descriptor (raddr:rsize:rkey:lid:qp:has_gid:gid) of a 1 MiB
// window
constexpr const char* TOKEN_1M = "7f0000000000:100000:1a2b:0:4d2:0:0";
constexpr uint64_t WINDOW = 1 << 20;

// an S3 GET of the plain chain: no filter engaged
constexpr bool PLAIN_CHAIN = true;

/// OSD-direct delivery of cuObject descriptors on, in a context of its
/// own: oob_client_transports cannot change once a context has started
struct OsdDirect {
  boost::intrusive_ptr<CephContext> cct{
    new CephContext(CEPH_ENTITY_TYPE_CLIENT), false};
  OsdDirect() {
    cct->_conf.set_val_or_die("rgw_rdma_osd_direct", "true");
    cct->_conf.set_val_or_die("oob_client_transports", "cuobj");
    cct->_conf.apply_changes(nullptr);
  }
};

/// a GET whose response body is what send_response_data() receives
class TestGetObj : public RGWGetObj {
public:
  using RGWGetObj::RdmaMode;
  using RGWGetObj::rdma_token;
  using RGWGetObj::rdma_mode;
  using RGWGetObj::rdma_switched_from;
  using RGWGetObj::rdma_fence_state;
  using RGWGetObj::total_len;
  using RGWGetObj::rdma_buf;
  using RGWGetObj::rdma_buf_offset;
  using RGWGetObj::select_rdma_mode;
  using RGWGetObj::note_rdma_reads;
  using RGWGetObj::rdma_fence_before;
  using RGWGetObj::rdma_write_staged;

  std::string body;
  int responses = 0;  ///< calls of send_response_data()
  bool eligible = true;

  explicit TestGetObj(req_state* s) {
    init(nullptr, s, nullptr);
    set_get_data(true);
  }

  int get_params(optional_yield) override { return 0; }
  int send_response_data_error(optional_yield) override { return 0; }
  int send_response_data(bufferlist& bl, off_t ofs, off_t len) override {
    ++responses;
    if (len > 0) {
      body.append(bl.c_str() + ofs, len);
    }
    return 0;
  }
  bool rdma_eligible() override { return eligible; }
};

/// an S3 GetObject, to ask whether its data may go out of band
class TestGetObjS3 : public RGWGetObj_ObjStore_S3 {
public:
  using RGWGetObj_ObjStore_S3::attrs;
  using RGWGetObj_ObjStore_S3::rdma_eligible;
  using RGWGetObj_ObjStore_S3::RdmaMode;
  using RGWGetObj_ObjStore_S3::rdma_token;
  using RGWGetObj_ObjStore_S3::rdma_mode;
  using RGWGetObj_ObjStore_S3::rdma_bytes;
  using RGWGetObj_ObjStore_S3::rdma_buf_offset;
  using RGWGetObj_ObjStore_S3::total_len;

  explicit TestGetObjS3(req_state* s) {
    init(nullptr, s, nullptr);
  }
};

#define DEFINE_REQ_STATE(cct) RGWProcessEnv pe; \
  RGWEnv e; \
  req_state s(cct, pe, &e, 0); \
  s.op = OP_GET

using rgw::cksum::Cksum;

// the ETags of a PUT and of a multipart upload of 3 parts
const std::string ETAG = "0123456789abcdef0123456789abcdef";
const std::string ETAG_3 = ETAG + "-3";

constexpr char RAW[8] = {1, 2, 3, 4, 5, 6, 7, 8};

/// the attrs of an object with a stored checksum of the given type and
/// flags, and the ETag as an attr holds it
rgw::sal::Attrs cksum_attrs(rgw::cksum::Type type, uint16_t flags,
                            const std::string& etag)
{
  Cksum ck(type, RAW, Cksum::CtorStyle::raw);
  ck.flags = flags;
  rgw::sal::Attrs attrs;
  ck.encode(attrs[RGW_ATTR_CKSUM]);
  attrs[RGW_ATTR_ETAG].append(etag.c_str(), etag.size() + 1);
  return attrs;
}

/// the same, as a release before CRC combining encoded the checksum:
/// version 1, without flags
rgw::sal::Attrs legacy_cksum_attrs(rgw::cksum::Type type,
                                   const std::string& etag)
{
  const uint16_t size = Cksum::checksums[uint16_t(type)].digest_size;
  rgw::sal::Attrs attrs;
  bufferlist& bl = attrs[RGW_ATTR_CKSUM];
  ENCODE_START(1, 1, bl);
  encode(uint16_t(type), bl);
  encode(size, bl);
  bl.append(RAW, size);
  ENCODE_FINISH(bl);
  attrs[RGW_ATTR_ETAG].append(etag.c_str(), etag.size() + 1);
  return attrs;
}

constexpr std::optional<int> WHOLE; // no partNumber
constexpr std::optional<int> NO_PARTS; // an atomic manifest

/// a client that keeps the headers of the response
class HeaderClient : public rgw::io::RestfulClient {
  RGWEnv env;

protected:
  int init_env(CephContext*) override { return 0; }

public:
  std::map<std::string, std::string> headers;

  RGWEnv& get_env() noexcept override { return env; }
  size_t complete_request() override { return 0; }
  size_t send_100_continue() override { return 0; }
  size_t send_status(int, const char*) override { return 0; }
  size_t send_header(const std::string_view& name,
                     const std::string_view& value) override {
    headers.emplace(name, value);
    return 0;
  }
  size_t send_content_length(uint64_t) override { return 0; }
  size_t complete_header() override { return 0; }
  size_t recv_body(char*, size_t) override { return 0; }
  size_t send_body(const char*, size_t len) override { return len; }
  void flush() override {}
};

/// a requester that is anonymous, or not
class TestIdentity : public rgw::auth::Identity {
  bool anonymous;
public:
  explicit TestIdentity(bool anonymous) : anonymous(anonymous) {}
  ACLOwner get_aclowner() const override { return {}; }
  uint32_t get_perms_from_aclspec(const DoutPrefixProvider*,
                                  const aclspec_t&) const override {
    return 0;
  }
  bool is_admin() const override { return false; }
  bool is_owner_of(const rgw_owner&) const override { return false; }
  bool is_root() const override { return false; }
  uint32_t get_perm_mask() const override { return 0; }
  bool is_anonymous() const override { return anonymous; }
  void to_str(std::ostream&) const override {}
  bool is_identity(const rgw::auth::Principal&) const override {
    return false;
  }
  uint32_t get_identity_type() const override { return TYPE_RGW; }
  std::optional<rgw::ARN> get_caller_identity() const override {
    return std::nullopt;
  }
  std::string get_acct_name() const override { return {}; }
  std::string get_subuser() const override { return {}; }
  const std::string& get_tenant() const override {
    static const std::string empty;
    return empty;
  }
  const std::optional<RGWAccountInfo>& get_account() const override {
    static const std::optional<RGWAccountInfo> none;
    return none;
  }
};

/// a client that also keeps the status and the length of the response
class RecordingClient : public HeaderClient {
public:
  std::vector<int> statuses;
  uint64_t content_length = 0;

  size_t send_status(int status, const char*) override {
    statuses.push_back(status);
    return 0;
  }
  size_t send_content_length(uint64_t len) override {
    content_length = len;
    return 0;
  }
};

/// an S3 GET or HEAD of a 4-byte object whose response goes to client
struct S3Response {
  RGWProcessEnv pe;
  RGWEnv e;
  req_state s{g_ceph_context, pe, &e, 0};
  RecordingClient client;
  RGWRestfulIO io{g_ceph_context, &client};
  TestGetObjS3 op{&s};

  explicit S3Response(bool get_data) {
    s.op = get_data ? OP_GET : OP_HEAD;
    s.cio = &io;
    s.formatter = ceph::Formatter::create("xml");
    s.format = RGWFormat::XML;
    s.prot_flags = RGW_REST_S3;
    s.object = std::make_unique<rgw::sal::RadosObject>(
      nullptr, rgw_obj_key("obj"));
    s.obj_size = 4;
    op.set_get_data(get_data);
    op.rdma_token = TOKEN_1M;
    op.total_len = 4;
  }
};

// a cuObject descriptor of a 4-byte window
constexpr const char* TOKEN_4 = "1234:4:1:0:1:0:0";

/// what a PUT did to the fakes of the store below
struct PutLog {
  unsigned writers = 0;    ///< processors made
  unsigned pulls = 0;      ///< process_pulled() calls
  unsigned processes = 0;  ///< process() calls with data
  unsigned completes = 0;
  unsigned archives = 0;   ///< swift_versioning_copy() calls
  unsigned reserves = 0;   ///< publish_reserve() calls
  int pull_result = 0;     ///< what process_pulled() returns
};

/// a writer whose storage pulls each object as one stripe
class PutWriter : public rgw::sal::Writer {
  PutLog& log;
public:
  explicit PutWriter(PutLog& log) : log(log) { ++log.writers; }
  int prepare(optional_yield) override { return 0; }
  int process(bufferlist&& data, uint64_t) override {
    if (data.length()) {
      ++log.processes;
    }
    return 0;
  }
  bool set_pulled() override { return true; }
  int process_pulled(uint64_t len, const std::string&, uint32_t,
                     std::vector<rgw::sal::PulledStripe>* stripes) override {
    ++log.pulls;
    rgw::sal::PulledStripe st;
    st.len = len;
    st.bytes = log.pull_result < 0 ? 0 : len;
    st.crc64 = 0x0123456789abcdefull;
    st.crc64_valid = true;
    stripes->push_back(st);
    return log.pull_result;
  }
  int complete(size_t, const std::string&, ceph::real_time*, ceph::real_time,
               std::map<std::string, bufferlist>&,
               const std::optional<rgw::cksum::Cksum>&, ceph::real_time,
               const char*, const char*, const std::string*, rgw_zone_set*,
               bool*, const req_context&, uint32_t) override {
    ++log.completes;
    return 0;
  }
};

class PutNotification : public rgw::sal::Notification {
  PutLog& log;
public:
  explicit PutNotification(PutLog& log) : log(log) {}
  int publish_reserve(const DoutPrefixProvider*, RGWObjTags*) override {
    ++log.reserves;
    return 0;
  }
  int publish_commit(const DoutPrefixProvider*, uint64_t,
                     const ceph::real_time&, const std::string&,
                     const std::string&) override {
    return 0;
  }
};

class PutDriver : public rgw::sal::FilterDriver {
  PutLog& log;
public:
  explicit PutDriver(PutLog& log) : FilterDriver(nullptr), log(log) {}
  CephContext* ctx() override { return g_ceph_context; }
  std::unique_ptr<rgw::sal::Notification> get_notification(
      rgw::sal::Object*, rgw::sal::Object*, req_state*,
      rgw::notify::EventType, optional_yield,
      const std::string*) override {
    return std::make_unique<PutNotification>(log);
  }
  std::unique_ptr<rgw::sal::Writer> get_atomic_writer(
      const DoutPrefixProvider*, optional_yield, rgw::sal::Object*,
      const ACLOwner&, const rgw_placement_rule*, uint64_t,
      const std::string&) override {
    return std::make_unique<PutWriter>(log);
  }
  const std::string& get_compression_type(const rgw_placement_rule&) override {
    static const std::string none = "none";
    return none;
  }
  int get_sync_policy_handler(const DoutPrefixProvider*,
                              std::optional<rgw_zone_id>,
                              std::optional<rgw_bucket>,
                              RGWBucketSyncPolicyHandlerRef*,
                              optional_yield) override {
    return 0;
  }
};

class PutBucket : public rgw::sal::RadosBucket {
public:
  PutBucket() : RadosBucket(nullptr, rgw_bucket("", "bucket")) {}
  int check_quota(const DoutPrefixProvider*, RGWQuota&, uint64_t,
                  optional_yield, bool) override { return 0; }
};

/// an object whose Swift versioning archives nothing but counts
class PutObject : public rgw::sal::RadosObject {
  PutLog& log;
public:
  PutObject(PutLog& log, rgw::sal::Bucket* bucket)
    : RadosObject(nullptr, rgw_obj_key("obj"), bucket), log(log) {}
  std::unique_ptr<rgw::sal::Object> clone() override {
    return std::make_unique<PutObject>(*this);
  }
  int swift_versioning_copy(const ACLOwner&, const rgw_user&,
                            const DoutPrefixProvider*,
                            optional_yield) override {
    ++log.archives;
    return 0;
  }
};

class TestPutObjS3 : public RGWPutObj_ObjStore_S3 {
public:
  // the request's encryption headers, which these requests have none of
  int get_encrypt_filter(std::unique_ptr<rgw::sal::DataProcessor>*,
                         rgw::sal::DataProcessor*) override { return 0; }
};

/// A PUT of api without a body, with x-amz-rdma-token naming a 4-byte
/// window, by an authenticated user unless anonymous, on a gateway
/// whose OSDs pull cuObject descriptors. Its response goes to client.
struct BodylessPut {
  OsdDirect osd_direct;
  PutLog log;
  PutDriver driver{log};
  std::unique_ptr<rgw::SiteConfig> site = rgw::SiteConfig::make_fake();
  RGWProcessEnv pe;
  RecordingClient client;
  RGWRestfulIO io{osd_direct.cct.get(), &client};
  std::unique_ptr<req_state> s;
  std::unique_ptr<RGWPutObj> op;

  explicit BodylessPut(bool anonymous = false, bool swift = false) {
    pe.driver = &driver;
    pe.site = site.get();
    client.get_env().set("REQUEST_METHOD", "PUT");
    client.get_env().set("HTTP_X_AMZ_RDMA_TOKEN", TOKEN_4);
    s = std::make_unique<req_state>(osd_direct.cct.get(), pe,
                                    &client.get_env(), 0);
    s->cio = &io;
    s->formatter = ceph::Formatter::create("xml");
    s->format = RGWFormat::XML;
    s->prot_flags = swift ? RGW_REST_SWIFT : RGW_REST_S3;
    s->op = OP_PUT;
    s->op_type = RGW_OP_PUT_OBJ;
    s->length = "0";
    s->content_length = 0;
    s->bucket_exists = true;
    s->bucket = std::make_unique<PutBucket>();
    s->object = std::make_unique<PutObject>(log, s->bucket.get());
    s->user = std::make_unique<rgw::sal::RadosUser>(nullptr,
                                                    rgw_user("test"));
    if (anonymous) {
      s->auth.identity = std::make_unique<TestIdentity>(true);
    } else {
      s->auth.identity = std::make_unique<rgw::auth::LocalApplier>(
        osd_direct.cct.get(), s->user->clone(), std::nullopt,
        std::vector<rgw::IAM::Policy>{}, rgw::auth::LocalApplier::NO_SUBUSER,
        std::nullopt, rgw::auth::LocalApplier::NO_ACCESS_KEY);
    }
    s->owner.id = rgw_user("test");
    s->owner.display_name = "test";
    s->bucket->get_info().owner = s->owner.id;
    s->trace = tracing::rgw::tracer.start_trace("put-test", false);
    if (swift) {
      op = std::make_unique<RGWPutObj_ObjStore_SWIFT>();
    } else {
      op = std::make_unique<TestPutObjS3>();
    }
    op->init(&driver, s.get(), nullptr);
  }

  /// get_params() and execute(), as the frontend runs them, and the
  /// response
  int run() {
    int r = op->get_params(null_yield);
    if (r == 0) {
      op->execute(null_yield);
      r = op->get_ret();
    }
    op->complete();
    return r;
  }
};

} // anonymous namespace

using Mode = TestGetObj::RdmaMode;

TEST(RgwRdmaGet, PassthroughWhenTheResponseFitsTheWindow)
{
  OsdDirect osd_direct;
  DEFINE_REQ_STATE(osd_direct.cct.get());
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;

  op.total_len = WINDOW;
  op.select_rdma_mode(PLAIN_CHAIN);
  EXPECT_EQ(Mode::PASSTHROUGH, op.rdma_mode);

  // the OSDs write each stripe at its offset in the window, which must
  // hold the whole response
  op.total_len = WINDOW + 1;
  op.select_rdma_mode(PLAIN_CHAIN);
  EXPECT_EQ(Mode::NONE, op.rdma_mode);
}

TEST(RgwRdmaGet, NoPassthroughWithAnOpTimeout)
{
  // a stripe read that times out can still write the window after the
  // fence: the GET goes over HTTP instead, as an OSD-direct PUT is
  // declined
  OsdDirect osd_direct;
  osd_direct.cct->_conf.set_val_or_die("rados_osd_op_timeout", "30");
  osd_direct.cct->_conf.apply_changes(nullptr);
  DEFINE_REQ_STATE(osd_direct.cct.get());
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 4096;
  op.select_rdma_mode(PLAIN_CHAIN);
  EXPECT_EQ(Mode::NONE, op.rdma_mode);

  osd_direct.cct->_conf.set_val_or_die("rados_osd_op_timeout", "0");
  osd_direct.cct->_conf.apply_changes(nullptr);
  op.select_rdma_mode(PLAIN_CHAIN);
  EXPECT_EQ(Mode::PASSTHROUGH, op.rdma_mode);
}

TEST(RgwRdmaGet, NoResponseOnACallbackWithoutDataOutOfBand)
{
  // a callback without data, as at the end of a part, sends nothing out
  // of band: execute() answers once it checked the reads and waited out
  // the fence
  OsdDirect osd_direct;
  DEFINE_REQ_STATE(osd_direct.cct.get());
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 8;
  op.select_rdma_mode(PLAIN_CHAIN);
  ASSERT_EQ(Mode::PASSTHROUGH, op.rdma_mode);
  bufferlist bl;
  EXPECT_EQ(0, op.get_data_cb(bl, 0, 0));
  EXPECT_EQ(0, op.responses);
  EXPECT_EQ(Mode::PASSTHROUGH, op.rdma_mode);
  EXPECT_EQ(Mode::NONE, op.rdma_switched_from);

  // over HTTP it goes to the response as before
  op.rdma_mode = Mode::NONE;
  EXPECT_EQ(0, op.get_data_cb(bl, 0, 0));
  EXPECT_EQ(1, op.responses);
}

TEST(RgwRdmaGet, HeadWithATokenGetsNoRdmaReply)
{
  // a HEAD moves no data: the length of the object, and no x-amz-rdma-*
  // header, as before S3 over RDMA
  S3Response head(false);
  bufferlist bl;
  ASSERT_EQ(0, head.op.send_response_data(bl, 0, 0));
  EXPECT_EQ(std::vector<int>{200}, head.client.statuses);
  EXPECT_EQ(4u, head.client.content_length);
  EXPECT_EQ(0u, head.client.headers.count("x-amz-rdma-reply"));
  EXPECT_EQ(0u, head.client.headers.count("x-amz-rdma-bytes-transferred"));

  // a GET that goes over HTTP says so
  S3Response get(true);
  ASSERT_EQ(0, get.op.send_response_data(bl, 0, 0));
  EXPECT_EQ(std::vector<int>{200}, get.client.statuses);
  EXPECT_EQ(4u, get.client.content_length);
  EXPECT_EQ("501", get.client.headers.at("x-amz-rdma-reply"));
}

TEST(RgwRdmaGet, BytesTransferredReportsTheResponse)
{
  // the request is charged for what an attempt that fell back moved as
  // well, but x-amz-rdma-bytes-transferred says what this response put
  // in the client's window
  using Mode = TestGetObjS3::RdmaMode;
  bufferlist bl;
  S3Response passthrough(true);
  passthrough.op.rdma_mode = Mode::PASSTHROUGH;
  passthrough.op.rdma_bytes = 4;
  passthrough.s.rdma_bytes_transferred = 4;
  ASSERT_EQ(0, passthrough.op.send_response_data(bl, 0, 0));
  EXPECT_EQ("200", passthrough.client.headers.at("x-amz-rdma-reply"));
  EXPECT_EQ("4", passthrough.client.headers.at(
              "x-amz-rdma-bytes-transferred"));

  // staged after a passthrough attempt that placed 3 bytes
  S3Response staged(true);
  staged.op.rdma_mode = Mode::STAGED;
  staged.op.rdma_buf_offset = 4;
  staged.s.rdma_bytes_transferred = 3 + 4;
  ASSERT_EQ(0, staged.op.send_response_data(bl, 0, 0));
  EXPECT_EQ("200", staged.client.headers.at("x-amz-rdma-reply"));
  EXPECT_EQ("4", staged.client.headers.at("x-amz-rdma-bytes-transferred"));
  EXPECT_EQ(0u, staged.client.content_length);
}

TEST(RgwRdmaGet, NoOutOfBandModeWhenNotEligible)
{
  OsdDirect osd_direct;
  DEFINE_REQ_STATE(osd_direct.cct.get());
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 4096;
  op.eligible = false;
  op.select_rdma_mode(PLAIN_CHAIN);
  EXPECT_EQ(Mode::NONE, op.rdma_mode);
}

TEST(RgwRdmaGet, InlineDataSwitchesToAFallbackInPlace)
{
  // a store or filter that ignores the token (as D4N does) streams the
  // data: the GET serves it in a fallback mode, here over HTTP, instead
  // of failing the read to run it again
  OsdDirect osd_direct;
  DEFINE_REQ_STATE(osd_direct.cct.get());
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 8;
  op.select_rdma_mode(PLAIN_CHAIN);
  ASSERT_EQ(Mode::PASSTHROUGH, op.rdma_mode);

  bufferlist bl;
  bl.append("xx01234567");
  EXPECT_EQ(0, op.get_data_cb(bl, 2, 5));
  EXPECT_EQ(Mode::NONE, op.rdma_mode);
  EXPECT_EQ(Mode::PASSTHROUGH, op.rdma_switched_from);
  EXPECT_EQ(0, op.get_data_cb(bl, 7, 3));
  EXPECT_EQ("01234567", op.body);
  EXPECT_EQ(Mode::PASSTHROUGH, op.rdma_switched_from);
}

TEST(RgwRdmaGet, ReadsOfASwitchedGetArmTheFence)
{
  // reads that a store sent with the token before it streamed the data
  // anyway went out in passthrough: the response and an error wait for
  // their writes to the client's window
  OsdDirect osd_direct;
  DEFINE_REQ_STATE(osd_direct.cct.get());
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 8;
  op.select_rdma_mode(PLAIN_CHAIN);
  bufferlist bl;
  bl.append("01234567");
  ASSERT_EQ(0, op.get_data_cb(bl, 0, 8));
  ASSERT_EQ(Mode::NONE, op.rdma_mode);

  op.note_rdma_reads(true, true, 0.25);
  EXPECT_TRUE(op.rdma_fence_state.sent);
  EXPECT_EQ(250u, op.rdma_fence_state.wait_ms);
}

TEST(RgwRdmaGet, ASuccessThatDidNotLandWaitsForTheFence)
{
  // every stripe placed, but not every one landed: an OSD may have
  // replied before its bytes were in the window, so the response waits
  // out the fence, as after a resend
  OsdDirect osd_direct;
  DEFINE_REQ_STATE(osd_direct.cct.get());
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 4096;
  op.select_rdma_mode(PLAIN_CHAIN);
  ASSERT_EQ(Mode::PASSTHROUGH, op.rdma_mode);

  op.note_rdma_reads(true, true, 0.05);
  const auto start = ceph::mono_clock::now();
  op.rdma_fence_before("the response", true);
  EXPECT_GE(ceph::mono_clock::now() - start, std::chrono::milliseconds(50));
  EXPECT_TRUE(op.rdma_fence_state.settled);

  // every stripe landed, and none was resent: answered at once
  op.note_rdma_reads(true, false, 60.0);
  op.rdma_fence_before("the response", true);
  EXPECT_TRUE(op.rdma_fence_state.settled);
}

TEST(RgwRdmaGet, NoPassthroughAfterAStripeThatDidNotLand)
{
  // a GET whose OSD placed a stripe without saying that it landed waits
  // out the fence before its response: the next GETs with tokens of its
  // kind go staged or over HTTP instead, for a while
  OsdDirect osd_direct;
  DEFINE_REQ_STATE(osd_direct.cct.get());
  auto& landing = rgw::rdma::passthrough_landing(TOKEN_1M);
  landing.clear();
  auto passthrough = [&s] {
    auto op = std::make_unique<TestGetObj>(&s);
    op->rdma_token = TOKEN_1M;
    op->total_len = 4096;
    op->select_rdma_mode(PLAIN_CHAIN);
    return op;
  };

  // stripes that all landed leave passthrough on
  auto op = passthrough();
  ASSERT_EQ(Mode::PASSTHROUGH, op->rdma_mode);
  RGWGetObj_CB landed(op.get());
  EXPECT_EQ(0, landed.handle_oob(0, 4096, true));
  op->note_rdma_reads(true, false, 0.0);
  EXPECT_EQ(Mode::PASSTHROUGH, passthrough()->rdma_mode);

  // one stripe that did not land: the next GET goes over HTTP, as no
  // cuObject server stages it here
  op = passthrough();
  ASSERT_EQ(Mode::PASSTHROUGH, op->rdma_mode);
  RGWGetObj_CB placed(op.get());
  EXPECT_EQ(0, placed.handle_oob(0, 2048, true));
  EXPECT_EQ(0, placed.handle_oob(2048, 2048, false));
  op->note_rdma_reads(true, true, 0.0);
  EXPECT_EQ(Mode::NONE, passthrough()->rdma_mode);
  EXPECT_EQ(Mode::NONE, passthrough()->rdma_mode);
  landing.clear();
  EXPECT_EQ(Mode::PASSTHROUGH, passthrough()->rdma_mode);
}

TEST(RgwRdmaGet, OnlyGetObjectIsEligible)
{
  DEFINE_REQ_STATE(g_ceph_context);
  TestGetObjS3 op(&s);
  EXPECT_TRUE(op.rdma_eligible());

  // SelectObjectContent, a POST, runs the data through its query engine
  s.op = OP_POST;
  EXPECT_FALSE(op.rdma_eligible());
  s.op = OP_GET;

  // the website endpoint serves an object that redirects with a 301
  s.prot_flags |= RGW_REST_WEBSITE;
  EXPECT_TRUE(op.rdma_eligible());
  bufferlist location;
  location.append("/elsewhere");
  op.attrs[RGW_ATTR_AMZ_WEBSITE_REDIRECT_LOCATION] = location;
  EXPECT_FALSE(op.rdma_eligible());

  // which the S3 endpoint ignores
  s.prot_flags &= ~RGW_REST_WEBSITE;
  EXPECT_TRUE(op.rdma_eligible());
}

TEST(RgwRdmaGet, AnAnonymousGetIsNotEligible)
{
  // a token names the endpoint that the OSDs connect to: an anonymous
  // reader of a public bucket gets the body over HTTP, unless the gateway
  // allows anonymous tokens
  boost::intrusive_ptr<CephContext> cct{
    new CephContext(CEPH_ENTITY_TYPE_CLIENT), false};
  DEFINE_REQ_STATE(cct.get());
  TestGetObjS3 op(&s);
  s.auth.identity = std::make_unique<TestIdentity>(false);
  EXPECT_TRUE(op.rdma_eligible());
  s.auth.identity = std::make_unique<TestIdentity>(true);
  EXPECT_FALSE(op.rdma_eligible());
  cct->_conf.set_val_or_die("rgw_rdma_token_allow_anonymous", "true");
  cct->_conf.apply_changes(nullptr);
  EXPECT_TRUE(op.rdma_eligible());
}

TEST(RgwRdmaGet, AnAnonymousGetWithATokenGetsNotImplemented)
{
  // which tells the client to read the body
  S3Response get(true);
  get.s.auth.identity = std::make_unique<TestIdentity>(true);
  ASSERT_FALSE(get.op.rdma_eligible());
  bufferlist bl;
  ASSERT_EQ(0, get.op.send_response_data(bl, 0, 0));
  EXPECT_EQ(4u, get.client.content_length);
  EXPECT_EQ("501", get.client.headers.at("x-amz-rdma-reply"));
}

TEST(RgwRdmaGet, NoErrorDocumentIsEligible)
{
  // the website endpoint serves an ErrorDocument with the 404 of the
  // error it answers, which the client window cannot carry
  DEFINE_REQ_STATE(g_ceph_context);
  s.prot_flags |= RGW_REST_WEBSITE;
  TestGetObjS3 op(&s);
  EXPECT_TRUE(op.rdma_eligible());
  op.set_custom_http_response(404);
  EXPECT_FALSE(op.rdma_eligible());
}

TEST(RgwRdmaGet, StoredCrcOfAWholeObject)
{
  using rgw::cksum::Type;
  // a PUT over HTTP stores its CRC without flags
  auto attrs = cksum_attrs(Type::crc32c, 0, ETAG);
  auto stored = stored_full_crc(attrs, WHOLE, NO_PARTS);
  ASSERT_TRUE(stored);
  EXPECT_EQ(Type::crc32c, stored->type);
  // and a release before CRC combining with version 1
  EXPECT_TRUE(stored_full_crc(legacy_cksum_attrs(Type::crc32c, ETAG),
                              WHOLE, NO_PARTS));
  // an OSD-direct PUT stores the CRC its OSDs report, under an ETag of
  // the multipart form
  attrs = cksum_attrs(Type::crc64nvme, Cksum::FLAG_V2, ETAG_3);
  stored = stored_full_crc(attrs, WHOLE, NO_PARTS);
  ASSERT_TRUE(stored);
  EXPECT_EQ(Type::crc64nvme, stored->type);
  // CRCs combined from those of the parts
  EXPECT_TRUE(stored_full_crc(
      cksum_attrs(Type::crc32c, Cksum::FLAG_V2 | Cksum::FULL_OBJECT_MASK,
                  ETAG_3),
      WHOLE, 3));

  // no CRC the OSDs compute
  EXPECT_FALSE(stored_full_crc(
      cksum_attrs(Type::sha256, Cksum::FLAG_V2, ETAG), WHOLE, NO_PARTS));
  attrs.erase(RGW_ATTR_CKSUM);
  EXPECT_FALSE(stored_full_crc(attrs, WHOLE, NO_PARTS));
}

TEST(RgwRdmaGet, NoStoredCrcThatIsComposite)
{
  using rgw::cksum::Type;
  // composite by its flags, with or without a multipart manifest
  const uint16_t composite = Cksum::FLAG_V2 | Cksum::COMPOSITE_MASK;
  EXPECT_FALSE(stored_full_crc(cksum_attrs(Type::crc32c, composite, ETAG_3),
                               WHOLE, 3));
  EXPECT_FALSE(stored_full_crc(cksum_attrs(Type::crc32c, composite, ETAG),
                               WHOLE, NO_PARTS));

  // a multipart upload's from before CRC combining has no flags
  const auto legacy = legacy_cksum_attrs(Type::crc32c, ETAG_3);
  EXPECT_FALSE(stored_full_crc(legacy, WHOLE, 3));
  // nor once a transition, sync or copy rewrote it with an atomic
  // manifest, which keeps the ETag
  EXPECT_FALSE(stored_full_crc(legacy, WHOLE, NO_PARTS));
  // where part 1 is the whole object
  EXPECT_FALSE(stored_full_crc(legacy, 1, NO_PARTS));
}

TEST(RgwRdmaGet, StoredCrcOfAPart)
{
  using rgw::cksum::Type;
  // a part's attrs are its own, but for the ETag of the whole upload
  EXPECT_TRUE(stored_full_crc(
      cksum_attrs(Type::crc32c, Cksum::FLAG_FULL_OBJECT, ETAG_3), 2, 3));
  EXPECT_TRUE(stored_full_crc(legacy_cksum_attrs(Type::crc32c, ETAG_3),
                              2, 3));
  // one of a composite upload is reported as composite
  EXPECT_FALSE(stored_full_crc(
      cksum_attrs(Type::crc32c, Cksum::FLAG_COMPOSITE, ETAG_3), 2, 3));
}

TEST(RgwRdmaGet, AnUncheckedWholeObjectGetIsCounted)
{
  // a whole-object GET whose delivered bytes the gateway could not
  // compare with the stored checksum: counted each time, and logged at
  // level 1 at most once a minute
  if (!perfcounter) {
    ASSERT_EQ(0, rgw_perf_start(g_ceph_context));
  }
  const uint64_t before = perfcounter->get(l_rgw_rdma_cksum_unchecked);
  NoDoutPrefix dpp(g_ceph_context, ceph_subsys_rgw);
  EXPECT_TRUE(rdma_crc_unchecked(&dpp, rgw::cksum::Type::crc64nvme));
  EXPECT_FALSE(rdma_crc_unchecked(&dpp, rgw::cksum::Type::crc32c));
  EXPECT_EQ(before + 2, perfcounter->get(l_rgw_rdma_cksum_unchecked));
}

TEST(RgwRdmaGet, ChecksumAlgorithmOfTheRequest)
{
  DEFINE_REQ_STATE(g_ceph_context);
  rgw::rdma::cksum_want want;
  // CRC64NVME when the request has no x-amz-rdma-checksum-algorithm
  ASSERT_EQ(0, rgw::rdma::parse_checksum_header(&s, &want));
  EXPECT_TRUE(want.crc64nvme);
  EXPECT_FALSE(want.crc32c);

  e.set("HTTP_X_AMZ_RDMA_CHECKSUM_ALGORITHM", "crc32c");
  ASSERT_EQ(0, rgw::rdma::parse_checksum_header(&s, &want));
  EXPECT_FALSE(want.crc64nvme);
  EXPECT_TRUE(want.crc32c);
  EXPECT_TRUE(s.err.message.empty());

  // an S3 checksum the OSDs do not compute
  e.set("HTTP_X_AMZ_RDMA_CHECKSUM_ALGORITHM", "CRC32");
  EXPECT_EQ(-EINVAL, rgw::rdma::parse_checksum_header(&s, &want));
  EXPECT_EQ("x-amz-rdma-checksum-algorithm must be CRC64NVME or CRC32C",
            s.err.message);
}

TEST(RgwRdmaGet, ChecksumHeaders)
{
  using Headers = std::map<std::string, std::string>;
  DEFINE_REQ_STATE(g_ceph_context);
  HeaderClient client;
  s.cio = &client;

  // the check values of "123456789", each in its own header
  rgw::rdma::dump_checksums(&s, 0xae8b14860a799888ull, std::nullopt);
  EXPECT_EQ((Headers{{"x-amz-rdma-checksum-crc64nvme", "rosUhgp5mIg="}}),
            client.headers);
  client.headers.clear();
  rgw::rdma::dump_checksums(&s, std::nullopt, 0xe3069283u);
  EXPECT_EQ((Headers{{"x-amz-rdma-checksum-crc32c", "4waSgw=="}}),
            client.headers);
  client.headers.clear();
  rgw::rdma::dump_checksums(&s, 0xae8b14860a799888ull, 0xe3069283u);
  EXPECT_EQ((Headers{{"x-amz-rdma-checksum-crc64nvme", "rosUhgp5mIg="},
                     {"x-amz-rdma-checksum-crc32c", "4waSgw=="}}),
            client.headers);
  client.headers.clear();
  rgw::rdma::dump_checksums(&s, std::nullopt, std::nullopt);
  EXPECT_TRUE(client.headers.empty());
  s.cio = nullptr;
}

TEST(RgwRdmaPut, AFailedPullLeavesNoArchiveBehind)
{
  // an OSD could not pull its stripe: the PUT is declined, and the client
  // sends it again with the body. Neither attempt may have archived the
  // current version for Swift versioning, or reserved a notification,
  // for this one
  BodylessPut put;
  put.log.pull_result = -EIO;
  EXPECT_EQ(-ERR_NOT_IMPLEMENTED, put.run());
  EXPECT_EQ(1u, put.log.pulls);
  EXPECT_EQ(0u, put.log.archives);
  EXPECT_EQ(0u, put.log.reserves);
  EXPECT_EQ(0u, put.log.completes);
  EXPECT_EQ(std::vector<int>{501}, put.client.statuses);
  EXPECT_EQ("501", put.client.headers.at("x-amz-rdma-reply"));
}

TEST(RgwRdmaPut, APulledPutArchivesOnce)
{
  // once the OSDs pulled the object, the PUT archives and reserves, and
  // then writes the head
  BodylessPut put;
  EXPECT_EQ(0, put.run());
  EXPECT_EQ(1u, put.log.pulls);
  EXPECT_EQ(0u, put.log.processes);
  EXPECT_EQ(1u, put.log.archives);
  EXPECT_EQ(1u, put.log.reserves);
  EXPECT_EQ(1u, put.log.completes);
  EXPECT_EQ(std::vector<int>{200}, put.client.statuses);
  EXPECT_EQ("200", put.client.headers.at("x-amz-rdma-reply"));
  EXPECT_EQ("4", put.client.headers.at("x-amz-rdma-bytes-transferred"));
}

TEST(RgwRdmaPut, AnAnonymousPutWithoutABodyIsDeclined)
{
  // ignoring its token would store an empty object: it is declined, as a
  // PUT that cannot be served out of band is, and the client sends the
  // body, which an anonymous PUT stores
  BodylessPut put(true);
  EXPECT_EQ(-ERR_NOT_IMPLEMENTED, put.run());
  EXPECT_EQ(0u, put.log.writers);
  EXPECT_EQ(0u, put.log.archives);
  EXPECT_EQ(0u, put.log.reserves);
  EXPECT_EQ(0u, put.log.completes);
  EXPECT_EQ(std::vector<int>{501}, put.client.statuses);
  EXPECT_EQ("501", put.client.headers.at("x-amz-rdma-reply"));
}

TEST(RgwRdmaPut, ASwiftPutWithoutABodyIsRefused)
{
  // the Swift API takes no token, and stores no empty object over the
  // object for one
  BodylessPut put(false, true);
  EXPECT_EQ(-ERR_NOT_IMPLEMENTED, put.run());
  EXPECT_EQ(0u, put.log.writers);
  EXPECT_EQ(0u, put.log.archives);
  EXPECT_EQ(0u, put.log.completes);
  EXPECT_EQ(std::vector<int>{501}, put.client.statuses);
}

#ifdef WITH_RADOSGW_CUOBJ
TEST(RgwRdmaGet, StagingStaysInsideTheResponse)
{
  // staged mode copies the data into a buffer that holds the response,
  // which fits the client's window, and sends nothing over HTTP
  DEFINE_REQ_STATE(g_ceph_context);
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 8;
  std::vector<char> mem(16);
  RGWCuObjServer::RDMABufEntry entry;
  entry.ptr = mem.data();
  entry.size = mem.size();
  op.rdma_buf = &entry;
  op.rdma_mode = Mode::STAGED;

  bufferlist bl;
  bl.append("xx01234567yy");
  EXPECT_EQ(0, op.get_data_cb(bl, 2, 5));
  EXPECT_EQ(0, op.get_data_cb(bl, 7, 3));
  EXPECT_EQ(8u, op.rdma_buf_offset);
  EXPECT_EQ("01234567", std::string(mem.data(), 8));
  EXPECT_TRUE(op.body.empty());

  // nothing past the response
  EXPECT_EQ(-EIO, op.get_data_cb(bl, 0, 1));
  EXPECT_EQ(8u, op.rdma_buf_offset);
  op.rdma_buf = nullptr;
}

TEST(RgwRdmaGet, StagingCopiesEachSegment)
{
  // data in several segments, as a decompressed or a RADOS read can
  // come, is staged without first being copied into one segment
  DEFINE_REQ_STATE(g_ceph_context);
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 8;
  std::vector<char> mem(8);
  RGWCuObjServer::RDMABufEntry entry;
  entry.ptr = mem.data();
  entry.size = mem.size();
  op.rdma_buf = &entry;
  op.rdma_mode = Mode::STAGED;

  bufferlist bl;
  bl.append(buffer::copy("xx012", 5));
  bl.append(buffer::copy("345", 3));
  bl.append(buffer::copy("67yy", 4));
  ASSERT_EQ(3u, bl.get_num_buffers());
  EXPECT_EQ(0, op.get_data_cb(bl, 2, 8));
  EXPECT_EQ(8u, op.rdma_buf_offset);
  EXPECT_EQ("01234567", std::string(mem.data(), 8));
  EXPECT_EQ(3u, bl.get_num_buffers());
  op.rdma_buf = nullptr;
}

TEST(RgwRdmaGet, StagingStaysInsideTheBuffer)
{
  DEFINE_REQ_STATE(g_ceph_context);
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 8;
  std::vector<char> mem(4);
  RGWCuObjServer::RDMABufEntry entry;
  entry.ptr = mem.data();
  entry.size = mem.size();
  op.rdma_buf = &entry;
  op.rdma_mode = Mode::STAGED;

  bufferlist bl;
  bl.append("01234567");
  EXPECT_EQ(-EIO, op.get_data_cb(bl, 0, 5));
  EXPECT_EQ(0u, op.rdma_buf_offset);
  op.rdma_buf = nullptr;
}

TEST(RgwRdmaGet, NoStagedWriteWithoutTheServer)
{
  // the response goes out after the write, and fails without it
  DEFINE_REQ_STATE(g_ceph_context);
  TestGetObj op(&s);
  op.rdma_token = TOKEN_1M;
  op.total_len = 8;
  std::vector<char> mem(8);
  RGWCuObjServer::RDMABufEntry entry;
  entry.ptr = mem.data();
  entry.size = mem.size();
  op.rdma_buf = &entry;
  op.rdma_mode = Mode::STAGED;
  op.rdma_buf_offset = 8;
  if (!RGWCuObjServer::get_instance()) {
    EXPECT_EQ(-EIO, op.rdma_write_staged());
  }
  op.rdma_buf = nullptr;
}
#endif
