// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_rest_rdma_rc.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <limits>
#include <map>
#include <sstream>

#include <arpa/inet.h>

#include "common/crc64nvme.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/scope_guard.h"
#include "rgw_auth.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_rdma_cksum.h"
#include "rgw_rdma_fence.h"
#include "rgw_rdma_rc_session.h"
#include "rgw_rdma_rc_wire.h"
#include "rgw_rest_s3.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::rdma::rc {

namespace {

enum class Route { PREPARE, READY, CANCEL };

/// completion markers of the client pushes
constexpr uint64_t WR_PUSH = 0x4853555052444d41ULL;   // "AMDRPUSH"
constexpr uint64_t WR_FINAL = 0x4c4e494652444d41ULL;  // "AMDRFINL"

constexpr const char* H_PROTOCOL = "HTTP_X_AMZ_RDMA_PROTOCOL";
constexpr const char* H_TOKEN = "HTTP_X_AMZ_RDMA_TOKEN";
constexpr const char* H_PSN = "HTTP_X_AMZ_RDMA_PSN";
constexpr const char* H_COOKIE = "HTTP_X_AMZ_RDMA_COOKIE";
constexpr const char* H_OP = "HTTP_X_AMZ_RDMA_OP";
constexpr const char* H_TARGET = "HTTP_X_AMZ_RDMA_TARGET";
constexpr const char* H_SIZE = "HTTP_X_AMZ_RDMA_SIZE";
constexpr const char* H_OFFSET = "HTTP_X_AMZ_RDMA_OFFSET";
constexpr const char* H_SESSION = "HTTP_X_AMZ_RDMA_SESSION";
constexpr const char* H_QPN = "HTTP_X_AMZ_RDMA_QPN";
constexpr const char* H_MR_ADDR = "HTTP_X_AMZ_RDMA_MR_ADDR";
constexpr const char* H_MR_RKEY = "HTTP_X_AMZ_RDMA_MR_RKEY";

std::string_view env(const req_state* s, const char* name)
{
  const char* v = s->info.env->get(name);
  return v ? std::string_view{v} : std::string_view{};
}

/// an HTTP answer the RGW error table has no entry for
struct Status {
  int http = 0;             ///< 0: use op_ret through set_req_state_err
  std::string code;
  std::string message;
  bool unsupported = false; ///< add the protocol's fall-back marker
};

Status status_of(Error e)
{
  switch (e) {
  case Error::OK: return {};
  case Error::ARG: return {400, "InvalidArgument", "malformed RDMA session request"};
  case Error::PEER: return {400, "InvalidArgument", "the x-amz-rdma-token names the gateway's host, which only a client on that host may name"};
  case Error::STATE: return {409, "RdmaTransferInProgress", "the session is already transferring"};
  case Error::SESSION:
  case Error::STALE:
  case Error::NO_SESSION: return {404, "NoSuchRdmaSession", "no such RDMA session"};
  case Error::CANCELLED: return {409, "RdmaSessionCancelled", "the RDMA session was cancelled"};
  case Error::LIMIT:
  case Error::NO_BUFFER: return {503, "SlowDown", "RDMA session capacity exhausted"};
  case Error::TOO_LARGE: return {413, "EntityTooLarge", "transfer exceeds the gateway RDMA buffer size"};
  case Error::WIRE: return {500, "InternalError", "RDMA transport failure"};
  case Error::UNSUPPORTED: return {501, "NotImplemented", "RDMA transfers are not available", true};
  case Error::INTERNAL: return {500, "InternalError", "RDMA session failure"};
  }
  return {500, "InternalError", ""};
}

Status status_of(Outcome o)
{
  switch (o) {
  case Outcome::OK: return {};
  case Outcome::BUSY: return {409, "RdmaPeerBusy", "the client queue pair was not ready; retry READY"};
  case Outcome::TIMEOUT: return {408, "RequestTimeout", "the RDMA transfer did not complete in time"};
  case Outcome::VERIFY_FAIL: return {500, "InternalError", "the RDMA completion did not match the session"};
  case Outcome::WIRE_FAIL: return {500, "InternalError", "RDMA transport failure"};
  case Outcome::BACKEND_FAIL: return {};
  case Outcome::CANCELLED: return {409, "RdmaSessionCancelled", "the RDMA session was cancelled"};
  }
  return {500, "InternalError", ""};
}

Status unsupported(std::string message)
{
  return {501, "NotImplemented", std::move(message), true};
}

/// who owns a session: READY and CANCEL must come from the same
/// principal PREPARE authenticated as
std::string principal_of(const req_state* s)
{
  std::ostringstream os;
  os << s->auth.identity->get_aclowner().id << '|'
     << s->auth.identity->get_subuser();
  if (auto arn = s->auth.identity->get_caller_identity(); arn) {
    os << '|' << arn->to_string();
  }
  return os.str();
}

/// whom the session limits count a session against: the account, or
/// the user where there is none. Every subuser and role session of it
/// shares its limits
std::string owner_of(const req_state* s)
{
  return to_string(s->auth.identity->get_aclowner().id);
}

int check_requester(const DoutPrefixProvider* dpp, req_state* s)
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }
  return check_signed(dpp, s->info);
}

/// PREPARE fields shared by GET and PUT
int parse_prepare(req_state* s, Op op, PrepareRequest& out)
{
  auto bad = [s](const char* what) {
    s->err.message = fmt::format("invalid or missing {} header", what);
    return -EINVAL;
  };
  out.op = op;
  const auto token = env(s, H_TOKEN);
  if (!token_is_zero(token) && !decode_token(token)) {
    return bad("x-amz-rdma-token");
  }
  // A client on this host may name this host's GIDs (the service refuses
  // them from others). By the peer of the connection, not a header such
  // as rgw_remote_addr_param names, which the client writes; and only a
  // loopback address, not one of the host's interfaces, which a proxy on
  // the host, or a translating container network, connects from for
  // clients of other hosts
  out.local_peer = s->cct->_conf.get_val<bool>("rgw_rdma_rc_trust_loopback") &&
    is_loopback_address(env(s, "REMOTE_ADDR"));
  if (token_is_zero(token) && !out.local_peer) {
    s->err.message = "the all-zero x-amz-rdma-token is accepted only "
      "from a loopback address of the gateway's host";
    return -EINVAL;
  }
  out.client_token = std::string{token};
  auto psn = parse_psn(env(s, H_PSN));
  if (!psn) return bad("x-amz-rdma-psn");
  out.client_psn = *psn;
  auto cookie = parse_cookie(env(s, H_COOKIE));
  if (!cookie) return bad("x-amz-rdma-cookie");
  out.cookie = *cookie;
  auto size = parse_decimal(env(s, H_SIZE));
  if (!size || *size == 0 || *size > MAX_TRANSFER_SIZE) return bad("x-amz-rdma-size");
  out.size = *size;
  out.offset = 0;
  if (auto o = env(s, H_OFFSET); !o.empty()) {
    // any offset whose range fits the signed offsets of a read: a
    // negative start would read a suffix of the object instead
    constexpr uint64_t max_end = std::numeric_limits<int64_t>::max();
    auto offset = parse_decimal(o);
    if (!offset || *offset > max_end - out.size) return bad("x-amz-rdma-offset");
    out.offset = *offset;
  }
  out.target = std::string{env(s, H_TARGET)};
  return 0;
}

int parse_ready(req_state* s, ReadyRequest& out)
{
  auto bad = [s](const char* what) {
    s->err.message = fmt::format("invalid or missing {} header", what);
    return -EINVAL;
  };
  const auto session = env(s, H_SESSION);
  if (!valid_session_id(session)) return bad("x-amz-rdma-session");
  out.session_id = std::string{session};
  auto cookie = parse_cookie(env(s, H_COOKIE));
  if (!cookie) return bad("x-amz-rdma-cookie");
  out.cookie = *cookie;
  auto qpn = parse_hex(env(s, H_QPN));
  if (!qpn || *qpn == 0 || *qpn > 0xffffff) return bad("x-amz-rdma-qpn");
  out.client_qpn = static_cast<uint32_t>(*qpn);
  if (auto a = env(s, H_MR_ADDR); !a.empty()) {
    auto addr = parse_hex(a);
    if (!addr) return bad("x-amz-rdma-mr-addr");
    out.client_mr_addr = *addr;
  }
  if (auto r = env(s, H_MR_RKEY); !r.empty()) {
    auto rkey = parse_hex(r);
    if (!rkey || *rkey > 0xffffffff) return bad("x-amz-rdma-mr-rkey");
    out.client_mr_rkey = static_cast<uint32_t>(*rkey);
  }
  return 0;
}

/// status line, protocol echo and (for errors) the error body
void send_status(req_state* s, RGWOp* op, int op_ret, const Status& st)
{
  if (st.http) {
    s->err.http_ret = st.http;
    s->err.err_code = st.code;
    s->err.message = st.message;
  } else if (op_ret < 0) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  if (st.unsupported) {
    // the protocol's explicit "fall back to plain HTTP" answer
    dump_header(s, "x-amz-rdma-protocol-status", STATUS_UNSUPPORTED);
  } else {
    dump_header(s, "x-amz-rdma-protocol", PROTOCOL);
  }
}

/// the service, or nullptr with the fall-back answer in status when it
/// is not running
Service* available_service(Status& status)
{
  auto* svc = Service::get();
  if (!svc || !svc->available()) {
    status = status_of(Error::UNSUPPORTED);
    return nullptr;
  }
  return svc;
}

/**
 * What the GET and the PUT share: the session, the PREPARE and READY
 * steps around it, and the headers their answers have in common. Base
 * is the S3 op that does the object's part of the work.
 */
template <class Base, Op OP>
class SessionOp : public Base {
 protected:
  const bool prepare_phase;
  PrepareRequest preq;
  ReadyRequest rreq;
  PrepareReply prep_reply;
  Status status;
  Session* sess = nullptr;
  uint64_t bytes_done = 0;
  /// the CRCs of the transferred bytes that the client asked for
  std::optional<uint64_t> final_crc64;
  std::optional<uint32_t> final_crc32c;

  explicit SessionOp(bool prepare) : prepare_phase(prepare) {}

  /// PREPARE: create the session, owned by the requester
  void prepare_session(Service* svc, optional_yield y) {
    preq.principal = principal_of(this->s);
    preq.owner = owner_of(this->s);
    if (auto e = svc->prepare(preq, prep_reply, y); e != Error::OK) {
      ldpp_dout(this, 4) << "hipobj-rc: prepare failed: " << to_string(e)
                         << dendl;
      status = status_of(e);
    }
  }

  /// READY: take the session that the requester prepared; false, with
  /// the answer set, when it cannot be taken
  bool claim_session(Service* svc) {
    rreq.principal = principal_of(this->s);
    Session* claimed = nullptr;
    if (auto e = svc->claim(rreq, &claimed); e != Error::OK) {
      ldpp_dout(this, 4) << "hipobj-rc: ready refused: " << to_string(e)
                         << dendl;
      status = status_of(e);
      return false;
    }
    sess = claimed;
    return true;
  }

  /// the status line and protocol echo; false, with the answer ended,
  /// for an error
  bool send_reply_status() {
    send_status(this->s, this, this->op_ret, status);
    if (this->op_ret < 0 || status.http) {
      end_header(this->s, this);
      return false;
    }
    return true;
  }

  /// the answer to PREPARE: the server's end of the queue pair
  void dump_prepare_reply() {
    dump_header(this->s, "x-amz-rdma-reply", "200:" + prep_reply.server_token);
    dump_header(this->s, "x-amz-rdma-token", prep_reply.server_token);
    dump_header(this->s, "x-amz-rdma-session", prep_reply.session_id);
    dump_header(this->s, "x-amz-rdma-psn", format_psn(prep_reply.server_psn));
    dump_header(this->s, "x-amz-rdma-qpn", format_hex(prep_reply.server_qpn));
  }

  /// the answer to READY: the outcome of the transfer
  void dump_ready_reply(const std::string& etag) {
    dump_header(this->s, "x-amz-rdma-bytes-transferred", bytes_done);
    dump_header(this->s, "x-amz-rdma-cookie", format_cookie(rreq.cookie));
    if (!etag.empty()) {
      dump_header(this->s, "x-amz-rdma-etag", etag);
      dump_etag(this->s, etag);
    }
    dump_header_if_nonempty(this->s, "x-amz-rdma-version-id",
                            this->version_id);
    rgw::rdma::dump_checksums(this->s, final_crc64, final_crc32c);
  }

 public:
  int verify_permission(optional_yield y) override {
    if (int r = check_requester(this, this->s); r < 0) {
      return r;
    }
    return Base::verify_permission(y);
  }

  int verify_params() override {
    return prepare_phase ? parse_prepare(this->s, OP, preq)
                         : parse_ready(this->s, rreq);
  }
};

// ---------------------------------------------------------------------
// GET: PREPARE stats and authorizes, READY reads the object into the
// session buffer (OSD-direct when the gateway has a DC target) and
// relays it to the client over the paired queue pair as it lands.

class GetOp : public SessionOp<RGWGetObj, Op::GET> {
  uint64_t range_ofs = 0;
  uint64_t range_size = 0;

  landed_ranges landed;
  uint64_t staged = 0;  ///< inline-copy cursor
  bool relayed = false; ///< OSDs placed bytes in the buffer
  /// the CRCs that the client asked for, of the bytes staged inline
  chained_crcs staged_crcs;
  uint64_t pushed = 0;  ///< bytes posted to the client
  uint32_t acked = 0;   ///< writes the client's queue pair took
  Outcome push_outcome = Outcome::OK;
  uint32_t depth = 1;
  /// how long the pushes may wait for the client: the session's deadline,
  /// later by the time a fence wait took
  clock::time_point push_deadline;

  std::string etag;

 public:
  explicit GetOp(bool prepare) : SessionOp(prepare) {}

  const char* name() const override {
    return prepare_phase ? "hipobj_rc_prepare_get" : "hipobj_rc_ready_get";
  }

  // data never goes through the head prefetch: it would arrive inline
  // and force a relay to restart in staged mode
  bool prefetch_data() override { return false; }

  int get_params(optional_yield y) override {
    // the CRC of the delivered bytes the READY answer reports
    if (int r = rgw::rdma::parse_checksum_header(s, &rdma_cksum_asked);
        r < 0) {
      return r;
    }
    // and the same CRCs of the bytes the read stages: RGWGetObj::execute()
    // gets here once, before it reads
    if (!prepare_phase && s->cct->_conf.get_val<bool>("rgw_rdma_checksum")) {
      staged_crcs.reset(rdma_cksum_asked);
    }
    // the range is the session's, never a Range header
    range_str = nullptr;
    if_mod = if_unmod = if_match = if_nomatch = nullptr;
    ofs = range_ofs;
    end = range_ofs + range_size - 1;
    partial_content = true;
    return 0;
  }

  int get_decrypt_filter(std::unique_ptr<RGWGetObj_Filter>* filter,
                         RGWGetObj_Filter* cb, bufferlist* manifest_bl) override {
    *filter = nullptr;
    if (attrs.count(RGW_ATTR_CRYPT_MODE)) {
      // SSE needs the decrypt filter and, for SSE-C, the customer key
      // headers of an S3 GET; serve these over plain HTTP
      status = unsupported("encrypted objects are not served over RDMA");
      return -ERR_NOT_IMPLEMENTED;
    }
    return 0;
  }

  int send_response_data_error(optional_yield y) override { return 0; }
  int send_response_data(bufferlist& bl, off_t bl_ofs, off_t len) override;
  int get_oob_cb(uint64_t ofs, uint64_t len, bool landed) override;

  void execute(optional_yield y) override;
  void send_response() override;

 private:
  bool eligible();
  /// whether the reads went out with the buffer's relay token, so that
  /// an OSD may write with it
  bool relayed_window() const {
    return rdma_mode == RdmaMode::RELAY ||
      rdma_switched_from == RdmaMode::RELAY;
  }
  int post(uint64_t off, uint64_t len, bool final);
  int reap_one();
  int push(bool final);
  void execute_prepare(optional_yield y);
  void execute_ready(optional_yield y);
};

bool GetOp::eligible()
{
  if (attrs.count(RGW_ATTR_CRYPT_MODE)) {
    status = unsupported("encrypted objects are not served over RDMA");
    return false;
  }
  if (attrs.count(RGW_ATTR_USER_MANIFEST) || attrs.count(RGW_ATTR_SLO_MANIFEST)) {
    status = unsupported("manifest objects are not served over RDMA");
    return false;
  }
  return true;
}

int GetOp::reap_one()
{
  ibv_wc wc{};
  switch (sess->conn.poll_one(wc, push_deadline, s->yield)) {
  case Completion::OK:
    sess->conn.reaped();
    ++acked;
    return 0;
  case Completion::TIMEOUT:
    push_outcome = Outcome::TIMEOUT;
    break;
  case Completion::BUSY:
    push_outcome = Outcome::BUSY;
    break;
  case Completion::WIRE_ERROR:
    ldpp_dout(this, 1) << "hipobj-rc: push completion failed: "
                       << ibv_wc_status_str(wc.status) << dendl;
    push_outcome = Outcome::WIRE_FAIL;
    break;
  }
  return -EIO;
}

int GetOp::post(uint64_t off, uint64_t len, bool final)
{
  while (sess->conn.outstanding() >= depth) {
    if (int r = reap_one(); r < 0) {
      return r;
    }
  }
  auto* base = static_cast<char*>(sess->buf->ptr);
  int r = sess->conn.post_write(sess->buf->mr, base + off,
                                static_cast<uint32_t>(len),
                                sess->client_mr_addr + off,
                                sess->client_mr_rkey, final, sess->cookie,
                                final ? WR_FINAL : WR_PUSH, true);
  if (r < 0) {
    ldpp_dout(this, 1) << "hipobj-rc: ibv_post_send failed: " << cpp_strerror(r)
                       << dendl;
    push_outcome = Outcome::WIRE_FAIL;
    return -EIO;
  }
  return 0;
}

int GetOp::push(bool final)
{
  if (sess->cancelled) {
    // CANCEL stops the transfer here: no more writes to the client, and
    // the read ends with the error
    return -ECANCELED;
  }
  auto writes = plan_pushes(landed.prefix(), pushed, total_len, final);
  if (!writes) {
    ldpp_dout(this, 0) << "ERROR: hipobj-rc: only " << landed.prefix()
                       << " of " << total_len
                       << " bytes landed in the relay buffer" << dendl;
    return -EIO;
  }
  // RC executes writes in order, so the client's completion for the
  // write carrying the immediate follows every earlier write
  for (const auto& w : *writes) {
    ldpp_dout(this, 20) << "hipobj-rc: push " << w.ofs << "~" << w.len
                        << (w.with_imm ? " with immediate" : "")
                        << " (landed " << landed.prefix() << "/" << total_len
                        << ")" << dendl;
    if (int r = post(w.ofs, w.len, w.with_imm); r < 0) {
      return r;
    }
    pushed = w.ofs + w.len;
  }
  if (final) {
    while (sess->conn.outstanding() > 0) {
      if (int r = reap_one(); r < 0) {
        return r;
      }
    }
  }
  return 0;
}

int GetOp::send_response_data(bufferlist& bl, off_t bl_ofs, off_t len)
{
  if (prepare_phase) {
    return 0;  // the stat is all PREPARE needs
  }
  if (!eligible()) {
    return -ERR_NOT_IMPLEMENTED;
  }
  if (len <= 0) {
    return 0;
  }
  if (staged + len > total_len || staged + len > sess->buf->size) {
    ldpp_dout(this, 0) << "ERROR: hipobj-rc: relay buffer overflow at "
                       << staged << "+" << len << dendl;
    return -EIO;
  }
  char* dst = static_cast<char*>(sess->buf->ptr) + staged;
  bl.begin(bl_ofs).copy(len, dst);
  staged_crcs.append(dst, len);
  landed.add(staged, len);
  staged += len;
  return push(false);
}

int GetOp::get_oob_cb(uint64_t o, uint64_t len, bool stripe_landed)
{
  if (prepare_phase || !sess) {
    return -EIO;
  }
  if (!eligible()) {
    return -ERR_NOT_IMPLEMENTED;
  }
  if (o + len > total_len || o + len > sess->buf->size) {
    ldpp_dout(this, 0) << "ERROR: hipobj-rc: OSD delivery outside the relay "
                       << "window at " << o << "+" << len << dendl;
    return -EIO;
  }
  relayed = true;
  if (stripe_landed) {
    landed.add(o, len);
  } else {
    // the OSD may have replied before these bytes were in the buffer:
    // they go to the client only after the fence (execute_ready())
    landed.add_placed(o, len);
  }
  return push(false);
}

void GetOp::execute(optional_yield y)
{
  if (prepare_phase) {
    execute_prepare(y);
  } else {
    execute_ready(y);
  }
}

void GetOp::execute_prepare(optional_yield y)
{
  auto* svc = available_service(status);
  if (!svc) {
    return;
  }
  range_ofs = preq.offset;
  range_size = preq.size;
  get_data = false;
  RGWGetObj::execute(y);  // stat, conditions, permissions already done
  if (op_ret < 0 || status.http) {
    return;
  }
  if (!eligible()) {
    return;
  }
  if (total_len == 0 || s->obj_size == 0) {
    // range_to_ofs() leaves a range over an empty object unclamped
    op_ret = -ERANGE;
    return;
  }
  // a range past the end transfers what exists
  preq.size = total_len;
  prepare_session(svc, y);
}

void GetOp::execute_ready(optional_yield y)
{
  auto* svc = available_service(status);
  if (!svc || !claim_session(svc)) {
    return;
  }
  // the session ends in finish() also when the data phase throws, or the
  // reaper never takes it; what the OSDs may still write into the buffer
  // is not known then. A throw out of the read itself leaves no fence
  // recorded although relayed reads may have gone out: hold the buffer
  // for the fence RGW sizes those reads by, the caps of every pool's
  // lease and drain and of the OSDs' late cut-off tolerance
  auto unclaim = make_scope_guard([this, svc] {
    if (sess) {
      std::chrono::milliseconds hold{rdma_window_hold_ms()};
      if (!rdma_fence_state.sent && relayed_window()) {
        hold = std::chrono::milliseconds{static_cast<uint64_t>(std::ceil(
          rgw::rdma::delivery_fence() * 1000.0))};
      }
      svc->finish(sess, Outcome::BACKEND_FAIL, 0, hold, relayed_window());
      sess = nullptr;
    }
  });
  if (sess->client_mr_rkey == 0 && sess->client_mr_addr == 0) {
    svc->finish(sess, Outcome::VERIFY_FAIL, 0);
    sess = nullptr;
    status = status_of(Error::ARG);
    status.message = "no client memory region for the GET";
    return;
  }
  depth = std::max<uint32_t>(1, svc->queue_depth());
  range_ofs = sess->offset;
  range_size = sess->size;
  relay_window = sess->size;
  relay_token = svc->osd_direct() ? sess->buf->osd_token : std::string{};
  relay_lands = svc->relay_lands();
  get_data = true;
  push_deadline = sess->deadline;

  RGWGetObj::execute(y);

  if (op_ret == 0 && !status.http && s->obj_size == 0) {
    op_ret = -ERANGE;  // replaced by an empty object since PREPARE
  }
  if (op_ret == 0 && !status.http && landed.unconfirmed() &&
      !sess->cancelled) {
    // stripes whose results did not say landed are in the buffer once
    // the fence passed: wait it out before they go to the client, which
    // also leaves the buffer nothing to be held for. Not after a CANCEL,
    // which the push answers, and the buffer is held instead. The relay
    // runs only over a transport that reports landed writes, so this is
    // rare, as with an OSD of an older release. The wait is not part of
    // the time the client has to take the pushes
    const auto fence_from = clock::now();
    rdma_fence_before("relaying stripes that did not say they landed",
                      false, true);
    landed.confirm_placed();
    push_deadline += clock::now() - fence_from;
  }
  if (op_ret == 0 && !status.http) {
    if (int r = push(true); r < 0) {
      op_ret = r;
    }
  }
  // the logs and the rate limits count what went to the client, also
  // when the transfer failed after some of it, and not what the OSDs
  // relayed into the buffer
  s->rdma_bytes_transferred = pushed;
  Outcome outcome = Outcome::OK;
  if (op_ret < 0 || status.http) {
    outcome = push_outcome != Outcome::OK ? push_outcome : Outcome::BACKEND_FAIL;
  } else {
    bytes_done = total_len;
    if (s->cct->_conf.get_val<bool>("rgw_rdma_checksum")) {
      // the OSDs' own checksums of the bytes they placed, when they
      // relayed them; else the gateway's, chained over its copies when
      // it staged every byte, or of its buffer
      rgw::rdma::crcs c;
      if (rdma_mode == RdmaMode::RELAY) {
        c = {rdma_crc64, rdma_crc32c};
      } else if (!relayed && staged == total_len) {
        c = staged_crcs.result();
      }
      if (rdma_cksum_asked.crc64nvme) {
        final_crc64 = c.crc64 ? *c.crc64 :
          ceph::crc64nvme(0, sess->buf->ptr, total_len);
      }
      if (rdma_cksum_asked.crc32c) {
        final_crc32c = c.crc32c ? *c.crc32c :
          rgw::rdma::crc32c_of(sess->buf->ptr, total_len);
      }
    }
    if (auto it = attrs.find(RGW_ATTR_ETAG); it != attrs.end()) {
      etag = it->second.to_str();
      while (!etag.empty() && etag.back() == '\0') {
        etag.pop_back();
      }
    }
  }
  // an OSD that received a delivery descriptor may still write into
  // the buffer until the fence runs out: unless every read came back
  // declined or landed and none was resent, whether or not the reads
  // delivered the range (see rgw::rdma::fence_needed()). The response
  // does not wait for that; keep the buffer out of the pool that long
  // instead
  const std::chrono::milliseconds quarantine{rdma_window_hold_ms()};
  if (outcome == Outcome::BUSY && quarantine.count() > 0) {
    // a retried READY would relay through this buffer, so finish() ends
    // the session instead of rolling the claim back; do not answer 409,
    // which tells the client to retry READY
    outcome = Outcome::WIRE_FAIL;
  }
  if (outcome == Outcome::BUSY && acked > 0) {
    // the client's queue pair took writes of this transfer and expects
    // a PSN past the one a retried READY starts from again. Unless the
    // client resets it, the retry's writes, split as the reads land
    // then, can miss that PSN, and the last can arrive as a duplicate
    // that completes nothing for the client. End the session instead
    outcome = Outcome::WIRE_FAIL;
  }
  // a BUSY that the session cannot be rolled back from ends it, and the
  // answer must not tell the client to retry READY
  outcome = svc->finish(sess, outcome, bytes_done, quarantine,
                        relayed_window());
  sess = nullptr;
  if (outcome != Outcome::OK && status.http == 0) {
    status = status_of(outcome);
  }
}

void GetOp::send_response()
{
  if (!send_reply_status()) {
    return;
  }
  if (prepare_phase) {
    dump_prepare_reply();
  } else {
    // the checksums of the delivered range, not of the object, so not
    // in S3's x-amz-checksum-<algorithm>
    dump_ready_reply(etag);
  }
  end_header(s, this, nullptr, 0);
}

// ---------------------------------------------------------------------
// PUT: PREPARE authorizes and arms the staging buffer, READY waits for
// the client's write-with-immediate and stores the buffer through the
// regular PUT pipeline (bucket default encryption, compression,
// checksums, notifications).

class PutOp : public SessionOp<RGWPutObj_ObjStore_S3, Op::PUT> {
  uint64_t cursor = 0;
  /// the CRCs of the session buffer that the client asked for
  rgw::rdma::cksum_want cksum_asked;
  /// and of the chunks get_data() copied
  chained_crcs copied_crcs;

 public:
  explicit PutOp(bool prepare) : SessionOp(prepare) {}
  using RGWPutObj_ObjStore_S3::get_data;

  const char* name() const override {
    return prepare_phase ? "hipobj_rc_prepare_put" : "hipobj_rc_ready_put";
  }

  int get_params(optional_yield y) override {
    // control requests carry no body; a client may omit Content-Length
    if (!s->length) {
      s->length = "0";
    }
    if (int r = rgw::rdma::parse_checksum_header(s, &cksum_asked); r < 0) {
      return r;
    }
    return RGWPutObj_ObjStore_S3::get_params(y);
  }

  /// the body comes from the session buffer, not the HTTP request
  int get_data(bufferlist& bl) override {
    if (sess && sess->cancelled) {
      // CANCEL stops the store at its next chunk: nothing is stored
      return -ECANCELED;
    }
    if (!sess || cursor >= bytes_done) {
      return 0;
    }
    const uint64_t n = std::min<uint64_t>(bytes_done - cursor,
                                          s->cct->_conf->rgw_max_chunk_size);
    const char* p = static_cast<const char*>(sess->buf->ptr) + cursor;
    bl.append(p, n);
    copied_crcs.append(p, n);
    cursor += n;
    return static_cast<int>(n);
  }

  void execute(optional_yield y) override;
  void send_response() override;
};

void PutOp::execute(optional_yield y)
{
  auto* svc = available_service(status);
  if (!svc) {
    return;
  }
  if (prepare_phase) {
    if (preq.size > s->cct->_conf->rgw_max_put_size) {
      op_ret = -ERR_TOO_LARGE;
      return;
    }
    prepare_session(svc, y);
    return;
  }

  if (!claim_session(svc)) {
    return;
  }
  // the session ends in finish() also when the store throws (a streaming
  // signature whose HMAC fails does), or the reaper never takes it
  auto unclaim = make_scope_guard([this, svc] {
    if (sess) {
      svc->finish(sess, Outcome::BACKEND_FAIL, 0);
      sess = nullptr;
    }
  });

  // the receive was armed at PREPARE; the client writes once the pair
  // is up and signals the session cookie in the immediate. The queue
  // pair shares a protection domain with the staging buffer alone, so a
  // write that it took landed there, and one of the session's size
  // filled it; PREPARE zeroed it besides (see Service::make_buffer())
  ibv_wc wc{};
  Outcome outcome = Outcome::OK;
  switch (sess->conn.poll_one(wc, sess->deadline, y)) {
  case Completion::OK:
    // the logs and the rate limits count what the client wrote, also
    // when it is not stored, as with the body of a PUT
    s->rdma_bytes_transferred = wc.byte_len;
    if (wc.opcode != IBV_WC_RECV_RDMA_WITH_IMM ||
        !(wc.wc_flags & IBV_WC_WITH_IMM) ||
        ntohl(wc.imm_data) != sess->cookie || wc.byte_len != sess->size) {
      ldpp_dout(this, 1) << "hipobj-rc: PUT completion does not match the "
                         << "session (opcode=" << wc.opcode << " bytes="
                         << wc.byte_len << ")" << dendl;
      outcome = Outcome::VERIFY_FAIL;
    }
    break;
  case Completion::TIMEOUT:
    outcome = Outcome::TIMEOUT;
    break;
  case Completion::BUSY:
    outcome = Outcome::BUSY;
    break;
  case Completion::WIRE_ERROR:
    outcome = Outcome::WIRE_FAIL;
    break;
  }

  if (outcome == Outcome::OK) {
    bytes_done = sess->size;
    s->content_length = sess->size;
    // the body is the session buffer, whatever token READY carries
    rdma_staging_allowed = false;
    if (s->cct->_conf.get_val<bool>("rgw_rdma_checksum")) {
      copied_crcs.reset(cksum_asked);
    }
    RGWPutObj_ObjStore_S3::execute(y);
    if (op_ret < 0) {
      outcome = Outcome::BACKEND_FAIL;
      bytes_done = 0;
    } else if (cursor == bytes_done) {
      // get_data() copied every byte, unless a copy source gave the body
      final_crc64 = copied_crcs.result().crc64;
      final_crc32c = copied_crcs.result().crc32c;
    }
  }
  outcome = svc->finish(sess, outcome, bytes_done);
  sess = nullptr;
  if (outcome != Outcome::OK && status.http == 0) {
    status = status_of(outcome);
  }
}

void PutOp::send_response()
{
  if (!send_reply_status()) {
    return;
  }
  if (prepare_phase) {
    dump_prepare_reply();
    // the staging buffer the client writes the object into
    dump_header(s, "x-amz-rdma-mr-addr", format_hex(prep_reply.staging_addr));
    dump_header(s, "x-amz-rdma-mr-rkey", format_hex(prep_reply.staging_rkey));
  } else {
    // the checksums of the bytes the client wrote
    dump_ready_reply(etag);
    // and the headers an S3 PUT answers with: the object's encryption,
    // version, expiration and stored checksum
    dump_stored_headers();
  }
  end_header(s, this, nullptr, 0);
}

// ---------------------------------------------------------------------

class CancelOp : public RGWOp {
  std::string session_id;
  Status status;

 public:
  const char* name() const override { return "hipobj_rc_cancel"; }

  int verify_permission(optional_yield y) override {
    // ownership is the only authorization: the session names its owner
    return check_requester(this, s);
  }
  int init_processing(optional_yield y) override { return 0; }
  int verify_params() override {
    const auto id = env(s, H_SESSION);
    if (!valid_session_id(id)) {
      s->err.message = "invalid or missing x-amz-rdma-session header";
      return -EINVAL;
    }
    session_id = std::string{id};
    return 0;
  }

  void execute(optional_yield y) override {
    auto* svc = available_service(status);
    if (!svc) {
      return;
    }
    switch (auto e = svc->cancel(session_id, principal_of(s), y); e) {
    case Error::OK:
    case Error::STALE:
    case Error::NO_SESSION:
      break;  // already gone: idempotent success
    default:
      status = status_of(e);
    }
  }

  void send_response() override {
    send_status(s, this, op_ret, status);
    if (op_ret < 0 || status.http) {
      end_header(s, this);
      return;
    }
    end_header(s, this, nullptr, 0);
  }
};

/// answers without authenticating: the fall-back marker when RDMA is
/// off, or a request too malformed to route
class RejectOp : public RGWOp {
  const Status status;

 public:
  explicit RejectOp(Status st) : status(std::move(st)) {}
  const char* name() const override { return "hipobj_rc_reject"; }
  int verify_permission(optional_yield y) override { return 0; }
  int init_processing(optional_yield y) override { return 0; }
  void execute(optional_yield y) override {}
  void send_response() override {
    send_status(s, this, 0, status);
    end_header(s, this);
  }
};

class RejectHandler : public RGWHandler_REST {
  const Status status;

 public:
  explicit RejectHandler(Status st) : status(std::move(st)) {}
  int init_permissions(RGWOp*, optional_yield) override { return 0; }
  int read_permissions(RGWOp*, optional_yield) override { return 0; }
  int authorize(const DoutPrefixProvider*, optional_yield) override { return 0; }
  int postauth_init(optional_yield) override { return 0; }
  RGWOp* op_post() override { return new RejectOp(status); }
};

class Handler : public RGWHandler_REST_S3 {
  const Route route;
  const Op op;

 public:
  Handler(const rgw::auth::StrategyRegistry& auth, Route route, Op op)
    : RGWHandler_REST_S3(auth), route(route), op(op) {}

  int init_permissions(RGWOp* o, optional_yield y) override {
    if (route == Route::CANCEL) {
      return 0;  // no bucket or object involved
    }
    return RGWHandler_REST_S3::init_permissions(o, y);
  }

  int read_permissions(RGWOp* o, optional_yield y) override {
    if (route == Route::CANCEL) {
      return 0;
    }
    // a GET authorizes against the object, a PUT against the bucket
    return do_read_permissions(o, op == Op::PUT, y);
  }

  RGWOp* op_post() override {
    switch (route) {
    case Route::PREPARE:
    case Route::READY: {
      const bool prepare = route == Route::PREPARE;
      if (op == Op::GET) {
        return new GetOp(prepare);
      }
      return new PutOp(prepare);
    }
    case Route::CANCEL:
      return new CancelOp;
    }
    return nullptr;
  }
};

RGWHandler_REST* reject(Status st)
{
  return new RejectHandler(std::move(st));
}

Status bad_request(std::string message)
{
  return {400, "InvalidRequest", std::move(message)};
}

} // anonymous namespace

RGWHandler_REST* RESTMgr::get_handler(rgw::sal::Driver* driver, req_state* s,
                                      const rgw::auth::StrategyRegistry& auth,
                                      const std::string& frontend_prefix)
{
  s->info.args.set(s->info.request_params);
  s->info.args.parse(s);
  if (RGWHandler_REST::allocate_formatter(s, RGWFormat::XML, false) < 0) {
    return nullptr;
  }
  if (s->op != OP_POST) {
    return nullptr;
  }

  Route route;
  if (s->relative_uri == "/prepare") {
    route = Route::PREPARE;
  } else if (s->relative_uri == "/ready") {
    route = Route::READY;
  } else if (s->relative_uri == "/cancel") {
    route = Route::CANCEL;
  } else {
    return nullptr;
  }

  auto* svc = Service::get();
  if (!svc || !svc->available()) {
    return reject(status_of(Error::UNSUPPORTED));
  }
  if (env(s, H_PROTOCOL) != PROTOCOL) {
    if (route == Route::PREPARE) {
      return reject(unsupported("unknown x-amz-rdma-protocol"));
    }
    return reject(bad_request("missing or unknown x-amz-rdma-protocol"));
  }

  Op op = Op::GET;
  std::string target;
  switch (route) {
  case Route::CANCEL:
    return new Handler(auth, route, op);
  case Route::PREPARE: {
    std::string o{env(s, H_OP)};
    std::transform(o.begin(), o.end(), o.begin(), ::toupper);
    if (o == "GET") {
      op = Op::GET;
    } else if (o == "PUT") {
      op = Op::PUT;
    } else {
      return reject(bad_request("x-amz-rdma-op must be GET or PUT"));
    }
    target = std::string{env(s, H_TARGET)};
    break;
  }
  case Route::READY: {
    // route by the session's target; READY re-authorizes against it
    const auto id = env(s, H_SESSION);
    SessionInfo info;
    if (!valid_session_id(id)) {
      return reject(bad_request("invalid or missing x-amz-rdma-session"));
    }
    if (svc->peek(std::string{id}, info) != Error::OK) {
      return reject(status_of(Error::NO_SESSION));
    }
    op = info.op;
    target = std::move(info.target);
    break;
  }
  }

  auto t = parse_target(target);
  if (!t) {
    return reject(bad_request("invalid or missing x-amz-rdma-target"));
  }
  std::string instance;
  if (!t->query.empty()) {
    // a version of a GET is the only subresource served over RDMA
    constexpr std::string_view vid = "versionId=";
    if (op != Op::GET || t->query.rfind(vid, 0) != 0 ||
        t->query.find('&') != std::string::npos) {
      return reject(unsupported("only versionId may qualify an RDMA target"));
    }
    // strictly, as the bucket and key: a lenient decode turns a bad
    // escape into no version, which reads the current one
    auto v = percent_decode(t->query.substr(vid.size()));
    if (!v) {
      return reject(bad_request("malformed versionId in x-amz-rdma-target"));
    }
    instance = std::move(*v);
  }
  s->init_state.url_bucket = t->bucket;
  s->object_key = rgw_obj_key(t->key, instance);
  s->object = driver->get_object(s->object_key);
  return new Handler(auth, route, op);
}

} // namespace rgw::rdma::rc
