// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/rdma_token.h"
#include "common/crc64nvme.h"
#include "messages/MOSDOpReply.h"
#include "osd/oob_xfer.h"
#include "osdc/Objecter.h"
#include "osdc/SplitOp.h"

#include <optional>
#include <vector>

#include "gtest/gtest.h"

// Pin the rdma delivery descriptor wire format (carried as a per-op
// vector after the v9 MOSDOp fields, aligned with the ops). Any change
// here is a wire format change and needs a struct version bump.
TEST(RdmaDelivery, WireFormat)
{
  ceph::rdma::delivery_t d;
  d.token = "deadbeef:1000:x";
  d.base_offset = 0xa1b2c3d4e5f60718ull;
  d.flags = 0;

  bufferlist bl;
  encode(d, bl);

  // exact bytes: ENCODE_START(1,1) header [u8 v, u8 compat, le32 len],
  // le32 token length + token bytes, le64 base_offset, le32 flags
  static const unsigned char expected_bytes[] = {
    0x01, 0x01, 0x1f, 0x00, 0x00, 0x00,              // struct v1, compat 1, len 31
    0x0f, 0x00, 0x00, 0x00,                          // token length (le32)
    'd', 'e', 'a', 'd', 'b', 'e', 'e', 'f', ':',
    '1', '0', '0', '0', ':', 'x',                    // token
    0x18, 0x07, 0xf6, 0xe5, 0xd4, 0xc3, 0xb2, 0xa1,  // base_offset (le64)
    0x00, 0x00, 0x00, 0x00,                          // flags (le32)
  };
  bufferlist expected;
  expected.append(reinterpret_cast<const char*>(expected_bytes),
                  sizeof(expected_bytes));
  EXPECT_TRUE(bl.contents_equal(expected))
    << "rdma delivery descriptor layout changed; this is a wire format break";

  // and it round-trips
  ceph::rdma::delivery_t out;
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_EQ(d.token, out.token);
  EXPECT_EQ(d.base_offset, out.base_offset);
  EXPECT_EQ(d.flags, out.flags);
  EXPECT_TRUE(p.end());
}

TEST(RdmaDelivery, OobResultWireFormat)
{
  ceph::rdma::oob_result_t r;
  r.bytes = 0x0102030405060708ull;
  r.crc64 = 0xae8b14860a799888ull;
  r.flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME;

  r.crc32c = 0xe3069283u;

  bufferlist bl;
  encode(r, bl);
  // ENCODE_START(1,1) header, le64 bytes, le64 crc64, le32 flags,
  // the ranges vector (le32 count, empty here), le32 crc32c
  static const unsigned char expected_bytes[] = {
    0x01, 0x01, 0x1c, 0x00, 0x00, 0x00,              // v1, compat 1, len 28
    0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,  // bytes (le64)
    0x88, 0x98, 0x79, 0x0a, 0x86, 0x14, 0x8b, 0xae,  // crc64 (le64)
    0x01, 0x00, 0x00, 0x00,                          // flags (le32)
    0x00, 0x00, 0x00, 0x00,                          // ranges: 0 entries
    0x83, 0x92, 0x06, 0xe3,                          // crc32c (le32)
  };
  bufferlist expected;
  expected.append(reinterpret_cast<const char*>(expected_bytes),
                  sizeof(expected_bytes));
  EXPECT_TRUE(bl.contents_equal(expected))
    << "oob result layout changed; this is a wire format break";

  ceph::rdma::oob_result_t out;
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_EQ(r.bytes, out.bytes);
  EXPECT_EQ(r.crc64, out.crc64);
  EXPECT_EQ(r.flags, out.flags);
  EXPECT_EQ(r.crc32c, out.crc32c);
  EXPECT_TRUE(p.end());
}

// A result with a range pins the range's layout too: ENCODE_START(1,1)
// header, le64 ofs, le64 len, le64 crc64, le32 crc32c.
TEST(RdmaDelivery, OobResultRangeWireFormat)
{
  ceph::rdma::oob_result_t r;
  r.bytes = 0x1000;
  r.crc64 = 0x1234;
  r.flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME |
            ceph::rdma::oob_result_t::FLAG_RANGES |
            ceph::rdma::oob_result_t::FLAG_CRC32C;
  r.ranges = {{0x2000, 0x1000, 0x5678, 0x9abc}};
  r.crc32c = 0xdef0;

  bufferlist bl;
  encode(r, bl);
  static const unsigned char expected_bytes[] = {
    0x01, 0x01, 0x3e, 0x00, 0x00, 0x00,              // v1, compat 1, len 62
    0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // bytes (le64)
    0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // crc64 (le64)
    0x45, 0x00, 0x00, 0x00,                          // flags (le32)
    0x01, 0x00, 0x00, 0x00,                          // ranges: 1 entry
    0x01, 0x01, 0x1c, 0x00, 0x00, 0x00,              // v1, compat 1, len 28
    0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // ofs (le64)
    0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // len (le64)
    0x78, 0x56, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // crc64 (le64)
    0xbc, 0x9a, 0x00, 0x00,                          // crc32c (le32)
    0xf0, 0xde, 0x00, 0x00,                          // crc32c (le32)
  };
  bufferlist expected;
  expected.append(reinterpret_cast<const char*>(expected_bytes),
                  sizeof(expected_bytes));
  EXPECT_TRUE(bl.contents_equal(expected))
    << "oob result range layout changed; this is a wire format break";

  ceph::rdma::oob_result_t out;
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_TRUE(p.end());
  EXPECT_EQ(r.flags, out.flags);
  EXPECT_EQ(r.ranges, out.ranges);
  EXPECT_EQ(r.crc32c, out.crc32c);
}

// The canonical CRC-32C is S3's, and two of them combine into the CRC of
// the concatenation, whatever the lengths and the buffer boundaries.
TEST(RdmaDelivery, Crc32cCanonicalAndCombine)
{
  bufferlist check;
  check.append("123456789");
  EXPECT_EQ(0xe3069283u, ceph::rdma::crc32c_canonical(check));
  EXPECT_EQ(0u, ceph::rdma::crc32c_canonical(bufferlist()));

  std::string data;
  for (int i = 0; i < 100000; ++i) {
    data += static_cast<char>((i * 131 + i / 7) & 0xff);
  }
  bufferlist whole;
  // several buffers, so that bufferlist's per-buffer CRC caching is used
  for (size_t o = 0; o < data.size(); o += 30001) {
    whole.append(data.data() + o, std::min<size_t>(30001, data.size() - o));
  }
  const uint32_t expect = ceph::rdma::crc32c_canonical(whole);
  for (size_t cut : {size_t(0), size_t(1), size_t(4095), size_t(4096),
                     size_t(65537), data.size() - 1, data.size()}) {
    bufferlist a, b;
    a.append(data.data(), cut);
    b.append(data.data() + cut, data.size() - cut);
    EXPECT_EQ(expect, ceph::rdma::crc32c_combine(
                ceph::rdma::crc32c_canonical(a),
                ceph::rdma::crc32c_canonical(b), b.length()))
      << "cut at " << cut;
  }
}

TEST(RdmaDelivery, FoldCrc32cRanges)
{
  // as FoldCrc64Ranges: interleaved chunks of two EC shards, out of order
  std::string data;
  for (int c = 0; c < 6; ++c) {
    data += std::string(1000, static_cast<char>('a' + c));
  }
  bufferlist whole;
  whole.append(data);
  const uint32_t expect = ceph::rdma::crc32c_canonical(whole);

  std::vector<ceph::rdma::crc_range_t> ranges;
  for (int c : {4, 0, 2, 5, 1, 3}) {
    bufferlist part;
    part.append(data.data() + c * 1000, 1000);
    ranges.push_back({uint64_t(c) * 1000, 1000, ceph::crc64nvme(part),
                      ceph::rdma::crc32c_canonical(part)});
  }
  auto folded = ceph::rdma::fold_crc32c_ranges(ranges);
  ASSERT_TRUE(folded.has_value());
  EXPECT_EQ(expect, *folded);
  // the same ranges still fold the CRC-64/NVME too
  auto folded64 = ceph::rdma::fold_crc64_ranges(ranges);
  ASSERT_TRUE(folded64.has_value());
  EXPECT_EQ(ceph::crc64nvme(whole), *folded64);

  auto gapped = ranges;
  gapped.erase(gapped.begin() + 2);
  EXPECT_FALSE(ceph::rdma::fold_crc32c_ranges(gapped).has_value());
  EXPECT_FALSE(ceph::rdma::fold_crc32c_ranges({}).has_value());
}

TEST(RdmaDelivery, PerOpVectorRoundTrip)
{
  // the descriptors ride as a per-op vector on the MOSDOp tail: empty
  // when nothing is requested, otherwise one entry per op where an
  // empty token means "inline for this op"
  std::vector<ceph::rdma::delivery_t> none;
  std::vector<ceph::rdma::delivery_t> some = {
    ceph::rdma::delivery_t{},                          // op 0: inline
    ceph::rdma::delivery_t{"aa:bb:opaque", 42, 0},     // op 1
    ceph::rdma::delivery_t{"aa:bb:opaque", 4096,
			   ceph::rdma::delivery_t::FLAG_CRC64NVME},  // op 2
  };
  EXPECT_TRUE(some[0].empty());
  EXPECT_FALSE(some[1].empty());

  bufferlist bl;
  encode(none, bl);
  encode(some, bl);

  std::vector<ceph::rdma::delivery_t> out1, out2;
  auto p = bl.cbegin();
  decode(out1, p);
  decode(out2, p);
  EXPECT_TRUE(out1.empty());
  ASSERT_EQ(3u, out2.size());
  EXPECT_TRUE(out2[0].empty());
  EXPECT_EQ("aa:bb:opaque", out2[1].token);
  EXPECT_EQ(42u, out2[1].base_offset);
  EXPECT_EQ(0u, out2[1].flags);
  EXPECT_EQ(4096u, out2[2].base_offset);
  EXPECT_EQ(ceph::rdma::delivery_t::FLAG_CRC64NVME, out2[2].flags);
  EXPECT_TRUE(p.end());
}

// The descriptors ride on a v9 MOSDOp after its v9 fields, and only
// when some op carries one: an OSD that does not know them (upstream
// Umbrella, which advertises SERVER_UMBRELLA too) stops decoding before
// them and serves the request inline, and a request without any is the
// plain v9 encoding. The reply's results trail a v8 MOSDOpReply alike.
TEST(RdmaDelivery, MessagesCarryRdmaFieldsAsSuffix)
{
  const hobject_t hobj(object_t("obj"), "", CEPH_NOSNAP, 0x1234, 1, "");
  spg_t pgid(pg_t(0x1234, 1));
  const std::vector<ceph::rdma::delivery_t> deliveries = {
    ceph::rdma::delivery_t{},                          // op 0: inline
    ceph::rdma::delivery_t{"aa:bb:opaque", 4096,
                           ceph::rdma::delivery_t::FLAG_CRC64NVME},
  };

  // what a client sends, with and without descriptors (the Objecter's
  // MOSDOp; an OSD decodes into the std::vector one)
  using ClientMOSDOp = _mosdop::MOSDOp<osdc_opvec>;
  auto encode_op = [&](bool with_deliveries) {
    auto m = ceph::make_message<ClientMOSDOp>(
      1, 42, hobj, pgid, 10, CEPH_OSD_FLAG_READ, CEPH_FEATURES_ALL);
    m->ops.resize(2);
    m->ops[0].op.op = CEPH_OSD_OP_STAT;
    m->ops[1].op.op = CEPH_OSD_OP_READ;
    m->ops[1].op.extent.offset = 0;
    m->ops[1].op.extent.length = 4096;
    if (with_deliveries) {
      m->set_rdma_deliveries(std::vector(deliveries));
    }
    m->encode_payload(CEPH_FEATURES_ALL);
    return m;
  };
  auto plain = encode_op(false);
  auto with = encode_op(true);
  EXPECT_EQ(9, plain->get_header().version);
  EXPECT_EQ(9, with->get_header().version);

  // the plain request, then the descriptors and nothing else
  bufferlist tail;
  encode(deliveries, tail);
  const auto plain_len = plain->get_payload().length();
  ASSERT_EQ(plain_len + tail.length(), with->get_payload().length());
  bufferlist prefix, suffix;
  prefix.substr_of(with->get_payload(), 0, plain_len);
  suffix.substr_of(with->get_payload(), plain_len, tail.length());
  EXPECT_TRUE(prefix.contents_equal(plain->get_payload()));
  EXPECT_TRUE(suffix.contents_equal(tail));

  // an OSD of this release decodes them back, and finds none in a
  // plain request
  auto decode_op = [](const ClientMOSDOp& sent) {
    auto m = ceph::make_message<MOSDOp>();
    m->set_header(sent.get_header());
    bufferlist bl = sent.get_payload();
    m->set_payload(bl);
    m->decode_payload();
    m->finish_decode();
    return m;
  };
  EXPECT_FALSE(decode_op(*plain)->has_rdma_delivery());
  auto req = decode_op(*with);
  ASSERT_EQ(2u, req->ops.size());
  EXPECT_EQ(4096u, req->ops[1].op.extent.length);
  ASSERT_EQ(2u, req->get_rdma_deliveries().size());
  EXPECT_TRUE(req->get_rdma_deliveries()[0].empty());
  EXPECT_EQ("aa:bb:opaque", req->get_rdma_deliveries()[1].token);
  EXPECT_EQ(4096u, req->get_rdma_deliveries()[1].base_offset);
  EXPECT_EQ(ceph::rdma::delivery_t::FLAG_CRC64NVME,
            req->get_rdma_deliveries()[1].flags);

  // and its reply
  std::vector<ceph::rdma::oob_result_t> results(2);
  results[1].bytes = 4096;
  results[1].crc64 = 0x1234;
  results[1].flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME |
                     ceph::rdma::oob_result_t::FLAG_LANDED;
  auto encode_reply = [&](bool with_results) {
    auto r = ceph::make_message<MOSDOpReply>(
      req.get(), 0, 10, CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK, false);
    if (with_results) {
      r->set_oob_results(std::vector(results));
    }
    r->encode_payload(CEPH_FEATURES_ALL);
    return r;
  };
  auto plain_reply = encode_reply(false);
  auto with_reply = encode_reply(true);
  EXPECT_EQ(8, plain_reply->get_header().version);
  EXPECT_EQ(8, with_reply->get_header().version);

  bufferlist rtail;
  encode(results, rtail);
  const auto plain_reply_len = plain_reply->get_payload().length();
  ASSERT_EQ(plain_reply_len + rtail.length(),
            with_reply->get_payload().length());
  bufferlist rprefix, rsuffix;
  rprefix.substr_of(with_reply->get_payload(), 0, plain_reply_len);
  rsuffix.substr_of(with_reply->get_payload(), plain_reply_len,
                    rtail.length());
  EXPECT_TRUE(rprefix.contents_equal(plain_reply->get_payload()));
  EXPECT_TRUE(rsuffix.contents_equal(rtail));

  auto decode_reply = [](const MOSDOpReply& sent) {
    auto r = ceph::make_message<MOSDOpReply>();
    r->set_header(sent.get_header());
    bufferlist bl = sent.get_payload();
    r->set_payload(bl);
    r->decode_payload();
    return r;
  };
  EXPECT_TRUE(decode_reply(*plain_reply)->get_oob_results().empty());
  auto reply = decode_reply(*with_reply);
  ASSERT_EQ(2u, reply->get_oob_results().size());
  EXPECT_EQ(0u, reply->get_oob_results()[0].bytes);
  EXPECT_EQ(4096u, reply->get_oob_results()[1].bytes);
  EXPECT_EQ(0x1234u, reply->get_oob_results()[1].crc64);
  EXPECT_EQ(results[1].flags, reply->get_oob_results()[1].flags);
}

// An OSD rejects a request of a version newer than it can read, as v10
// and v11 here: its decode fails, and the messenger drops it. The OSD
// does not read the fields of such a request as v9 ones.
TEST(RdmaDelivery, NewerRequestVersionIsRejected)
{
  const hobject_t hobj(object_t("obj"), "", CEPH_NOSNAP, 0x1234, 1, "");
  spg_t pgid(pg_t(0x1234, 1));
  using ClientMOSDOp = _mosdop::MOSDOp<osdc_opvec>;
  auto sent = ceph::make_message<ClientMOSDOp>(
    1, 42, hobj, pgid, 10, CEPH_OSD_FLAG_READ, CEPH_FEATURES_ALL);
  sent->ops.resize(1);
  sent->ops[0].op.op = CEPH_OSD_OP_READ;
  sent->ops[0].op.extent.offset = 0;
  sent->ops[0].op.extent.length = 4096;
  sent->encode_payload(CEPH_FEATURES_ALL);
  ASSERT_EQ(9, sent->get_header().version);

  auto decode_as = [&](int version,
                       const std::vector<ceph::rdma::delivery_t>& tail) {
    bufferlist bl = sent->get_payload();
    encode(tail, bl);
    auto header = sent->get_header();
    header.version = version;
    auto m = ceph::make_message<MOSDOp>();
    m->set_header(header);
    m->set_payload(bl);
    m->decode_payload();
    return m;
  };
  const std::vector<ceph::rdma::delivery_t> one = {
    ceph::rdma::delivery_t{"aa:bb:opaque", 0, 0}};
  // the same bytes as v9 decode, with the descriptor
  auto m = decode_as(9, one);
  EXPECT_EQ(pgid, m->get_spg());
  m->finish_decode();
  ASSERT_EQ(1u, m->get_rdma_deliveries().size());
  EXPECT_EQ("aa:bb:opaque", m->get_rdma_deliveries()[0].token);
  for (int version : {10, 11}) {
    SCOPED_TRACE(version);
    EXPECT_THROW(decode_as(version, {}), ceph::buffer::error);
    EXPECT_THROW(decode_as(version, one), ceph::buffer::error);
  }
}

// The OSD queues a request, and charges its scheduler the request's
// cost, before the op thread decodes its ops and descriptors
// (finish_decode()). A look at the rest of a received write finds the
// payloads its writes pull, which then cost as if the message carried
// them, as PrimaryLogPG counts them for osd_max_write_size.
TEST(RdmaDelivery, PulledBytesCountInTheCost)
{
  using ceph::rdma::delivery_t;
  using ClientMOSDOp = _mosdop::MOSDOp<osdc_opvec>;
  // a namespace, so that the locator the look skips is not empty
  const hobject_t hobj(object_t("obj"), "", CEPH_NOSNAP, 0x1234, 1,
                       "a-namespace");
  spg_t pgid(pg_t(0x1234, 1));
  const delivery_t pull{"aa:bb:opaque", 4096,
                        delivery_t::FLAG_PULL | delivery_t::FLAG_CRC64NVME};
  const delivery_t push{"aa:bb:opaque", 0, delivery_t::FLAG_CRC64NVME};
  struct sent_t {
    int flags = CEPH_OSD_FLAG_WRITE;
    std::vector<delivery_t> deliveries;
    uint64_t first_len = 4 << 20;
    bool cut = false;  ///< the payload's last byte lost
  };
  // a create, two writes to pull and one carrying 4 bytes, sent and
  // received as the messenger hands it to the OSD
  auto received = [&](const sent_t& s) {
    auto m = ceph::make_message<ClientMOSDOp>(
      1, 42, hobj, pgid, 10, s.flags, CEPH_FEATURES_ALL);
    m->set_snaps({3, 2, 1});
    m->ops.resize(4);
    m->ops[0].op.op = CEPH_OSD_OP_CREATE;
    m->ops[1].op.op = CEPH_OSD_OP_WRITEFULL;
    m->ops[1].op.extent.length = s.first_len;
    m->ops[2].op.op = CEPH_OSD_OP_WRITE;
    m->ops[2].op.extent.offset = 1 << 20;
    m->ops[2].op.extent.length = 1 << 20;
    m->ops[3].op.op = CEPH_OSD_OP_WRITE;
    m->ops[3].op.extent.length = 4;
    m->ops[3].indata.append("data");
    if (!s.deliveries.empty()) {
      m->set_rdma_deliveries(std::vector(s.deliveries));
    }
    m->encode_payload(CEPH_FEATURES_ALL);
    bufferlist payload;
    payload.substr_of(m->get_payload(), 0,
                      m->get_payload().length() - (s.cut ? 1 : 0));
    auto r = ceph::make_message<MOSDOp>();
    r->set_header(m->get_header());
    r->set_payload(payload);
    bufferlist data = m->get_data();
    r->set_data(data);
    r->decode_payload();
    return r;
  };
  const std::vector<delivery_t> pulls = {{}, pull, pull, {}};
  const int both = (4 << 20) + (1 << 20) + 4;

  auto m = received({.deliveries = pulls});
  EXPECT_EQ(both, m->get_cost());
  // the pulled bytes alone, which PrimaryLogPG holds to
  // osd_max_write_size
  EXPECT_EQ(uint64_t((4 << 20) + (1 << 20)), m->get_pulled_len());
  // the same once the op thread decoded the rest
  ASSERT_TRUE(m->finish_decode());
  EXPECT_EQ(both, m->get_cost());
  EXPECT_EQ(uint64_t((4 << 20) + (1 << 20)), m->get_pulled_len());
  EXPECT_EQ(pulls.size(), m->get_rdma_deliveries().size());
  EXPECT_TRUE(m->has_pulls());

  // a pull too large to count saturates
  EXPECT_EQ(std::numeric_limits<int>::max(),
            received({.deliveries = pulls,
                      .first_len = 1ull << 40})->get_cost());
  // only the data: no descriptors, none that pulls, or descriptors that
  // do not mirror the ops, which PrimaryLogPG pulls nothing for
  EXPECT_EQ(4, received({})->get_cost());
  EXPECT_EQ(4, received({.deliveries = {{}, push, push, {}}})->get_cost());
  EXPECT_EQ(4, received({.deliveries = {pull, pull}})->get_cost());
  // nor for a request that is no write, which PrimaryLogPG refuses when
  // it pulls all the same
  auto unflagged = received({.flags = CEPH_OSD_FLAG_READ,
                             .deliveries = pulls});
  EXPECT_EQ(4, unflagged->get_cost());
  EXPECT_EQ(0u, unflagged->get_pulled_len());
  ASSERT_TRUE(unflagged->finish_decode());
  EXPECT_TRUE(unflagged->has_pulls());
  // a pull of no bytes costs nothing, and still pulls
  auto empty = received({.deliveries = {{}, pull, {}, {}},
                         .first_len = 0});
  EXPECT_EQ(0u, empty->get_pulled_len());
  ASSERT_TRUE(empty->finish_decode());
  EXPECT_TRUE(empty->has_pulls());
  auto pushes = received({.deliveries = {{}, push, push, {}}});
  ASSERT_TRUE(pushes->finish_decode());
  EXPECT_FALSE(pushes->has_pulls());
  // nor for a payload cut short, which finish_decode() fails
  EXPECT_EQ(4, received({.deliveries = pulls, .cut = true})->get_cost());
}

// finish_decode() takes an object locator of struct_v 1 or 2, which has
// no length to skip it by, as a client that encodes its request by hand
// may send. The look decodes such a locator, and counts the pulls of the
// request as it counts any other's: counted as 0, they would pass the
// osd_max_write_size check of PrimaryLogPG and reach the OSD's throttle
// with any length.
TEST(RdmaDelivery, PulledBytesCountWithALegacyLocator)
{
  using ceph::rdma::delivery_t;
  using ClientMOSDOp = _mosdop::MOSDOp<osdc_opvec>;
  const hobject_t hobj(object_t("obj"), "", CEPH_NOSNAP, 0x1234, 1,
                       "a-namespace");
  spg_t pgid(pg_t(0x1234, 1));
  const delivery_t pull{"aa:bb:opaque", 0, delivery_t::FLAG_PULL};
  auto m = ceph::make_message<ClientMOSDOp>(
    1, 42, hobj, pgid, 10, CEPH_OSD_FLAG_WRITE, CEPH_FEATURES_ALL);
  m->ops.resize(2);
  m->ops[0].op.op = CEPH_OSD_OP_WRITEFULL;
  m->ops[0].op.extent.length = 1ull << 63;
  m->ops[1].op.op = CEPH_OSD_OP_WRITE;
  m->ops[1].op.extent.length = 1 << 20;
  m->set_rdma_deliveries({pull, pull});
  m->encode_payload(CEPH_FEATURES_ALL);
  const uint64_t pulled = (1ull << 63) + (1 << 20);

  // the locator as the client encoded it, found once in the payload
  bufferlist oloc;
  encode(m->get_object_locator(), oloc);
  const std::string payload = m->get_payload().to_str();
  const auto at = payload.find(oloc.to_str());
  ASSERT_NE(std::string::npos, at);
  ASSERT_EQ(std::string::npos, payload.find(oloc.to_str(), at + 1));

  // the same request with the locator encoded as struct_v v
  auto received = [&](__u8 v) {
    bufferlist legacy;
    encode(v, legacy);
    if (v < 2) {
      encode(int32_t(1), legacy);   // pool
      encode(int16_t(-1), legacy);  // preferred
    } else {
      encode(int64_t(1), legacy);
      encode(int32_t(-1), legacy);
    }
    encode(std::string(), legacy);  // key
    bufferlist bl;
    bl.append(payload.substr(0, at));
    bl.append(legacy);
    bl.append(payload.substr(at + oloc.length()));
    auto r = ceph::make_message<MOSDOp>();
    r->set_header(m->get_header());
    r->set_payload(bl);
    r->decode_payload();
    return r;
  };
  for (__u8 v : {__u8(1), __u8(2)}) {
    SCOPED_TRACE(int(v));
    auto r = received(v);
    EXPECT_EQ(pulled, r->get_pulled_len());
    EXPECT_EQ(std::numeric_limits<int>::max(), r->get_cost());
    // the op thread reads the same request, and its pulls
    ASSERT_TRUE(r->finish_decode());
    ASSERT_EQ(2u, r->ops.size());
    EXPECT_EQ(1ull << 63, r->ops[0].op.extent.length);
    EXPECT_EQ(2u, r->get_rdma_deliveries().size());
    EXPECT_TRUE(r->has_pulls());
    EXPECT_EQ(pulled, r->get_pulled_len());
  }
}

// The client asks for the return data of a request that pulls
// (RETURNVEC), for the PG log to keep the record of each pull, only when
// no other op of it returns anything that the flag changes: with it, a
// write returns the data and the result of a CALL or a read, and fails
// when one returns more than osd_max_write_op_reply_len.
TEST(RdmaDelivery, PullRecordsOnlyWhenNothingElseReturns)
{
  using ceph::rdma::delivery_t;
  using ClientMOSDOp = _mosdop::MOSDOp<osdc_opvec>;
  const delivery_t pull{"aa:bb:opaque", 0, delivery_t::FLAG_PULL};
  const delivery_t push{"aa:bb:opaque", 0, delivery_t::FLAG_CRC64NVME};
  auto wants = [](std::initializer_list<int> codes,
                  const std::vector<delivery_t>& deliveries) {
    osdc_opvec ops(codes.size());
    size_t i = 0;
    for (const int c : codes) {
      ops[i++].op.op = c;
    }
    return ClientMOSDOp::wants_pull_records(ops, deliveries);
  };
  // the gateway's stripe write: an allocation hint, and the pull
  EXPECT_TRUE(wants({CEPH_OSD_OP_SETALLOCHINT, CEPH_OSD_OP_WRITEFULL},
                    {{}, pull}));
  EXPECT_TRUE(wants({CEPH_OSD_OP_WRITE}, {pull}));
  EXPECT_TRUE(wants({CEPH_OSD_OP_CREATE, CEPH_OSD_OP_WRITE,
                     CEPH_OSD_OP_SETXATTR, CEPH_OSD_OP_OMAPSETVALS},
                    {{}, pull, {}, {}}));
  // nothing pulled: no descriptors, or none that pulls
  EXPECT_FALSE(wants({CEPH_OSD_OP_WRITEFULL}, {}));
  EXPECT_FALSE(wants({CEPH_OSD_OP_WRITEFULL}, {{}}));
  EXPECT_FALSE(wants({CEPH_OSD_OP_READ}, {push}));
  // another op returns data, or a result of its own
  EXPECT_FALSE(wants({CEPH_OSD_OP_CALL, CEPH_OSD_OP_WRITEFULL},
                     {{}, pull}));
  EXPECT_FALSE(wants({CEPH_OSD_OP_WRITEFULL, CEPH_OSD_OP_STAT},
                     {pull, {}}));
  EXPECT_FALSE(wants({CEPH_OSD_OP_WRITE, CEPH_OSD_OP_READ}, {pull, push}));
}

TEST(RdmaDelivery, Crc64ValidityIsSeparateFromCombinability)
{
  // distinct bits: a scattered placement reports a checksum of the
  // bytes it moved without claiming it folds with its neighbours
  static_assert(ceph::rdma::oob_result_t::FLAG_CRC64NVME !=
		ceph::rdma::oob_result_t::FLAG_COMBINABLE);

  ceph::rdma::oob_result_t scattered;
  scattered.bytes = 1048576;
  scattered.crc64 = 0x0123456789abcdefull;
  scattered.flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME;
  EXPECT_TRUE(scattered.flags & ceph::rdma::oob_result_t::FLAG_CRC64NVME);
  EXPECT_FALSE(scattered.flags &
	       ceph::rdma::oob_result_t::FLAG_COMBINABLE);

  ceph::rdma::oob_result_t linear = scattered;
  linear.flags |= ceph::rdma::oob_result_t::FLAG_COMBINABLE;

  // the extra bit rides in the existing flags field, so neither
  // combination changes the encoded length
  bufferlist a, b;
  encode(scattered, a);
  encode(linear, b);
  EXPECT_EQ(a.length(), b.length());

  ceph::rdma::oob_result_t out;
  auto p = b.cbegin();
  decode(out, p);
  EXPECT_EQ(linear.crc64, out.crc64);
  EXPECT_TRUE(out.flags & ceph::rdma::oob_result_t::FLAG_CRC64NVME);
  EXPECT_TRUE(out.flags & ceph::rdma::oob_result_t::FLAG_COMBINABLE);
  EXPECT_TRUE(p.end());
}

TEST(RdmaDelivery, OobResultRangesRoundTrip)
{
  ceph::rdma::oob_result_t r;
  r.bytes = 3 * 65536;
  r.flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME |
	    ceph::rdma::oob_result_t::FLAG_RANGES;
  r.ranges = {{0, 65536, 1}, {131072, 65536, 2}, {262144, 65536, 3}};
  r.crc64 = 42;

  bufferlist bl;
  encode(r, bl);
  ceph::rdma::oob_result_t out;
  auto p = bl.cbegin();
  decode(out, p);
  EXPECT_TRUE(p.end());
  EXPECT_EQ(r.bytes, out.bytes);
  EXPECT_EQ(r.flags, out.flags);
  ASSERT_EQ(3u, out.ranges.size());
  EXPECT_EQ(r.ranges, out.ranges);

  // an empty result round-trips with no ranges
  ceph::rdma::oob_result_t empty, eout;
  bufferlist ebl;
  encode(empty, ebl);
  auto q = ebl.cbegin();
  decode(eout, q);
  EXPECT_TRUE(q.end());
  EXPECT_EQ(0u, eout.bytes);
  EXPECT_TRUE(eout.ranges.empty());
}

TEST(RdmaDelivery, FoldCrc64Ranges)
{
  // three chunks of a 6-chunk logical range, as two EC shards would
  // place them: shard 0 holds chunks 0,2,4 and shard 1 holds 1,3,5
  std::string data;
  for (int c = 0; c < 6; ++c) {
    data += std::string(1000, static_cast<char>('a' + c));
  }
  bufferlist whole;
  whole.append(data);
  const uint64_t expect = ceph::crc64nvme(whole);

  std::vector<ceph::rdma::crc_range_t> ranges;
  for (int c : {4, 0, 2, 5, 1, 3}) {  // deliberately out of order
    bufferlist part;
    part.append(data.data() + c * 1000, 1000);
    ranges.push_back({uint64_t(c) * 1000, 1000, ceph::crc64nvme(part)});
  }
  auto folded = ceph::rdma::fold_crc64_ranges(ranges);
  ASSERT_TRUE(folded.has_value());
  EXPECT_EQ(expect, *folded);

  // a single range folds to itself
  auto one = ceph::rdma::fold_crc64_ranges({ranges[1]});
  ASSERT_TRUE(one.has_value());
  EXPECT_EQ(ranges[1].crc64, *one);

  // a gap or an overlap is not a contiguous extent
  auto gapped = ranges;
  gapped.erase(gapped.begin() + 2);
  EXPECT_FALSE(ceph::rdma::fold_crc64_ranges(gapped).has_value());
  auto overlapped = ranges;
  overlapped[0].ofs -= 1;
  EXPECT_FALSE(ceph::rdma::fold_crc64_ranges(overlapped).has_value());
  EXPECT_FALSE(ceph::rdma::fold_crc64_ranges({}).has_value());
}

// a result handler that copies the op's result into *got
static ::ObjectOperation::rdma_oob_handler_t copy_to(
  ceph::rdma::oob_result_t* got)
{
  return [got](const ceph::rdma::oob_result_t& r) { *got = r; };
}

// An op's out-of-band result reaches the handler its caller registered,
// once. Both the reply path and the split completion go through
// rdma_oob_wanted() and deliver_rdma_oob_result(); this pins what those
// promise, on an Op built the way prepare_read_op() builds it.
TEST(RdmaDelivery, ResultReachesItsHandler)
{
  ::ObjectOperation op;
  ceph::buffer::list bl0, bl1, bl2;
  int rv0 = 0, rv1 = 0, rv2 = 0;

  ceph::rdma::oob_result_t got0;
  op.read(0, 4096, &bl0, &rv0, nullptr);
  op.set_rdma_delivery("t0", 0, 0, copy_to(&got0));

  std::optional<ceph::rdma::oob_result_t> by_callback;
  int calls = 0;
  op.read(4096, 4096, &bl1, &rv1, nullptr);
  op.set_rdma_delivery("t1", 4096,
                       ceph::rdma::delivery_t::FLAG_CRC64NVME,
                       [&](const ceph::rdma::oob_result_t& r) {
                         by_callback = r;
                         ++calls;
                       });

  op.read(8192, 4096, &bl2, &rv2, nullptr);  // no descriptor

  auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                             std::move(op.ops), 0, (Context*)nullptr,
                             nullptr);
  Objecter::take_rdma(o, op);

  EXPECT_TRUE(Objecter::rdma_oob_wanted(o, 0));
  EXPECT_TRUE(Objecter::rdma_oob_wanted(o, 1));
  EXPECT_FALSE(Objecter::rdma_oob_wanted(o, 2));
  EXPECT_FALSE(Objecter::rdma_oob_wanted(o, 7));

  ceph::rdma::oob_result_t r0;
  r0.bytes = 4096;
  Objecter::deliver_rdma_oob_result(o, 0, r0);
  EXPECT_EQ(4096u, got0.bytes);

  ceph::rdma::oob_result_t r1;
  r1.bytes = 4096;
  r1.crc64 = 0x1234;
  r1.flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME |
             ceph::rdma::oob_result_t::FLAG_RANGES;
  r1.ranges = {{4096, 2048, 0x11}, {6144, 2048, 0x22}};
  Objecter::deliver_rdma_oob_result(o, 1, r1);
  ASSERT_TRUE(by_callback.has_value());
  EXPECT_EQ(1, calls);
  EXPECT_EQ(4096u, by_callback->bytes);
  EXPECT_EQ(0x1234u, by_callback->crc64);
  EXPECT_EQ(r1.flags, by_callback->flags);
  ASSERT_EQ(2u, by_callback->ranges.size());
  EXPECT_EQ(6144u, by_callback->ranges[1].ofs);

  // the handler is one-shot: a later delivery for the same op (a
  // resend's reply) does not call it again
  Objecter::deliver_rdma_oob_result(o, 1, r1);
  EXPECT_EQ(1, calls);
  EXPECT_FALSE(Objecter::rdma_oob_wanted(o, 1));

  // an op with no descriptor and no result asked for is left alone
  Objecter::deliver_rdma_oob_result(o, 2, r1);

  o->put();
}

// A descriptor may go out without a handler for its result: the op is
// still delivered out of band, no handler storage is allocated for it,
// and its result in a reply goes nowhere, while a later op's handler
// still gets its own.
TEST(RdmaDelivery, DescriptorWithoutHandlerAsksNoResult)
{
  using R = ceph::rdma::oob_result_t;
  ::ObjectOperation op;
  ceph::buffer::list bl0, bl1;
  int rv0 = 0, rv1 = 0;
  op.read(0, 4096, &bl0, &rv0, nullptr);
  op.set_rdma_delivery("t0", 0, 0, nullptr);
  EXPECT_TRUE(op.has_rdma_delivery());
  EXPECT_FALSE(op.rdma_oob_handler);

  R got;
  op.read(4096, 4096, &bl1, &rv1, nullptr);
  op.set_rdma_delivery("t1", 4096, 0, copy_to(&got));
  auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                             std::move(op.ops), 0, (Context*)nullptr,
                             nullptr);
  Objecter::take_rdma(o, op);
  EXPECT_FALSE(o->rdma_delivery[0].empty());
  EXPECT_FALSE(Objecter::rdma_oob_wanted(o, 0));
  EXPECT_TRUE(Objecter::rdma_oob_wanted(o, 1));

  R r0, r1;
  r0.bytes = 4096;
  r1.bytes = 2048;
  Objecter::deliver_rdma_oob_results(o, {r0, r1});
  EXPECT_EQ(2048u, got.bytes);
  EXPECT_FALSE(Objecter::rdma_oob_wanted(o, 1));
  o->put();
}

// A write whose payload the primary pulls out of client memory carries no
// data, only its length and a pull descriptor, and the builders of write
// ops hand its descriptor and result handler to the Op as read builders
// do. They once did not: the OSD then saw a write without data, and
// failed it.
TEST(RdmaDelivery, PulledWriteCarriesItsDescriptor)
{
  ::ObjectOperation op;
  op.create(false);
  op.write_full_pulled(4 << 20);
  std::optional<ceph::rdma::oob_result_t> got;
  op.set_rdma_delivery("t", 8 << 20,
                       ceph::rdma::delivery_t::FLAG_PULL |
                       ceph::rdma::delivery_t::FLAG_CRC64NVME,
                       [&](const ceph::rdma::oob_result_t& r) { got = r; });
  ASSERT_EQ(2u, op.ops.size());
  EXPECT_EQ(CEPH_OSD_OP_WRITEFULL, op.ops[1].op.op);
  EXPECT_EQ(uint64_t(4 << 20), op.ops[1].op.extent.length);
  EXPECT_EQ(0u, op.ops[1].indata.length());

  auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                             std::move(op.ops), CEPH_OSD_FLAG_WRITE,
                             (Context*)nullptr, nullptr);
  Objecter::take_rdma(o, op);
  ASSERT_TRUE(o->has_rdma_delivery());
  EXPECT_TRUE(o->rdma_delivery[0].empty());
  EXPECT_TRUE(o->rdma_delivery[1].is_pull());
  EXPECT_EQ(uint64_t(8 << 20), o->rdma_delivery[1].base_offset);
  EXPECT_FALSE(op.has_rdma_delivery());

  // the pulled bytes and their checksum reach the caller
  EXPECT_TRUE(Objecter::rdma_oob_wanted(o, 1));
  ceph::rdma::oob_result_t r;
  r.bytes = 4 << 20;
  r.crc64 = 0xabcdef;
  r.flags = ceph::rdma::oob_result_t::FLAG_CRC64NVME |
            ceph::rdma::oob_result_t::FLAG_COMBINABLE;
  Objecter::deliver_rdma_oob_result(o, 1, r);
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(uint64_t(4 << 20), got->bytes);
  EXPECT_EQ(0xabcdefu, got->crc64);
  o->put();
}

// A cluster whose OSDs get no descriptors (MOSDOp::RDMA_DELIVERY_RELEASE)
// cannot pull a write's payload, so the Objecter fails a pulled write
// before it sends it. A read that asks for delivery goes out, with its
// descriptors withheld, and its data comes back inline.
TEST(RdmaDelivery, PulledWriteRefusedBelowTheDeliveryRelease)
{
  const auto old_release = ceph_release_t::tentacle;
  ASSERT_LT(old_release, MOSDOp::RDMA_DELIVERY_RELEASE);
  EXPECT_FALSE(Objecter::rdma_delivery_sent(old_release));
  EXPECT_TRUE(Objecter::rdma_delivery_sent(MOSDOp::RDMA_DELIVERY_RELEASE));

  // a pulled WRITEFULL, of any length: sent without its descriptor, one
  // of no bytes would empty the object
  for (uint64_t len : {uint64_t(0), uint64_t(4 << 20)}) {
    ::ObjectOperation op;
    op.write_full_pulled(len);
    op.set_rdma_delivery("t", 0, ceph::rdma::delivery_t::FLAG_PULL, {});
    auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                               std::move(op.ops), CEPH_OSD_FLAG_WRITE,
                               (Context*)nullptr, nullptr);
    Objecter::take_rdma(o, op);
    EXPECT_TRUE(Objecter::rdma_pull_refused(o, old_release)) << len;
    EXPECT_FALSE(Objecter::rdma_pull_refused(
      o, MOSDOp::RDMA_DELIVERY_RELEASE)) << len;
    o->put();
  }

  // a read that asks for delivery, and a write that does not pull
  ::ObjectOperation rd;
  rd.read(0, 4096, nullptr, nullptr, nullptr);
  rd.set_rdma_delivery("t", 0, 0, {});
  auto* r = new Objecter::Op(object_t("obj"), object_locator_t(1),
                             std::move(rd.ops), CEPH_OSD_FLAG_READ,
                             (Context*)nullptr, nullptr);
  Objecter::take_rdma(r, rd);
  ASSERT_TRUE(r->has_rdma_delivery());
  EXPECT_FALSE(Objecter::rdma_pull_refused(r, old_release));
  r->put();

  ::ObjectOperation wr;
  bufferlist bl;
  bl.append("x");
  wr.write_full(bl);
  auto* w = new Objecter::Op(object_t("obj"), object_locator_t(1),
                             std::move(wr.ops), CEPH_OSD_FLAG_WRITE,
                             (Context*)nullptr, nullptr);
  Objecter::take_rdma(w, wr);
  EXPECT_FALSE(Objecter::rdma_pull_refused(w, old_release));
  w->put();
}

// A descriptor with an empty token requests nothing, whatever its flags:
// every check for a pull, on the client and on the OSD, reads one that
// sets FLAG_PULL as no pull, so that none sends or runs the write as a
// pull while another runs it without its payload. The client library
// fails such a write instead, before it sends it (-EINVAL), and its
// result reads declined.
TEST(RdmaDelivery, PullWithoutATokenIsMalformed)
{
  using D = ceph::rdma::delivery_t;
  EXPECT_FALSE((D{"", 0, D::FLAG_PULL}.is_pull()));
  EXPECT_TRUE((D{"t", 0, D::FLAG_PULL}.is_pull()));
  EXPECT_FALSE((D{"t", 0, D::FLAG_CRC64NVME}.is_pull()));
  const std::vector<D> none = {{}, {"", 0, D::FLAG_PULL}};
  EXPECT_FALSE(ceph::rdma::any_pull(none));
  EXPECT_TRUE(ceph::rdma::any_pull(
    std::vector<D>{{}, {"t", 0, D::FLAG_PULL}}));

  // what an OSD reads of a request whose pull has no token: no pull, as
  // the look at its payload counts none
  const hobject_t hobj(object_t("obj"), "", CEPH_NOSNAP, 0x1234, 1, "");
  spg_t pgid(pg_t(0x1234, 1));
  using ClientMOSDOp = _mosdop::MOSDOp<osdc_opvec>;
  auto sent = ceph::make_message<ClientMOSDOp>(
    1, 42, hobj, pgid, 10, CEPH_OSD_FLAG_WRITE, CEPH_FEATURES_ALL);
  sent->ops.resize(2);
  sent->ops[0].op.op = CEPH_OSD_OP_CREATE;
  sent->ops[1].op.op = CEPH_OSD_OP_WRITEFULL;
  sent->ops[1].op.extent.length = 4096;
  sent->set_rdma_deliveries(std::vector(none));
  sent->encode_payload(CEPH_FEATURES_ALL);
  auto m = ceph::make_message<MOSDOp>();
  m->set_header(sent->get_header());
  bufferlist bl = sent->get_payload();
  m->set_payload(bl);
  m->decode_payload();
  EXPECT_EQ(0u, m->get_pulled_len());
  ASSERT_TRUE(m->finish_decode());
  EXPECT_FALSE(m->has_pulls());
  EXPECT_FALSE(MOSDOp::wants_pull_records(m->ops, m->get_rdma_deliveries()));

  // the client library's write of no window, of any length
  const auto old_release = ceph_release_t::tentacle;
  for (uint64_t len : {uint64_t(0), uint64_t(4096)}) {
    SCOPED_TRACE(len);
    ::ObjectOperation op;
    op.write_full_pulled(len);
    std::optional<ceph::rdma::oob_result_t> got;
    op.set_rdma_delivery("", 0, D::FLAG_PULL,
                         [&](const ceph::rdma::oob_result_t& r) { got = r; });
    auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                               std::move(op.ops), CEPH_OSD_FLAG_WRITE,
                               (Context*)nullptr, nullptr);
    Objecter::take_rdma(o, op);
    EXPECT_FALSE(o->has_rdma_delivery());
    EXPECT_TRUE(Objecter::rdma_pull_malformed(o));
    EXPECT_FALSE(Objecter::rdma_pull_refused(o, old_release));
    // it fails before it is sent: its result reads declined, not resent
    Objecter::deliver_rdma_unsent(o);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(ceph::rdma::oob_result_t::FLAG_DECLINED, got->flags);
    EXPECT_EQ(0u, got->bytes);
    o->put();
  }

  // a pull that names its window, and a read without one, are well formed
  ::ObjectOperation ok;
  ok.write_full_pulled(4096);
  ok.set_rdma_delivery("t", 0, D::FLAG_PULL, {});
  ok.read(0, 4096, nullptr, nullptr, nullptr);
  ok.set_rdma_delivery("", 0, 0, {});
  auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                             std::move(ok.ops), CEPH_OSD_FLAG_WRITE,
                             (Context*)nullptr, nullptr);
  Objecter::take_rdma(o, ok);
  EXPECT_FALSE(Objecter::rdma_pull_malformed(o));
  o->put();
}

// A result for an op sent more than once is marked resent: an earlier
// attempt may have started a transfer the reply knows nothing of, and a
// caller must then fence its window. One sent once is not.
TEST(RdmaDelivery, ResentResultsAreMarked)
{
  ::ObjectOperation op;
  ceph::buffer::list bl;
  int rv = 0;
  ceph::rdma::oob_result_t got;
  op.read(0, 4096, &bl, &rv, nullptr);
  op.set_rdma_delivery("t0", 0, 0, copy_to(&got));
  auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                             std::move(op.ops), 0, (Context*)nullptr,
                             nullptr);
  Objecter::take_rdma(o, op);
  // a handler runs once, for the reply that completes the op: re-arm it
  // to replay another attempt's reply
  auto rearm = [&] {
    (*o->rdma_oob_handler)[0] = copy_to(&got);
  };

  ceph::rdma::oob_result_t declined;
  declined.flags = ceph::rdma::oob_result_t::FLAG_DECLINED;
  o->attempts = 1;
  Objecter::deliver_rdma_oob_result(o, 0, declined);
  EXPECT_EQ(ceph::rdma::oob_result_t::FLAG_DECLINED, got.flags);

  rearm();
  o->attempts = 2;
  Objecter::deliver_rdma_oob_result(o, 0, declined);
  EXPECT_EQ(ceph::rdma::oob_result_t::FLAG_DECLINED |
            ceph::rdma::oob_result_t::FLAG_RESENT, got.flags);

  // a resend whose earlier attempts were all answered declined or
  // landed went out vouched for, and its result is not resent
  rearm();
  o->rdma_retry_settled = true;
  Objecter::deliver_rdma_oob_result(o, 0, declined);
  EXPECT_EQ(ceph::rdma::oob_result_t::FLAG_DECLINED, got.flags);
  o->put();
}

// An attempt that went out without its descriptors - withheld while the
// cluster's require_osd_release is below umbrella - moved nothing out of
// band, and its results say declined, so a caller need not fence its
// window. A resend withheld again is not resent; one withheld after an
// attempt that went out unanswered is.
TEST(RdmaDelivery, WithheldDescriptorsReadDeclined)
{
  using R = ceph::rdma::oob_result_t;
  ::ObjectOperation op;
  ceph::buffer::list bl0, bl1;
  int rv0 = 0, rv1 = 0;
  R got;
  op.read(0, 4096, &bl0, &rv0, nullptr);
  op.set_rdma_delivery("t0", 0, 0, copy_to(&got));
  op.read(4096, 4096, &bl1, &rv1, nullptr);  // no descriptor
  auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                             std::move(op.ops), 0, (Context*)nullptr,
                             nullptr);
  Objecter::take_rdma(o, op);
  // a handler runs once, for the reply that completes the op: re-arm it
  // to replay another attempt's reply
  auto rearm = [&] {
    (*o->rdma_oob_handler)[0] = copy_to(&got);
  };

  // attempts as _prepare_osd_op() counts them before stamping
  o->attempts = 1;
  EXPECT_TRUE(Objecter::stamp_rdma_attempt(o, false).empty());
  EXPECT_TRUE(o->rdma_withheld);
  // the OSD saw no descriptors, so its reply carries no results
  Objecter::deliver_rdma_oob_results(o, {});
  EXPECT_EQ(R::FLAG_DECLINED, got.flags);
  EXPECT_EQ(0u, got.bytes);

  rearm();
  o->attempts = 2;
  EXPECT_TRUE(Objecter::stamp_rdma_attempt(o, false).empty());
  Objecter::deliver_rdma_oob_results(o, {});
  EXPECT_EQ(R::FLAG_DECLINED, got.flags);

  // once every OSD may honor them, the descriptors go out, vouched for
  // as no earlier attempt carried any, and a reply without results
  // reads as before
  o->attempts = 3;
  auto d = Objecter::stamp_rdma_attempt(o, true);
  EXPECT_FALSE(o->rdma_withheld);
  ASSERT_EQ(2u, d.size());
  EXPECT_EQ("t0", d[0].token);
  EXPECT_TRUE(d[0].flags & ceph::rdma::delivery_t::FLAG_PRIOR_SETTLED);
  EXPECT_TRUE(d[1].empty());
  rearm();
  Objecter::deliver_rdma_oob_results(o, {});
  EXPECT_EQ(0u, got.flags);

  // that attempt had no settled reply: a later withheld one is declined
  // for itself, but resent
  rearm();
  o->attempts = 4;
  EXPECT_TRUE(Objecter::stamp_rdma_attempt(o, false).empty());
  Objecter::deliver_rdma_oob_results(o, {});
  EXPECT_EQ(R::FLAG_DECLINED | R::FLAG_RESENT, got.flags);

  // an op without a descriptor carries nothing to withhold
  ::ObjectOperation plain;
  plain.read(0, 4096, &bl0, &rv0, nullptr);
  auto* p = new Objecter::Op(object_t("obj"), object_locator_t(1),
                             std::move(plain.ops), 0, (Context*)nullptr,
                             nullptr);
  Objecter::take_rdma(p, plain);
  p->attempts = 1;
  EXPECT_TRUE(Objecter::stamp_rdma_attempt(p, false).empty());
  EXPECT_FALSE(p->rdma_withheld);
  p->put();
  o->put();
}

// FLAG_PRIOR_SETTLED is the client library's to set, from the attempts it
// sent, and the OSD keeps a resend inline without it. A request that carries
// it already (librados's rdma_delivery_result::COMBINABLE result flag has its
// value) goes out without it, on a first attempt and on a resend of an
// unanswered one, and so cannot switch off that guard. A resend of a settled
// attempt carries it, the caller's own bits kept.
TEST(RdmaDelivery, OnlyTheClientLibraryVouchesForEarlierAttempts)
{
  using D = ceph::rdma::delivery_t;
  auto make_op = [](uint32_t flags) {
    ::ObjectOperation op;
    op.write_full_pulled(4096);
    op.set_rdma_delivery("t0", 0, flags | D::FLAG_PULL, nullptr);
    auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                               std::move(op.ops), CEPH_OSD_FLAG_WRITE,
                               (Context*)nullptr, nullptr);
    Objecter::take_rdma(o, op);
    return o;
  };

  auto* o = make_op(D::FLAG_CRC64NVME | D::FLAG_PRIOR_SETTLED);
  o->attempts = 1;
  auto d = Objecter::stamp_rdma_attempt(o, true);
  ASSERT_EQ(1u, d.size());
  EXPECT_EQ(D::FLAG_CRC64NVME | D::FLAG_PULL, d[0].flags);

  // no reply came: a write of that attempt may still land, and its
  // resend goes out unvouched
  o->attempts = 2;
  d = Objecter::stamp_rdma_attempt(o, true);
  ASSERT_EQ(1u, d.size());
  EXPECT_EQ(D::FLAG_CRC64NVME | D::FLAG_PULL, d[0].flags);
  EXPECT_FALSE(ceph::rdma::prior_attempts_settled(d));
  o->put();

  auto* s = make_op(D::FLAG_CRC32C);
  s->attempts = 1;
  d = Objecter::stamp_rdma_attempt(s, true);
  ASSERT_EQ(1u, d.size());
  EXPECT_EQ(D::FLAG_CRC32C | D::FLAG_PULL, d[0].flags);
  // its reply declined the op, as handle_osd_op_reply() records
  s->rdma_attempt_unsettled = false;
  s->attempts = 2;
  d = Objecter::stamp_rdma_attempt(s, true);
  ASSERT_EQ(1u, d.size());
  EXPECT_EQ(D::FLAG_CRC32C | D::FLAG_PULL | D::FLAG_PRIOR_SETTLED,
            d[0].flags);
  EXPECT_TRUE(ceph::rdma::prior_attempts_settled(d));
  s->put();
}

// ObjectOperation::dup() takes over another request's ops, as an OSD
// does when it proxies a read or copies a manifest object's chunks, and
// keeps every per-op vector aligned with them, so that an op added
// afterwards passes add_op()'s checks. The copied ops go inline.
TEST(RdmaDelivery, DupKeepsPerOpVectorsAligned)
{
  std::vector<OSDOp> sops(2);
  sops[0].op.op = CEPH_OSD_OP_READ;
  sops[0].op.extent.length = 4096;
  sops[1].op.op = CEPH_OSD_OP_STAT;

  ::ObjectOperation op;
  ceph::buffer::list bl;
  int rv = 0;
  ceph::rdma::oob_result_t got;
  op.read(0, 8192, &bl, &rv, nullptr);
  op.set_rdma_delivery("t0", 0, 0, copy_to(&got));
  op.dup(sops);
  ASSERT_EQ(2u, op.ops.size());
  EXPECT_EQ(2u, op.rdma_delivery.size());
  EXPECT_FALSE(op.has_rdma_delivery());
  EXPECT_FALSE(op.rdma_oob_handler);

  op.assert_version(1);
  ASSERT_EQ(3u, op.ops.size());
  EXPECT_EQ(3u, op.rdma_delivery.size());
}

// An attempt is settled - nothing of it can land after its reply - when
// every descriptor-bearing op of it was declined or landed and none was
// resent. A replica's -EAGAIN bounce reports declined for each op.
TEST(RdmaDelivery, AttemptSettled)
{
  using R = ceph::rdma::oob_result_t;
  using ceph::rdma::attempt_settled;
  // no results at all (an old OSD, no reply): not settled
  EXPECT_FALSE(attempt_settled({}));
  EXPECT_TRUE(attempt_settled({R::FLAG_DECLINED}));
  EXPECT_TRUE(attempt_settled({R::FLAG_LANDED | R::FLAG_CRC64NVME}));
  // a split whose one shard bounced while the other landed its share
  EXPECT_TRUE(attempt_settled({R::FLAG_DECLINED, R::FLAG_LANDED}));
  // a push cut off after its writes went out, or a result that says
  // nothing: not settled
  EXPECT_FALSE(attempt_settled({R::FLAG_DECLINED, 0}));
  EXPECT_FALSE(attempt_settled({R::FLAG_CRC64NVME}));
  // a part that was itself resent may hide an unanswered attempt
  EXPECT_FALSE(attempt_settled({R::FLAG_DECLINED,
                                R::FLAG_LANDED | R::FLAG_RESENT}));
}

// An OSD delivers a resend out of band only when every descriptor it
// carries is vouched for; the bit is one the OSD knows, so a first
// attempt carrying it is not refused for an unknown flag.
TEST(RdmaDelivery, PriorAttemptsSettled)
{
  using D = ceph::rdma::delivery_t;
  using ceph::rdma::prior_attempts_settled;
  EXPECT_NE(0u, D::KNOWN_FLAGS & D::FLAG_PRIOR_SETTLED);
  D vouched{"t0", 0, D::FLAG_PRIOR_SETTLED | D::FLAG_CRC64NVME};
  D plain{"t1", 4096, D::FLAG_CRC64NVME};
  D none;  // an op of the request without a descriptor
  EXPECT_TRUE(prior_attempts_settled({vouched}));
  EXPECT_TRUE(prior_attempts_settled({none, vouched, none}));
  EXPECT_FALSE(prior_attempts_settled({vouched, plain}));
  EXPECT_FALSE(prior_attempts_settled({plain}));
  // nothing to deliver: nothing vouched for
  EXPECT_FALSE(prior_attempts_settled({none}));
  EXPECT_FALSE(prior_attempts_settled({}));
}

// The OSD declines what no transfer started for (osd/oob_xfer.h): an op
// that carries a descriptor is declined, and keeps its other flags. One
// without, or past either vector, is left as it is
TEST(RdmaDelivery, DeclineAll)
{
  using D = ceph::rdma::delivery_t;
  using R = ceph::rdma::oob_result_t;
  using ceph::osd::oob::decline_all;
  const std::vector<D> deliveries{D{"t0", 512, 0}, D{}, D{"t2", 512, 0}};
  std::vector<R> oob(3);
  oob[2].flags = R::FLAG_LANDED;
  decline_all(deliveries, oob);
  EXPECT_EQ(R::FLAG_DECLINED, oob[0].flags);
  EXPECT_EQ(0u, oob[1].flags);
  EXPECT_EQ(R::FLAG_DECLINED | R::FLAG_LANDED, oob[2].flags);
  std::vector<R> fewer(1);
  decline_all(deliveries, fewer);
  EXPECT_EQ(R::FLAG_DECLINED, fewer[0].flags);
  std::vector<R> more(5);
  decline_all(deliveries, more);
  EXPECT_EQ(R::FLAG_DECLINED, more[2].flags);
  EXPECT_EQ(0u, more[3].flags);
  EXPECT_EQ(0u, more[4].flags);
}

// The results of a reply that runs none of the request's reads
// (OSDService::reply_op_error()): an error declines each op that carries
// a descriptor. A dup does too, whatever its result, but for a pulled
// op, which reports nothing, since the attempt that did the write may
// have pulled. A result of 0 that is not a dup sets nothing
TEST(RdmaDelivery, ReplyErrorResults)
{
  using D = ceph::rdma::delivery_t;
  using R = ceph::rdma::oob_result_t;
  using ceph::osd::oob::reply_error_results;
  const D read{"t0", 0, D::FLAG_CRC64NVME};
  const D pull{"t1", 4096, D::FLAG_PULL | D::FLAG_CRC64NVME};
  const std::vector<D> deliveries{read, D{}, pull};
  {
    SCOPED_TRACE("an error");
    std::vector<R> oob(3);
    reply_error_results(deliveries, -ENOENT, false, oob);
    EXPECT_EQ(R::FLAG_DECLINED, oob[0].flags);
    EXPECT_EQ(0u, oob[1].flags);
    EXPECT_EQ(R::FLAG_DECLINED, oob[2].flags);
  }
  for (const int r : {0, -EIO}) {
    SCOPED_TRACE(r);
    std::vector<R> oob(3);
    // what the caller had for the pulled op is dropped
    oob[2].bytes = 4096;
    oob[2].flags = R::FLAG_CRC64NVME;
    reply_error_results(deliveries, r, true, oob);
    EXPECT_EQ(R::FLAG_DECLINED, oob[0].flags);
    EXPECT_EQ(0u, oob[1].flags);
    EXPECT_EQ(0u, oob[2].flags);
    EXPECT_EQ(0u, oob[2].bytes);
  }
  {
    SCOPED_TRACE("a result of 0, not a dup");
    std::vector<R> oob(3);
    reply_error_results(deliveries, 0, false, oob);
    for (const auto& o : oob) {
      EXPECT_EQ(0u, o.flags);
    }
  }
}

// A split read fans each plain READ's descriptor out to its sub-reads,
// shifted to each one's own extent, and has each sub-read's result left
// in the split's slot, which SplitOp::complete() folds into the parent's
// result. SplitOp::create() does this through fan_out_rdma(), on sub-ops
// built as these are. A result that went nowhere, or into a copy of its
// slot, would fold as nothing delivered. That create() calls it before
// the sub-op goes out takes a cluster to show.
TEST(RdmaDelivery, SplitFansOutDescriptorsAndResults)
{
  using R = ceph::rdma::oob_result_t;
  // the parent: a READ with a descriptor, a READ without one and a
  // SPARSE_READ with one, the way librados builds them
  ::ObjectOperation op;
  ceph::buffer::list bl0, bl1, bl2;
  int rv0 = 0, rv1 = 0, rv2 = 0;
  std::map<uint64_t, uint64_t> m2;
  std::optional<R> got0, got2;
  op.read(8192, 16384, &bl0, &rv0, nullptr);
  op.set_rdma_delivery("t0", 1 << 20, ceph::rdma::delivery_t::FLAG_CRC64NVME,
                       [&](const R& r) { got0 = r; });
  op.read(24576, 4096, &bl1, &rv1, nullptr);
  op.sparse_read(28672, 4096, &m2, &bl2, &rv2);
  op.set_rdma_delivery("t2", 2 << 20, 0, [&](const R& r) { got2 = r; });
  auto* parent = new Objecter::Op(object_t("obj"), object_locator_t(1),
                                  std::move(op.ops), 0, (Context*)nullptr,
                                  nullptr);
  Objecter::take_rdma(parent, op);

  // a replica sub-read: the second half of op 0, ops 1 and 2 whole, and
  // the split's own version read
  ::ObjectOperation rd;
  ceph::buffer::list sbl0, sbl1, sbl2, vbl;
  boost::system::error_code ec0, ec1, vec;
  std::map<uint64_t, uint64_t> sm2;
  int srv2 = 0;
  rd.read(16384, 8192, &ec0, &sbl0);
  rd.read(24576, 4096, &ec1, &sbl1);
  rd.sparse_read(28672, 4096, &sm2, &sbl2, &srv2);
  rd.get_internal_versions(&vec, &vbl);
  auto* sub = new Objecter::Op(object_t("obj"), object_locator_t(1),
                               std::move(rd.ops), 0, (Context*)nullptr,
                               nullptr);
  Objecter::take_rdma(sub, rd);
  ASSERT_FALSE(sub->has_rdma_delivery());

  // the slots start out empty, whatever they held
  std::vector<R> results(1);
  results[0].bytes = 1;
  Objecter::fan_out_rdma(parent, sub, {0, 1, 2, -1}, results);
  ASSERT_EQ(4u, results.size());
  EXPECT_EQ(0u, results[0].bytes);
  ASSERT_EQ(4u, sub->rdma_delivery.size());
  EXPECT_EQ("t0", sub->rdma_delivery[0].token);
  EXPECT_EQ(uint64_t((1 << 20) + 8192), sub->rdma_delivery[0].base_offset);
  EXPECT_EQ(ceph::rdma::delivery_t::FLAG_CRC64NVME,
            sub->rdma_delivery[0].flags);
  // none for an op whose parent has none, a sparse read or the version
  // read, and none of those asks for a result
  EXPECT_TRUE(sub->rdma_delivery[1].empty());
  EXPECT_TRUE(sub->rdma_delivery[2].empty());
  EXPECT_TRUE(sub->rdma_delivery[3].empty());
  EXPECT_TRUE(Objecter::rdma_oob_wanted(sub, 0));
  EXPECT_FALSE(Objecter::rdma_oob_wanted(sub, 1));
  EXPECT_FALSE(Objecter::rdma_oob_wanted(sub, 2));
  EXPECT_FALSE(Objecter::rdma_oob_wanted(sub, 3));

  // the sub-read's reply lands in the split's slot, not in the parent's
  // handler, which waits for the fold
  R r0;
  r0.bytes = 8192;
  r0.crc64 = 0x55;
  r0.flags = R::FLAG_CRC64NVME | R::FLAG_RANGES;
  r0.ranges = {{(1 << 20) + 8192, 8192, 0x55}};
  Objecter::deliver_rdma_oob_results(sub, {r0, R{}, R{}, R{}});
  EXPECT_EQ(8192u, results[0].bytes);
  EXPECT_EQ(0x55u, results[0].crc64);
  EXPECT_EQ(r0.flags, results[0].flags);
  ASSERT_EQ(1u, results[0].ranges.size());
  EXPECT_EQ(uint64_t((1 << 20) + 8192), results[0].ranges[0].ofs);
  EXPECT_EQ(0u, results[1].bytes);
  EXPECT_FALSE(got0.has_value());
  EXPECT_FALSE(got2.has_value());
  EXPECT_TRUE(Objecter::rdma_oob_wanted(parent, 0));
  EXPECT_TRUE(Objecter::rdma_oob_wanted(parent, 2));

  // what SplitOp::complete() reports for the parent's ops from these
  // slots: op 0 as its sub-read placed it, and the sparse read, whose
  // descriptor went to no sub-read, declined
  R folded0 = SplitOp::fold_rdma_result(true, 8192, {&results[0]});
  EXPECT_EQ(8192u, folded0.bytes);
  EXPECT_EQ(0x55u, folded0.crc64);
  EXPECT_EQ(R::FLAG_CRC64NVME | R::FLAG_RANGES | R::FLAG_COMBINABLE,
            folded0.flags);
  R folded2 = SplitOp::fold_rdma_result(false, 0, {&results[2]});
  EXPECT_EQ(R::FLAG_DECLINED, folded2.flags);
  EXPECT_EQ(0u, folded2.bytes);

  // an EC-direct sub-read carries the parent's extent: no shift. A resent
  // sub-read's result is marked in its slot, for the fold to pass on.
  ::ObjectOperation ec_rd;
  ec_rd.read(8192, 16384, &ec0, &sbl0);
  ec_rd.get_internal_versions(&vec, &vbl);
  auto* ec_sub = new Objecter::Op(object_t("obj"), object_locator_t(1),
                                  std::move(ec_rd.ops), 0, (Context*)nullptr,
                                  nullptr);
  Objecter::take_rdma(ec_sub, ec_rd);
  std::vector<R> ec_results;
  Objecter::fan_out_rdma(parent, ec_sub, {0, -1}, ec_results);
  ASSERT_EQ(2u, ec_results.size());
  EXPECT_EQ("t0", ec_sub->rdma_delivery[0].token);
  EXPECT_EQ(uint64_t(1 << 20), ec_sub->rdma_delivery[0].base_offset);
  ec_sub->attempts = 2;
  R declined;
  declined.flags = R::FLAG_DECLINED;
  Objecter::deliver_rdma_oob_results(ec_sub, {declined});
  EXPECT_EQ(R::FLAG_DECLINED | R::FLAG_RESENT, ec_results[0].flags);

  ec_sub->put();
  sub->put();
  parent->put();
}

// What a split read reports for a parent op whose descriptor it did not
// send: a sparse read's, which it does not fan out, or an op no sub-read
// reads. Nothing of it went out of band, so it reads declined, not as a
// transfer that may have started. And when the split ends in EAGAIN (a
// torn read), a split whose sub-reads carried no descriptor settled, so
// that its retry goes out as a first attempt, not as a resend.
TEST(RdmaDelivery, SplitDeclinesWhatItDidNotSend)
{
  using R = ceph::rdma::oob_result_t;
  R inline_part;  // a sub-read op without a descriptor: its slot as reset
  for (const auto& parts :
         {std::vector<const R*>{}, std::vector<const R*>{&inline_part}}) {
    SCOPED_TRACE(parts.size());
    const R r = SplitOp::fold_rdma_result(false, 0, parts);
    EXPECT_EQ(R::FLAG_DECLINED, r.flags);
    EXPECT_EQ(0u, r.bytes);
    EXPECT_TRUE(r.ranges.empty());
  }
  // a READ whose descriptor no sub-read carried
  EXPECT_EQ(R::FLAG_DECLINED, SplitOp::fold_rdma_result(true, 0, {}).flags);
  // a sent READ whose sub-read gave no result may have started one
  EXPECT_EQ(0u, SplitOp::fold_rdma_result(true, 0, {&inline_part}).flags);
  R declined;
  declined.flags = R::FLAG_DECLINED;
  EXPECT_EQ(R::FLAG_DECLINED,
            SplitOp::fold_rdma_result(true, 0, {&declined}).flags);

  // nothing sent: settled
  EXPECT_TRUE(SplitOp::rdma_split_settled({}));
  // sent: settled only when each part declined or landed, and none was
  // resent
  EXPECT_TRUE(SplitOp::rdma_split_settled({R::FLAG_DECLINED,
                                           R::FLAG_LANDED}));
  EXPECT_FALSE(SplitOp::rdma_split_settled({R::FLAG_DECLINED, 0}));
  EXPECT_FALSE(SplitOp::rdma_split_settled({R::FLAG_LANDED |
                                            R::FLAG_RESENT}));
}

// A split read's result: declined only when every sub-read declined,
// landed only when bytes moved and every one landed or declined, resent
// when any was.
TEST(RdmaDelivery, FoldTransferFlags)
{
  using R = ceph::rdma::oob_result_t;
  using ceph::rdma::fold_transfer_flags;
  EXPECT_EQ(0u, fold_transfer_flags({}, 0));
  EXPECT_EQ(R::FLAG_DECLINED,
            fold_transfer_flags({R::FLAG_DECLINED, R::FLAG_DECLINED}, 0));
  // one sub-read started a transfer: not declined
  EXPECT_EQ(0u, fold_transfer_flags({R::FLAG_DECLINED, 0}, 0));
  EXPECT_EQ(R::FLAG_LANDED,
            fold_transfer_flags({R::FLAG_LANDED, R::FLAG_LANDED}, 8192));
  // a replica's share wholly past the object's end declined: nothing of
  // it can land, so the shares that landed settle the read
  EXPECT_EQ(R::FLAG_LANDED,
            fold_transfer_flags({R::FLAG_LANDED, R::FLAG_LANDED,
                                 R::FLAG_DECLINED}, 8192));
  // a sub-read delivered without the promise: not landed
  EXPECT_EQ(0u, fold_transfer_flags({R::FLAG_LANDED, 0}, 8192));
  EXPECT_EQ(0u, fold_transfer_flags({R::FLAG_LANDED, R::FLAG_DECLINED, 0},
                                    8192));
  // landed needs bytes: a failed split does not sum them
  EXPECT_EQ(0u, fold_transfer_flags({R::FLAG_LANDED}, 0));
  EXPECT_EQ(0u, fold_transfer_flags({R::FLAG_LANDED, R::FLAG_DECLINED}, 0));
  // and a part that landed: a result is never both declined and landed
  EXPECT_EQ(R::FLAG_DECLINED,
            fold_transfer_flags({R::FLAG_DECLINED, R::FLAG_DECLINED}, 4096));
  EXPECT_EQ(R::FLAG_DECLINED | R::FLAG_RESENT,
            fold_transfer_flags({R::FLAG_DECLINED,
                                 R::FLAG_DECLINED | R::FLAG_RESENT}, 0));
  // resent rides along with landed, as on a resent op's own result
  EXPECT_EQ(R::FLAG_LANDED | R::FLAG_RESENT,
            fold_transfer_flags({R::FLAG_LANDED,
                                 R::FLAG_DECLINED | R::FLAG_RESENT}, 8192));
  // the crc flags are not folded here
  EXPECT_EQ(0u, fold_transfer_flags({R::FLAG_CRC64NVME}, 4096));
}
