// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_rdma_fence.h"

#include "common/ceph_context.h"
#include "common/rdma_token.h"
#include "librados/rdma_result.h"
#include "messages/MOSDOpReply.h"
#include "osdc/Objecter.h"

#include <deque>
#include <sstream>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

using R = librados::ObjectReadOperation::rdma_delivery_result;
using result = R;

namespace {
result with(uint32_t flags, uint64_t bytes = 0)
{
  result r;
  r.flags = flags;
  r.bytes = bytes;
  return r;
}
} // anonymous namespace

TEST(RgwRdmaFence, FenceCoversTheLargestLateCutOffTolerance)
{
  // the caps of every pool's lease and drain, and how late a libfabric
  // cut-off can end past them on any OSD, which set_rdma_delivery() asks
  // a caller to add for a resent read and for one that got no reply
  EXPECT_DOUBLE_EQ(pg_pool_t::MAX_RDMA_DELIVERY_LEASE +
                   pg_pool_t::MAX_RDMA_DELIVERY_DRAIN + 2.0,
                   rgw::rdma::delivery_fence());
}

TEST(RgwRdmaFence, NoOsdTakesALargerLateCutOffTolerance)
{
  // the fence counts the option's largest value, which every daemon
  // enforces, and not the gateway's own value: an OSD whose tolerance
  // is set apart from the gateway's still cuts off within the fence
  boost::intrusive_ptr<CephContext> cct{
    new CephContext(CEPH_ENTITY_TYPE_OSD), false};
  auto& conf = cct->_conf;
  const Option* opt = conf.get_schema("osd_oob_cutoff_late_tolerance");
  ASSERT_TRUE(opt);
  ASSERT_TRUE(std::holds_alternative<std::chrono::milliseconds>(opt->max));
  EXPECT_EQ(rgw::rdma::MAX_CUTOFF_LATE_TOLERANCE,
            std::get<std::chrono::milliseconds>(opt->max));
  EXPECT_EQ(-EINVAL, conf.set_val("osd_oob_cutoff_late_tolerance", "3000"));
  EXPECT_EQ(std::chrono::milliseconds(1000),
            conf.get_val<std::chrono::milliseconds>(
              "osd_oob_cutoff_late_tolerance"));
  EXPECT_EQ(0, conf.set_val("osd_oob_cutoff_late_tolerance", "2000"));
  EXPECT_EQ(rgw::rdma::MAX_CUTOFF_LATE_TOLERANCE,
            conf.get_val<std::chrono::milliseconds>(
              "osd_oob_cutoff_late_tolerance"));
}

TEST(RgwRdmaFence, AllDeclined)
{
  // one OSD of the pool delivers nothing out of band: every stripe it
  // serves comes back declined, and the fallback need not wait
  std::deque<result> rs = {with(R::DECLINED),
                           with(R::DECLINED)};
  EXPECT_FALSE(rgw::rdma::fence_needed(rs));
}

TEST(RgwRdmaFence, DeclinedAndLanded)
{
  // stripes delivered with every byte placed before the reply, next to
  // one declined: no write can land any more
  std::deque<result> rs = {
    with(R::LANDED | R::CRC64_VALID, 4 << 20),
    with(R::DECLINED),
    with(R::LANDED, 4 << 20)};
  EXPECT_FALSE(rgw::rdma::fence_needed(rs));
}

TEST(RgwRdmaFence, StartedTransferNeedsTheFence)
{
  // a stripe an OSD started a transfer for, and then returned inline
  std::deque<result> rs = {with(R::DECLINED), with(0)};
  EXPECT_TRUE(rgw::rdma::fence_needed(rs));
}

TEST(RgwRdmaFence, DeliveredWithoutThePromise)
{
  // delivered by an OSD whose transport does not promise that the bytes
  // landed before the reply, or by an older OSD
  std::deque<result> rs = {with(R::CRC64_VALID, 4 << 20),
                           with(R::DECLINED)};
  EXPECT_TRUE(rgw::rdma::fence_needed(rs));
}

TEST(RgwRdmaFence, ResentNeedsTheFence)
{
  // the original attempt may have started a transfer on another OSD
  std::deque<result> rs = {
    with(R::DECLINED | R::RESENT)};
  EXPECT_TRUE(rgw::rdma::fence_needed(rs));
  std::deque<result> landed = {
    with(R::LANDED | R::RESENT, 4096)};
  EXPECT_TRUE(rgw::rdma::fence_needed(landed));
}

TEST(RgwRdmaFence, NoResultNeedsTheFence)
{
  // a read that never came back, or one from an OSD that reports neither
  std::deque<result> rs = {with(R::DECLINED), result{}};
  EXPECT_TRUE(rgw::rdma::fence_needed(rs));
}

TEST(RgwRdmaFence, LandedStripesNeedNoFence)
{
  // a GET whose every stripe an OSD placed with the promise that the
  // bytes were in the window before it replied: answered at once
  std::deque<result> rs = {
    with(R::LANDED | R::CRC64_VALID, 4 << 20),
    with(R::LANDED | R::CRC64_VALID, 4 << 20)};
  EXPECT_FALSE(rgw::rdma::fence_needed(rs));
}

TEST(RgwRdmaFence, PlacedStripesThatDidNotLandNeedTheFence)
{
  // a GET that delivered everything over a transport that does not
  // promise delivery-complete writes (a libfabric provider without
  // FI_DELIVERY_COMPLETE): every stripe placed, and none of them
  // landed, so a reply may have come before its bytes. The response waits
  // out the fence, as after a resend
  std::deque<result> rs = {with(R::CRC64_VALID, 4 << 20),
                           with(R::CRC64_VALID, 4 << 20)};
  EXPECT_TRUE(rgw::rdma::fence_needed(rs));
  // one stripe that did not land is enough
  rs.front() = with(R::LANDED, 4 << 20);
  EXPECT_TRUE(rgw::rdma::fence_needed(rs));
  rs.back() = with(R::LANDED, 4 << 20);
  EXPECT_FALSE(rgw::rdma::fence_needed(rs));
}

TEST(RgwRdmaFence, PassthroughHoldsAfterAStripeThatDidNotLand)
{
  // a GET whose OSD placed a stripe without saying that it landed waits
  // out the fence: the next GETs do not pass through for a while
  using reports = rgw::rdma::landing_reports;
  using namespace std::chrono_literals;
  reports r;
  const auto t = reports::clock::now();
  EXPECT_TRUE(r.allow(t));
  EXPECT_TRUE(r.allow(t));
  // a GET whose stripes all landed changes nothing
  EXPECT_FALSE(r.note(t, false, true));
  EXPECT_TRUE(r.allow(t));
  // nor does one whose OSDs placed nothing
  EXPECT_FALSE(r.note(t, false, false));
  EXPECT_TRUE(r.allow(t));

  // one stripe that did not land starts the hold, landed ones or not
  EXPECT_TRUE(r.note(t, true, true));
  EXPECT_FALSE(r.allow(t));
  EXPECT_FALSE(r.allow(t + reports::HOLD - 1s));
  // a GET that went out before the hold, and whose stripes landed, may
  // have met other OSDs: the hold stays
  EXPECT_FALSE(r.note(t + 1s, false, true));
  EXPECT_FALSE(r.allow(t + 2s));

  // once it ran out, one GET probes, and the others wait for it
  const auto probe = t + reports::HOLD;
  EXPECT_TRUE(r.allow(probe));
  EXPECT_FALSE(r.allow(probe));
  EXPECT_FALSE(r.allow(probe + reports::PROBE - 1s));
  // a probe that never says goes to another GET
  EXPECT_TRUE(r.allow(probe + reports::PROBE));
  EXPECT_FALSE(r.allow(probe + reports::PROBE));

  // a probe whose stripe did not land starts the next hold
  const auto again = probe + reports::PROBE + 1s;
  EXPECT_FALSE(r.note(again, true, false));
  EXPECT_FALSE(r.allow(again + reports::HOLD - 1s));

  // a probe whose stripes landed ends the hold
  EXPECT_TRUE(r.allow(again + reports::HOLD));
  EXPECT_FALSE(r.note(again + reports::HOLD + 1s, false, true));
  EXPECT_TRUE(r.allow(again + reports::HOLD + 1s));
  EXPECT_TRUE(r.allow(again + reports::HOLD + 1s));
}

TEST(RgwRdmaFence, PassthroughHoldsForEachKindOfToken)
{
  // libfabric tokens and cuObject descriptors go to other executors
  auto& ofi = rgw::rdma::passthrough_landing("0:10:ofi1:xnet.1:0200:1");
  auto& cuobj = rgw::rdma::passthrough_landing(
    "7f0000000000:100000:1a2b:0:4d2:0:0");
  EXPECT_NE(&ofi, &cuobj);
  EXPECT_EQ(&ofi, &rgw::rdma::passthrough_landing(
              "7f0012345000:c00000:ofi1:rxm.1:020012340a00000100ff:1f"));
}

TEST(RgwRdmaFence, StripeReplies)
{
  using rgw::rdma::classify_stripe;
  using rgw::rdma::stripe_reply;
  EXPECT_EQ(stripe_reply::placed, classify_stripe(0, 0));
  // returned inline - declined, or a push cut off: the GET falls back
  EXPECT_EQ(stripe_reply::inline_data, classify_stripe(0, 4 << 20));
  // a failed read fails the GET even if data came with it
  EXPECT_EQ(stripe_reply::failed, classify_stripe(-ENOENT, 0));
  EXPECT_EQ(stripe_reply::failed, classify_stripe(-EIO, 4096));
}

TEST(RgwRdmaFence, Summary)
{
  std::deque<result> rs = {
    with(R::LANDED, 4 << 20),
    with(R::DECLINED),
    with(R::DECLINED | R::RESENT),
    with(0, 1 << 20),
    with(0)};
  const auto s = rgw::rdma::summarize(rs);
  EXPECT_EQ(5u, s.reads);
  EXPECT_EQ(2u, s.declined);
  EXPECT_EQ(1u, s.landed);
  EXPECT_EQ(2u, s.open);
  EXPECT_EQ(1u, s.resent);
  EXPECT_EQ(uint64_t(5 << 20), s.bytes);
  std::ostringstream o;
  o << s;
  EXPECT_EQ("5 reads (2 declined, 1 landed, 2 open, 1 resent), 5242880 "
            "bytes placed", o.str());
}

// What the fold of a split read's sub-reads tells a gateway, from
// fold_transfer_flags() alone, not through SplitOp::complete(). A read
// past the object's end, whose last share declined, needs no fence after
// the shares that landed. The fold of a failed split, its bytes not
// summed, needs none when every share declined, as each OSD does that
// answers with an error, and needs one when a share pushed. That
// complete() reports a failed split at all takes a cluster to show.
TEST(RgwRdmaFence, FoldedSplitResultSettlesTheFence)
{
  using L = librados::ObjectReadOperation::rdma_delivery_result;
  using ceph::rdma::attempt_flags;
  using ceph::rdma::fold_transfer_flags;
  using rgw::rdma::fence_needed;
  const uint32_t landed = attempt_flags(true, true, true);
  const uint32_t declined = attempt_flags(false, false, false);
  auto slot = [](const std::vector<uint32_t>& parts, uint64_t bytes) {
    L r;
    r.bytes = bytes;
    r.flags = fold_transfer_flags(parts, bytes);
    return std::vector{r};
  };
  EXPECT_FALSE(fence_needed(slot({landed, landed, declined}, 1536 << 10)));
  EXPECT_FALSE(fence_needed(slot({declined, declined}, 0)));
  EXPECT_TRUE(fence_needed(slot({landed, declined}, 0)));
}

// A stripe read whose push its OSD cut off - per plan, after the writes
// went out - from the OSD's reply, through the Objecter, into what a
// gateway reads: the data comes back inline with a result that is neither
// declined nor landed, which sends the GET to its HTTP fallback behind
// the fence. It does not fail the GET.
TEST(RgwRdmaFence, CutOffPushFallsBackToHttp)
{
  using R = ceph::rdma::oob_result_t;
  using L = librados::ObjectReadOperation::rdma_delivery_result;
  using ceph::rdma::attempt_flags;
  using rgw::rdma::classify_stripe;
  using rgw::rdma::stripe_reply;

  // what a gateway reads of one stripe read: its rval and inline data,
  // and its result, copied into its slot by librados' own copy
  struct stripe_t {
    int rval = 0;
    ceph::buffer::list data;
    L slot;
  };
  // the OSD's reply, encoded and decoded, then demuxed into the read as
  // handle_osd_op_reply() does it
  auto through_objecter = [](int osd_rval, uint64_t inline_bytes,
                             const R& osd, int attempts) {
    stripe_t s;
    ::ObjectOperation op;
    op.read(0, 4096, &s.data, &s.rval, nullptr);
    op.set_rdma_delivery("t", 0, 0, [&s](const R& r) {
      librados::copy_rdma_result(r, &s.slot);
    });
    auto* o = new Objecter::Op(object_t("obj"), object_locator_t(1),
                               std::move(op.ops), 0, (Context*)nullptr,
                               nullptr);
    o->out_bl.swap(op.out_bl);
    o->out_rval.swap(op.out_rval);
    o->rdma_delivery.swap(op.rdma_delivery);
    o->rdma_oob_handler.swap(op.rdma_oob_handler);
    o->attempts = attempts;

    const hobject_t hobj(object_t("obj"), "", CEPH_NOSNAP, 0x1234, 1, "");
    spg_t pgid(pg_t(0x1234, 1));
    auto req = ceph::make_message<MOSDOp>(
      1, 42, hobj, pgid, 10, CEPH_OSD_FLAG_READ, CEPH_FEATURES_ALL);
    req->ops.resize(1);
    req->ops[0].op.op = CEPH_OSD_OP_READ;
    req->ops[0].op.extent.length = 4096;
    req->ops[0].rval = osd_rval;
    req->ops[0].outdata.append_zero(inline_bytes);
    auto sent = ceph::make_message<MOSDOpReply>(
      req.get(), osd_rval, 10, CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK,
      false);
    sent->set_oob_results({osd});
    sent->encode_payload(CEPH_FEATURES_ALL);
    auto m = ceph::make_message<MOSDOpReply>();
    m->set_header(sent->get_header());
    bufferlist payload = sent->get_payload();
    m->set_payload(payload);
    m->set_data(sent->get_data());
    m->decode_payload();

    Objecter::deliver_rdma_oob_results(o, m->get_oob_results());
    std::vector<OSDOp> out_ops;
    m->claim_ops(out_ops);
    EXPECT_EQ(1u, out_ops.size());
    for (unsigned i = 0; i < out_ops.size() && i < o->out_bl.size(); ++i) {
      if (o->out_bl[i]) {
        *o->out_bl[i] = out_ops[i].outdata;
      }
      if (o->out_rval[i]) {
        *o->out_rval[i] = out_ops[i].rval;
      }
    }
    o->put();
    return s;
  };
  auto classify = [](const stripe_t& s) {
    return classify_stripe(s.rval, s.data.length());
  };

  // cut off after its writes went out: inline, and the fence holds
  R cut;
  cut.flags = attempt_flags(false, true, false);
  auto s = through_objecter(0, 4096, cut, 1);
  EXPECT_EQ(stripe_reply::inline_data, classify(s));
  EXPECT_EQ(0u, s.slot.bytes);
  EXPECT_TRUE(rgw::rdma::fence_needed(std::vector{s.slot}));

  // cut off before anything went out: inline, declined, no fence
  R refused;
  refused.flags = attempt_flags(false, false, false);
  s = through_objecter(0, 4096, refused, 1);
  EXPECT_EQ(stripe_reply::inline_data, classify(s));
  EXPECT_EQ(L::DECLINED, s.slot.flags);
  EXPECT_FALSE(rgw::rdma::fence_needed(std::vector{s.slot}));

  // a resend of a read that was declined: an earlier attempt may have
  // pushed, so the fence holds
  s = through_objecter(0, 4096, refused, 2);
  EXPECT_EQ(stripe_reply::inline_data, classify(s));
  EXPECT_TRUE(rgw::rdma::fence_needed(std::vector{s.slot}));

  // placed with the promise: nothing inline, landed, no fence, and the
  // result's checksums and ranges as the OSD sent them: each range with
  // its own offset and checksums, in the OSD's order
  R placed;
  placed.bytes = 4096;
  placed.crc64 = 0x1234;
  placed.crc32c = 0x5678;
  placed.ranges = {{2048, 2048, 0xabcd, 0x9876}, {0, 2048, 0xef01, 0x2345}};
  placed.flags = attempt_flags(true, true, true) | R::FLAG_CRC64NVME |
                 R::FLAG_CRC32C | R::FLAG_RANGES;
  s = through_objecter(0, 0, placed, 1);
  EXPECT_EQ(stripe_reply::placed, classify(s));
  EXPECT_EQ(L::LANDED | L::CRC64_VALID |
            L::CRC32C_VALID | L::RANGES,
            s.slot.flags);
  EXPECT_EQ(4096u, s.slot.bytes);
  EXPECT_EQ(0x1234u, s.slot.crc64);
  EXPECT_EQ(0x5678u, s.slot.crc32c);
  ASSERT_EQ(2u, s.slot.ranges.size());
  EXPECT_EQ(2048u, s.slot.ranges[0].ofs);
  EXPECT_EQ(2048u, s.slot.ranges[0].len);
  EXPECT_EQ(0xabcdu, s.slot.ranges[0].crc64);
  EXPECT_EQ(0x9876u, s.slot.ranges[0].crc32c);
  EXPECT_EQ(0u, s.slot.ranges[1].ofs);
  EXPECT_EQ(2048u, s.slot.ranges[1].len);
  EXPECT_EQ(0xef01u, s.slot.ranges[1].crc64);
  EXPECT_EQ(0x2345u, s.slot.ranges[1].crc32c);
  EXPECT_FALSE(rgw::rdma::fence_needed(std::vector{s.slot}));

  // a read that failed outright fails the GET, inline data or not
  s = through_objecter(-EIO, 0, cut, 1);
  EXPECT_EQ(stripe_reply::failed, classify(s));
  s = through_objecter(-EIO, 4096, cut, 1);
  EXPECT_EQ(stripe_reply::failed, classify(s));
}
