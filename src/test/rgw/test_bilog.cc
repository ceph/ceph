// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_bilog.h"

#include <string>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <fmt/format.h>

#include "include/neorados/RADOS.hpp"

#include "cls/rgw/cls_rgw_types.h"
#include "cls/rgw/cls_rgw_ops.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace fifo = neorados::cls::fifo;

namespace {

// encode a bilog entry into a bufferlist, matching what RGWBILogUpdateBatch
// does internally before calling fifo_->push().
ceph::buffer::list encode_entry(const rgw_bi_log_entry& e)
{
  ceph::buffer::list bl;
  encode(e, bl);
  return bl;
}

// decode the raw FIFO payload back to a rgw_bi_log_entry.
rgw_bi_log_entry decode_entry(const fifo::entry& raw)
{
  rgw_bi_log_entry e;
  auto p = raw.data.cbegin();
  decode(e, p);
  return e;
}

}

// these tests exercise the FIFO push/list/trim pipeline directly without
// going through the batch helper.

// single-shard push + list round-trip: object name, instance, op, state and tag
// must survive encode-push-list-decode intact.
CORO_TEST_F(BilogFIFO, PushListRoundtrip, NeoRadosTest)
{
  const std::vector<std::string> shard_oids{"blt.rt.0"};
  auto store = std::make_shared<RGWBILogFIFO>(rados(), pool(), shard_oids);

  rgw_bi_log_entry entry;
  entry.object   = "myobject";
  entry.instance = "v1";
  entry.op       = CLS_RGW_OP_ADD;
  entry.state    = CLS_RGW_STATE_COMPLETE;
  entry.tag      = "tag-rt";

  co_await store->push(dpp(), 0, encode_entry(entry));

  std::vector<fifo::entry> buf(10);
  auto [got, next] = co_await store->list(dpp(), 0, {}, buf);

  EXPECT_EQ(1u, got.size());
  EXPECT_FALSE(next.has_value());

  const auto d = decode_entry(got[0]);
  EXPECT_EQ("myobject",            d.object);
  EXPECT_EQ("v1",                  d.instance);
  EXPECT_EQ(CLS_RGW_OP_ADD,        d.op);
  EXPECT_EQ(CLS_RGW_STATE_COMPLETE, d.state);
  EXPECT_EQ("tag-rt",              d.tag);
}

// two-shard push: entries pushed to distinct shards must not appear in each
// other's shard.
CORO_TEST_F(BilogFIFO, MultiShard, NeoRadosTest)
{
  const std::vector<std::string> shard_oids{"blt.ms.0", "blt.ms.1"};
  auto store = std::make_shared<RGWBILogFIFO>(rados(), pool(), shard_oids);

  rgw_bi_log_entry e0, e1;
  e0.object = "obj-s0";  e0.tag = "t0";  e0.op = CLS_RGW_OP_ADD;
  e1.object = "obj-s1";  e1.tag = "t1";  e1.op = CLS_RGW_OP_DEL;

  co_await store->push(dpp(), 0, encode_entry(e0));
  co_await store->push(dpp(), 1, encode_entry(e1));

  std::vector<fifo::entry> buf(10);

  auto [got0, _0] = co_await store->list(dpp(), 0, {}, buf);
  EXPECT_EQ(1u, got0.size());
  EXPECT_EQ("obj-s0", decode_entry(got0[0]).object);

  auto [got1, _1] = co_await store->list(dpp(), 1, {}, buf);
  EXPECT_EQ(1u, got1.size());
  EXPECT_EQ("obj-s1", decode_entry(got1[0]).object);
}

// trim (exclusive=false): push 3 entries, trim inclusive up to the 2nd entry's
// marker, then verify only the 3rd entry survives.
CORO_TEST_F(BilogFIFO, Trim, NeoRadosTest)
{
  const std::vector<std::string> shard_oids{"blt.trim.0"};
  auto store = std::make_shared<RGWBILogFIFO>(rados(), pool(), shard_oids);

  for (int i = 0; i < 3; ++i) {
    rgw_bi_log_entry e;
    e.object = fmt::format("obj-{}", i);
    e.op     = CLS_RGW_OP_ADD;
    e.state  = CLS_RGW_STATE_COMPLETE;
    co_await store->push(dpp(), 0, encode_entry(e));
  }

  std::vector<fifo::entry> buf(10);
  auto [all, _a] = co_await store->list(dpp(), 0, {}, buf);
  EXPECT_EQ(3u, all.size());

  // exclusive=false trims everything up to *and including* marker[1].
  co_await store->trim(dpp(), 0, all[1].marker, /*exclusive=*/false);

  auto [after, _b] = co_await store->list(dpp(), 0, {}, buf);
  EXPECT_EQ(1u, after.size());
  EXPECT_EQ("obj-2", decode_entry(after[0]).object);
}

// list from marker: push 3 entries, list starting after the first entry's
// marker; should yield exactly entries 2 and 3.
CORO_TEST_F(BilogFIFO, ListFromMarker, NeoRadosTest)
{
  const std::vector<std::string> shard_oids{"blt.lm.0"};
  auto store = std::make_shared<RGWBILogFIFO>(rados(), pool(), shard_oids);

  for (int i = 0; i < 3; ++i) {
    rgw_bi_log_entry e;
    e.object = fmt::format("obj-{}", i);
    e.op     = CLS_RGW_OP_ADD;
    co_await store->push(dpp(), 0, encode_entry(e));
  }

  std::vector<fifo::entry> buf(10);
  auto [all, _a] = co_await store->list(dpp(), 0, {}, buf);
  EXPECT_EQ(3u, all.size());

  // resume listing after the first entry's marker (exclusive start).
  auto [from, _b] = co_await store->list(dpp(), 0, all[0].marker, buf);
  EXPECT_EQ(2u, from.size());
  EXPECT_EQ("obj-1", decode_entry(from[0]).object);
  EXPECT_EQ("obj-2", decode_entry(from[1]).object);
}

// trim exclusive=true: the entry at the trim marker should NOT be removed.
CORO_TEST_F(BilogFIFO, TrimExclusive, NeoRadosTest)
{
  const std::vector<std::string> shard_oids{"blt.trimex.0"};
  auto store = std::make_shared<RGWBILogFIFO>(rados(), pool(), shard_oids);

  for (int i = 0; i < 3; ++i) {
    rgw_bi_log_entry e;
    e.object = fmt::format("obj-{}", i);
    e.op     = CLS_RGW_OP_ADD;
    co_await store->push(dpp(), 0, encode_entry(e));
  }

  std::vector<fifo::entry> buf(10);
  auto [all, _a] = co_await store->list(dpp(), 0, {}, buf);
  EXPECT_EQ(3u, all.size());

  // exclusive=true: trim everything *before* marker[1] — entry[1] must remain.
  co_await store->trim(dpp(), 0, all[1].marker, /*exclusive=*/true);

  auto [after, _b] = co_await store->list(dpp(), 0, {}, buf);
  EXPECT_EQ(2u, after.size());
  EXPECT_EQ("obj-1", decode_entry(after[0]).object);
  EXPECT_EQ("obj-2", decode_entry(after[1]).object);
}

// these tests exercise the batch wrapper.  co_flush() is the awaitable variant
// of flush.

// round-trip through the awaitable flush path: stage one entry via
// add_maybe_flush, flush it via co_flush(), then read back the raw FIFO entry
// and verify all fields match.
CORO_TEST_F(BilogBatch, RoundTripYield, NeoRadosTest)
{
  const std::vector<std::string> shard_oids{"blt.batch.rt.0"};
  auto store = std::make_shared<RGWBILogFIFO>(rados(), pool(), shard_oids);

  {
    RGWBILogUpdateBatch batch(dpp(), rados(), store);

    cls_rgw_bi_log_related_op op_info;
    op_info.key         = cls_rgw_obj_key{"roundtrip-obj", "inst-1"};
    op_info.op_tag      = "op-tag-rt";
    op_info.bilog_flags = RGW_BILOG_FLAG_VERSIONED_OP;
    op_info.op          = CLS_RGW_OP_ADD;

    batch.add_maybe_flush(7 /* olh_epoch */, ceph::real_clock::now(), op_info);

    // co_flush() is the awaitable variant: suspends the coroutine until all
    // pending shards have been pushed, without blocking the io_context thread.
    co_await batch.co_flush();
  }

  std::vector<fifo::entry> buf(10);
  auto [got, _] = co_await store->list(dpp(), 0, {}, buf);

  EXPECT_EQ(1u, got.size());

  const auto d = decode_entry(got[0]);
  EXPECT_EQ("roundtrip-obj",       d.object);
  EXPECT_EQ("inst-1",              d.instance);
  EXPECT_EQ("op-tag-rt",           d.tag);
  EXPECT_EQ(CLS_RGW_OP_ADD,        d.op);
  EXPECT_EQ(CLS_RGW_STATE_COMPLETE, d.state);
  EXPECT_EQ(7u,                    d.ver.epoch);
  EXPECT_NE(0, d.bilog_flags & RGW_BILOG_FLAG_VERSIONED_OP);
}

// OLH link overload: add_maybe_flush(epoch, key, tag, delete_marker, time,
// zones_trace) should produce a CLS_RGW_OP_LINK_OLH_DM entry when
// delete_marker=true and CLS_RGW_OP_LINK_OLH otherwise.
CORO_TEST_F(BilogBatch, OLHDeleteMarker, NeoRadosTest)
{
  const std::vector<std::string> shard_oids{"blt.olh.dm.0"};
  auto store = std::make_shared<RGWBILogFIFO>(rados(), pool(), shard_oids);

  {
    RGWBILogUpdateBatch batch(dpp(), rados(), store);

    rgw_zone_set zones_trace;
    batch.add_maybe_flush(5 /* epoch */,
                          cls_rgw_obj_key{"olh-obj", "v2"},
                          "olh-tag",
                          /*delete_marker=*/true,
                          ceph::real_clock::now(),
                          zones_trace);

    co_await batch.co_flush();
  }

  std::vector<fifo::entry> buf(10);
  auto [got, _] = co_await store->list(dpp(), 0, {}, buf);

  EXPECT_EQ(1u, got.size());
  const auto d = decode_entry(got[0]);
  EXPECT_EQ("olh-obj",              d.object);
  EXPECT_EQ("v2",                   d.instance);
  EXPECT_EQ(CLS_RGW_OP_LINK_OLH_DM, d.op);
  EXPECT_EQ(CLS_RGW_STATE_COMPLETE,  d.state);
  EXPECT_EQ(5u,                     d.ver.epoch);
  // OLH link entries from the 2nd overload are always versioned.
  EXPECT_NE(0, d.bilog_flags & RGW_BILOG_FLAG_VERSIONED_OP);
}

CORO_TEST_F(BilogBatch, OLHLink, NeoRadosTest)
{
  const std::vector<std::string> shard_oids{"blt.olh.lnk.0"};
  auto store = std::make_shared<RGWBILogFIFO>(rados(), pool(), shard_oids);

  {
    RGWBILogUpdateBatch batch(dpp(), rados(), store);

    rgw_zone_set zones_trace;
    batch.add_maybe_flush(3 /* epoch */,
                          cls_rgw_obj_key{"link-obj", "v1"},
                          "link-tag",
                          /*delete_marker=*/false,
                          ceph::real_clock::now(),
                          zones_trace);

    co_await batch.co_flush();
  }

  std::vector<fifo::entry> buf(10);
  auto [got, _] = co_await store->list(dpp(), 0, {}, buf);

  EXPECT_EQ(1u, got.size());
  const auto d = decode_entry(got[0]);
  EXPECT_EQ("link-obj",            d.object);
  EXPECT_EQ(CLS_RGW_OP_LINK_OLH,  d.op);
  EXPECT_EQ(CLS_RGW_STATE_COMPLETE, d.state);
  EXPECT_EQ(3u,                    d.ver.epoch);
}

// NOTE: The destructor-flush path (pending entries flushed by ~RGWBILogUpdateBatch)
// is not tested here. it calls do_flush() which internally uses use_blocked,
// which blocks the calling thread. In a CORO_TEST_F, the io_context runs on a
// single thread.