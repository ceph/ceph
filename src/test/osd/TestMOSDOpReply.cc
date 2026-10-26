// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <gtest/gtest.h>

#include "include/ceph_features.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

using ceph::bufferlist;

namespace {

bufferlist make_bl(const std::string& s)
{
  bufferlist bl;
  bl.append(s);
  return bl;
}

// Builds a request with the given op codes and a reply to it whose ops carry
// the supplied outdata/outmetadata.
ceph::ref_t<MOSDOpReply> make_reply(
  const std::vector<int>& opcodes,
  const std::vector<std::string>& outdata,
  const std::vector<std::string>& outmetadata)
{
  hobject_t hobj(object_t("obj"), "", CEPH_NOSNAP, 0, 1, "");
  spg_t spgid(pg_t(0, 1), shard_id_t::NO_SHARD);
  auto req = ceph::make_message<MOSDOp>(0, 1, hobj, spgid, 1, 0, 0);
  for (int o : opcodes) {
    req->add_simple_op(o, 0, 4096);
  }

  auto reply = ceph::make_message<MOSDOpReply>(req.get(), 0, 1, 0, false);
  std::vector<OSDOp> out(opcodes.size());
  for (size_t i = 0; i < opcodes.size(); ++i) {
    out[i].outdata = make_bl(outdata[i]);
    out[i].outmetadata = make_bl(outmetadata[i]);
  }
  reply->claim_op_out_data(out);
  return reply;
}

// Mimics what the messenger does: the receiver gets the payload and data
// sections plus the header version, then calls decode_payload().
ceph::ref_t<MOSDOpReply> round_trip(MOSDOpReply& sent, uint64_t features,
                                    bool drop_trailing_flag = false)
{
  sent.encode_payload(features);
  auto received = ceph::make_message<MOSDOpReply>();
  received->get_header().version = sent.get_header().version;
  bufferlist payload = sent.get_payload();
  if (drop_trailing_flag) {
    // Reproduce a v8 sender: same bytes minus the trailing has_metadata bool.
    bufferlist trimmed;
    trimmed.substr_of(payload, 0, payload.length() - 1);
    payload = std::move(trimmed);
    received->get_header().version = 8;
  }
  received->get_payload() = payload;
  received->get_data() = sent.get_data();
  received->decode_payload();
  return received;
}

} // namespace

TEST(MOSDOpReply, MetadataRoundTrip)
{
  auto sent = make_reply({CEPH_OSD_OP_SPARSE_READ, CEPH_OSD_OP_READ,
                          CEPH_OSD_OP_SPARSE_READ},
                         {"data0", "data1", "data2"},
                         {"meta0", "", "meta2"});
  auto received = round_trip(*sent, CEPH_FEATURES_ALL);

  std::vector<OSDOp> ops;
  received->claim_ops(ops);
  ASSERT_EQ(3u, ops.size());
  EXPECT_EQ(make_bl("data0"), ops[0].outdata);
  EXPECT_EQ(make_bl("data1"), ops[1].outdata);
  EXPECT_EQ(make_bl("data2"), ops[2].outdata);
  EXPECT_EQ(make_bl("meta0"), ops[0].outmetadata);
  EXPECT_EQ(0u, ops[1].outmetadata.length());
  EXPECT_EQ(make_bl("meta2"), ops[2].outmetadata);
}

// outmetadata must not leak into the message data section, otherwise the
// read data would no longer start at the beginning of that section.
TEST(MOSDOpReply, MetadataNotInDataSection)
{
  auto sent = make_reply({CEPH_OSD_OP_SPARSE_READ}, {"data0"}, {"meta0"});
  sent->encode_payload(CEPH_FEATURES_ALL);
  EXPECT_EQ(make_bl("data0"), sent->get_data());
}

// Metadata is only paid for when present: a reply carrying metadata for one op
// is larger than the same reply without by exactly the encoded size of the
// per-op buffers (4-byte length + bytes, for every op), and nothing else.
TEST(MOSDOpReply, MetadataOnlyCostsWhenPresent)
{
  auto none = make_reply({CEPH_OSD_OP_READ, CEPH_OSD_OP_STAT},
                         {"data0", "data1"}, {"", ""});
  none->encode_payload(CEPH_FEATURES_ALL);
  EXPECT_EQ(9, none->get_header().version);
  const size_t without = none->get_payload().length();

  // Decoding the same bytes with the trailing flag removed (as a v8 sender
  // would have produced) still works, and the flag is exactly 1 byte.
  auto received = round_trip(*none, CEPH_FEATURES_ALL,
                             /*drop_trailing_flag=*/true);
  std::vector<OSDOp> ops;
  received->claim_ops(ops);
  ASSERT_EQ(2u, ops.size());
  EXPECT_EQ(make_bl("data0"), ops[0].outdata);
  EXPECT_EQ(make_bl("data1"), ops[1].outdata);

  auto with = make_reply({CEPH_OSD_OP_READ, CEPH_OSD_OP_STAT},
                         {"data0", "data1"}, {"m", ""});
  with->encode_payload(CEPH_FEATURES_ALL);
  // op0 "m": 4 + 1 bytes, op1 "": 4 bytes (the has_metadata flag is already
  // present in both replies).
  EXPECT_EQ(without + (4 + 1) + 4, with->get_payload().length());
}

// A v8 reply (old OSD) has no trailing flag and must still decode, leaving
// outmetadata empty so the client falls back to the legacy combined format.
TEST(MOSDOpReply, DecodesV8Reply)
{
  auto sent = make_reply({CEPH_OSD_OP_SPARSE_READ}, {"legacy-combined"}, {""});
  auto received = round_trip(*sent, CEPH_FEATURES_ALL,
                             /*drop_trailing_flag=*/true);
  std::vector<OSDOp> ops;
  received->claim_ops(ops);
  ASSERT_EQ(1u, ops.size());
  EXPECT_EQ(make_bl("legacy-combined"), ops[0].outdata);
  EXPECT_EQ(0u, ops[0].outmetadata.length());
}

// ignore_out_data (legacy write behaviour) must drop metadata as well.
TEST(MOSDOpReply, IgnoreOutDataClearsMetadata)
{
  hobject_t hobj(object_t("obj"), "", CEPH_NOSNAP, 0, 1, "");
  spg_t spgid(pg_t(0, 1), shard_id_t::NO_SHARD);
  auto req = ceph::make_message<MOSDOp>(0, 1, hobj, spgid, 1, 0, 0);
  req->add_simple_op(CEPH_OSD_OP_SPARSE_READ, 0, 4096);
  req->ops[0].outdata = make_bl("data");
  req->ops[0].outmetadata = make_bl("meta");
  auto reply = ceph::make_message<MOSDOpReply>(req.get(), 0, 1, 0, true);
  std::vector<OSDOp> ops;
  reply->claim_ops(ops);
  ASSERT_EQ(1u, ops.size());
  EXPECT_EQ(0u, ops[0].outdata.length());
  EXPECT_EQ(0u, ops[0].outmetadata.length());
}

TEST(OSDOp, SplitReplyFlag)
{
  EXPECT_EQ(0x200, CEPH_OSD_OP_FLAG_SUPPORT_OUTMETADATA);
  EXPECT_STREQ("support_outmetadata",
               ceph_osd_op_flag_name(CEPH_OSD_OP_FLAG_SUPPORT_OUTMETADATA));
}
