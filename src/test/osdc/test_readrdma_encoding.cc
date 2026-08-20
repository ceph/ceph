// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "osdc/Objecter.h"

#include "gtest/gtest.h"

// Pin the CEPH_OSD_OP_READ_RDMA wire format. The payload is
// hand-encoded (not a DENC type), so the encoding corpus cannot catch
// drift, and the OSD-side decode only surfaces mismatches as
// wrong-offset RDMA writes on RDMA-capable clusters that CI never
// runs. Any change here is a wire format change and needs a payload
// version bump.
TEST(ReadRdmaEncoding, WireFormat)
{
  ObjectOperation op;
  uint64_t bytes = 0;
  int rval = 0;
  const std::string token = "deadbeef:1000:x";
  op.read_rdma(0x1122334455667788ull, 0x1000, token,
               0xa1b2c3d4e5f60718ull, &bytes, &rval);

  ASSERT_EQ(1u, op.ops.size());
  auto& osd_op = op.ops[0];
  EXPECT_EQ(CEPH_OSD_OP_READ_RDMA, (int)osd_op.op.op);
  EXPECT_EQ(0x1122334455667788ull, osd_op.op.extent.offset);
  EXPECT_EQ(0x1000ull, osd_op.op.extent.length);

  // exact payload bytes: u8 version, le32 token length + token bytes,
  // le64 client offset, le32 reserved flags
  static const unsigned char expected_bytes[] = {
    0x01,                                            // payload version
    0x0f, 0x00, 0x00, 0x00,                          // token length (le32)
    'd', 'e', 'a', 'd', 'b', 'e', 'e', 'f', ':',
    '1', '0', '0', '0', ':', 'x',                    // token
    0x18, 0x07, 0xf6, 0xe5, 0xd4, 0xc3, 0xb2, 0xa1,  // client offset (le64)
    0x00, 0x00, 0x00, 0x00,                          // reserved flags (le32)
  };
  bufferlist expected;
  expected.append(reinterpret_cast<const char*>(expected_bytes),
                  sizeof(expected_bytes));
  EXPECT_TRUE(osd_op.indata.contents_equal(expected))
    << "READ_RDMA payload layout changed; this is a wire format break";

  // and it round-trips through the decode sequence the OSD uses
  auto p = osd_op.indata.cbegin();
  uint8_t ver = 0;
  std::string tok;
  uint64_t cofs = 0;
  uint32_t rflags = 1;
  decode(ver, p);
  decode(tok, p);
  decode(cofs, p);
  decode(rflags, p);
  EXPECT_EQ(1, ver);
  EXPECT_EQ(token, tok);
  EXPECT_EQ(0xa1b2c3d4e5f60718ull, cofs);
  EXPECT_EQ(0u, rflags);
  EXPECT_TRUE(p.end());
}
