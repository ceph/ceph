// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"
#include "mgr/MetricTypes.h"

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

// Replace the leading uint32 type field of an encoded metric message.
static bufferlist with_type(const bufferlist& bl, uint32_t type)
{
  bufferlist out;
  encode(type, out);
  bufferlist rest;
  rest.substr_of(bl, sizeof(uint32_t), bl.length() - sizeof(uint32_t));
  out.claim_append(rest);
  return out;
}

TEST(MetricReportMessage, DecodeUnknownTypeIsMalformed)
{
  // a report whose type field arrived garbled must fail to decode (the
  // messenger then drops it and resets the connection) instead of aborting
  // the daemon
  bufferlist bl;
  encode(MetricReportMessage(OSDMetricPayload()), bl);
  bufferlist garbled = with_type(bl, 0xdeadbeef);

  MetricReportMessage m;
  auto p = garbled.cbegin();
  EXPECT_THROW(decode(m, p), ceph::buffer::malformed_input);
}

TEST(MetricConfigMessage, DecodeUnknownTypeIsMalformed)
{
  bufferlist bl;
  encode(MetricConfigMessage(MDSConfigPayload()), bl);
  bufferlist garbled = with_type(bl, 0xdeadbeef);

  MetricConfigMessage m;
  auto p = garbled.cbegin();
  EXPECT_THROW(decode(m, p), ceph::buffer::malformed_input);
}
