// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <map>
#include <list>
#include <vector>
#include <cstdint>
#include <utility>
#include <iterator>

#include <gtest/gtest.h>

#include "include/ceph_features.h"
#include "osd/ECMsgTypes.h"

namespace {

hobject_t make_oid(const char *name, snapid_t snap = CEPH_NOSNAP)
{
  return hobject_t {sobject_t {name, snap}};
}

ECSubRead make_read()
{
  const auto first = make_oid("first", 1);
  const auto second = make_oid("second");

  ECSubRead read;
  read.from = pg_shard_t {2, shard_id_t {1}};
  read.tid = 37;
  read.to_read[first].emplace_back(10, 20, 1);
  read.to_read[first].emplace_back(40, 50, 2);
  read.to_read[second].emplace_back(70, 80, 4);
  read.attrs_to_read.insert(first);
  read.subchunks[first].emplace_back(0, 2);
  read.subchunks[second].emplace_back(2, 2);
  read.omap_headers_to_read.insert(second);
  read.omap_read_from.emplace(first, std::pair {"marker", 4096});
  return read;
}

auto legacy_read_extents(const ECSubRead& read)
{
  std::map<hobject_t, std::list<std::pair<uint64_t, uint64_t>>> result;

  for (const auto& [oid, extents] : read.to_read) {
    auto& legacy = result[oid];

    for (const auto& extent : extents) {
      legacy.emplace_back(extent.get<0>(), extent.get<1>());
    }
  }

  return result;
}

// Recreate each historical wire schema independently of ECSubRead::encode():
ceph::buffer::list encode_version_one(const ECSubRead& read)
{
  ceph::buffer::list encoded;
  ENCODE_START(1, 1, encoded);
  encode(read.from, encoded);
  encode(read.tid, encoded);
  encode(legacy_read_extents(read), encoded);
  encode(read.attrs_to_read, encoded);
  ENCODE_FINISH(encoded);

  return encoded;
}

ceph::buffer::list encode_version_two_with_subchunks(const ECSubRead& read)
{
  ceph::buffer::list encoded;
  ENCODE_START(2, 1, encoded);
  encode(read.from, encoded);
  encode(read.tid, encoded);
  encode(legacy_read_extents(read), encoded);
  encode(read.attrs_to_read, encoded);
  encode(read.subchunks, encoded);
  ENCODE_FINISH(encoded);

  return encoded;
}

ceph::buffer::list encode_version_two_with_flags(const ECSubRead& read)
{
  ceph::buffer::list encoded;
  ENCODE_START(2, 2, encoded);
  encode(read.from, encoded);
  encode(read.tid, encoded);
  encode(read.to_read, encoded);
  encode(read.attrs_to_read, encoded);
  ENCODE_FINISH(encoded);

  return encoded;
}

ceph::buffer::list encode_version_three(const ECSubRead& read)
{
  ceph::buffer::list encoded;
  ENCODE_START(3, 2, encoded);
  encode(read.from, encoded);
  encode(read.tid, encoded);
  encode(read.to_read, encoded);
  encode(read.attrs_to_read, encoded);
  encode(read.subchunks, encoded);
  ENCODE_FINISH(encoded);

  return encoded;
}

ceph::buffer::list encode_version_four(const ECSubRead& read)
{
  ceph::buffer::list encoded;
  ENCODE_START(4, 2, encoded);
  encode(read.from, encoded);
  encode(read.tid, encoded);
  encode(read.to_read, encoded);
  encode(read.attrs_to_read, encoded);
  encode(read.subchunks, encoded);
  encode(read.omap_read_from, encoded);
  encode(read.omap_headers_to_read, encoded);
  ENCODE_FINISH(encoded);

  return encoded;
}

void seed_stale_state(ECSubRead& read)
{
  const auto stale = make_oid("stale");

  read.to_read[stale].emplace_back(1, 2, 3);
  read.attrs_to_read.insert(stale);
  read.subchunks[stale].emplace_back(4, 5);
  read.omap_headers_to_read.insert(stale);
  read.omap_read_from.emplace(stale, std::pair {"stale", 6});
}

ECSubRead decode_read(const ceph::buffer::list& encoded)
{
  ECSubRead decoded;
  seed_stale_state(decoded);
  auto input = encoded.cbegin();
  decoded.decode(input);
  EXPECT_EQ(encoded.length(), input.get_off());

  return decoded;
}

ECSubReadReply make_reply()
{
  const auto first = make_oid("first", 1);
  const auto second = make_oid("second");

  ceph::buffer::list alpha;
  alpha.append("alpha");
  ceph::buffer::list beta;
  beta.append("beta");

  ECSubReadReply reply;
  reply.from = pg_shard_t {3, shard_id_t {2}};
  reply.tid = 91;
  reply.buffers_read[first].emplace_back(10, alpha);
  reply.buffers_read[first].emplace_back(40, beta);
  reply.buffers_read[second].emplace_back(70, alpha);
  reply.attrs_read[first].emplace("attr", beta);
  reply.errors.emplace(second, -2);
  reply.omap_headers_read.emplace(first, alpha);
  reply.omap_entries_read[first].emplace("key", beta);
  reply.omaps_complete.emplace(first, true);
  return reply;
}

void expect_read_payload(const ECSubRead& read, uint32_t first_flags,
                         uint32_t second_flags, uint32_t third_flags)
{
  const auto first = make_oid("first", 1);
  const auto second = make_oid("second");
  const auto& first_extents = read.to_read.at(first);

  ASSERT_EQ(2, std::size(first_extents));
  EXPECT_EQ(10, first_extents[0].get<0>());
  EXPECT_EQ(20, first_extents[0].get<1>());
  EXPECT_EQ(first_flags, first_extents[0].get<2>());
  EXPECT_EQ(40, first_extents[1].get<0>());
  EXPECT_EQ(50, first_extents[1].get<1>());
  EXPECT_EQ(second_flags, first_extents[1].get<2>());

  const auto& second_extents = read.to_read.at(second);

  ASSERT_EQ(1, std::size(second_extents));
  EXPECT_EQ(70, second_extents[0].get<0>());
  EXPECT_EQ(80, second_extents[0].get<1>());
  EXPECT_EQ(third_flags, second_extents[0].get<2>());
}

void expect_common_fields(const ECSubRead& expected, const ECSubRead& actual)
{
  EXPECT_EQ(expected.from, actual.from);
  EXPECT_EQ(expected.tid, actual.tid);
  EXPECT_EQ(expected.attrs_to_read, actual.attrs_to_read);
  EXPECT_FALSE(actual.to_read.contains(make_oid("stale")));
  EXPECT_FALSE(actual.subchunks.contains(make_oid("stale")));
  EXPECT_FALSE(actual.omap_read_from.contains(make_oid("stale")));
  EXPECT_FALSE(actual.omap_headers_to_read.contains(make_oid("stale")));
}

void expect_reply_payload(const ECSubReadReply& reply)
{
  const auto first = make_oid("first", 1);
  const auto second = make_oid("second");
  const auto& first_extents = reply.buffers_read.at(first);

  ASSERT_EQ(2, std::size(first_extents));
  EXPECT_EQ(10, first_extents[0].first);
  EXPECT_TRUE(first_extents[0].second.contents_equal("alpha", 5));
  EXPECT_EQ(40, first_extents[1].first);
  EXPECT_TRUE(first_extents[1].second.contents_equal("beta", 4));

  const auto& second_extents = reply.buffers_read.at(second);

  ASSERT_EQ(1, std::size(second_extents));
  EXPECT_EQ(70, second_extents[0].first);
  EXPECT_TRUE(second_extents[0].second.contents_equal("alpha", 5));
}

} // namespace

TEST(ECMsgTypes, readExtentVectorPreservesWireEncoding)
{
  using extent = ECSubRead::read_extent;

  const auto oid = make_oid("object");
  const std::map<hobject_t, std::list<extent>> list_extents {
    {oid, {{10, 20, 1}, {40, 50, 2}}}
  };
  const std::map<hobject_t, ECSubRead::read_extents> vector_extents {
    {oid, {{10, 20, 1}, {40, 50, 2}}}
  };
  ceph::buffer::list list_encoding;
  ceph::buffer::list vector_encoding;
  ceph::encode(list_extents, list_encoding);
  ceph::encode(vector_extents, vector_encoding);

  EXPECT_TRUE(list_encoding.contents_equal(vector_encoding));
}

TEST(ECMsgTypes, returnedExtentVectorPreservesWireEncoding)
{
  using extent = ECSubReadReply::returned_extent;

  ceph::buffer::list alpha;
  alpha.append("alpha");
  ceph::buffer::list beta;
  beta.append("beta");

  const auto oid = make_oid("object");
  const std::map<hobject_t, std::list<extent>> list_extents {
    {oid, {{10, alpha}, {40, beta}}}
  };
  const std::map<hobject_t, ECSubReadReply::returned_extents> vector_extents {
    {oid, {{10, alpha}, {40, beta}}}
  };
  ceph::buffer::list list_encoding;
  ceph::buffer::list vector_encoding;
  ceph::encode(list_extents, list_encoding);
  ceph::encode(vector_extents, vector_encoding);

  EXPECT_TRUE(list_encoding.contents_equal(vector_encoding));
}

TEST(ECMsgTypes, recoveryObjectVectorPreservesWireEncoding)
{
  using object = std::pair<hobject_t, eversion_t>;

  const std::list<object> list_objects {
    {make_oid("first"), eversion_t {1, 2}},
    {make_oid("second", 3), eversion_t {4, 5}}
  };
  const std::vector<object> vector_objects {
    {make_oid("first"), eversion_t {1, 2}},
    {make_oid("second", 3), eversion_t {4, 5}}
  };
  ceph::buffer::list list_encoding;
  ceph::buffer::list vector_encoding;
  ceph::encode(list_objects, list_encoding);
  ceph::encode(vector_objects, vector_encoding);

  EXPECT_TRUE(list_encoding.contents_equal(vector_encoding));
}

TEST(ECMsgTypes, currentReadRoundTripsWithoutWireChanges)
{
  const auto source = make_read();
  const auto expected = encode_version_four(source);
  ceph::buffer::list encoded;
  source.encode(encoded, CEPH_FEATURE_OSD_FADVISE_FLAGS);

  EXPECT_TRUE(expected.contents_equal(encoded));

  const auto decoded = decode_read(encoded);
  expect_common_fields(source, decoded);
  EXPECT_EQ(source.subchunks, decoded.subchunks);
  EXPECT_EQ(source.omap_headers_to_read, decoded.omap_headers_to_read);
  EXPECT_EQ(source.omap_read_from, decoded.omap_read_from);
  expect_read_payload(decoded, 1, 2, 4);

  ceph::buffer::list reencoded;
  decoded.encode(reencoded, CEPH_FEATURE_OSD_FADVISE_FLAGS);
  EXPECT_TRUE(encoded.contents_equal(reencoded));
}

TEST(ECMsgTypes, versionTwoEncoderPreservesWireEncoding)
{
  const auto source = make_read();
  const auto expected = encode_version_two_with_subchunks(source);
  ceph::buffer::list encoded;
  source.encode(encoded, 0);

  EXPECT_TRUE(expected.contents_equal(encoded));
}

TEST(ECMsgTypes, versionOneReadDecodes)
{
  const auto source = make_read();
  const auto decoded = decode_read(encode_version_one(source));

  expect_common_fields(source, decoded);
  expect_read_payload(decoded, 0, 0, 0);
  const auto default_subchunks = std::vector {std::pair {0, 1}};
  EXPECT_EQ(default_subchunks, decoded.subchunks.at(make_oid("first", 1)));
  EXPECT_EQ(default_subchunks, decoded.subchunks.at(make_oid("second")));
  EXPECT_TRUE(std::empty(decoded.omap_read_from));
  EXPECT_TRUE(std::empty(decoded.omap_headers_to_read));
}

TEST(ECMsgTypes, versionTwoReadDecodes)
{
  const auto source = make_read();
  const auto encoded = encode_version_two_with_subchunks(source);
  const auto decoded = decode_read(encoded);

  expect_common_fields(source, decoded);
  expect_read_payload(decoded, 0, 0, 0);
  EXPECT_EQ(source.subchunks, decoded.subchunks);
  EXPECT_TRUE(std::empty(decoded.omap_read_from));
  EXPECT_TRUE(std::empty(decoded.omap_headers_to_read));

  ceph::buffer::list reencoded;
  decoded.encode(reencoded, 0);
  EXPECT_TRUE(encoded.contents_equal(reencoded));
}

TEST(ECMsgTypes, versionTwoFlaggedReadDecodes)
{
  const auto source = make_read();
  const auto decoded = decode_read(encode_version_two_with_flags(source));

  expect_common_fields(source, decoded);
  expect_read_payload(decoded, 1, 2, 4);
  const auto default_subchunks = std::vector {std::pair {0, 1}};
  EXPECT_EQ(default_subchunks, decoded.subchunks.at(make_oid("first", 1)));
  EXPECT_EQ(default_subchunks, decoded.subchunks.at(make_oid("second")));
  EXPECT_TRUE(std::empty(decoded.omap_read_from));
  EXPECT_TRUE(std::empty(decoded.omap_headers_to_read));
}

TEST(ECMsgTypes, versionThreeReadDecodes)
{
  const auto source = make_read();
  const auto decoded = decode_read(encode_version_three(source));

  expect_common_fields(source, decoded);
  expect_read_payload(decoded, 1, 2, 4);
  EXPECT_EQ(source.subchunks, decoded.subchunks);
  EXPECT_TRUE(std::empty(decoded.omap_read_from));
  EXPECT_TRUE(std::empty(decoded.omap_headers_to_read));
}

TEST(ECMsgTypes, legacyReplyRoundTrips)
{
  const auto source = make_reply();
  ceph::buffer::list encoded_header;
  ceph::buffer::list encoded_data;
  source.encode(encoded_header, encoded_data, 0);

  ECSubReadReply decoded;
  decoded.buffers_read[make_oid("stale")].emplace_back();
  auto header = encoded_header.cbegin();
  auto data = encoded_data.cbegin();
  decoded.decode(header, data);

  EXPECT_EQ(source.from, decoded.from);
  EXPECT_EQ(source.tid, decoded.tid);
  EXPECT_EQ(2, std::size(decoded.buffers_read));
  EXPECT_TRUE(decoded.omap_headers_read.empty());
  EXPECT_TRUE(decoded.omap_entries_read.empty());
  EXPECT_TRUE(decoded.omaps_complete.empty());
  expect_reply_payload(decoded);

  ceph::buffer::list reencoded_header;
  ceph::buffer::list reencoded_data;
  decoded.encode(reencoded_header, reencoded_data, 0);
  EXPECT_TRUE(encoded_header.contents_equal(reencoded_header));
  EXPECT_TRUE(encoded_data.contents_equal(reencoded_data));
}

TEST(ECMsgTypes, splitReplyRoundTrips)
{
  const auto source = make_reply();
  ceph::buffer::list encoded_header;
  ceph::buffer::list encoded_data;
  source.encode(encoded_header, encoded_data,
                CEPH_FEATUREMASK_SERVER_TENTACLE);

  ECSubReadReply decoded;
  decoded.buffers_read[make_oid("stale")].emplace_back();
  auto header = encoded_header.cbegin();
  auto data = encoded_data.cbegin();
  decoded.decode(header, data);

  EXPECT_EQ(source.from, decoded.from);
  EXPECT_EQ(source.tid, decoded.tid);
  EXPECT_EQ(2, std::size(decoded.buffers_read));
  EXPECT_EQ(source.errors, decoded.errors);
  EXPECT_EQ(source.omaps_complete, decoded.omaps_complete);

  const auto first = make_oid("first", 1);
  EXPECT_TRUE(decoded.attrs_read.at(first).at("attr").contents_equal("beta", 4));
  EXPECT_TRUE(decoded.omap_headers_read.at(first).contents_equal("alpha", 5));
  EXPECT_TRUE(decoded.omap_entries_read.at(first).at("key").contents_equal("beta", 4));
  expect_reply_payload(decoded);

  ceph::buffer::list reencoded_header;
  ceph::buffer::list reencoded_data;
  decoded.encode(reencoded_header, reencoded_data,
                 CEPH_FEATUREMASK_SERVER_TENTACLE);
  EXPECT_TRUE(encoded_header.contents_equal(reencoded_header));
  EXPECT_TRUE(encoded_data.contents_equal(reencoded_data));
}
