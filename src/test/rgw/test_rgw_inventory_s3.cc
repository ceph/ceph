// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <catch2/catch_all.hpp>

#include "rgw/rgw_common.h"
#include "rgw/rgw_inventory_s3.h"
#include "rgw/rgw_xml.h"
#include "common/Formatter.h"
#include "common/XMLFormatter.h"

#include <sstream>

using namespace rgw::inventory;

namespace {

Configuration roundtrip_bl(const Configuration& in)
{
  bufferlist bl;
  in.encode(bl);
  auto it = bl.cbegin();
  Configuration out;
  out.decode(it);
  return out;
}

Configuration sample_config()
{
  Configuration cfg;
  cfg.id = "daily-inv";
  cfg.enabled = true;
  cfg.included_object_versions = "All";
  cfg.schedule.frequency = "Daily";
  cfg.destination.s3.bucket_arn = "arn:aws:s3:::dest-bucket";
  cfg.destination.s3.format = "Parquet";
  cfg.destination.s3.prefix = "inv";
  cfg.optional_fields = {"Size", "ETag", "StorageClass"};
  return cfg;
}

} // anonymous namespace

TEST_CASE("is_valid_optional_field accepts AWS-defined fields", "[inventory][s3]")
{
  for (const char* f : {"Size", "LastModifiedDate", "StorageClass", "ETag",
                        "IsMultipartUploaded", "ReplicationStatus",
                        "EncryptionStatus", "ObjectLockRetainUntilDate",
                        "ObjectLockMode", "ObjectLockLegalHoldStatus",
                        "IntelligentTieringAccessTier", "BucketKeyStatus"}) {
    CHECK(is_valid_optional_field(f));
  }
}

TEST_CASE("is_valid_optional_field rejects non-selectable fields", "[inventory][s3]")
{
  CHECK_FALSE(is_valid_optional_field("VersionId"));
  CHECK_FALSE(is_valid_optional_field("IsDeleteMarker"));
  CHECK_FALSE(is_valid_optional_field("ChecksumAlgorithm"));
  CHECK_FALSE(is_valid_optional_field(""));
  CHECK_FALSE(is_valid_optional_field("size"));
}

TEST_CASE("bucket_name extracts name from ARN", "[inventory][s3]")
{
  S3BucketDestination d;
  d.bucket_arn = "arn:aws:s3:::my-dest-bucket";
  CHECK(d.bucket_name() == "my-dest-bucket");
}

TEST_CASE("bucket_name keeps a tenant:bucket name intact", "[inventory][s3]")
{
  S3BucketDestination d;
  d.bucket_arn = "arn:aws:s3:::tenant:bucket";
  CHECK(d.bucket_name() == "tenant:bucket");
}

TEST_CASE("bucket_name passes a bare name through", "[inventory][s3]")
{
  S3BucketDestination d;
  d.bucket_arn = "just-a-name";
  CHECK(d.bucket_name() == "just-a-name");
}

TEST_CASE("validate accepts a well-formed config", "[inventory][s3][validate]")
{
  std::string err;
  CHECK(sample_config().validate(&err) == 0);
  CHECK(err.empty());
}

TEST_CASE("validate rejects malformed configs", "[inventory][s3][validate]")
{
  std::string err;

  auto c = sample_config(); c.id = "";
  CHECK(c.validate(&err) < 0);

  c = sample_config(); c.id = std::string(65, 'a');
  CHECK(c.validate(&err) < 0);

  c = sample_config(); c.id = "bad/id";
  CHECK(c.validate(&err) < 0);

  c = sample_config(); c.included_object_versions = "Everything";
  CHECK(c.validate(&err) < 0);

  c = sample_config(); c.schedule.frequency = "Hourly";
  CHECK(c.validate(&err) < 0);

  c = sample_config(); c.destination.s3.bucket_arn = "";
  CHECK(c.validate(&err) < 0);

  c = sample_config(); c.destination.s3.bucket_arn = "dest-bucket";
  CHECK(c.validate(&err) < 0);

  c = sample_config(); c.destination.s3.format = "CSV";
  CHECK(c.validate(&err) < 0);

  c = sample_config(); c.optional_fields.insert("NotARealField");
  CHECK(c.validate(&err) < 0);
  CHECK(err.find("NotARealField") != std::string::npos);
}

TEST_CASE("Configuration round-trips through bufferlist", "[inventory][s3][encode]")
{
  auto cfg = sample_config();
  auto out = roundtrip_bl(cfg);
  CHECK(out.id == cfg.id);
  CHECK(out.enabled == cfg.enabled);
  CHECK(out.included_object_versions == cfg.included_object_versions);
  CHECK(out.schedule.frequency == cfg.schedule.frequency);
  CHECK(out.destination.s3.bucket_arn == cfg.destination.s3.bucket_arn);
  CHECK(out.destination.s3.format == cfg.destination.s3.format);
  CHECK(out.destination.s3.prefix == cfg.destination.s3.prefix);
  CHECK(out.optional_fields == cfg.optional_fields);
}

TEST_CASE("BucketConfigurations add, get, remove", "[inventory][s3][bucketconfig]")
{
  BucketConfigurations bcs;
  CHECK(bcs.add_or_replace(sample_config()) == 0);
  REQUIRE(bcs.get("daily-inv") != nullptr);
  CHECK(bcs.get("missing") == nullptr);
  CHECK(bcs.remove("daily-inv"));
  CHECK_FALSE(bcs.remove("daily-inv"));
}

TEST_CASE("BucketConfigurations enforces MAX_CONFIGS for new ids", "[inventory][s3][bucketconfig]")
{
  BucketConfigurations bcs;
  for (size_t i = 0; i < BucketConfigurations::MAX_CONFIGS; ++i) {
    auto c = sample_config();
    c.id = "cfg-" + std::to_string(i);
    REQUIRE(bcs.add_or_replace(c) == 0);
  }
  auto overflow = sample_config();
  overflow.id = "one-too-many";
  CHECK(bcs.add_or_replace(overflow) < 0);

  auto replace = sample_config();
  replace.id = "cfg-0";
  replace.schedule.frequency = "Weekly";
  CHECK(bcs.add_or_replace(replace) == 0);
  CHECK(bcs.get("cfg-0")->schedule.frequency == "Weekly");
}

TEST_CASE("bucket_has_inventory reflects attr content", "[inventory][s3]")
{
  std::map<std::string, bufferlist> attrs;
  CHECK_FALSE(bucket_has_inventory(attrs));

  BucketConfigurations empty;
  bufferlist bl;
  empty.encode(bl);
  attrs[RGW_ATTR_INVENTORY] = bl;
  CHECK_FALSE(bucket_has_inventory(attrs));

  BucketConfigurations one;
  one.add_or_replace(sample_config());
  bufferlist bl2;
  one.encode(bl2);
  attrs[RGW_ATTR_INVENTORY] = bl2;
  CHECK(bucket_has_inventory(attrs));

  BucketConfigurations decoded;
  CHECK(decode_bucket_inventory(attrs, &decoded) == 0);
  CHECK(decoded.configs.size() == 1);
}

TEST_CASE("Configuration XML round-trips", "[inventory][s3][xml]")
{
  auto cfg = sample_config();

  ceph::XMLFormatter xf;
  xf.open_object_section("InventoryConfiguration");
  cfg.dump_xml(&xf);
  xf.close_section();
  std::ostringstream oss;
  xf.flush(oss);
  const std::string xml = oss.str();
  REQUIRE_FALSE(xml.empty());

  RGWXMLParser parser;
  REQUIRE(parser.init());
  REQUIRE(parser.parse(xml.c_str(), xml.length(), 1));

  Configuration parsed;
  REQUIRE_NOTHROW(
      RGWXMLDecoder::decode_xml("InventoryConfiguration", parsed, &parser));
  CHECK(parsed.id == cfg.id);
  CHECK(parsed.enabled == cfg.enabled);
  CHECK(parsed.included_object_versions == cfg.included_object_versions);
  CHECK(parsed.schedule.frequency == cfg.schedule.frequency);
  CHECK(parsed.destination.s3.bucket_arn == cfg.destination.s3.bucket_arn);
  CHECK(parsed.optional_fields == cfg.optional_fields);
}
