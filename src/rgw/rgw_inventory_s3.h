// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <map>
#include <set>
#include <string>
#include <string_view>

#include "include/encoding.h"
#include "rgw_xml.h"

class XMLObj;
namespace ceph { class Formatter; }

namespace rgw::inventory {

// Destination.S3BucketDestination
struct S3BucketDestination {
  std::string account_id;
  std::string bucket_arn;   // "arn:aws:s3:::bucket-name"
  std::string format{"Parquet"};
  std::string prefix;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(account_id, bl);
    encode(bucket_arn, bl);
    encode(format, bl);
    encode(prefix, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(account_id, bl);
    decode(bucket_arn, bl);
    decode(format, bl);
    decode(prefix, bl);
    DECODE_FINISH(bl);
  }
  void decode_xml(XMLObj* obj);
  void dump_xml(Formatter* f) const;

  // "arn:aws:s3:::name" -> "name". Only the ARN prefix is stripped, so a
  // "tenant:bucket" name survives intact for rgw_parse_url_bucket().
  std::string bucket_name() const {
    static constexpr std::string_view prefix = "arn:aws:s3:::";
    if (bucket_arn.compare(0, prefix.size(), prefix) == 0) {
      return bucket_arn.substr(prefix.size());
    }
    return bucket_arn;
  }
};
WRITE_CLASS_ENCODER(S3BucketDestination)

struct Destination {
  S3BucketDestination s3;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(s3, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(s3, bl);
    DECODE_FINISH(bl);
  }
  void decode_xml(XMLObj* obj);
  void dump_xml(Formatter* f) const;
};
WRITE_CLASS_ENCODER(Destination)

struct Schedule {
  std::string frequency;  // "Daily" | "Weekly"

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(frequency, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(frequency, bl);
    DECODE_FINISH(bl);
  }
  void decode_xml(XMLObj* obj);
  void dump_xml(Formatter* f) const;
};
WRITE_CLASS_ENCODER(Schedule)

// One InventoryConfiguration document
struct Configuration {
  std::string id;
  bool enabled{true};
  std::string included_object_versions;  // "All" | "Current"
  Schedule schedule;
  Destination destination;
  std::set<std::string> optional_fields;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(enabled, bl);
    encode(included_object_versions, bl);
    encode(schedule, bl);
    encode(destination, bl);
    encode(optional_fields, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(enabled, bl);
    decode(included_object_versions, bl);
    decode(schedule, bl);
    decode(destination, bl);
    decode(optional_fields, bl);
    DECODE_FINISH(bl);
  }
  void decode_xml(XMLObj* obj);
  void dump_xml(Formatter* f) const;

  // returns 0 if valid, -EINVAL otherwise; fills err on failure
  int validate(std::string* err) const;
};
WRITE_CLASS_ENCODER(Configuration)

// Bucket-level container stored in RGW_ATTR_INVENTORY
struct BucketConfigurations {
  static constexpr size_t MAX_CONFIGS = 1000;  // AWS limit
  std::map<std::string, Configuration> configs;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(configs, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(configs, bl);
    DECODE_FINISH(bl);
  }

  int add_or_replace(Configuration cfg) {
    if (configs.size() >= MAX_CONFIGS && !configs.count(cfg.id)) {
      return -ERANGE;
    }
    configs[cfg.id] = std::move(cfg);
    return 0;
  }
  bool remove(const std::string& id) { return configs.erase(id) > 0; }
  const Configuration* get(const std::string& id) const {
    auto it = configs.find(id);
    return it == configs.end() ? nullptr : &it->second;
  }
};
WRITE_CLASS_ENCODER(BucketConfigurations)

bool is_valid_optional_field(const std::string& f);

// Decode RGW_ATTR_INVENTORY from a bucket's attrs.
// Returns 0, -ENOENT if the attr is absent, or -EIO if undecodable.
int decode_bucket_inventory(const std::map<std::string, bufferlist>& attrs,
                            BucketConfigurations* out);

// True if the bucket has at least one inventory configuration.
bool bucket_has_inventory(const std::map<std::string, bufferlist>& attrs);

} // namespace rgw::inventory
