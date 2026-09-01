// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_inventory_s3.h"
#include "common/Formatter.h"
#include <errno.h>

namespace rgw::inventory {

bool is_valid_optional_field(const std::string& f)
{
  static const std::set<std::string> valid = {
    "Size", "LastModifiedDate", "StorageClass", "ETag",
    "IsMultipartUploaded", "ReplicationStatus", "EncryptionStatus",
    "ObjectLockRetainUntilDate", "ObjectLockMode",
    "ObjectLockLegalHoldStatus", "IntelligentTieringAccessTier",
    "BucketKeyStatus"
  };
  return valid.count(f) > 0;
}

void S3BucketDestination::decode_xml(XMLObj* obj)
{
  RGWXMLDecoder::decode_xml("AccountId", account_id, obj);
  RGWXMLDecoder::decode_xml("Bucket", bucket_arn, obj, true);
  RGWXMLDecoder::decode_xml("Format", format, obj, true);
  RGWXMLDecoder::decode_xml("Prefix", prefix, obj);
}

void S3BucketDestination::dump_xml(Formatter* f) const
{
  if (!account_id.empty()) {
    encode_xml("AccountId", account_id, f);
  }
  encode_xml("Bucket", bucket_arn, f);
  encode_xml("Format", format, f);
  if (!prefix.empty()) {
    encode_xml("Prefix", prefix, f);
  }
}

void Destination::decode_xml(XMLObj* obj)
{
  RGWXMLDecoder::decode_xml("S3BucketDestination", s3, obj, true);
}

void Destination::dump_xml(Formatter* f) const
{
  f->open_object_section("S3BucketDestination");
  s3.dump_xml(f);
  f->close_section();
}

void Schedule::decode_xml(XMLObj* obj)
{
  RGWXMLDecoder::decode_xml("Frequency", frequency, obj, true);
}

void Schedule::dump_xml(Formatter* f) const
{
  encode_xml("Frequency", frequency, f);
}

void Configuration::decode_xml(XMLObj* obj)
{
  RGWXMLDecoder::decode_xml("Id", id, obj, true);
  RGWXMLDecoder::decode_xml("IsEnabled", enabled, obj, true);
  RGWXMLDecoder::decode_xml("IncludedObjectVersions",
                            included_object_versions, obj, true);
  RGWXMLDecoder::decode_xml("Schedule", schedule, obj, true);
  RGWXMLDecoder::decode_xml("Destination", destination, obj, true);

  optional_fields.clear();
  XMLObjIter iter = obj->find("OptionalFields");
  XMLObj* fields = iter.get_next();
  if (fields) {
    XMLObjIter fi = fields->find("Field");
    XMLObj* field = fi.get_next();
    while (field) {
      optional_fields.insert(field->get_data());
      field = fi.get_next();
    }
  }
}

void Configuration::dump_xml(Formatter* f) const
{
  encode_xml("Id", id, f);
  encode_xml("IsEnabled", enabled, f);
  encode_xml("IncludedObjectVersions", included_object_versions, f);
  f->open_object_section("Schedule");
  schedule.dump_xml(f);
  f->close_section();
  f->open_object_section("Destination");
  destination.dump_xml(f);
  f->close_section();
  if (!optional_fields.empty()) {
    f->open_object_section("OptionalFields");
    for (const auto& field : optional_fields) {
      encode_xml("Field", field, f);
    }
    f->close_section();
  }
}

int Configuration::validate(std::string* err) const
{
  if (id.empty() || id.size() > 64) {
    if (err) *err = "Id must be 1-64 characters";
    return -EINVAL;
  }
  if (included_object_versions != "All" &&
      included_object_versions != "Current") {
    if (err) *err = "IncludedObjectVersions must be All or Current";
    return -EINVAL;
  }
  if (schedule.frequency != "Daily" && schedule.frequency != "Weekly") {
    if (err) *err = "Schedule.Frequency must be Daily or Weekly";
    return -EINVAL;
  }
  if (destination.s3.bucket_arn.empty()) {
    if (err) *err = "Destination bucket is required";
    return -EINVAL;
  }
  if (destination.s3.format != "Parquet") {
    if (err) *err = "Only Parquet format is supported";
    return -EINVAL;
  }
  for (const auto& field : optional_fields) {
    if (!is_valid_optional_field(field)) {
      if (err) *err = "Invalid optional field: " + field;
      return -EINVAL;
    }
  }
  return 0;
}

} // namespace rgw::inventory
