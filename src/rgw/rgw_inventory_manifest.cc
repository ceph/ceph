// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_inventory_manifest.h"

#include <array>
#include <ctime>
#include <sstream>
#include <string_view>

#include <openssl/evp.h>

#include "common/JSONFormatter.h"
#include "common/ceph_crypto.h"
#include "include/uuid.h"
#include "rgw_hex.h"

using ceph::crypto::MD5;

namespace rgw::inventory {

std::string build_file_schema(const FieldSelection& sel)
{
  std::string s = "message s3.inventory {";
  s += "  required binary bucket (STRING);";
  s += "  required binary key (STRING);";
  if (sel.version_id)
    s += "  optional binary version_id (STRING);";
  if (sel.is_latest)
    s += "  optional boolean is_latest;";
  if (sel.is_delete_marker)
    s += "  optional boolean is_delete_marker;";
  if (sel.size)
    s += "  optional int64 size;";
  if (sel.last_modified)
    s += "  optional int64 last_modified_date (TIMESTAMP(MILLIS,true));";
  if (sel.etag)
    s += "  optional binary e_tag (STRING);";
  if (sel.storage_class)
    s += "  optional binary storage_class (STRING);";
  if (sel.is_multipart_uploaded)
    s += "  optional boolean is_multipart_uploaded;";
  if (sel.replication_status)
    s += "  optional binary replication_status (STRING);";
  if (sel.encryption_status)
    s += "  optional binary encryption_status (STRING);";
  if (sel.object_lock_retain_until_date)
    s += "  optional int64 object_lock_retain_until_date (TIMESTAMP(MILLIS,true));";
  if (sel.object_lock_mode)
    s += "  optional binary object_lock_mode (STRING);";
  if (sel.object_lock_legal_hold_status)
    s += "  optional binary object_lock_legal_hold_status (STRING);";
  if (sel.intelligent_tiering_access_tier)
    s += "  optional binary intelligent_tiering_access_tier (STRING);";
  if (sel.bucket_key_status)
    s += "  optional binary bucket_key_status (STRING);";
  s += "}";
  return s;
}

std::string md5_hex(std::string_view s)
{
  MD5 hash;
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  hash.Update(reinterpret_cast<const unsigned char*>(s.data()), s.size());
  std::array<unsigned char, CEPH_CRYPTO_MD5_DIGESTSIZE> digest;
  hash.Final(digest.data());
  return buf_to_hex(digest).data();
}

std::string inventory_config_prefix(const std::string& prefix,
                                    const std::string& source,
                                    const std::string& config_id)
{
  return (prefix.empty() ? "" : prefix + "/") + source + "/" + config_id + "/";
}

std::string inventory_data_key(const std::string& config_prefix)
{
  uuid_d uuid;
  uuid.generate_random();
  return config_prefix + "data/" + uuid.to_string() + ".parquet";
}

std::string inventory_run_prefix(const std::string& config_prefix, time_t now)
{
  std::tm tm_buf{};
  gmtime_r(&now, &tm_buf);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%dT00-00Z/", &tm_buf);
  return config_prefix + buf;
}

void Manifest::dump(ceph::Formatter* f) const
{
  f->dump_string("sourceBucket", source_bucket);
  f->dump_string("destinationBucket", destination_bucket_arn);
  f->dump_string("version", version);
  // AWS writes the timestamp as a string
  f->dump_string("creationTimestamp", std::to_string(creation_timestamp_ms));
  f->dump_string("fileFormat", to_string(file_format));
  f->dump_string("fileSchema", file_schema);
  f->open_array_section("files");
  for (const auto& file : files) {
    f->open_object_section("");
    f->dump_string("key", file.key);
    f->dump_unsigned("size", file.size);
    f->dump_string("MD5checksum", file.md5checksum);
    f->close_section();
  }
  f->close_section();
}

std::string Manifest::to_json() const
{
  ceph::JSONFormatter f(true);
  f.open_object_section("");
  dump(&f);
  f.close_section();
  std::ostringstream oss;
  f.flush(oss);
  return oss.str();
}

} // namespace rgw::inventory
