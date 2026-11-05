// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_inventory_manifest.h"

#include <chrono>
#include <cstdio>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string_view>

#include <openssl/evp.h>

#include "common/ceph_crypto.h"
#include "include/uuid.h"

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

std::string json_escape(std::string_view s)
{
  std::string out;
  out.reserve(s.size());
  for (const char ch : s) {
    const auto c = static_cast<unsigned char>(ch);
    switch (c) {
    case '"':  out += "\\\""; break;
    case '\\': out += "\\\\"; break;
    case '\b': out += "\\b"; break;
    case '\f': out += "\\f"; break;
    case '\n': out += "\\n"; break;
    case '\r': out += "\\r"; break;
    case '\t': out += "\\t"; break;
    default:
      if (c < 0x20) {
        char buf[8];
        std::snprintf(buf, sizeof(buf), "\\u%04x", c);
        out += buf;
      } else {
        out += ch;  // UTF-8 multi-byte sequences pass through unchanged
      }
    }
  }
  return out;
}

static std::string hex_digest(const unsigned char* digest, size_t len)
{
  std::ostringstream oss;
  for (size_t i = 0; i < len; ++i) {
    oss << std::hex << std::setw(2) << std::setfill('0')
        << static_cast<int>(digest[i]);
  }
  return oss.str();
}

std::string md5_of_string(const std::string& s)
{
  MD5 hash;
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  hash.Update(reinterpret_cast<const unsigned char*>(s.data()), s.size());
  unsigned char digest[CEPH_CRYPTO_MD5_DIGESTSIZE];
  hash.Final(digest);
  return hex_digest(digest, CEPH_CRYPTO_MD5_DIGESTSIZE);
}

int64_t current_time_millis()
{
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string inventory_run_folder(time_t now)
{
  std::tm tm_buf{};
  gmtime_r(&now, &tm_buf);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%dT00-00Z", &tm_buf);
  return std::string(buf);
}

std::string inventory_data_key(const std::string& prefix,
                               const std::string& source_bucket,
                               const std::string& config_id)
{
  uuid_d uuid;
  uuid.generate_random();
  const std::string p = prefix.empty() ? "" : prefix + "/";
  return p + source_bucket + "/" + config_id + "/data/" +
         uuid.to_string() + ".parquet";
}

std::string inventory_manifest_key(const std::string& prefix,
                                   const std::string& source_bucket,
                                   const std::string& config_id,
                                   const std::string& folder)
{
  const std::string p = prefix.empty() ? "" : prefix + "/";
  return p + source_bucket + "/" + config_id + "/" + folder + "/manifest.json";
}

std::string inventory_checksum_key(const std::string& manifest_key)
{
  static constexpr std::string_view kManifestJson = "manifest.json";
  std::string key = manifest_key;
  const auto pos = key.rfind(kManifestJson);
  if (pos != std::string::npos) {
    key.replace(pos, kManifestJson.size(), "manifest.checksum");
  }
  return key;
}

std::string Manifest::to_json() const
{
  std::ostringstream o;
  o << "{\n";
  o << "  \"sourceBucket\": \"" << json_escape(source_bucket) << "\",\n";
  o << "  \"destinationBucket\": \"" << json_escape(destination_bucket_arn) << "\",\n";
  o << "  \"version\": \"" << json_escape(version) << "\",\n";
  o << "  \"creationTimestamp\": \"" << creation_timestamp_ms << "\",\n";
  o << "  \"fileFormat\": \"" << json_escape(file_format) << "\",\n";
  o << "  \"fileSchema\": \"" << json_escape(file_schema) << "\",\n";
  o << "  \"files\": [\n";
  for (size_t i = 0; i < files.size(); ++i) {
    const auto& f = files[i];
    o << "    {\n";
    o << "      \"key\": \"" << json_escape(f.key) << "\",\n";
    o << "      \"size\": " << f.size << ",\n";
    o << "      \"MD5checksum\": \"" << json_escape(f.md5checksum) << "\"\n";
    o << "    }";
    if (i + 1 < files.size()) o << ",";
    o << "\n";
  }
  o << "  ]\n";
  o << "}";
  return o.str();
}

} // namespace rgw::inventory
