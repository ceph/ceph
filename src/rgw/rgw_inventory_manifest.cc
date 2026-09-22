// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_inventory_manifest.h"
#include "common/ceph_crypto.h"
#include "include/uuid.h"

#include <openssl/evp.h>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <ctime>

using namespace ceph::crypto;

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

static std::string hex_digest(const unsigned char* digest, size_t len)
{
  std::ostringstream oss;
  for (size_t i = 0; i < len; ++i)
    oss << std::hex << std::setw(2) << std::setfill('0')
        << static_cast<int>(digest[i]);
  return oss.str();
}

std::string md5_of_file(const std::string& path)
{
  std::ifstream f(path, std::ios::binary);
  if (!f) return "";

  MD5 hash;
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  char buf[65536];
  while (f.read(buf, sizeof(buf)) || f.gcount()) {
    hash.Update(reinterpret_cast<const unsigned char*>(buf), f.gcount());
  }
  unsigned char digest[CEPH_CRYPTO_MD5_DIGESTSIZE];
  hash.Final(digest);
  return hex_digest(digest, CEPH_CRYPTO_MD5_DIGESTSIZE);
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
  auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch()).count();
}

std::string inventory_run_timestamp()
{
  auto now = std::chrono::system_clock::now();
  std::time_t t = std::chrono::system_clock::to_time_t(now);
  std::tm tm_buf{};
  gmtime_r(&t, &tm_buf);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H-%MZ", &tm_buf);
  return std::string(buf);
}

std::string inventory_data_key(const std::string& prefix,
                               const std::string& source_bucket,
                               const std::string& config_id)
{
  uuid_d uuid;
  uuid.generate_random();
  std::string p = prefix.empty() ? "" : prefix + "/";
  return p + source_bucket + "/" + config_id + "/data/" +
         uuid.to_string() + ".parquet";
}

std::string inventory_manifest_key(const std::string& prefix,
                                   const std::string& source_bucket,
                                   const std::string& config_id,
                                   const std::string& timestamp)
{
  std::string p = prefix.empty() ? "" : prefix + "/";
  return p + source_bucket + "/" + config_id + "/" + timestamp + "/manifest.json";
}

std::string inventory_checksum_key(const std::string& manifest_key)
{
  static constexpr std::string_view kManifestJson = "manifest.json";
  std::string key = manifest_key;
  auto pos = key.rfind(kManifestJson);
  if (pos != std::string::npos) {
    key.replace(pos, kManifestJson.size(), "manifest.checksum");
  }
  return key;
}

std::string Manifest::to_json() const
{
  // Uses the caller-supplied creation_timestamp_ms rather than
  // recomputing now() here, so the manifest content is deterministic:
  // callers that hash this output (md5_of_string) get a checksum that
  // matches the exact bytes, and the value is testable/reproducible.
  std::ostringstream o;
  o << "{\n";
  o << "  \"sourceBucket\": \"" << source_bucket << "\",\n";
  o << "  \"destinationBucket\": \"" << destination_bucket_arn << "\",\n";
  o << "  \"version\": \"" << version << "\",\n";
  o << "  \"creationTimestamp\": \"" << creation_timestamp_ms << "\",\n";
  o << "  \"fileFormat\": \"" << file_format << "\",\n";
  o << "  \"fileSchema\": \"" << file_schema << "\",\n";
  o << "  \"files\": [\n";
  for (size_t i = 0; i < files.size(); ++i) {
    const auto& f = files[i];
    o << "    {\n";
    o << "      \"key\": \"" << f.key << "\",\n";
    o << "      \"size\": " << f.size << ",\n";
    o << "      \"MD5checksum\": \"" << f.md5checksum << "\"\n";
    o << "    }";
    if (i + 1 < files.size()) o << ",";
    o << "\n";
  }
  o << "  ]\n";
  o << "}";
  return o.str();
}

} // namespace rgw::inventory