// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_inventory.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <system_error>
#include <vector>

#include <openssl/evp.h>

#include "include/ceph_hash.h"
#include "include/uuid.h"
#include "common/ceph_crypto.h"
#include "rgw_acl.h"
#include "rgw_arn.h"
#include "rgw_auth.h"
#include "rgw_bucket.h"
#include "rgw_common.h"
#include "rgw_iam_policy.h"
#include "rgw_inventory_manifest.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace fs = std::filesystem;
using ceph::crypto::MD5;

namespace rgw::inventory {

namespace {

constexpr int kListChunk = 1000;
constexpr size_t kUploadChunk = 4 * 1024 * 1024;
constexpr auto kStaleTempAge = std::chrono::hours(48);

int64_t to_millis(ceph::real_time t)
{
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      t.time_since_epoch()).count();
}

std::string to_hex(const unsigned char* d, size_t n)
{
  static constexpr char digits[] = "0123456789abcdef";
  std::string out;
  out.reserve(n * 2);
  for (size_t i = 0; i < n; ++i) {
    out.push_back(digits[d[i] >> 4]);
    out.push_back(digits[d[i] & 0xf]);
  }
  return out;
}

// Key path under which this configuration's output lands, used as the
// policy resource: "<prefix>/<source>/<id>/" (prefix omitted if empty).
std::string key_prefix(const Configuration& cfg, const std::string& source)
{
  const std::string& p = cfg.destination.s3.prefix;
  return (p.empty() ? "" : p + "/") + source + "/" + cfg.id + "/";
}

InventoryEntry entry_from_listing(const std::string& bucket_name,
                                  const rgw_bucket_dir_entry& e,
                                  const FieldSelection& sel)
{
  InventoryEntry ie;
  ie.bucket = bucket_name;
  ie.key = e.key.name;
  if (sel.version_id && !e.key.instance.empty()) {
    ie.version_id = e.key.instance;
  }
  if (sel.is_latest) {
    ie.is_latest = e.is_current();
  }
  if (sel.is_delete_marker) {
    ie.is_delete_marker = e.is_delete_marker();
  }
  if (e.is_delete_marker()) {
    return ie;  // delete markers carry no size/etag/storage class
  }
  if (sel.size) {
    ie.size = static_cast<int64_t>(e.meta.accounted_size);
  }
  if (sel.last_modified) {
    ie.last_modified_ms = to_millis(e.meta.mtime);
  }
  if (sel.etag) {
    ie.etag = e.meta.etag;
  }
  if (sel.storage_class) {
    ie.storage_class = e.meta.storage_class.empty()
                           ? std::string("STANDARD")
                           : e.meta.storage_class;
  }
  if (sel.is_multipart_uploaded) {
    // multipart ETags carry a "-<parts>" suffix
    ie.is_multipart_uploaded = e.meta.etag.find('-') != std::string::npos;
  }
  // Replication, encryption, object-lock, tiering and bucket-key status
  // live in per-object attrs, not the bucket index entry. Left null in v1
  // rather than issuing one HEAD per object; the columns stay in the schema.
  return ie;
}

int list_and_write(const DoutPrefixProvider* dpp,
                   rgw::sal::Bucket* bucket,
                   const Configuration& cfg,
                   const FieldSelection& sel,
                   const std::string& path,
                   uint64_t* rows,
                   optional_yield y)
{
  try {
    ParquetInventoryWriter writer(path, sel);

    rgw::sal::Bucket::ListParams params;
    params.list_versions = (cfg.included_object_versions == "All");
    params.allow_unordered = false;  // inventory output is key-ordered

    rgw::sal::Bucket::ListResults results;
    do {
      results.objs.clear();
      int r = bucket->list(dpp, params, kListChunk, results, y);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "inventory: listing " << bucket->get_key()
                          << " failed: " << r << dendl;
        return r;
      }
      for (const auto& e : results.objs) {
        writer.append(entry_from_listing(bucket->get_name(), e, sel));
        ++*rows;
      }
      params.marker = results.next_marker;
    } while (results.is_truncated);

    writer.close();
  } catch (const std::exception& ex) {
    ldpp_dout(dpp, 0) << "inventory: parquet writer error: " << ex.what()
                      << dendl;
    return -EIO;
  }
  return 0;
}

// Write a complete object through the same SAL atomic-writer path a normal
// PUT uses (rgw_op.cc), so it works on every SAL backend.
int put_object(const DoutPrefixProvider* dpp, optional_yield y,
               rgw::sal::Driver* driver, rgw::sal::Bucket* dest,
               const std::string& key, std::istream& in,
               const std::string& content_type,
               const std::string& unique_tag,
               uint64_t* size_out, std::string* md5_out)
{
  std::unique_ptr<rgw::sal::Object> obj = dest->get_object(rgw_obj_key(key));

  ACLOwner owner;
  owner.id = dest->get_owner();

  std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
      dpp, y, obj.get(), owner, &dest->get_placement_rule(),
      /*olh_epoch=*/0, unique_tag);

  int r = writer->prepare(y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "inventory: prepare failed for " << key << ": "
                      << r << dendl;
    return r;
  }

  MD5 hash;
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  uint64_t ofs = 0;
  std::vector<char> buf(kUploadChunk);
  while (in) {
    in.read(buf.data(), buf.size());
    const std::streamsize n = in.gcount();
    if (n <= 0) {
      break;
    }
    hash.Update(reinterpret_cast<const unsigned char*>(buf.data()), n);
    bufferlist bl;
    bl.append(buf.data(), n);
    r = writer->process(std::move(bl), ofs);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "inventory: write failed for " << key << ": "
                        << r << dendl;
      return r;
    }
    ofs += n;
  }
  if (in.bad()) {
    return -EIO;
  }
  r = writer->process({}, ofs);  // empty buffer flushes
  if (r < 0) {
    return r;
  }

  unsigned char digest[CEPH_CRYPTO_MD5_DIGESTSIZE];
  hash.Final(digest);
  const std::string etag = to_hex(digest, CEPH_CRYPTO_MD5_DIGESTSIZE);

  std::map<std::string, bufferlist> attrs;
  {
    RGWAccessControlPolicy policy;
    policy.create_default(owner.id, owner.display_name);
    bufferlist acl_bl;
    policy.encode(acl_bl);
    attrs[RGW_ATTR_ACL] = std::move(acl_bl);
  }
  {
    bufferlist ct;
    ct.append(content_type);
    attrs[RGW_ATTR_CONTENT_TYPE] = std::move(ct);
  }

  const req_context rctx{dpp, y, nullptr};
  r = writer->complete(ofs, etag, nullptr, ceph::real_time(), attrs,
                       std::nullopt, ceph::real_time(), nullptr, nullptr,
                       nullptr, nullptr, nullptr, rctx,
                       rgw::sal::FLAG_LOG_OP);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "inventory: complete failed for " << key << ": "
                      << r << dendl;
    return r;
  }
  if (size_out) *size_out = ofs;
  if (md5_out) *md5_out = etag;
  return 0;
}

// 0 = exists, -ENOENT = missing, other = error (caller skips this run)
int object_exists(const DoutPrefixProvider* dpp, rgw::sal::Bucket* dest,
                  const std::string& key, optional_yield y)
{
  std::unique_ptr<rgw::sal::Object> obj = dest->get_object(rgw_obj_key(key));
  std::unique_ptr<rgw::sal::Object::ReadOp> rop = obj->get_read_op();
  int r = rop->prepare(y, dpp);
  return r < 0 ? r : 0;
}

int run_config(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver,
               rgw::sal::Bucket* bucket, const Configuration& cfg,
               optional_yield y)
{
  CephContext* cct = dpp->get_cct();
  const time_t now = ceph::real_clock::to_time_t(ceph::real_clock::now());
  const std::string& src = bucket->get_name();
  const std::string& prefix = cfg.destination.s3.prefix;
  const std::string manifest_key =
      inventory_manifest_key(prefix, src, cfg.id, inventory_run_folder(now));

  std::unique_ptr<rgw::sal::Bucket> dest;
  int r = load_destination_bucket(dpp, driver, bucket->get_tenant(), cfg,
                                  &dest, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "inventory: destination for " << src << "/"
                      << cfg.id << " unavailable: " << r << dendl;
    return r;
  }

  // manifest.json is uploaded last, so its presence means today's run
  // completed; nothing else needs to be stored.
  r = object_exists(dpp, dest.get(), manifest_key, y);
  if (r == 0) {
    ldpp_dout(dpp, 20) << "inventory: " << manifest_key
                       << " already delivered" << dendl;
    return 0;
  }
  if (r != -ENOENT) {
    return r;
  }

  // Re-checked at delivery so later policy changes take effect.
  std::string err;
  r = verify_destination_policy(dpp, cct, bucket, dest.get(), cfg, &err);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "inventory: " << err << dendl;
    return r;
  }

  const std::string tmp_dir =
      cct->_conf.get_val<std::string>("rgw_inventory_temp_dir");
  std::error_code ec;
  fs::create_directories(tmp_dir, ec);
  if (ec) {
    ldpp_dout(dpp, 0) << "inventory: cannot create " << tmp_dir << ": "
                      << ec.message() << dendl;
    return -EIO;
  }
  uuid_d run_id;
  run_id.generate_random();
  const std::string tmp_path = tmp_dir + "/" + run_id.to_string() + ".parquet";
  struct TempGuard {
    std::string path;
    ~TempGuard() { std::error_code e; fs::remove(path, e); }
  } guard{tmp_path};

  const FieldSelection sel = field_selection_from_config(cfg);
  uint64_t rows = 0;
  r = list_and_write(dpp, bucket, cfg, sel, tmp_path, &rows, y);
  if (r < 0) {
    return r;
  }

  // 1) parquet data file
  const std::string data_key = inventory_data_key(prefix, src, cfg.id);
  uint64_t parquet_size = 0;
  std::string parquet_md5;
  {
    std::ifstream pf(tmp_path, std::ios::binary);
    if (!pf) {
      return -EIO;
    }
    r = put_object(dpp, y, driver, dest.get(), data_key, pf,
                   "application/octet-stream", run_id.to_string(),
                   &parquet_size, &parquet_md5);
    if (r < 0) {
      return r;
    }
  }

  Manifest m;
  m.source_bucket = src;
  m.destination_bucket_arn = cfg.destination.s3.bucket_arn;
  m.creation_timestamp_ms = current_time_millis();
  m.file_schema = build_file_schema(sel);
  m.files.push_back({data_key, parquet_size, parquet_md5});
  const std::string json = m.to_json();

  // 2) checksum, then 3) manifest last: manifest.json is the commit marker
  std::istringstream cs(md5_of_string(json));
  r = put_object(dpp, y, driver, dest.get(),
                 inventory_checksum_key(manifest_key), cs, "text/plain",
                 run_id.to_string(), nullptr, nullptr);
  if (r < 0) {
    return r;
  }
  std::istringstream js(json);
  r = put_object(dpp, y, driver, dest.get(), manifest_key, js,
                 "application/json", run_id.to_string(), nullptr, nullptr);
  if (r < 0) {
    return r;
  }

  ldpp_dout(dpp, 5) << "inventory: " << src << "/" << cfg.id << " -> "
                    << dest->get_name() << " (" << rows << " rows)" << dendl;
  return 0;
}

} // anonymous namespace

FieldSelection field_selection_from_config(const Configuration& cfg)
{
  FieldSelection s;
  s.size = s.last_modified = s.etag = s.storage_class = false;
  const bool all = (cfg.included_object_versions == "All");
  s.version_id = s.is_latest = s.is_delete_marker = all;
  for (const auto& f : cfg.optional_fields) {
    if (f == "Size") s.size = true;
    else if (f == "LastModifiedDate") s.last_modified = true;
    else if (f == "ETag") s.etag = true;
    else if (f == "StorageClass") s.storage_class = true;
    else if (f == "IsMultipartUploaded") s.is_multipart_uploaded = true;
    else if (f == "ReplicationStatus") s.replication_status = true;
    else if (f == "EncryptionStatus") s.encryption_status = true;
    else if (f == "ObjectLockRetainUntilDate") s.object_lock_retain_until_date = true;
    else if (f == "ObjectLockMode") s.object_lock_mode = true;
    else if (f == "ObjectLockLegalHoldStatus") s.object_lock_legal_hold_status = true;
    else if (f == "IntelligentTieringAccessTier") s.intelligent_tiering_access_tier = true;
    else if (f == "BucketKeyStatus") s.bucket_key_status = true;
  }
  return s;
}

bool is_due(const Configuration& cfg, const std::string& bucket_key,
            time_t now)
{
  if (cfg.schedule.frequency == "Daily") {
    return true;
  }
  if (cfg.schedule.frequency == "Weekly") {
    std::tm tm_buf{};
    gmtime_r(&now, &tm_buf);
    const unsigned h = ceph_str_hash_linux(bucket_key.c_str(),
                                           bucket_key.size());
    return static_cast<int>(h % 7) == tm_buf.tm_wday;
  }
  return false;
}

int load_destination_bucket(const DoutPrefixProvider* dpp,
                            rgw::sal::Driver* driver,
                            const std::string& source_tenant,
                            const Configuration& cfg,
                            std::unique_ptr<rgw::sal::Bucket>* dest,
                            optional_yield y)
{
  std::string tenant;
  std::string name;
  int r = rgw_parse_url_bucket(cfg.destination.s3.bucket_name(),
                               source_tenant, tenant, name);
  if (r < 0) {
    return r;
  }
  return driver->load_bucket(dpp, rgw_bucket(tenant, name), dest, y);
}

int verify_destination_policy(const DoutPrefixProvider* dpp,
                              CephContext* cct,
                              rgw::sal::Bucket* source,
                              rgw::sal::Bucket* dest,
                              const Configuration& cfg,
                              std::string* err_message)
{
  const auto& dest_id = dest->get_key();
  const auto& dest_attrs = dest->get_attrs();
  const auto policy_it = dest_attrs.find(RGW_ATTR_IAM_POLICY);
  if (policy_it == dest_attrs.end()) {
    if (err_message) {
      *err_message = "Destination bucket '" + dest->get_name() +
                     "' must have a bucket policy allowing inventory delivery";
    }
    return -EACCES;
  }
  try {
    const rgw::IAM::Policy policy{cct, &dest_id.tenant,
                                  policy_it->second.to_str(), false};
    rgw::auth::ServiceIdentity ident(service_principal);
    rgw::IAM::Environment env;
    env.emplace("aws:SourceArn", rgw::ARN(source->get_key()).to_string());
    env.emplace("aws:SourceAccount", to_string(source->get_owner()));
    const rgw::ARN resource(dest_id, key_prefix(cfg, source->get_name()));
    boost::optional<rgw::auth::Principal> principal;
    if (policy.eval(dpp, env, ident, rgw::IAM::s3PutObject, resource,
                    principal) != rgw::IAM::Effect::Allow) {
      if (err_message) {
        *err_message = "Destination bucket '" + dest->get_name() +
                       "' policy does not allow inventory delivery";
      }
      return -EACCES;
    }
  } catch (const rgw::IAM::PolicyParseException&) {
    if (err_message) {
      *err_message = "Destination bucket '" + dest->get_name() +
                     "' has an invalid policy";
    }
    return -EACCES;
  }
  return 0;
}

int process_bucket(const DoutPrefixProvider* dpp,
                   rgw::sal::Driver* driver,
                   rgw::sal::Bucket* bucket,
                   optional_yield y)
{
  if (!dpp->get_cct()->_conf.get_val<bool>("rgw_lc_inventory_enable")) {
    return 0;
  }
  BucketConfigurations all;
  int r = decode_bucket_inventory(bucket->get_attrs(), &all);
  if (r == -ENOENT) {
    return 0;
  }
  if (r < 0) {
    ldpp_dout(dpp, 0) << "inventory: cannot decode config on "
                      << bucket->get_key() << dendl;
    return r;
  }

  const time_t now = ceph::real_clock::to_time_t(ceph::real_clock::now());
  const std::string bucket_key =
      bucket->get_tenant() + ":" + bucket->get_name();
  int first_err = 0;
  for (const auto& [id, cfg] : all.configs) {
    if (!cfg.enabled || !is_due(cfg, bucket_key, now)) {
      continue;
    }
    r = run_config(dpp, driver, bucket, cfg, y);
    if (r < 0 && first_err == 0) {
      first_err = r;
    }
  }
  return first_err;
}

void cleanup_stale_temp_files(const DoutPrefixProvider* dpp,
                              CephContext* cct)
{
  const std::string dir =
      cct->_conf.get_val<std::string>("rgw_inventory_temp_dir");
  std::error_code ec;
  if (!fs::exists(dir, ec)) {
    return;
  }
  const auto now = fs::file_time_type::clock::now();
  for (const auto& entry : fs::directory_iterator(dir, ec)) {
    if (entry.path().extension() != ".parquet") {
      continue;
    }
    std::error_code e;
    const auto mtime = fs::last_write_time(entry.path(), e);
    if (!e && now - mtime > kStaleTempAge) {
      fs::remove(entry.path(), e);
      ldpp_dout(dpp, 5) << "inventory: removed stale temp file "
                        << entry.path() << dendl;
    }
  }
}

} // namespace rgw::inventory
