// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_inventory.h"

#include <chrono>
#include <tuple>

#include <arrow/util/io_util.h>

#include "include/ceph_hash.h"
#include "include/uuid.h"
#include "rgw_acl.h"
#include "rgw_arn.h"
#include "rgw_auth.h"
#include "rgw_bucket.h"
#include "rgw_common.h"
#include "rgw_iam_policy.h"
#include "rgw_inventory_manifest.h"
#include "rgw_output_stream.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::inventory {

static constexpr int kListChunk = 1000;

// see verify_destination_policy() in rgw_inventory.h
static const std::string service_principal = "s3.amazonaws.com";

static int64_t to_millis(ceph::real_time t)
{
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      t.time_since_epoch()).count();
}

// RGWOutputStream reports SAL errors as IOErrors carrying the errno.
static int status_to_errno(const arrow::Status& s)
{
  const int e = arrow::internal::ErrnoFromStatus(s);
  return e > 0 ? -e : -EIO;
}

// the source bucket's path segment in the destination: "tenant:bucket" for a
// bucket in a non-default tenant, so that same-named buckets of different
// tenants don't share a folder
static std::string report_source(const rgw_bucket& b)
{
  return b.tenant.empty() ? b.name : b.tenant + ":" + b.name;
}

static std::string config_prefix(const Configuration& cfg,
                                 const rgw_bucket& source)
{
  return inventory_config_prefix(cfg.destination.s3.prefix,
                                 report_source(source), cfg.id);
}

// full-control ACL for the destination bucket owner, plus content type
static rgw::sal::Attrs object_attrs(rgw::sal::Bucket* dest,
                                    const std::string& content_type)
{
  rgw::sal::Attrs attrs;
  {
    RGWAccessControlPolicy policy;
    policy.create_default(dest->get_owner(), "");
    bufferlist bl;
    policy.encode(bl);
    attrs[RGW_ATTR_ACL] = std::move(bl);
  }
  {
    bufferlist bl;
    bl.append(content_type);
    attrs[RGW_ATTR_CONTENT_TYPE] = std::move(bl);
  }
  return attrs;
}

static InventoryEntry entry_from_listing(const std::string& bucket_name,
                                         const rgw_bucket_dir_entry& e,
                                         const FieldSelection& sel)
{
  InventoryEntry ie;
  ie.bucket = bucket_name;
  ie.key = e.key.name;
  if (sel.version_id) {
    ie.version_id = e.key.instance.empty() ? "null" : e.key.instance;
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
  return ie;
}

namespace {

// Produces one report for one configuration of a source bucket: the
// parquet data file, then manifest.checksum, then manifest.json. The
// manifest is written last, so its presence means the day's report is
// complete.
class ReportRun {
public:
  ReportRun(const DoutPrefixProvider* dpp, optional_yield y,
            rgw::sal::Driver* driver, rgw::sal::Bucket* source,
            const Configuration& cfg, ceph::real_time now);

  int run();

private:
  int manifest_exists(const std::string& key);
  int open_object(const std::string& key, const std::string& content_type,
                  std::shared_ptr<RGWOutputStream>* out);
  int write_object(const std::string& key, const std::string& data,
                   const std::string& content_type);
  int write_data(const FieldSelection& sel,
                 const std::shared_ptr<RGWOutputStream>& out);

  const DoutPrefixProvider* dpp;
  optional_yield y;
  rgw::sal::Driver* driver;
  rgw::sal::Bucket* source;
  const Configuration& cfg;
  const ceph::real_time now;
  std::unique_ptr<rgw::sal::Bucket> dest;
  std::string tag;  // unique per run, for the atomic writes
};

} // anonymous namespace

ReportRun::ReportRun(const DoutPrefixProvider* dpp, optional_yield y,
                     rgw::sal::Driver* driver, rgw::sal::Bucket* source,
                     const Configuration& cfg, ceph::real_time now)
  : dpp(dpp), y(y), driver(driver), source(source), cfg(cfg), now(now)
{
  uuid_d run_id;
  run_id.generate_random();
  tag = run_id.to_string();
}

// 0 = exists, -ENOENT = missing, other = error
int ReportRun::manifest_exists(const std::string& key)
{
  std::unique_ptr<rgw::sal::Object> obj = dest->get_object(rgw_obj_key(key));
  std::unique_ptr<rgw::sal::Object::ReadOp> rop = obj->get_read_op();
  int r = rop->prepare(y, dpp);
  return r < 0 ? r : 0;
}

// the object becomes visible only when the stream is closed
int ReportRun::open_object(const std::string& key,
                           const std::string& content_type,
                           std::shared_ptr<RGWOutputStream>* out)
{
  auto res = RGWOutputStream::Open(dpp, y, driver, dest.get(),
                                   rgw_obj_key(key),
                                   object_attrs(dest.get(), content_type), tag);
  if (!res.ok()) {
    ldpp_dout(dpp, 0) << "ERROR: inventory: cannot open " << dest->get_name()
                      << "/" << key << " for writing: "
                      << res.status().ToString() << dendl;
    return status_to_errno(res.status());
  }
  *out = std::move(*res);
  return 0;
}

int ReportRun::write_object(const std::string& key, const std::string& data,
                            const std::string& content_type)
{
  std::shared_ptr<RGWOutputStream> out;
  int r = open_object(key, content_type, &out);
  if (r < 0) {
    return r;
  }
  arrow::Status s = out->Write(data.data(), static_cast<int64_t>(data.size()));
  if (!s.ok()) {
    std::ignore = out->Abort();
  } else {
    s = out->Close();
  }
  if (!s.ok()) {
    ldpp_dout(dpp, 0) << "ERROR: inventory: writing " << dest->get_name()
                      << "/" << key << " failed: " << s.ToString() << dendl;
    return status_to_errno(s);
  }
  return 0;
}

// lists the source bucket into `out`. on failure the stream is aborted, so
// nothing partial gets published
int ReportRun::write_data(const FieldSelection& sel,
                          const std::shared_ptr<RGWOutputStream>& out)
{
  auto report_error = [this](const arrow::Status& s) {
    ldpp_dout(dpp, 0) << "ERROR: inventory: writing report for "
                      << source->get_key() << " failed: " << s.ToString()
                      << dendl;
    return status_to_errno(s);
  };

  // if Open() fails, the stream has already been aborted
  auto writer = ParquetInventoryWriter::Open(out, sel);
  if (!writer.ok()) {
    return report_error(writer.status());
  }

  rgw::sal::Bucket::ListParams params;
  params.list_versions = (cfg.included_object_versions == ObjectVersions::All);
  params.allow_unordered = false;  // inventory output is key-ordered

  rgw::sal::Bucket::ListResults results;
  do {
    results.objs.clear();
    int r = source->list(dpp, params, kListChunk, results, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: inventory: listing " << source->get_key()
                        << " failed: " << r << dendl;
      return r;  // the writer's destructor aborts the stream
    }
    for (const auto& e : results.objs) {
      auto s = (*writer)->append(entry_from_listing(source->get_name(), e,
                                                    sel));
      if (!s.ok()) {
        return report_error(s);
      }
    }
    params.marker = results.next_marker;
  } while (results.is_truncated);

  if (auto s = (*writer)->close(); !s.ok()) {
    return report_error(s);
  }
  ldpp_dout(dpp, 5) << "inventory: " << source->get_name() << "/" << cfg.id
                    << " -> " << dest->get_name() << " ("
                    << (*writer)->rows_written() << " rows, " << out->size()
                    << " bytes)" << dendl;
  return 0;
}

int ReportRun::run()
{
  const std::string& src = source->get_name();
  int r = load_destination_bucket(dpp, driver, source->get_tenant(), cfg,
                                  &dest, y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "WARNING: inventory: destination for " << src << "/"
                      << cfg.id << " unavailable: " << r << dendl;
    return r;
  }

  const std::string prefix = config_prefix(cfg, source->get_key());
  const std::string run_prefix =
      inventory_run_prefix(prefix, ceph::real_clock::to_time_t(now));
  const std::string manifest_key = run_prefix + "manifest.json";

  r = manifest_exists(manifest_key);
  if (r == 0) {
    ldpp_dout(dpp, 20) << "inventory: " << manifest_key
                       << " already delivered" << dendl;
    return 0;
  }
  if (r != -ENOENT) {
    return r;
  }

  // policy may have changed since the config was PUT
  std::string err;
  r = verify_destination_policy(dpp, source, dest.get(), cfg, &err);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "WARNING: inventory: " << src << "/" << cfg.id
                      << ": " << err << dendl;
    return r;
  }

  const FieldSelection sel = field_selection_from_config(cfg);
  const std::string data_key = inventory_data_key(prefix);
  std::shared_ptr<RGWOutputStream> data_out;
  r = open_object(data_key, "application/octet-stream", &data_out);
  if (r < 0) {
    return r;
  }
  r = write_data(sel, data_out);
  if (r < 0) {
    return r;
  }

  Manifest m;
  m.source_bucket = src;
  m.destination_bucket_arn = cfg.destination.s3.bucket_arn;
  m.creation_timestamp_ms = to_millis(now);
  m.file_schema = build_file_schema(sel);
  m.files.push_back({data_key, static_cast<uint64_t>(data_out->size()),
                     data_out->etag()});
  const std::string json = m.to_json();

  r = write_object(run_prefix + "manifest.checksum", md5_hex(json),
                   "text/plain");
  if (r < 0) {
    return r;
  }
  return write_object(manifest_key, json, "application/json");
}

FieldSelection field_selection_from_config(const Configuration& cfg)
{
  FieldSelection s;
  const bool all = (cfg.included_object_versions == ObjectVersions::All);
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
  switch (cfg.schedule.frequency) {
  case Frequency::Daily:
    return true;
  case Frequency::Weekly: {
    std::tm tm_buf{};
    gmtime_r(&now, &tm_buf);
    const unsigned h = ceph_str_hash_linux(bucket_key.c_str(),
                                           bucket_key.size());
    return static_cast<int>(h % 7) == tm_buf.tm_wday;
  }
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
    const rgw::IAM::Policy policy{dpp->get_cct(), &dest_id.tenant,
                                  policy_it->second.to_str(), false};
    rgw::auth::ServiceIdentity ident(service_principal);
    rgw::IAM::Environment env;
    env.emplace("aws:SourceArn", rgw::ARN(source->get_key()).to_string());
    env.emplace("aws:SourceAccount", to_string(source->get_owner()));
    const rgw::ARN resource(dest_id, config_prefix(cfg, source->get_key()));
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
  BucketConfigurations all;
  int r = decode_bucket_inventory(bucket->get_attrs(), &all);
  if (r == -ENOENT) {
    return 0;
  }
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: inventory: cannot decode config on "
                      << bucket->get_key() << dendl;
    return r;
  }

  // one clock reading for the whole pass: it picks the configs that are due,
  // the day folder of each report and the manifests' creationTimestamp
  const ceph::real_time now = ceph::real_clock::now();
  const std::string bucket_key =
      bucket->get_tenant() + ":" + bucket->get_name();
  int first_err = 0;
  for (const auto& [id, cfg] : all.configs) {
    if (!cfg.enabled ||
        !is_due(cfg, bucket_key, ceph::real_clock::to_time_t(now))) {
      continue;
    }
    r = ReportRun(dpp, y, driver, bucket, cfg, now).run();
    if (r < 0 && first_err == 0) {
      first_err = r;
    }
  }
  return first_err;
}

} // namespace rgw::inventory
