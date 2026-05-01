// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_rest_s3files.h"

#include <atomic>
#include <cstring>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "common/ceph_json.h"
#include "common/dout.h"

#include "rgw_auth_s3.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_s3files_errors.h"
#include "file_state/memory_store.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace rgw::file_state;

// ---------------------------------------------------------------- store

namespace rgw::s3files {

namespace {
std::atomic<rgw::file_state::Store*> g_store{nullptr};

rgw::file_state::Store& fallback_memory_store() {
  static rgw::file_state::MemoryStore inst;
  return inst;
}
}  // namespace

rgw::file_state::Store& default_store() {
  if (auto* p = g_store.load(std::memory_order_acquire); p != nullptr) {
    return *p;
  }
  return fallback_memory_store();
}

void set_default_store(rgw::file_state::Store* store) {
  g_store.store(store, std::memory_order_release);
}

}  // namespace rgw::s3files

// ---------------------------------------------------------------- helpers

namespace {

using namespace rgw::s3files;  // ERR_* errorCode constants

// ---- error -> HTTP/JSON ---------------------------------------

int http_status_for(StoreError::Kind kind) {
  switch (kind) {
    case StoreError::Kind::InvalidArgument: return 400;
    case StoreError::Kind::QuotaExceeded:   return 402;
    case StoreError::Kind::NotFound:        return 404;
    case StoreError::Kind::Conflict:        return 409;
    case StoreError::Kind::Throttled:       return 429;
    case StoreError::Kind::Internal:        return 500;
  }
  return 500;
}

std::string_view exception_type_for(StoreError::Kind kind) {
  switch (kind) {
    case StoreError::Kind::InvalidArgument: return "ValidationException";
    case StoreError::Kind::QuotaExceeded:   return "ServiceQuotaExceededException";
    case StoreError::Kind::NotFound:        return "ResourceNotFoundException";
    case StoreError::Kind::Conflict:        return "ConflictException";
    case StoreError::Kind::Throttled:       return "ThrottlingException";
    case StoreError::Kind::Internal:        return "InternalServerException";
  }
  return "InternalServerException";
}

void send_store_error(req_state* s, RGWOp* op, const StoreError& err) {
  const auto status = http_status_for(err.kind);
  // Build the body up front so we can declare its length and bypass
  // the framework's default error envelope. end_header(...) auto-emits
  // a `{"Code","Message","RequestId","HostId"}` body whenever
  // s->is_err() is true, which both clobbers the restJson1 shape we
  // need and pre-sets content-length to its own envelope size.
  JSONFormatter body(false);
  body.open_object_section("");
  encode_json("__type", std::string(exception_type_for(err.kind)), &body);
  encode_json("errorCode", err.error_code, &body);
  if (!err.message.empty()) {
    encode_json("message", err.message, &body);
  }
  body.close_section();

  std::stringstream ss;
  body.flush(ss);
  std::string out = ss.str();

  s->err.http_ret = status;
  dump_errno(s, status);
  end_header(s, op, "application/json", out.size(),
             /*force_content_type=*/true, /*force_no_error=*/true);
  if (!out.empty()) {
    dump_body(s, out);
  }
}

// ---- JSON read helpers ----------------------------------------

std::string json_string(JSONObj* obj, std::string_view name) {
  if (!obj) return {};
  JSONObj* f = obj->find_obj(std::string(name));
  if (!f) return {};
  return f->get_data();
}

bool json_bool(JSONObj* obj, std::string_view name) {
  if (!obj) return false;
  JSONObj* f = obj->find_obj(std::string(name));
  if (!f) return false;
  return f->get_data() == "true" || f->get_data() == "1";
}

// ---- field validators ----------------------------------------
//
// Server-side checks for Smithy patterns/ranges that boto3's
// client-side validator does NOT enforce (notably regex patterns
// on optional fields and integer @range traits). All return
// true on valid; on failure the caller emits ValidationException
// with the appropriate errorCode.

bool valid_bucket_arn(std::string_view s) {
  // Smithy: arn:aws:s3:::<bucket>
  constexpr std::string_view P = "arn:aws:s3:::";
  if (s.size() <= P.size() || s.substr(0, P.size()) != P) return false;
  std::string_view name = s.substr(P.size());
  if (name.empty() || name.find('/') != std::string_view::npos) return false;
  return true;
}

bool valid_role_arn(std::string_view s) {
  // Smithy: arn:aws:iam::<acct>:role/<name> (acct may be empty in some forms;
  // we accept any ":<digits>:role/<name>" form).
  constexpr std::string_view P = "arn:aws:iam::";
  if (s.size() <= P.size() || s.substr(0, P.size()) != P) return false;
  auto rest = s.substr(P.size());
  auto colon = rest.find(':');
  if (colon == std::string_view::npos) return false;
  auto acct = rest.substr(0, colon);
  for (char c : acct) {
    if (c < '0' || c > '9') return false;
  }
  auto tail = rest.substr(colon + 1);
  constexpr std::string_view R = "role/";
  if (tail.size() <= R.size() || tail.substr(0, R.size()) != R) return false;
  return !tail.substr(R.size()).empty();
}

bool valid_prefix(std::string_view s) {
  // Smithy: ^(|.*/)$ — empty or ending in '/'
  return s.empty() || s.back() == '/';
}

// Extract the bucket name from an `arn:aws:s3:::<bucket>` ARN.
// Caller should have run valid_bucket_arn() first.
std::string_view bucket_name_from_arn(std::string_view arn) {
  constexpr std::string_view P = "arn:aws:s3:::";
  return arn.substr(P.size());
}

// Extract `<account>` and `<role-name>` from
// `arn:aws:iam::<account>:role/<role-name>`. Returns false if the
// ARN doesn't parse; caller should have run valid_role_arn() first.
bool parse_role_arn(std::string_view arn,
                     std::string& account,
                     std::string& role_name) {
  constexpr std::string_view P = "arn:aws:iam::";
  if (arn.size() <= P.size() || arn.substr(0, P.size()) != P) return false;
  auto rest = arn.substr(P.size());
  auto colon = rest.find(':');
  if (colon == std::string_view::npos) return false;
  account.assign(rest.substr(0, colon));
  auto tail = rest.substr(colon + 1);
  constexpr std::string_view R = "role/";
  if (tail.size() <= R.size() || tail.substr(0, R.size()) != R) return false;
  role_name.assign(tail.substr(R.size()));
  return true;
}

// Parse a maxResults query arg into `out`. Returns false if the
// arg is present but malformed or outside [min,max]. The Smithy
// range trait is per-op (1..1000 for list-fs/aps/mts, 1..50 for
// list-tags); the caller passes max.
bool parse_max_results(std::string_view raw, int& out, int max) {
  if (raw.empty()) return true;
  int v = 0;
  try {
    size_t pos = 0;
    v = std::stoi(std::string(raw), &pos);
    if (pos != raw.size()) return false;
  } catch (...) { return false; }
  if (v < 1 || v > max) return false;
  out = v;
  return true;
}

// ---- path -----------------------------------------------------

std::string_view trim_leading_slashes(std::string_view path) {
  while (!path.empty() && path.front() == '/') path.remove_prefix(1);
  return path;
}

std::vector<std::string_view> split_path(std::string_view path) {
  std::vector<std::string_view> out;
  size_t i = 0;
  while (i <= path.size()) {
    size_t j = path.find('/', i);
    if (j == std::string_view::npos) {
      if (i < path.size()) out.push_back(path.substr(i));
      break;
    }
    if (j > i) out.push_back(path.substr(i, j - i));
    i = j + 1;
  }
  return out;
}

// Strip the optional ARN form on a Smithy ResourceId / FileSystemId
// / AccessPointId path component, returning the bare server-assigned
// id. ARNs always end with the bare id after the last slash.
//
// Boto3 substitutes ARN values into non-greedy `{fileSystemId}` URL
// labels with full %-encoding (`:` -> `%3A`, `/` -> `%2F`), so the
// path component arrives URL-encoded. URL-decode first, then take
// the tail after the final `/`.
std::string strip_arn(std::string_view in) {
  std::string decoded = url_decode(in);
  auto pos = decoded.rfind('/');
  if (pos == std::string::npos) return decoded;
  return decoded.substr(pos + 1);
}

// ---- account / owner -----------------------------------------

std::string current_owner_account_id(const req_state* s) {
  // Placeholder until #11 wires proper account-scoped IAM. Today
  // this returns the request's user-id which doubles as the
  // account-id under the test config.
  return s->user ? s->user->get_id().to_str() : std::string{};
}

// ---- response encoders ----------------------------------------

void encode_tags(ceph::Formatter* f, const std::vector<Tag>& tags) {
  f->open_array_section("tags");
  for (const auto& t : tags) {
    f->open_object_section("");
    encode_json("key",   t.key,   f);
    encode_json("value", t.value, f);
    f->close_section();
  }
  f->close_section();
}

void encode_sync_config(ceph::Formatter* f, const SyncConfig& cfg) {
  encode_json("latestVersionNumber", cfg.latest_version_number, f);
  f->open_array_section("importDataRules");
  for (const auto& r : cfg.import_rules) {
    f->open_object_section("");
    encode_json("prefix",       r.prefix,         f);
    encode_json("trigger",      r.trigger,        f);
    encode_json("sizeLessThan", r.size_less_than, f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("expirationDataRules");
  for (const auto& r : cfg.expiration_rules) {
    f->open_object_section("");
    encode_json("daysAfterLastAccess", r.days_after_last_access, f);
    f->close_section();
  }
  f->close_section();
}

bool parse_sync_config(JSONObj* obj, SyncConfig& cfg, bool& has_version) {
  has_version = false;
  if (!obj) return false;
  if (auto* v = obj->find_obj("latestVersionNumber")) {
    try {
      cfg.latest_version_number = std::stoll(v->get_data());
      has_version = true;
    } catch (...) {
      return false;
    }
  }
  if (auto* arr = obj->find_obj("importDataRules");
      arr && arr->is_array()) {
    for (const auto& es : arr->get_array_elements()) {
      JSONParser p;
      if (!p.parse(es.c_str(), es.size())) return false;
      ImportDataRule r;
      r.prefix = json_string(&p, "prefix");
      r.trigger = json_string(&p, "trigger");
      try {
        r.size_less_than = std::stoll(json_string(&p, "sizeLessThan"));
      } catch (...) { return false; }
      cfg.import_rules.push_back(std::move(r));
    }
  }
  if (auto* arr = obj->find_obj("expirationDataRules");
      arr && arr->is_array()) {
    for (const auto& es : arr->get_array_elements()) {
      JSONParser p;
      if (!p.parse(es.c_str(), es.size())) return false;
      ExpirationDataRule r;
      try {
        r.days_after_last_access = std::stoi(
            json_string(&p, "daysAfterLastAccess"));
      } catch (...) { return false; }
      cfg.expiration_rules.push_back(std::move(r));
    }
  }
  return true;
}

void encode_mount_target(ceph::Formatter* f, const MountTargetView& mt) {
  encode_json("mountTargetId",     mt.spec.id,                              f);
  encode_json("ownerId",           mt.spec.owner_account_id,                f);
  encode_json("fileSystemId",      mt.spec.parent_filesystem_id,            f);
  // Encode the request-form subnetId (subnet-{zone}); the bare
  // zone-id appears in availabilityZoneId per the design doc. The
  // Smithy SubnetId pattern is hex-only, so strip hyphens from the
  // canonical (UUID-form) zone-id when emitting it.
  std::string zone_hex;
  zone_hex.reserve(mt.spec.zone_id.size());
  for (char c : mt.spec.zone_id) {
    if (c != '-') zone_hex.push_back(c);
  }
  encode_json("subnetId",          "subnet-" + zone_hex,                    f);
  encode_json("availabilityZoneId", zone_hex,                               f);
  encode_json("availabilityZoneName", zone_hex,                             f);
  if (!mt.spec.ip_address_type.empty()) {
    encode_json("ipAddressType",   mt.spec.ip_address_type,                 f);
  }
  if (!mt.status.ipv4_address.empty()) {
    encode_json("ipv4Address",     mt.status.ipv4_address,                  f);
  }
  if (!mt.status.ipv6_address.empty()) {
    encode_json("ipv6Address",     mt.status.ipv6_address,                  f);
  }
  if (!mt.status.network_interface_id.empty()) {
    encode_json("networkInterfaceId", mt.status.network_interface_id,       f);
  }
  if (!mt.status.vpc_id.empty()) {
    encode_json("vpcId",           mt.status.vpc_id,                        f);
  }
  if (!mt.spec.security_groups.empty()) {
    f->open_array_section("securityGroups");
    for (const auto& sg : mt.spec.security_groups) f->dump_string("", sg);
    f->close_section();
  }
  encode_json("status",            std::string(to_string(mt.status.state)), f);
  if (!mt.status.status_message.empty()) {
    encode_json("statusMessage",   mt.status.status_message,                f);
  }
}

// Decode the AWS subnet-id form (`subnet-{zone}`) to the bare
// Ceph zone-id. Returns nullopt if the prefix is missing.
std::optional<std::string> zone_from_subnet_id(std::string_view sid) {
  constexpr std::string_view kPrefix = "subnet-";
  if (sid.size() <= kPrefix.size() ||
      sid.substr(0, kPrefix.size()) != kPrefix) {
    return std::nullopt;
  }
  return std::string(sid.substr(kPrefix.size()));
}

void encode_posix_user(ceph::Formatter* f, const PosixUser& pu) {
  f->open_object_section("posixUser");
  encode_json("uid", pu.uid, f);
  encode_json("gid", pu.gid, f);
  if (!pu.secondary_gids.empty()) {
    f->open_array_section("secondaryGids");
    for (auto g : pu.secondary_gids) {
      f->dump_int("", g);
    }
    f->close_section();
  }
  f->close_section();
}

void encode_root_directory(ceph::Formatter* f, const RootDirectory& rd) {
  f->open_object_section("rootDirectory");
  if (!rd.path.empty()) encode_json("path", rd.path, f);
  if (rd.creation_permissions) {
    f->open_object_section("creationPermissions");
    encode_json("ownerUid",   rd.creation_permissions->owner_uid,   f);
    encode_json("ownerGid",   rd.creation_permissions->owner_gid,   f);
    encode_json("permissions", rd.creation_permissions->permissions, f);
    f->close_section();
  }
  f->close_section();
}

void encode_access_point(ceph::Formatter* f, const AccessPointView& ap) {
  encode_json("accessPointId",  ap.spec.id,                              f);
  encode_json("accessPointArn", ap.spec.arn,                             f);
  encode_json("fileSystemId",   ap.spec.parent_filesystem_id,            f);
  encode_json("ownerId",        ap.spec.owner_account_id,                f);
  if (!ap.spec.client_token.empty()) {
    encode_json("clientToken",  ap.spec.client_token,                    f);
  }
  encode_json("status",         std::string(to_string(ap.status.state)), f);
  if (ap.spec.posix_user)        encode_posix_user(f, *ap.spec.posix_user);
  if (ap.spec.root_directory)    encode_root_directory(f, *ap.spec.root_directory);
  if (auto n = ap.name(); n) encode_json("name", *n, f);
  encode_tags(f, ap.spec.tags);
}

// Parse a JSON object representing a posixUser. Sets `out` and
// returns true on success; on malformed input returns false and
// leaves out partially populated.
bool parse_posix_user(JSONObj* obj, PosixUser& out) {
  if (!obj) return false;
  try {
    out.uid = std::stoll(json_string(obj, "uid"));
    out.gid = std::stoll(json_string(obj, "gid"));
  } catch (...) { return false; }
  if (auto* sg = obj->find_obj("secondaryGids");
      sg && sg->is_array()) {
    for (const auto& s : sg->get_array_elements()) {
      try { out.secondary_gids.push_back(std::stoll(s)); }
      catch (...) { return false; }
    }
  }
  return true;
}

bool parse_root_directory(JSONObj* obj, RootDirectory& out) {
  if (!obj) return false;
  out.path = json_string(obj, "path");
  if (auto* cp = obj->find_obj("creationPermissions")) {
    CreationPermissions p;
    try {
      p.owner_uid = std::stoll(json_string(cp, "ownerUid"));
      p.owner_gid = std::stoll(json_string(cp, "ownerGid"));
    } catch (...) { return false; }
    p.permissions = json_string(cp, "permissions");
    out.creation_permissions = std::move(p);
  }
  return true;
}

void encode_file_system(ceph::Formatter* f, const FileSystemView& fs) {
  encode_json("fileSystemId",  fs.spec.id,                              f);
  encode_json("fileSystemArn", fs.spec.arn,                             f);
  encode_json("ownerId",       fs.spec.owner_account_id,                f);
  encode_json("bucket",        fs.spec.bucket_arn,                      f);
  encode_json("prefix",        fs.spec.prefix,                          f);
  encode_json("roleArn",       fs.spec.role_arn,                        f);
  if (!fs.spec.kms_key_id.empty()) {
    encode_json("kmsKeyId",    fs.spec.kms_key_id,                      f);
  }
  if (!fs.spec.client_token.empty()) {
    encode_json("clientToken", fs.spec.client_token,                    f);
  }
  encode_json("status",        std::string(to_string(fs.status.state)), f);
  encode_json("creationTime",  fs.spec.creation_time_unix_seconds,      f);
  if (auto n = fs.name(); n) {
    encode_json("name",        *n,                                      f);
  }
  encode_tags(f, fs.spec.tags);
}

}  // namespace

// ---------------------------------------------------------------- ops
namespace {

// CreateFileSystem — PUT /file-systems
//
// Smithy: com.amazonaws.s3files#CreateFileSystem.
class RGWCreateFileSystem : public RGWOp {
 public:
  explicit RGWCreateFileSystem(ceph::bufferlist body) : body_(std::move(body)) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "create_file_system"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_CREATE_FILE_SYSTEM; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

 private:
  CreateFileSystemRequest req_;
  std::optional<FileSystemView> created_;
  std::optional<StoreError> err_;
  ceph::bufferlist body_;
};

int RGWCreateFileSystem::init_processing(optional_yield y) {
  s->format = RGWFormat::JSON;

  // Body was pre-read in RGWRESTMgr_S3Files::get_handler so the
  // sigv4 PayloadHash arg is set before authorize() runs.
  ceph::bufferlist& body = body_;
  JSONParser parser;
  if (!parser.parse(body.c_str(), body.length())) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_POLICY_DOCUMENT),
        .message = "request body is not valid JSON",
    };
    return 0;
  }

  req_.owner_account_id = current_owner_account_id(s);
  req_.bucket_arn = json_string(&parser, "bucket");
  req_.prefix = json_string(&parser, "prefix");
  req_.role_arn = json_string(&parser, "roleArn");
  req_.kms_key_id = json_string(&parser, "kmsKeyId");
  req_.client_token = json_string(&parser, "clientToken");
  req_.accept_bucket_warning = json_bool(&parser, "acceptBucketWarning");

  if (!valid_bucket_arn(req_.bucket_arn)) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_BUCKET_ARN),
        .message = "bucket is not a valid S3 ARN",
    };
    return 0;
  }
  if (!valid_role_arn(req_.role_arn)) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_ROLE_ARN),
        .message = "roleArn is not a valid IAM role ARN",
    };
    return 0;
  }
  if (!valid_prefix(req_.prefix)) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_PREFIX),
        .message = "prefix must be empty or end in '/'",
    };
    return 0;
  }

  if (JSONObj* tags = parser.find_obj("tags");
      tags && tags->is_array()) {
    for (const auto& tag_str : tags->get_array_elements()) {
      JSONParser tag_parser;
      if (!tag_parser.parse(tag_str.c_str(), tag_str.size())) continue;
      Tag tag;
      tag.key = json_string(&tag_parser, "key");
      tag.value = json_string(&tag_parser, "value");
      req_.tags.push_back(std::move(tag));
    }
  }
  return 0;
}

void RGWCreateFileSystem::execute(optional_yield y) {
  if (err_) {
    op_ret = -EINVAL;
    return;
  }
  std::string tenant = s->user ? s->user->get_tenant() : std::string{};
  // Verify the bucket exists in the local zone before binding it.
  {
    std::string bname{bucket_name_from_arn(req_.bucket_arn)};
    std::unique_ptr<rgw::sal::Bucket> bucket;
    int r = driver->load_bucket(this, rgw_bucket(tenant, bname), &bucket, y);
    if (r == -ENOENT) {
      err_ = StoreError{
          .kind = StoreError::Kind::NotFound,
          .error_code = std::string(ERR_BUCKET_NOT_FOUND),
          .message = "bucket does not exist",
      };
      op_ret = -ENOENT;
      return;
    }
    if (r < 0) {
      err_ = StoreError{
          .kind = StoreError::Kind::Internal,
          .error_code = std::string(ERR_INTERNAL),
          .message = "failed to look up bucket",
      };
      op_ret = r;
      return;
    }
  }
  // Verify the IAM role referenced by roleArn exists. We only
  // confirm existence; assumption / delegation is deferred.
  {
    std::string account, role_name;
    if (!parse_role_arn(req_.role_arn, account, role_name)) {
      err_ = StoreError{
          .kind = StoreError::Kind::InvalidArgument,
          .error_code = std::string(ERR_INVALID_ROLE_ARN),
          .message = "roleArn is not a valid IAM role ARN",
      };
      op_ret = -EINVAL;
      return;
    }
    auto role = driver->get_role(role_name, tenant, rgw_account_id{account});
    int r = role ? role->load_by_name(this, y) : -ENOENT;
    if (r == -ENOENT) {
      err_ = StoreError{
          .kind = StoreError::Kind::NotFound,
          .error_code = std::string(ERR_ROLE_NOT_FOUND),
          .message = "IAM role does not exist",
      };
      op_ret = -ENOENT;
      return;
    }
    if (r < 0) {
      err_ = StoreError{
          .kind = StoreError::Kind::Internal,
          .error_code = std::string(ERR_INTERNAL),
          .message = "failed to look up IAM role",
      };
      op_ret = r;
      return;
    }
  }
  auto result = rgw::s3files::default_store().create_file_system(req_);
  if (!ok(result)) {
    err_ = error(result);
    op_ret = -ECANCELED;
    return;
  }
  created_ = take(std::move(result));
  op_ret = 0;
}

void RGWCreateFileSystem::send_response() {
  if (err_) {
    send_store_error(s, this, *err_);
    return;
  }
  s->err.http_ret = 201;
  dump_errno(s, 201);
  end_header(s, this, "application/json");
  s->formatter->open_object_section("");
  encode_file_system(s->formatter, *created_);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// GetFileSystem — GET /file-systems/{fileSystemId}
class RGWGetFileSystem : public RGWOp {
 public:
  explicit RGWGetFileSystem(std::string fs_id) : fs_id_(std::move(fs_id)) {}

  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "get_file_system"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_GET_FILE_SYSTEM; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

 private:
  std::string fs_id_;
  std::optional<FileSystemView> view_;
  std::optional<StoreError> err_;
};

void RGWGetFileSystem::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  auto result = rgw::s3files::default_store().get_file_system(
      current_owner_account_id(s), strip_arn(fs_id_));
  if (!ok(result)) {
    err_ = error(result);
    op_ret = -ENOENT;
    return;
  }
  view_ = take(std::move(result));
  op_ret = 0;
}

void RGWGetFileSystem::send_response() {
  if (err_) {
    send_store_error(s, this, *err_);
    return;
  }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  s->formatter->open_object_section("");
  encode_file_system(s->formatter, *view_);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// ListFileSystems — GET /file-systems
class RGWListFileSystems : public RGWOp {
 public:
  RGWListFileSystems() = default;

  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "list_file_systems"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_LIST_FILE_SYSTEMS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

 private:
  std::optional<PagedResult<FileSystemView>> result_;
  std::optional<StoreError> err_;
};

void RGWListFileSystems::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  ListOptions opts;
  if (!parse_max_results(s->info.args.get("maxResults"),
                          opts.max_results, /*max=*/100)) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_MAX_RESULTS),
        .message = "maxResults must be in 1..100",
    };
    op_ret = -EINVAL;
    return;
  }
  opts.next_token = s->info.args.get("nextToken");

  auto r = rgw::s3files::default_store().list_file_systems(
      current_owner_account_id(s), opts);
  if (!ok(r)) {
    err_ = error(r);
    op_ret = -EIO;
    return;
  }
  result_ = take(std::move(r));
  op_ret = 0;
}

void RGWListFileSystems::send_response() {
  if (err_) {
    send_store_error(s, this, *err_);
    return;
  }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  s->formatter->open_object_section("");
  s->formatter->open_array_section("fileSystems");
  for (const auto& fs : result_->items) {
    s->formatter->open_object_section("");
    encode_file_system(s->formatter, fs);
    s->formatter->close_section();
  }
  s->formatter->close_section();
  if (!result_->next_token.empty()) {
    encode_json("nextToken", result_->next_token, s->formatter);
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// DeleteFileSystem — DELETE /file-systems/{fileSystemId}
class RGWDeleteFileSystem : public RGWOp {
 public:
  explicit RGWDeleteFileSystem(std::string fs_id) : fs_id_(std::move(fs_id)) {}

  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "delete_file_system"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_DELETE_FILE_SYSTEM; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }

 private:
  std::string fs_id_;
  std::optional<StoreError> err_;
};

void RGWDeleteFileSystem::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  auto r = rgw::s3files::default_store().delete_file_system(
      current_owner_account_id(s), strip_arn(fs_id_));
  if (!ok(r)) {
    err_ = error(r);
    op_ret = -ENOENT;
    return;
  }
  op_ret = 0;
}

void RGWDeleteFileSystem::send_response() {
  if (err_) {
    send_store_error(s, this, *err_);
    return;
  }
  // 204 No Content per the AWS Smithy http trait.
  s->err.http_ret = 204;
  dump_errno(s, 204);
  end_header(s, this, "application/json");
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// PutFileSystemPolicy — PUT /file-systems/{id}/policy
class RGWPutFileSystemPolicy : public RGWOp {
 public:
  RGWPutFileSystemPolicy(std::string fs_id, ceph::bufferlist body)
    : fs_id_(std::move(fs_id)), body_(std::move(body)) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "put_file_system_policy"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_PUT_FILE_SYSTEM_POLICY; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

 private:
  std::string fs_id_;
  std::string policy_;
  std::optional<StoreError> err_;
  ceph::bufferlist body_;
};

int RGWPutFileSystemPolicy::init_processing(optional_yield y) {
  s->format = RGWFormat::JSON;
  // Body was pre-read in RGWRESTMgr_S3Files::get_handler so the
  // sigv4 PayloadHash arg is set before authorize() runs.
  ceph::bufferlist& body = body_;
  JSONParser parser;
  if (!parser.parse(body.c_str(), body.length())) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_POLICY_DOCUMENT),
        .message = "request body is not valid JSON",
    };
    return 0;
  }
  policy_ = json_string(&parser, "policy");
  return 0;
}

void RGWPutFileSystemPolicy::execute(optional_yield y) {
  if (err_) { op_ret = -EINVAL; return; }
  auto r = rgw::s3files::default_store().put_file_system_policy(
      current_owner_account_id(s), strip_arn(fs_id_), policy_);
  if (!ok(r)) { err_ = error(r); op_ret = -ECANCELED; return; }
  op_ret = 0;
}

void RGWPutFileSystemPolicy::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// GetFileSystemPolicy — GET /file-systems/{id}/policy
class RGWGetFileSystemPolicy : public RGWOp {
 public:
  explicit RGWGetFileSystemPolicy(std::string fs_id)
    : fs_id_(std::move(fs_id)) {}

  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "get_file_system_policy"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_GET_FILE_SYSTEM_POLICY; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

 private:
  std::string fs_id_;
  std::string policy_;
  std::optional<StoreError> err_;
};

void RGWGetFileSystemPolicy::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  auto r = rgw::s3files::default_store().get_file_system_policy(
      current_owner_account_id(s), strip_arn(fs_id_));
  if (!ok(r)) { err_ = error(r); op_ret = -ENOENT; return; }
  policy_ = take(std::move(r));
  op_ret = 0;
}

void RGWGetFileSystemPolicy::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  s->formatter->open_object_section("");
  encode_json("fileSystemId", strip_arn(fs_id_), s->formatter);
  encode_json("policy",       policy_,           s->formatter);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// DeleteFileSystemPolicy — DELETE /file-systems/{id}/policy
class RGWDeleteFileSystemPolicy : public RGWOp {
 public:
  explicit RGWDeleteFileSystemPolicy(std::string fs_id)
    : fs_id_(std::move(fs_id)) {}

  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "delete_file_system_policy"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_DELETE_FILE_SYSTEM_POLICY; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }

 private:
  std::string fs_id_;
  std::optional<StoreError> err_;
};

void RGWDeleteFileSystemPolicy::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  auto r = rgw::s3files::default_store().delete_file_system_policy(
      current_owner_account_id(s), strip_arn(fs_id_));
  if (!ok(r)) { err_ = error(r); op_ret = -ENOENT; return; }
  op_ret = 0;
}

void RGWDeleteFileSystemPolicy::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 204;
  dump_errno(s, 204);
  end_header(s, this, "application/json");
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// PutSynchronizationConfiguration — PUT /file-systems/{id}/synchronization-configuration
class RGWPutSyncConfig : public RGWOp {
 public:
  RGWPutSyncConfig(std::string fs_id, ceph::bufferlist body)
    : fs_id_(std::move(fs_id)), body_(std::move(body)) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "put_synchronization_configuration"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_PUT_SYNCHRONIZATION_CONFIGURATION; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

 private:
  std::string fs_id_;
  SyncConfig cfg_;
  bool has_expected_version_ = false;
  std::int64_t expected_version_ = 0;
  std::optional<StoreError> err_;
  ceph::bufferlist body_;
};

int RGWPutSyncConfig::init_processing(optional_yield y) {
  s->format = RGWFormat::JSON;
  // Body was pre-read in RGWRESTMgr_S3Files::get_handler so the
  // sigv4 PayloadHash arg is set before authorize() runs.
  ceph::bufferlist& body = body_;
  JSONParser parser;
  if (!parser.parse(body.c_str(), body.length())) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_POLICY_DOCUMENT),
        .message = "request body is not valid JSON",
    };
    return 0;
  }
  if (!parse_sync_config(&parser, cfg_, has_expected_version_)) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_POLICY_DOCUMENT),
        .message = "synchronization configuration body is malformed",
    };
    return 0;
  }
  // Smithy length/range traits.
  if (cfg_.import_rules.empty() || cfg_.import_rules.size() > 10) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_SYNC_RULES),
        .message = "importDataRules length must be 1..10",
    };
    return 0;
  }
  if (cfg_.expiration_rules.size() != 1) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_SYNC_RULES),
        .message = "expirationDataRules must contain exactly one rule",
    };
    return 0;
  }
  for (const auto& r : cfg_.import_rules) {
    if (r.trigger != "ON_DIRECTORY_FIRST_ACCESS" &&
        r.trigger != "ON_FILE_ACCESS") {
      err_ = StoreError{
          .kind = StoreError::Kind::InvalidArgument,
          .error_code = std::string(ERR_INVALID_SYNC_RULES),
          .message = "importDataRules.trigger is not a valid enum value",
      };
      return 0;
    }
  }
  for (const auto& r : cfg_.expiration_rules) {
    if (r.days_after_last_access < 1 || r.days_after_last_access > 365) {
      err_ = StoreError{
          .kind = StoreError::Kind::InvalidArgument,
          .error_code = std::string(ERR_INVALID_SYNC_RULES),
          .message = "expirationDataRules.daysAfterLastAccess must be in 1..365",
      };
      return 0;
    }
  }
  if (has_expected_version_) {
    expected_version_ = cfg_.latest_version_number;
  }
  return 0;
}

void RGWPutSyncConfig::execute(optional_yield y) {
  if (err_) { op_ret = -EINVAL; return; }
  std::optional<std::int64_t> ev;
  if (has_expected_version_) ev = expected_version_;
  auto r = rgw::s3files::default_store().put_synchronization_configuration(
      current_owner_account_id(s), strip_arn(fs_id_), cfg_, ev);
  if (!ok(r)) { err_ = error(r); op_ret = -ECANCELED; return; }
  op_ret = 0;
}

void RGWPutSyncConfig::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// GetSynchronizationConfiguration — GET /file-systems/{id}/synchronization-configuration
class RGWGetSyncConfig : public RGWOp {
 public:
  explicit RGWGetSyncConfig(std::string fs_id) : fs_id_(std::move(fs_id)) {}

  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "get_synchronization_configuration"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_GET_SYNCHRONIZATION_CONFIGURATION; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

 private:
  std::string fs_id_;
  std::optional<SyncConfig> cfg_;
  std::optional<StoreError> err_;
};

void RGWGetSyncConfig::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  auto r = rgw::s3files::default_store().get_synchronization_configuration(
      current_owner_account_id(s), strip_arn(fs_id_));
  if (!ok(r)) { err_ = error(r); op_ret = -ENOENT; return; }
  cfg_ = take(std::move(r));
  op_ret = 0;
}

void RGWGetSyncConfig::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  s->formatter->open_object_section("");
  encode_sync_config(s->formatter, *cfg_);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// CreateAccessPoint — PUT /access-points
class RGWCreateAccessPoint : public RGWOp {
 public:
  explicit RGWCreateAccessPoint(ceph::bufferlist body) : body_(std::move(body)) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "create_access_point"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_CREATE_ACCESS_POINT; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

 private:
  CreateAccessPointRequest req_;
  std::optional<AccessPointView> created_;
  std::optional<StoreError> err_;
  ceph::bufferlist body_;
};

int RGWCreateAccessPoint::init_processing(optional_yield y) {
  s->format = RGWFormat::JSON;
  // Body was pre-read in RGWRESTMgr_S3Files::get_handler so the
  // sigv4 PayloadHash arg is set before authorize() runs.
  ceph::bufferlist& body = body_;
  JSONParser parser;
  if (!parser.parse(body.c_str(), body.length())) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_POLICY_DOCUMENT),
        .message = "request body is not valid JSON",
    };
    return 0;
  }
  req_.owner_account_id = current_owner_account_id(s);
  req_.filesystem_id    = strip_arn(json_string(&parser, "fileSystemId"));
  req_.client_token     = json_string(&parser, "clientToken");

  if (auto* pu = parser.find_obj("posixUser"); pu) {
    PosixUser p;
    if (!parse_posix_user(pu, p)) {
      err_ = StoreError{
          .kind = StoreError::Kind::InvalidArgument,
          .error_code = std::string(ERR_INVALID_POSIX_USER),
          .message = "posixUser missing required fields",
      };
      return 0;
    }
    req_.posix_user = std::move(p);
  }
  if (auto* rd = parser.find_obj("rootDirectory"); rd) {
    RootDirectory r;
    if (!parse_root_directory(rd, r)) {
      err_ = StoreError{
          .kind = StoreError::Kind::InvalidArgument,
          .error_code = std::string(ERR_INVALID_ROOT_DIRECTORY),
          .message = "rootDirectory is malformed",
      };
      return 0;
    }
    req_.root_directory = std::move(r);
  }
  if (auto* tags = parser.find_obj("tags"); tags && tags->is_array()) {
    for (const auto& tag_str : tags->get_array_elements()) {
      JSONParser tp;
      if (!tp.parse(tag_str.c_str(), tag_str.size())) continue;
      req_.tags.push_back({json_string(&tp, "key"), json_string(&tp, "value")});
    }
  }
  return 0;
}

void RGWCreateAccessPoint::execute(optional_yield y) {
  if (err_) { op_ret = -EINVAL; return; }
  auto r = rgw::s3files::default_store().create_access_point(req_);
  if (!ok(r)) { err_ = error(r); op_ret = -ECANCELED; return; }
  created_ = take(std::move(r));
  op_ret = 0;
}

void RGWCreateAccessPoint::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  s->formatter->open_object_section("");
  encode_access_point(s->formatter, *created_);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// GetAccessPoint — GET /access-points/{accessPointId}
class RGWGetAccessPoint : public RGWOp {
 public:
  explicit RGWGetAccessPoint(std::string ap_id) : ap_id_(std::move(ap_id)) {}

  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "get_access_point"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_GET_ACCESS_POINT; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

 private:
  std::string ap_id_;
  std::optional<AccessPointView> view_;
  std::optional<StoreError> err_;
};

void RGWGetAccessPoint::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  auto r = rgw::s3files::default_store().get_access_point(
      current_owner_account_id(s), strip_arn(ap_id_));
  if (!ok(r)) { err_ = error(r); op_ret = -ENOENT; return; }
  view_ = take(std::move(r));
  op_ret = 0;
}

void RGWGetAccessPoint::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  s->formatter->open_object_section("");
  encode_access_point(s->formatter, *view_);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// ListAccessPoints — GET /access-points?fileSystemId=...
class RGWListAccessPoints : public RGWOp {
 public:
  RGWListAccessPoints() = default;

  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "list_access_points"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_LIST_ACCESS_POINTS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

 private:
  std::optional<PagedResult<AccessPointView>> result_;
  std::optional<StoreError> err_;
};

void RGWListAccessPoints::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  auto fs_id = s->info.args.get("fileSystemId");
  if (fs_id.empty()) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_BUCKET_ARN),
        .message = "fileSystemId query parameter is required",
    };
    op_ret = -EINVAL;
    return;
  }
  ListOptions opts;
  if (!parse_max_results(s->info.args.get("maxResults"),
                          opts.max_results, /*max=*/1000)) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_MAX_RESULTS),
        .message = "maxResults must be in 1..1000",
    };
    op_ret = -EINVAL;
    return;
  }
  opts.next_token = s->info.args.get("nextToken");

  auto r = rgw::s3files::default_store().list_access_points(
      current_owner_account_id(s), strip_arn(fs_id), opts);
  if (!ok(r)) { err_ = error(r); op_ret = -EIO; return; }
  result_ = take(std::move(r));
  op_ret = 0;
}

void RGWListAccessPoints::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  s->formatter->open_object_section("");
  s->formatter->open_array_section("accessPoints");
  for (const auto& ap : result_->items) {
    s->formatter->open_object_section("");
    encode_access_point(s->formatter, ap);
    s->formatter->close_section();
  }
  s->formatter->close_section();
  if (!result_->next_token.empty()) {
    encode_json("nextToken", result_->next_token, s->formatter);
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// DeleteAccessPoint — DELETE /access-points/{accessPointId}
class RGWDeleteAccessPoint : public RGWOp {
 public:
  explicit RGWDeleteAccessPoint(std::string ap_id) : ap_id_(std::move(ap_id)) {}

  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "delete_access_point"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_DELETE_ACCESS_POINT; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }

 private:
  std::string ap_id_;
  std::optional<StoreError> err_;
};

void RGWDeleteAccessPoint::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  auto r = rgw::s3files::default_store().delete_access_point(
      current_owner_account_id(s), strip_arn(ap_id_));
  if (!ok(r)) { err_ = error(r); op_ret = -ENOENT; return; }
  op_ret = 0;
}

void RGWDeleteAccessPoint::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 204;
  dump_errno(s, 204);
  end_header(s, this, "application/json");
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// CreateMountTarget — PUT /mount-targets
class RGWCreateMountTarget : public RGWOp {
 public:
  explicit RGWCreateMountTarget(ceph::bufferlist body) : body_(std::move(body)) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "create_mount_target"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_CREATE_MOUNT_TARGET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

 private:
  CreateMountTargetRequest req_;
  std::optional<MountTargetView> created_;
  std::optional<StoreError> err_;
  ceph::bufferlist body_;
};

int RGWCreateMountTarget::init_processing(optional_yield y) {
  s->format = RGWFormat::JSON;
  // Body was pre-read in RGWRESTMgr_S3Files::get_handler so the
  // sigv4 PayloadHash arg is set before authorize() runs.
  ceph::bufferlist& body = body_;
  JSONParser parser;
  if (!parser.parse(body.c_str(), body.length())) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_POLICY_DOCUMENT),
        .message = "request body is not valid JSON",
    };
    return 0;
  }
  req_.owner_account_id = current_owner_account_id(s);
  req_.filesystem_id    = strip_arn(json_string(&parser, "fileSystemId"));
  if (auto z = zone_from_subnet_id(json_string(&parser, "subnetId")); z) {
    req_.zone_id = *z;
  }
  req_.ip_address_type = json_string(&parser, "ipAddressType");
  // ipv4Address / ipv6Address are accepted and silently ignored
  // per the design doc; security groups are stored but not
  // enforced.
  if (auto* sgs = parser.find_obj("securityGroups");
      sgs && sgs->is_array()) {
    for (const auto& v : sgs->get_array_elements()) {
      // get_array_elements yields the JSON-encoded form, including
      // surrounding quotes for strings; strip them.
      std::string sg = v;
      if (sg.size() >= 2 && sg.front() == '"' && sg.back() == '"') {
        sg = sg.substr(1, sg.size() - 2);
      }
      req_.security_groups.push_back(std::move(sg));
    }
  }
  return 0;
}

void RGWCreateMountTarget::execute(optional_yield y) {
  if (err_) { op_ret = -EINVAL; return; }
  // Verify the supplied zone-id (carried in `subnet-{zone_id}`)
  // names a zone in the local zonegroup. AWS would have returned
  // InvalidSubnetId.NotFound here.
  //
  // The Smithy SubnetId pattern is `^subnet-[0-9a-f]{8,40}$`, so
  // zone-ids encoded into the wire form have any hyphens stripped
  // (Ceph zone-ids are UUIDs with hyphens). Normalize both sides
  // before comparing.
  if (!req_.zone_id.empty()) {
    auto strip_dashes = [](std::string_view in) {
      std::string out;
      out.reserve(in.size());
      for (char c : in) {
        if (c != '-') out.push_back(c);
      }
      return out;
    };
    const std::string want = strip_dashes(req_.zone_id);
    auto& zg = driver->get_zone()->get_zonegroup();
    std::list<std::string> zone_ids;
    bool match = false;
    if (zg.list_zones(zone_ids) >= 0) {
      for (const auto& zid : zone_ids) {
        if (strip_dashes(zid) == want) {
          // Carry the canonical (dashed) form through to the store.
          req_.zone_id = zid;
          match = true;
          break;
        }
      }
    }
    if (!match) {
      err_ = StoreError{
          .kind = StoreError::Kind::InvalidArgument,
          .error_code = std::string(ERR_ZONE_NOT_IN_ZONEGROUP),
          .message = "zone-id is not part of the local zonegroup",
      };
      op_ret = -EINVAL;
      return;
    }
  }
  auto r = rgw::s3files::default_store().create_mount_target(req_);
  if (!ok(r)) { err_ = error(r); op_ret = -ECANCELED; return; }
  created_ = take(std::move(r));
  op_ret = 0;
}

void RGWCreateMountTarget::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  s->formatter->open_object_section("");
  encode_mount_target(s->formatter, *created_);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// GetMountTarget — GET /mount-targets/{mountTargetId}
class RGWGetMountTarget : public RGWOp {
 public:
  explicit RGWGetMountTarget(std::string mt_id) : mt_id_(std::move(mt_id)) {}

  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "get_mount_target"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_GET_MOUNT_TARGET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

 private:
  std::string mt_id_;
  std::optional<MountTargetView> view_;
  std::optional<StoreError> err_;
};

void RGWGetMountTarget::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  auto r = rgw::s3files::default_store().get_mount_target(
      current_owner_account_id(s), mt_id_);
  if (!ok(r)) { err_ = error(r); op_ret = -ENOENT; return; }
  view_ = take(std::move(r));
  op_ret = 0;
}

void RGWGetMountTarget::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  s->formatter->open_object_section("");
  encode_mount_target(s->formatter, *view_);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// ListMountTargets — GET /mount-targets[?fileSystemId=...&accessPointId=...]
class RGWListMountTargets : public RGWOp {
 public:
  RGWListMountTargets() = default;

  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "list_mount_targets"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_LIST_MOUNT_TARGETS; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

 private:
  std::optional<PagedResult<MountTargetView>> result_;
  std::optional<StoreError> err_;
};

void RGWListMountTargets::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  ListOptions opts;
  if (!parse_max_results(s->info.args.get("maxResults"),
                          opts.max_results, /*max=*/100)) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_MAX_RESULTS),
        .message = "maxResults must be in 1..100",
    };
    op_ret = -EINVAL;
    return;
  }
  opts.next_token = s->info.args.get("nextToken");

  std::optional<std::string> fs_filter;
  std::optional<std::string> ap_filter;
  if (auto v = s->info.args.get("fileSystemId"); !v.empty()) {
    fs_filter = strip_arn(v);
  }
  if (auto v = s->info.args.get("accessPointId"); !v.empty()) {
    ap_filter = strip_arn(v);
  }

  auto r = rgw::s3files::default_store().list_mount_targets(
      current_owner_account_id(s),
      fs_filter ? std::optional<std::string_view>{*fs_filter} : std::nullopt,
      ap_filter ? std::optional<std::string_view>{*ap_filter} : std::nullopt,
      opts);
  if (!ok(r)) { err_ = error(r); op_ret = -EIO; return; }
  result_ = take(std::move(r));
  op_ret = 0;
}

void RGWListMountTargets::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  s->formatter->open_object_section("");
  s->formatter->open_array_section("mountTargets");
  for (const auto& mt : result_->items) {
    s->formatter->open_object_section("");
    encode_mount_target(s->formatter, mt);
    s->formatter->close_section();
  }
  s->formatter->close_section();
  if (!result_->next_token.empty()) {
    encode_json("nextToken", result_->next_token, s->formatter);
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// UpdateMountTarget — PUT /mount-targets/{mountTargetId}
class RGWUpdateMountTarget : public RGWOp {
 public:
  RGWUpdateMountTarget(std::string mt_id, ceph::bufferlist body)
    : mt_id_(std::move(mt_id)), body_(std::move(body)) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "update_mount_target"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_UPDATE_MOUNT_TARGET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

 private:
  std::string mt_id_;
  UpdateMountTargetRequest req_;
  std::optional<MountTargetView> updated_;
  std::optional<StoreError> err_;
  ceph::bufferlist body_;
};

int RGWUpdateMountTarget::init_processing(optional_yield y) {
  s->format = RGWFormat::JSON;
  // Body was pre-read in RGWRESTMgr_S3Files::get_handler so the
  // sigv4 PayloadHash arg is set before authorize() runs.
  ceph::bufferlist& body = body_;
  JSONParser parser;
  if (!parser.parse(body.c_str(), body.length())) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_POLICY_DOCUMENT),
        .message = "request body is not valid JSON",
    };
    return 0;
  }
  req_.id = mt_id_;
  if (auto* sgs = parser.find_obj("securityGroups");
      sgs && sgs->is_array()) {
    for (const auto& v : sgs->get_array_elements()) {
      std::string sg = v;
      if (sg.size() >= 2 && sg.front() == '"' && sg.back() == '"') {
        sg = sg.substr(1, sg.size() - 2);
      }
      req_.security_groups.push_back(std::move(sg));
    }
  }
  return 0;
}

void RGWUpdateMountTarget::execute(optional_yield y) {
  if (err_) { op_ret = -EINVAL; return; }
  auto r = rgw::s3files::default_store().update_mount_target(
      current_owner_account_id(s), req_);
  if (!ok(r)) { err_ = error(r); op_ret = -ECANCELED; return; }
  updated_ = take(std::move(r));
  op_ret = 0;
}

void RGWUpdateMountTarget::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  s->formatter->open_object_section("");
  encode_mount_target(s->formatter, *updated_);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// DeleteMountTarget — DELETE /mount-targets/{mountTargetId}
class RGWDeleteMountTarget : public RGWOp {
 public:
  explicit RGWDeleteMountTarget(std::string mt_id)
    : mt_id_(std::move(mt_id)) {}

  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "delete_mount_target"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_DELETE_MOUNT_TARGET; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }

 private:
  std::string mt_id_;
  std::optional<StoreError> err_;
};

void RGWDeleteMountTarget::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  auto r = rgw::s3files::default_store().delete_mount_target(
      current_owner_account_id(s), mt_id_);
  if (!ok(r)) { err_ = error(r); op_ret = -ENOENT; return; }
  op_ret = 0;
}

void RGWDeleteMountTarget::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 204;
  dump_errno(s, 204);
  end_header(s, this, "application/json");
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// ListTagsForResource — GET /resource-tags/{resourceId}
class RGWListTagsForResource : public RGWOp {
 public:
  explicit RGWListTagsForResource(std::string resource_id)
    : resource_id_(std::move(resource_id)) {}

  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "list_tags_for_resource"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_LIST_TAGS_FOR_RESOURCE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

 private:
  std::string resource_id_;
  std::optional<PagedResult<Tag>> result_;
  std::optional<StoreError> err_;
};

void RGWListTagsForResource::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  ListOptions opts;
  if (!parse_max_results(s->info.args.get("MaxResults"),
                          opts.max_results, /*max=*/50)) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_MAX_RESULTS),
        .message = "MaxResults must be in 1..50",
    };
    op_ret = -EINVAL;
    return;
  }
  opts.next_token = s->info.args.get("NextToken");
  auto r = rgw::s3files::default_store().list_tags_for_resource(
      current_owner_account_id(s), strip_arn(resource_id_), opts);
  if (!ok(r)) { err_ = error(r); op_ret = -ENOENT; return; }
  result_ = take(std::move(r));
  op_ret = 0;
}

void RGWListTagsForResource::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  s->formatter->open_object_section("");
  encode_tags(s->formatter, result_->items);
  if (!result_->next_token.empty()) {
    encode_json("nextToken", result_->next_token, s->formatter);
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// TagResource — POST /resource-tags/{resourceId}
class RGWTagResource : public RGWOp {
 public:
  RGWTagResource(std::string resource_id, ceph::bufferlist body)
    : resource_id_(std::move(resource_id)), body_(std::move(body)) {}

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "tag_resource"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_TAG_RESOURCE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

 private:
  std::string resource_id_;
  std::vector<Tag> tags_;
  std::optional<StoreError> err_;
  ceph::bufferlist body_;
};

int RGWTagResource::init_processing(optional_yield y) {
  s->format = RGWFormat::JSON;
  // Body was pre-read in RGWRESTMgr_S3Files::get_handler so the
  // sigv4 PayloadHash arg is set before authorize() runs.
  ceph::bufferlist& body = body_;
  JSONParser parser;
  if (!parser.parse(body.c_str(), body.length())) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_POLICY_DOCUMENT),
        .message = "request body is not valid JSON",
    };
    return 0;
  }
  if (auto* arr = parser.find_obj("tags"); arr && arr->is_array()) {
    for (const auto& tag_str : arr->get_array_elements()) {
      JSONParser tp;
      if (!tp.parse(tag_str.c_str(), tag_str.size())) continue;
      tags_.push_back({json_string(&tp, "key"), json_string(&tp, "value")});
    }
  }
  return 0;
}

void RGWTagResource::execute(optional_yield y) {
  if (err_) { op_ret = -EINVAL; return; }
  auto r = rgw::s3files::default_store().tag_resource(
      current_owner_account_id(s), strip_arn(resource_id_), tags_);
  if (!ok(r)) { err_ = error(r); op_ret = -ECANCELED; return; }
  op_ret = 0;
}

void RGWTagResource::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// UntagResource — DELETE /resource-tags/{resourceId}?tagKeys=...
class RGWUntagResource : public RGWOp {
 public:
  explicit RGWUntagResource(std::string resource_id)
    : resource_id_(std::move(resource_id)) {}

  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "untag_resource"; }
  RGWOpType get_type() override { return RGW_OP_S3FILES_UNTAG_RESOURCE; }
  uint32_t op_mask() override { return RGW_OP_TYPE_DELETE; }

 private:
  std::string resource_id_;
  std::optional<StoreError> err_;
};

void RGWUntagResource::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  // tagKeys is `@httpQuery: tagKeys` per the Smithy. RGW's
  // args.get_params() returns a std::map<string,string> — each
  // key has at most one value, so multi-key untag would lose
  // values past the first. v1 supports the single-key form.
  std::vector<std::string> keys;
  if (auto v = s->info.args.get("tagKeys"); !v.empty()) {
    keys.push_back(v);
  }
  if (keys.empty()) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_TAG_KEY),
        .message = "tagKeys query parameter is required",
    };
    op_ret = -EINVAL;
    return;
  }
  auto r = rgw::s3files::default_store().untag_resource(
      current_owner_account_id(s), strip_arn(resource_id_), keys);
  if (!ok(r)) { err_ = error(r); op_ret = -ECANCELED; return; }
  op_ret = 0;
}

void RGWUntagResource::send_response() {
  if (err_) { send_store_error(s, this, *err_); return; }
  s->err.http_ret = 200;
  dump_errno(s, 200);
  end_header(s, this, "application/json");
  rgw_flush_formatter_and_reset(s, s->formatter);
}

}  // namespace

// ---------------------------------------------------------------- handler

int RGWHandler_REST_S3Files::init(
    rgw::sal::Driver* driver, req_state* s, rgw::io::BasicClient* cio) {
  s->dialect = "s3files";
  s->prot_flags = RGW_REST_S3FILES;
  s->format = RGWFormat::JSON;
  // The framework's formatter setup runs before handler init for
  // S3-style requests and won't have allocated one for restJson1.
  // Force a JSONFormatter so send_response() can write into it.
  if (!s->formatter) {
    s->formatter = new JSONFormatter(false /*pretty*/);
  }
  return RGWHandler_REST::init(driver, s, cio);
}

int RGWHandler_REST_S3Files::authorize(
    const DoutPrefixProvider* dpp, optional_yield y) {
  return RGW_Auth_S3::authorize(dpp, driver, auth_registry, s, y);
}

RGWOp* RGWHandler_REST_S3Files::op_get() {
  const auto segs = split_path(trim_leading_slashes(s->info.request_uri));
  if (segs.empty()) return nullptr;

  if (segs[0] == "file-systems") {
    if (segs.size() == 1) return new RGWListFileSystems();
    if (segs.size() == 2) return new RGWGetFileSystem(std::string(segs[1]));
    if (segs.size() == 3 && segs[2] == "policy") {
      return new RGWGetFileSystemPolicy(std::string(segs[1]));
    }
    if (segs.size() == 3 && segs[2] == "synchronization-configuration") {
      return new RGWGetSyncConfig(std::string(segs[1]));
    }
  }
  if (segs[0] == "access-points") {
    if (segs.size() == 1) return new RGWListAccessPoints();
    if (segs.size() == 2) return new RGWGetAccessPoint(std::string(segs[1]));
  }
  if (segs[0] == "mount-targets") {
    if (segs.size() == 1) return new RGWListMountTargets();
    if (segs.size() == 2) return new RGWGetMountTarget(std::string(segs[1]));
  }
  if (segs[0] == "resource-tags" && segs.size() == 2) {
    return new RGWListTagsForResource(std::string(segs[1]));
  }
  return nullptr;
}

RGWOp* RGWHandler_REST_S3Files::op_put() {
  const auto segs = split_path(trim_leading_slashes(s->info.request_uri));
  if (segs.empty()) return nullptr;

  if (segs[0] == "file-systems") {
    if (segs.size() == 1) return new RGWCreateFileSystem(std::move(body_));
    if (segs.size() == 3 && segs[2] == "policy") {
      return new RGWPutFileSystemPolicy(std::string(segs[1]), std::move(body_));
    }
    if (segs.size() == 3 && segs[2] == "synchronization-configuration") {
      return new RGWPutSyncConfig(std::string(segs[1]), std::move(body_));
    }
  }
  if (segs[0] == "access-points" && segs.size() == 1) {
    return new RGWCreateAccessPoint(std::move(body_));
  }
  if (segs[0] == "mount-targets") {
    if (segs.size() == 1) return new RGWCreateMountTarget(std::move(body_));
    if (segs.size() == 2) {
      return new RGWUpdateMountTarget(std::string(segs[1]), std::move(body_));
    }
  }
  return nullptr;
}

RGWOp* RGWHandler_REST_S3Files::op_post() {
  const auto segs = split_path(trim_leading_slashes(s->info.request_uri));
  if (segs.empty()) return nullptr;
  if (segs[0] == "resource-tags" && segs.size() == 2) {
    return new RGWTagResource(std::string(segs[1]), std::move(body_));
  }
  return nullptr;
}

RGWOp* RGWHandler_REST_S3Files::op_delete() {
  const auto segs = split_path(trim_leading_slashes(s->info.request_uri));
  if (segs.empty()) return nullptr;

  if (segs[0] == "file-systems") {
    if (segs.size() == 2) return new RGWDeleteFileSystem(std::string(segs[1]));
    if (segs.size() == 3 && segs[2] == "policy") {
      return new RGWDeleteFileSystemPolicy(std::string(segs[1]));
    }
  }
  if (segs[0] == "access-points" && segs.size() == 2) {
    return new RGWDeleteAccessPoint(std::string(segs[1]));
  }
  if (segs[0] == "mount-targets" && segs.size() == 2) {
    return new RGWDeleteMountTarget(std::string(segs[1]));
  }
  if (segs[0] == "resource-tags" && segs.size() == 2) {
    return new RGWUntagResource(std::string(segs[1]));
  }
  return nullptr;
}

// ---------------------------------------------------------------- mgr

RGWHandler_REST* RGWRESTMgr_S3Files::get_handler(
    rgw::sal::Driver* driver,
    req_state* const s,
    const rgw::auth::StrategyRegistry& auth_registry,
    const std::string& frontend_prefix) {
  // restJson1 services sign with service=s3files; their canonical
  // request includes the SHA256 of the body, but RGW's auth path
  // doesn't read it from a wire header. Pre-read here and stuff
  // the hash into args as `PayloadHash` (consumed by the auth
  // engine when is_non_s3_op() returns true for our op types).
  // The body is then handed off to the per-op handler so it
  // doesn't have to re-read it.
  // S3 and Swift handlers each call args.set+args.parse from their
  // own init_from_header to populate s->info.args from the query
  // string. The framework doesn't do it for us, so the args bag is
  // empty by default — and our ops would never see query params
  // like `?tagKeys=...`. Parse the query string here, before we
  // append PayloadHash (set() clears val_map).
  s->info.args.set(s->info.request_params);
  s->info.args.parse(s);

  // Set PayloadHash for every method, including GET/DELETE: the
  // client signs with SHA256("") for empty bodies, so the server
  // needs the same hash to construct the matching canonical
  // request. The body-read itself still only happens for
  // body-bearing methods.
  ceph::bufferlist body;
  if (s->info.method &&
      (std::strcmp(s->info.method, "PUT") == 0 ||
       std::strcmp(s->info.method, "POST") == 0)) {
    const auto max_size = s->cct->_conf->rgw_max_put_param_size;
    int rc;
    std::tie(rc, body) =
        rgw_rest_read_all_input(s, max_size, /*allow_chunked=*/false);
    if (rc < 0) {
      return nullptr;
    }
  }
  if (!s->info.args.exists("PayloadHash")) {
    const auto payload_hash =
        rgw::auth::s3::calc_v4_payload_hash(body.to_str());
    s->info.args.append("PayloadHash", payload_hash);
  }
  return new RGWHandler_REST_S3Files(auth_registry, std::move(body));
}
