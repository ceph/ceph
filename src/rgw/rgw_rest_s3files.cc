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
  s->err.http_ret = status;
  dump_errno(s, status);
  end_header(s, op, "application/json");

  s->formatter->open_object_section("");
  encode_json("__type", std::string(exception_type_for(err.kind)),
              s->formatter);
  encode_json("errorCode", err.error_code, s->formatter);
  if (!err.message.empty()) {
    encode_json("message", err.message, s->formatter);
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

// ---- JSON read helpers ----------------------------------------

std::string json_string(const JSONObj* obj, std::string_view name) {
  if (!obj) return {};
  JSONObj* f = obj->find_obj(std::string(name));
  if (!f) return {};
  return f->get_data();
}

bool json_bool(const JSONObj* obj, std::string_view name) {
  if (!obj) return false;
  JSONObj* f = obj->find_obj(std::string(name));
  if (!f) return false;
  return f->get_data() == "true" || f->get_data() == "1";
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
std::string strip_arn(std::string_view in) {
  auto pos = in.rfind('/');
  if (pos == std::string_view::npos) return std::string(in);
  return std::string(in.substr(pos + 1));
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
    encode_json("Key",   t.key,   f);
    encode_json("Value", t.value, f);
    f->close_section();
  }
  f->close_section();
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
  RGWCreateFileSystem() = default;

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override { return 0; }
  void execute(optional_yield y) override;
  void send_response() override;

  const char* name() const override { return "create_file_system"; }
  RGWOpType get_type() override { return RGW_OP_UNKNOWN; }
  uint32_t op_mask() override { return RGW_OP_TYPE_WRITE; }

 private:
  CreateFileSystemRequest req_;
  std::optional<FileSystemView> created_;
  std::optional<StoreError> err_;
};

int RGWCreateFileSystem::init_processing(optional_yield y) {
  s->format = RGWFormat::JSON;

  const auto max = s->cct->_conf->rgw_max_put_param_size;
  auto [rc, body] = rgw_rest_read_all_input(s, max);
  if (rc < 0) return rc;

  JSONParser parser;
  if (!parser.parse(body.c_str(), body.length())) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_POLICY_DOCUMENT),
        .message = "request body is not valid JSON",
    };
    return -EINVAL;
  }

  req_.owner_account_id = current_owner_account_id(s);
  req_.bucket_arn = json_string(&parser, "bucket");
  req_.prefix = json_string(&parser, "prefix");
  req_.role_arn = json_string(&parser, "roleArn");
  req_.kms_key_id = json_string(&parser, "kmsKeyId");
  req_.client_token = json_string(&parser, "clientToken");
  req_.accept_bucket_warning = json_bool(&parser, "acceptBucketWarning");

  if (JSONObj* tags = parser.find_obj("tags");
      tags && tags->is_array()) {
    for (const auto& tag_str : tags->get_array_elements()) {
      JSONParser tag_parser;
      if (!tag_parser.parse(tag_str.c_str(), tag_str.size())) continue;
      Tag tag;
      tag.key = json_string(&tag_parser, "Key");
      tag.value = json_string(&tag_parser, "Value");
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
  RGWOpType get_type() override { return RGW_OP_UNKNOWN; }
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
  RGWOpType get_type() override { return RGW_OP_UNKNOWN; }
  uint32_t op_mask() override { return RGW_OP_TYPE_READ; }

 private:
  std::optional<PagedResult<FileSystemView>> result_;
  std::optional<StoreError> err_;
};

void RGWListFileSystems::execute(optional_yield y) {
  s->format = RGWFormat::JSON;
  ListOptions opts;
  if (auto v = s->info.args.get("maxResults"); !v.empty()) {
    try { opts.max_results = std::stoi(v); } catch (...) {}
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
  RGWOpType get_type() override { return RGW_OP_UNKNOWN; }
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

}  // namespace

// ---------------------------------------------------------------- handler

int RGWHandler_REST_S3Files::init(
    rgw::sal::Driver* driver, req_state* s, rgw::io::BasicClient* cio) {
  s->dialect = "s3files";
  s->prot_flags = RGW_REST_S3FILES;
  s->format = RGWFormat::JSON;
  return RGWHandler_REST::init(driver, s, cio);
}

int RGWHandler_REST_S3Files::authorize(
    const DoutPrefixProvider* dpp, optional_yield y) {
  return RGW_Auth_S3::authorize(dpp, driver, auth_registry, s, y);
}

RGWOp* RGWHandler_REST_S3Files::op_get() {
  const auto segs = split_path(trim_leading_slashes(s->info.request_uri));
  if (segs.empty()) return nullptr;

  // /file-systems[/<id>[/policy|/synchronization-configuration]]
  if (segs[0] == "file-systems") {
    if (segs.size() == 1) return new RGWListFileSystems();
    if (segs.size() == 2) return new RGWGetFileSystem(std::string(segs[1]));
    // /file-systems/<id>/policy and /synchronization-configuration
    // are wired in follow-on commits.
  }
  return nullptr;
}

RGWOp* RGWHandler_REST_S3Files::op_put() {
  const auto segs = split_path(trim_leading_slashes(s->info.request_uri));
  if (segs.empty()) return nullptr;

  if (segs[0] == "file-systems" && segs.size() == 1) {
    return new RGWCreateFileSystem();
  }

  // /access-points, /mount-targets, /file-systems/<id>/{policy,sync},
  // /mount-targets/<id> (UpdateMountTarget) wire in follow-on commits.
  return nullptr;
}

RGWOp* RGWHandler_REST_S3Files::op_post() {
  // /resource-tags/<id> → TagResource. Wired in a follow-on commit.
  return nullptr;
}

RGWOp* RGWHandler_REST_S3Files::op_delete() {
  const auto segs = split_path(trim_leading_slashes(s->info.request_uri));
  if (segs.empty()) return nullptr;

  if (segs[0] == "file-systems") {
    if (segs.size() == 2) return new RGWDeleteFileSystem(std::string(segs[1]));
    // /file-systems/<id>/policy → DeleteFileSystemPolicy: follow-on
  }
  // /access-points/<id>, /mount-targets/<id>, /resource-tags/<id>
  // also follow.
  return nullptr;
}

// ---------------------------------------------------------------- mgr

RGWHandler_REST* RGWRESTMgr_S3Files::get_handler(
    rgw::sal::Driver* driver,
    req_state* const s,
    const rgw::auth::StrategyRegistry& auth_registry,
    const std::string& frontend_prefix) {
  return new RGWHandler_REST_S3Files(auth_registry);
}
