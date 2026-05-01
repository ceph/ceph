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

// Map a StoreError kind to the corresponding HTTP status code per
// the AWS Smithy `@httpError` traits.
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

// Map a StoreError kind to the AWS Smithy exception shape name.
// Used as the `__type` discriminator in the restJson1 error envelope.
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

// Extract a string field from a parsed JSON object; returns empty
// string if absent.
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

}  // namespace

// ---------------------------------------------------------------- ops

namespace {

// CreateFileSystem — PUT /file-systems
//
// Smithy: com.amazonaws.s3files#CreateFileSystem.
// Required: bucket, roleArn. Optional: prefix, kmsKeyId,
// clientToken, tags, acceptBucketWarning. Response: 201 with the
// created FileSystem fields.
class RGWCreateFileSystem : public RGWOp {
 public:
  RGWCreateFileSystem() = default;

  int init_processing(optional_yield y) override;
  int verify_permission(optional_yield y) override { return 0; }  // #11 wires IAM
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
  // Force JSON formatting for the response.
  s->format = RGWFormat::JSON;

  // Read the JSON body.
  const auto max = s->cct->_conf->rgw_max_put_param_size;
  auto [rc, body] = rgw_rest_read_all_input(s, max);
  if (rc < 0) {
    return rc;
  }

  JSONParser parser;
  if (!parser.parse(body.c_str(), body.length())) {
    err_ = StoreError{
        .kind = StoreError::Kind::InvalidArgument,
        .error_code = std::string(ERR_INVALID_POLICY_DOCUMENT),
        .message = "request body is not valid JSON",
    };
    return -EINVAL;
  }

  req_.owner_account_id = s->user ? s->user->get_id().to_str() : std::string{};
  req_.bucket_arn = json_string(&parser, "bucket");
  req_.prefix = json_string(&parser, "prefix");
  req_.role_arn = json_string(&parser, "roleArn");
  req_.kms_key_id = json_string(&parser, "kmsKeyId");
  req_.client_token = json_string(&parser, "clientToken");
  req_.accept_bucket_warning = json_bool(&parser, "acceptBucketWarning");

  // tags: array of { Key, Value } objects. Use the existing
  // get_array_elements() + re-parse idiom (see rgw_rest_sts.cc).
  if (JSONObj* tags = parser.find_obj("tags");
      tags && tags->is_array()) {
    for (const auto& tag_str : tags->get_array_elements()) {
      JSONParser tag_parser;
      if (!tag_parser.parse(tag_str.c_str(), tag_str.size())) {
        continue;
      }
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
    op_ret = -ECANCELED;  // surface a non-zero code; send_response handles the body
    return;
  }
  created_ = take(std::move(result));
  op_ret = 0;
}

void RGWCreateFileSystem::send_response() {
  if (err_) {
    const auto status = http_status_for(err_->kind);
    s->err.http_ret = status;
    dump_errno(s, status);
    end_header(s, this, "application/json");

    s->formatter->open_object_section("");
    encode_json("__type", std::string(exception_type_for(err_->kind)),
                s->formatter);
    encode_json("errorCode", err_->error_code, s->formatter);
    if (!err_->message.empty()) {
      encode_json("message", err_->message, s->formatter);
    }
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
    return;
  }

  // 201 Created with the FileSystem JSON.
  const auto& fs = *created_;
  s->err.http_ret = 201;
  dump_errno(s, 201);
  end_header(s, this, "application/json");

  s->formatter->open_object_section("");
  encode_json("fileSystemId",   fs.spec.id,                   s->formatter);
  encode_json("fileSystemArn",  fs.spec.arn,                  s->formatter);
  encode_json("ownerId",        fs.spec.owner_account_id,     s->formatter);
  encode_json("bucket",         fs.spec.bucket_arn,           s->formatter);
  encode_json("prefix",         fs.spec.prefix,               s->formatter);
  encode_json("roleArn",        fs.spec.role_arn,             s->formatter);
  if (!fs.spec.kms_key_id.empty()) {
    encode_json("kmsKeyId",     fs.spec.kms_key_id,           s->formatter);
  }
  if (!fs.spec.client_token.empty()) {
    encode_json("clientToken",  fs.spec.client_token,         s->formatter);
  }
  encode_json("status",
              std::string(to_string(fs.status.state)),        s->formatter);
  encode_json("creationTime",   fs.spec.creation_time_unix_seconds,
              s->formatter);
  if (auto n = fs.name(); n) {
    encode_json("name",         *n,                           s->formatter);
  }
  s->formatter->open_array_section("tags");
  for (const auto& t : fs.spec.tags) {
    s->formatter->open_object_section("");
    encode_json("Key",   t.key,   s->formatter);
    encode_json("Value", t.value, s->formatter);
    s->formatter->close_section();
  }
  s->formatter->close_section();
  s->formatter->close_section();

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

RGWOp* RGWHandler_REST_S3Files::op_put() {
  // The manager is registered at one of the four top-level prefixes
  // (file-systems, access-points, mount-targets, resource-tags); the
  // request_uri here is the full path including that prefix.
  const auto& uri = s->info.request_uri;

  // Strip leading slash and any frontend prefix for matching.
  std::string_view path = uri;
  while (!path.empty() && path.front() == '/') path.remove_prefix(1);

  // PUT /file-systems → CreateFileSystem
  if (path == "file-systems") {
    return new RGWCreateFileSystem();
  }

  // Other PUT routes (PutFileSystemPolicy, PutSynchronizationConfiguration,
  // CreateAccessPoint, CreateMountTarget, UpdateMountTarget) are wired in
  // follow-on commits.
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
