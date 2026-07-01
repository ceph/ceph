// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include <string_view>
#include "common/ceph_time.h"
#include "rgw_sal_fwd.h"

namespace rgw::bucketlogging {

// Per-record input metadata for log_record(). The req_state* overloads populate
// this from the request state; no-req_state callers (e.g. the lifecycle thread)
// build it directly. Non-owning views point into request-lifetime storage.
struct record_input {
  rgw::sal::Bucket* bucket{nullptr};
  // Requester field for Standard records. HTTP callers: assumed-role ARN,
  // account name, or user id. LC callers: the source bucket's owner.
  // std::string (not view) because the populate code builds it in place.
  std::string user_or_account;
  ceph::coarse_real_time time;
  // Object's version id for the record. HTTP callers derive this from the
  // request's object; LC callers pass the pre-deletion instance.
  std::string_view version_id;
  // Where to write the Journal-mode error message on failure. HTTP callers
  // point at s->err.message; no-req_state callers leave it null.
  std::string* journal_err_out{nullptr};

  // Standard-mode-only fields. Views point at request-lifetime storage in
  // req_state / env_map. The four std::string fields below are built in
  // place by the populate code and therefore cannot be views.
  std::chrono::milliseconds time_elapsed{};
  std::string_view remote_addr;
  std::string_view trans_id;
  std::string_view method;
  std::string_view request_uri;
  std::string_view request_params;
  std::string_view referrer;
  std::string_view user_agent;
  std::string_view ssl_cipher;
  std::string_view tls_version;
  std::string fqdn;          // appended in place with the domain suffix
  std::string_view x_amz_id_2;
  std::string aws_version;   // written by get_aws_version_and_auth_type
  std::string auth_type;     // written by get_aws_version_and_auth_type
  int http_ret{0};
  std::string_view err_code;
  size_t content_length{0};
  bool granted_by_acl{false};

  // For COPY operations (used only with log_source_bucket=true, Standard mode).
  const sal::Object* src_object{nullptr};
  std::string_view src_bucket_name;
};

} // namespace rgw::bucketlogging
