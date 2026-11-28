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

#pragma once

#include <list>
#include <map>
#include <memory>
#include <string>
#include <string_view>

#include "common/async/yield_context.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "include/buffer.h"
#include "include/encoding.h"

class DoutPrefixProvider;
class RGWObjVersionTracker;
namespace rgw::sal { class Driver; }

/// Zone-wide configuration of the external authorizer that is consulted
/// before RGW issues temporary credentials from AssumeRoleWithWebIdentity.
/// Stored as a replicated metadata entry (section "ext_authz", key "default").
struct RGWExtAuthzConfig {
  static constexpr std::string_view redacted = "<redacted>";
  static constexpr uint32_t default_connect_timeout_secs = 2;
  static constexpr uint32_t default_request_timeout_secs = 5;
  static constexpr uint32_t max_timeout_secs = 60;
  // default IAM action names, until the service definition is final
  static constexpr std::string_view default_action_readonly =
      "software-defined-storage.s3.readonly";
  static constexpr std::string_view default_action_readwrite =
      "software-defined-storage.s3.readwrite";
  static constexpr std::string_view default_action_assume_role =
      "software-defined-storage.s3.assume-role";

  /// when true, every AssumeRoleWithWebIdentity requires an explicit Allow
  bool enabled = false;
  /// provider implementation, e.g. "ibm-iam"
  std::string provider;
  /// base URL of the identity service (token API), must be https
  std::string endpoint;
  /// optional base URL of the authorization (PEP) service, defaults to endpoint
  std::string pep_endpoint;
  /// API key used to obtain the access token that authenticates RGW
  std::string service_credential;
  /// optional path of a per-zone file holding the API key; wins over the
  /// inline credential so secrets can be provisioned per site
  std::string service_credential_file;
  /// provider specific identifier of the service instance (a CRN for ibm-iam)
  std::string service_instance;
  /// optional PEM contents of the CA bundle used to verify the services
  std::string tls_ca_bundle;
  uint32_t connect_timeout_secs = default_connect_timeout_secs;
  uint32_t request_timeout_secs = default_request_timeout_secs;
  /// names of the predefined roles that map to the standard actions
  std::string readonly_role_name;
  std::string readwrite_role_name;
  /// IAM actions checked for the readonly role, the readwrite role and any
  /// other role. They must be non-empty and differ from each other.
  std::string action_readonly = std::string(default_action_readonly);
  std::string action_readwrite = std::string(default_action_readwrite);
  std::string action_assume_role = std::string(default_action_assume_role);

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(enabled, bl);
    encode(provider, bl);
    encode(endpoint, bl);
    encode(pep_endpoint, bl);
    encode(service_credential, bl);
    encode(service_credential_file, bl);
    encode(service_instance, bl);
    encode(tls_ca_bundle, bl);
    encode(connect_timeout_secs, bl);
    encode(request_timeout_secs, bl);
    encode(readonly_role_name, bl);
    encode(readwrite_role_name, bl);
    encode(action_readonly, bl);
    encode(action_readwrite, bl);
    encode(action_assume_role, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(enabled, bl);
    decode(provider, bl);
    decode(endpoint, bl);
    decode(pep_endpoint, bl);
    decode(service_credential, bl);
    decode(service_credential_file, bl);
    decode(service_instance, bl);
    decode(tls_ca_bundle, bl);
    decode(connect_timeout_secs, bl);
    decode(request_timeout_secs, bl);
    decode(readonly_role_name, bl);
    decode(readwrite_role_name, bl);
    decode(action_readonly, bl);
    decode(action_readwrite, bl);
    decode(action_assume_role, bl);
    DECODE_FINISH(bl);
  }

  /// dump as json. With redact=true the service credential is not exposed.
  void dump(Formatter* f, bool redact = false) const;
  void decode_json(JSONObj* obj);

  /// provider independent sanity checks, returns 0 or -EINVAL with err set
  int validate(std::string& err) const;
};
WRITE_CLASS_ENCODER(RGWExtAuthzConfig)

namespace rgw::ext_authz {

/// A role carrying this tag requires the external authorizer named by the
/// tag value (a provider name such as "ibm-iam"). Unless that provider is the
/// configured and enabled one, the role cannot be assumed, so removing,
/// disabling or mistyping the authorizer never leaves the role open.
constexpr std::string_view required_tag_key = "rgw:ext-authz";

/// The facts RGW hands to an authorizer for one AssumeRoleWithWebIdentity.
struct Request {
  /// kind of STS request, "AssumeRoleWithWebIdentity"
  std::string request_type;
  /// the verified web identity token (a secret, never logged)
  std::string subject_token;
  /// verified identity claims
  std::string subject;
  std::string issuer;
  /// Object Account ID that owns the role
  std::string account_id;
  /// the role being assumed, as resolved by RGW
  std::string role_arn;
  std::string role_name;
  /// request correlation id
  std::string correlation_id;
  /// provider named by the role's required_tag_key tag, empty if untagged
  std::string required_provider;
  /// the tag is present but unusable: empty, or more than one distinct value
  bool authorizer_tag_invalid = false;
};

/// Set required_provider / authorizer_tag_invalid from the role's tags.
void apply_role_tags(Request& req,
                     const std::multimap<std::string, std::string>& tags);

/// A pluggable authorization provider. Implementations must be thread safe;
/// one instance serves concurrent requests.
class Provider {
 public:
  virtual ~Provider() = default;

  virtual std::string_view name() const = 0;

  /// provider specific config checks (beyond RGWExtAuthzConfig::validate)
  virtual int validate_config(const RGWExtAuthzConfig& cfg,
                              std::string& err) const = 0;

  /// Ask the service for a decision.
  /// returns 0 only for an explicit Allow, -EPERM for an explicit Deny, and
  /// another negative errno for anything else (the caller treats all non-zero
  /// results as Deny). 'action' is set to the mapped action for audit logs.
  virtual int authorize(const DoutPrefixProvider* dpp, optional_yield y,
                        const RGWExtAuthzConfig& cfg, const Request& req,
                        std::string& action) = 0;
};

/// The process wide provider instance for a name, or null if unknown.
Provider* get_provider(std::string_view name);

/// Create a new, independent IBM IAM provider (rgw_ext_authz_ibm.cc).
std::unique_ptr<Provider> create_ibm_provider();

/// Validate and persist the config. A credential equal to
/// RGWExtAuthzConfig::redacted keeps the stored credential, so the output of
/// a 'get' can be edited and submitted again.
int store_config(const DoutPrefixProvider* dpp, optional_yield y,
                 rgw::sal::Driver* driver, RGWExtAuthzConfig& cfg);

/// Authorize an AssumeRoleWithWebIdentity request. Returns 0 if no external
/// authorizer is enabled (and the role does not require one) or it allowed the
/// request, and -EPERM otherwise, which includes provider failures, timeouts,
/// invalid configuration, and a role that requires an authorizer that is
/// absent or disabled.
int authorize(const DoutPrefixProvider* dpp, optional_yield y,
              rgw::sal::Driver* driver, const Request& req);

} // namespace rgw::ext_authz

/// helpers of the ibm-iam provider exposed for unit tests
namespace rgw::ext_authz::ibm {

struct Crn {
  std::string service_name;
  std::string account_id;
  std::string instance_id;
};

/// Parse crn:v1:<cname>:<ctype>:<service>:<location>:a/<account>:<guid>::
bool parse_crn(std::string_view crn, Crn& out);

/// True if the role resolved by RGW is one of the configured predefined roles.
bool is_predefined_role(const RGWExtAuthzConfig& cfg, const Request& req);

/// Map the role resolved by RGW to the IAM action that authorizes assuming it.
/// Only the configured predefined roles map to the standard actions; every
/// other role requires the assume-role action.
std::string select_action(const RGWExtAuthzConfig& cfg, const Request& req);

/// The payload (middle) segment of a JWT, or empty if 'token' is not of the
/// form header.payload.signature. IBM IAM takes this as the subject.
std::string_view jwt_payload(std::string_view token);

/// JSON body of a PEP /v2/authz request. With scope_to_role the resource also
/// carries the role ARN, which is how a custom role is authorized individually.
std::string build_pep_request(const Crn& crn, std::string_view action,
                              const Request& req, bool scope_to_role);

/// Parse a PEP /v2/authz response. Returns 0 and sets 'permitted' if the
/// response holds exactly one well formed decision, -EBADMSG otherwise.
int parse_pep_response(std::string_view body, bool& permitted);

/// Parse the access token and its lifetime from an IAM token response.
int parse_token_response(std::string_view body, std::string& token,
                         int64_t& expires_in);

} // namespace rgw::ext_authz::ibm
