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

#include "rgw_ext_authz.h"

#include <chrono>
#include <format>

#include "common/Formatter.h"
#include "common/ceph_time.h"
#include "common/dout.h"
#include "rgw_common.h"
#include "rgw_perf_counters.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

void RGWExtAuthzConfig::dump(Formatter* f, bool redact) const
{
  encode_json("enabled", enabled, f);
  encode_json("provider", provider, f);
  encode_json("endpoint", endpoint, f);
  encode_json("pep_endpoint", pep_endpoint, f);
  if (redact && !service_credential.empty()) {
    encode_json("service_credential", std::string(redacted), f);
  } else {
    encode_json("service_credential", service_credential, f);
  }
  encode_json("service_credential_file", service_credential_file, f);
  encode_json("service_instance", service_instance, f);
  encode_json("tls_ca_bundle", tls_ca_bundle, f);
  encode_json("connect_timeout_secs", connect_timeout_secs, f);
  encode_json("request_timeout_secs", request_timeout_secs, f);
  encode_json("readonly_role_name", readonly_role_name, f);
  encode_json("readwrite_role_name", readwrite_role_name, f);
  encode_json("action_readonly", action_readonly, f);
  encode_json("action_readwrite", action_readwrite, f);
  encode_json("action_assume_role", action_assume_role, f);
}

void RGWExtAuthzConfig::decode_json(JSONObj* obj)
{
  JSONDecoder::decode_json("enabled", enabled, obj);
  JSONDecoder::decode_json("provider", provider, obj);
  JSONDecoder::decode_json("endpoint", endpoint, obj);
  JSONDecoder::decode_json("pep_endpoint", pep_endpoint, obj);
  JSONDecoder::decode_json("service_credential", service_credential, obj);
  JSONDecoder::decode_json("service_credential_file",
                           service_credential_file, obj);
  JSONDecoder::decode_json("service_instance", service_instance, obj);
  JSONDecoder::decode_json("tls_ca_bundle", tls_ca_bundle, obj);
  // absent timeouts fall back to the defaults
  JSONDecoder::decode_json("connect_timeout_secs", connect_timeout_secs,
                           default_connect_timeout_secs, obj);
  JSONDecoder::decode_json("request_timeout_secs", request_timeout_secs,
                           default_request_timeout_secs, obj);
  JSONDecoder::decode_json("readonly_role_name", readonly_role_name, obj);
  JSONDecoder::decode_json("readwrite_role_name", readwrite_role_name, obj);
  // absent actions fall back to the defaults
  JSONDecoder::decode_json("action_readonly", action_readonly,
                           std::string(default_action_readonly), obj);
  JSONDecoder::decode_json("action_readwrite", action_readwrite,
                           std::string(default_action_readwrite), obj);
  JSONDecoder::decode_json("action_assume_role", action_assume_role,
                           std::string(default_action_assume_role), obj);
}

static bool is_https_url(const std::string& url)
{
  std::string_view u = url;
  constexpr std::string_view scheme = "https://";
  return u.size() > scheme.size() && u.substr(0, scheme.size()) == scheme;
}

int RGWExtAuthzConfig::validate(std::string& err) const
{
  if (provider.empty()) {
    err = "provider is required";
    return -EINVAL;
  }
  if (!is_https_url(endpoint)) {
    err = "endpoint is required and must be an https:// URL";
    return -EINVAL;
  }
  if (!pep_endpoint.empty() && !is_https_url(pep_endpoint)) {
    err = "pep_endpoint must be an https:// URL";
    return -EINVAL;
  }
  if (service_credential.empty() && service_credential_file.empty()) {
    err = "service_credential or service_credential_file is required";
    return -EINVAL;
  }
  if (connect_timeout_secs == 0 || connect_timeout_secs > max_timeout_secs ||
      request_timeout_secs == 0 || request_timeout_secs > max_timeout_secs) {
    err = "timeouts must be between 1 and " + std::to_string(max_timeout_secs)
        + " seconds";
    return -EINVAL;
  }
  if (!readonly_role_name.empty() && readonly_role_name == readwrite_role_name) {
    err = "readonly_role_name and readwrite_role_name must differ";
    return -EINVAL;
  }
  if (action_readonly.empty() || action_readwrite.empty() ||
      action_assume_role.empty()) {
    err = "the actions must not be empty";
    return -EINVAL;
  }
  if (action_readonly == action_readwrite ||
      action_readonly == action_assume_role ||
      action_readwrite == action_assume_role) {
    // equal actions would let a read-only grant assume a read/write or
    // custom role
    err = "action_readonly, action_readwrite and action_assume_role must differ";
    return -EINVAL;
  }
  return 0;
}

namespace rgw::ext_authz {

Provider* get_provider(std::string_view name)
{
  // one long lived instance, shared by all requests, so that e.g. the
  // access token cache is shared
  static const std::unique_ptr<Provider> ibm = create_ibm_provider();
  if (name == ibm->name()) {
    return ibm.get();
  }
  return nullptr;
}

int store_config(const DoutPrefixProvider* dpp, optional_yield y,
                 rgw::sal::Driver* driver, RGWExtAuthzConfig& cfg)
{
  std::string err;
  if (int r = cfg.validate(err); r < 0) {
    ldpp_dout(dpp, 1) << "ext_authz: invalid config: " << err << dendl;
    return r;
  }
  auto provider = get_provider(cfg.provider);
  if (!provider) {
    ldpp_dout(dpp, 1) << "ext_authz: unknown provider '" << cfg.provider
        << "'" << dendl;
    return -EINVAL;
  }
  if (int r = provider->validate_config(cfg, err); r < 0) {
    ldpp_dout(dpp, 1) << "ext_authz: invalid config: " << err << dendl;
    return r;
  }

  RGWObjVersionTracker objv;
  RGWExtAuthzConfig existing;
  int r = driver->load_ext_authz_config(dpp, y, existing, &objv);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  // the output of 'get' hides the credential. Accept it back unchanged.
  if (cfg.service_credential == RGWExtAuthzConfig::redacted) {
    cfg.service_credential = r == 0 ? existing.service_credential : "";
    if (cfg.service_credential.empty() && cfg.service_credential_file.empty()) {
      ldpp_dout(dpp, 1) << "ext_authz: no stored credential to keep" << dendl;
      return -EINVAL;
    }
  }
  return driver->store_ext_authz_config(dpp, y, cfg, r == 0 ? &objv : nullptr);
}

namespace {

static const char* outcome_str(int r)
{
  if (r == 0) return "allow";
  if (r == -EPERM) return "deny";
  return "error";
}

// one audit line per decision. Contains no credential, token or claim values
// other than the identity the decision was made for.
static void audit(const DoutPrefixProvider* dpp, const Request& req,
                  const std::string& action, int r, double latency_ms,
                  std::string_view note)
{
  ldpp_dout(dpp, 1) << "ext_authz audit: decision=" << outcome_str(r)
      << " request_type=" << req.request_type
      << " corr_id=" << req.correlation_id
      << " sub=" << req.subject << " iss=" << req.issuer
      << " account=" << req.account_id << " role=" << req.role_arn
      << " action=" << action
      << " latency_ms=" << std::format("{:.3f}", latency_ms)
      << " result=" << r << (note.empty() ? "" : " note=") << note << dendl;
}

static void count(int r)
{
  if (!perfcounter) {
    return;
  }
  perfcounter->inc(l_rgw_ext_authz_req);
  if (r == 0) {
    perfcounter->inc(l_rgw_ext_authz_allow);
  } else if (r == -EPERM) {
    perfcounter->inc(l_rgw_ext_authz_deny);
  } else {
    perfcounter->inc(l_rgw_ext_authz_error);
  }
}

} // anonymous namespace

void apply_role_tags(Request& req,
                     const std::multimap<std::string, std::string>& tags)
{
  req.required_provider.clear();
  req.authorizer_tag_invalid = false;
  const auto range = tags.equal_range(std::string(required_tag_key));
  for (auto i = range.first; i != range.second; ++i) {
    // An empty value, or two different values, cannot name one provider. The
    // role then cannot be assumed, so a typo never drops the requirement.
    if (i->second.empty() ||
        (!req.required_provider.empty() && req.required_provider != i->second)) {
      req.authorizer_tag_invalid = true;
    }
    req.required_provider = i->second;
  }
}

int authorize(const DoutPrefixProvider* dpp, optional_yield y,
              rgw::sal::Driver* driver, const Request& req)
{
  if (req.authorizer_tag_invalid) {
    ldpp_dout(dpp, 0) << "ERROR: ext_authz: role tag " << required_tag_key
        << " must name exactly one provider" << dendl;
    count(-EINVAL);
    audit(dpp, req, "", -EINVAL, 0, "invalid role tag");
    return -EPERM;
  }
  const bool tagged = !req.required_provider.empty();

  RGWExtAuthzConfig cfg;
  int r = driver->load_ext_authz_config(dpp, y, cfg, nullptr);
  const bool not_configured =
      r == -ENOENT || r == -ENOTSUP || (r == 0 && !cfg.enabled);
  if (not_configured) {
    if (!tagged) {
      return 0; // no external authorizer, STS is unchanged
    }
    // the role names an authorizer that must decide, and there is none
    ldpp_dout(dpp, 0) << "ERROR: ext_authz: role requires authorizer "
        << req.required_provider << " but none is enabled" << dendl;
    count(-ENOENT);
    audit(dpp, req, "", -ENOENT, 0, "required authorizer not enabled");
    return -EPERM;
  }
  if (r == 0 && tagged && cfg.provider != req.required_provider) {
    ldpp_dout(dpp, 0) << "ERROR: ext_authz: role requires authorizer "
        << req.required_provider << " but the enabled one is "
        << cfg.provider << dendl;
    count(-EINVAL);
    audit(dpp, req, "", -EINVAL, 0, "required authorizer differs");
    return -EPERM;
  }
  if (r < 0) {
    // we cannot tell whether an authorizer is required, so do not issue
    ldpp_dout(dpp, 0) << "ERROR: ext_authz: failed to read the config: "
        << r << dendl;
    count(r);
    audit(dpp, req, "", r, 0, "config unreadable");
    return -EPERM;
  }

  const auto start = ceph::mono_clock::now();
  std::string action;
  std::string note;

  std::string err;
  auto provider = get_provider(cfg.provider);
  if (!provider) {
    r = -EINVAL;
    note = "unknown provider";
  } else if (cfg.validate(err) < 0 ||
             provider->validate_config(cfg, err) < 0) {
    r = -EINVAL;
    note = "invalid config";
  } else {
    try {
      r = provider->authorize(dpp, y, cfg, req, action);
    } catch (const std::exception& e) {
      r = -EIO;
      note = "exception";
      ldpp_dout(dpp, 0) << "ERROR: ext_authz: provider threw: " << e.what()
          << dendl;
    }
  }
  // only an explicit Allow lets the request through
  if (r > 0) {
    r = -EIO;
  }

  const auto elapsed = ceph::mono_clock::now() - start;
  if (perfcounter) {
    perfcounter->tinc(l_rgw_ext_authz_lat, elapsed);
  }
  count(r);
  audit(dpp, req, action, r,
        std::chrono::duration<double, std::milli>(elapsed).count(), note);

  return r == 0 ? 0 : -EPERM;
}

} // namespace rgw::ext_authz
