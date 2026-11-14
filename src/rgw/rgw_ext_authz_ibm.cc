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

// IBM IAM provider for the RGW external authorizer.
//
// Wire protocol (see the IBM IAM PEP library and utilities/auth/auth.go):
//   POST {endpoint}/identity/token   form: grant_type=...apikey&apikey=<key>
//        -> {"access_token": "...", "expires_in": 3600}
//   POST {pep_endpoint or endpoint}/v2/authz
//        Authorization: Bearer <access token>, transaction-id: <corr id>
//        [{"action": "...",
//          "resource": {"attributes": {...}},
//          "subject": {"accessTokenBody": "<JWT payload segment>"}}]
//        -> {"responses": [{"authorizationDecision": {"permitted": true}}]}

#include <algorithm>
#include <charconv>
#include <fstream>
#include <mutex>
#include <sstream>

#include "common/Formatter.h"
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"
#include "common/dout.h"
#include "rgw_common.h"
#include "rgw_ext_authz.h"
#include "rgw_http_client.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::ext_authz::ibm {

namespace {

constexpr std::string_view provider_name = "ibm-iam";
constexpr std::string_view expected_service_name = "software-defined-storage";
// a response larger than this is not a valid answer from the services
constexpr size_t max_response_bytes = 1024 * 1024;
// renew the access token this long before it expires
constexpr int64_t token_renew_margin_secs = 60;

static inline std::vector<std::string_view> split(std::string_view s, char sep)
{
  std::vector<std::string_view> out;
  size_t pos = 0;
  while (true) {
    size_t next = s.find(sep, pos);
    if (next == std::string_view::npos) {
      out.push_back(s.substr(pos));
      return out;
    }
    out.push_back(s.substr(pos, next - pos));
    pos = next + 1;
  }
}

static inline std::string join_url(const std::string& base, std::string_view path)
{
  std::string url = base;
  while (!url.empty() && url.back() == '/') {
    url.pop_back();
  }
  url.append(path);
  return url;
}

static inline std::string trim(std::string s)
{
  const char* ws = " \t\r\n";
  s.erase(s.find_last_not_of(ws) + 1);
  s.erase(0, s.find_first_not_of(ws));
  return s;
}

} // anonymous namespace

bool parse_crn(std::string_view crn, Crn& out)
{
  const auto f = split(crn, ':');
  // crn:v1:<cname>:<ctype>:<service>:<location>:a/<account>:<guid>:<type>:<id>
  if (f.size() < 8 || f[0] != "crn" || f[1] != "v1") {
    return false;
  }
  constexpr std::string_view account_prefix = "a/";
  if (f[6].substr(0, account_prefix.size()) != account_prefix) {
    return false;
  }
  const auto account = f[6].substr(account_prefix.size());
  if (f[4].empty() || account.empty() || f[7].empty()) {
    return false;
  }
  out.service_name = std::string(f[4]);
  out.account_id = std::string(account);
  out.instance_id = std::string(f[7]);
  return true;
}

bool is_predefined_role(const RGWExtAuthzConfig& cfg, const Request& req)
{
  // an empty role name must not match an empty configured name
  return !req.role_name.empty() &&
      (req.role_name == cfg.readonly_role_name ||
       req.role_name == cfg.readwrite_role_name);
}

std::string select_action(const RGWExtAuthzConfig& cfg, const Request& req)
{
  // Decided only from the role RGW resolved, never from caller input, so the
  // standard actions cannot be used to assume a custom role.
  if (!req.role_name.empty()) {
    if (req.role_name == cfg.readonly_role_name) {
      return cfg.action_readonly;
    }
    if (req.role_name == cfg.readwrite_role_name) {
      return cfg.action_readwrite;
    }
  }
  return cfg.action_assume_role;
}

std::string_view jwt_payload(std::string_view token)
{
  // header.payload.signature, each non-empty
  const auto first = token.find('.');
  if (first == std::string_view::npos || first == 0) {
    return {};
  }
  const auto second = token.find('.', first + 1);
  if (second == std::string_view::npos || second == first + 1 ||
      second + 1 >= token.size() ||
      token.find('.', second + 1) != std::string_view::npos) {
    return {};
  }
  return token.substr(first + 1, second - first - 1);
}

std::string build_pep_request(const Crn& crn, std::string_view action,
                              const Request& req, bool scope_to_role)
{
  JSONFormatter f;
  f.open_array_section("");
  f.open_object_section("");
  f.dump_string("action", action);
  f.open_object_section("resource");
  f.open_object_section("attributes");
  f.dump_string("serviceName", crn.service_name);
  f.dump_string("serviceInstance", crn.instance_id);
  f.dump_string("accountId", crn.account_id);
  f.dump_string("objectAccount", req.account_id);
  if (scope_to_role) {
    // scopes the authorization to this one role
    f.dump_string("objectRole", req.role_arn);
  }
  f.close_section(); // attributes
  f.close_section(); // resource
  f.open_object_section("subject");
  // IBM IAM expects the payload segment of the JWT, not the whole token
  f.dump_string("accessTokenBody", jwt_payload(req.subject_token));
  f.close_section(); // subject
  f.close_section(); // request
  f.close_section(); // array
  std::stringstream ss;
  f.flush(ss);
  return ss.str();
}

int parse_pep_response(std::string_view body, bool& permitted)
{
  JSONParser parser;
  if (!parser.parse(body.data(), body.size()) || !parser.is_object()) {
    return -EBADMSG;
  }
  JSONObj* responses = parser.find_obj("responses");
  if (!responses || !responses->is_array()) {
    return -EBADMSG;
  }
  JSONObj* only = nullptr;
  int n = 0;
  for (auto it = responses->find_first(); !it.end(); ++it) {
    only = *it;
    ++n;
  }
  if (n != 1) {
    return -EBADMSG;
  }
  JSONObj* decision = only->find_obj("authorizationDecision");
  if (!decision || !decision->is_object()) {
    return -EBADMSG;
  }
  JSONObj* p = decision->find_obj("permitted");
  if (!p) {
    return -EBADMSG;
  }
  const auto& v = p->get_data_val();
  if (v.quoted) { // the string "true" is not a boolean
    return -EBADMSG;
  }
  if (v.str == "true") {
    permitted = true;
  } else if (v.str == "false") {
    permitted = false;
  } else {
    return -EBADMSG;
  }
  return 0;
}

int parse_token_response(std::string_view body, std::string& token,
                         int64_t& expires_in)
{
  JSONParser parser;
  if (!parser.parse(body.data(), body.size()) || !parser.is_object()) {
    return -EBADMSG;
  }
  JSONObj* t = parser.find_obj("access_token");
  JSONObj* e = parser.find_obj("expires_in");
  if (!t || !e || !t->get_data_val().quoted || t->get_data().empty()) {
    return -EBADMSG;
  }
  const std::string& s = e->get_data();
  int64_t secs = 0;
  auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), secs);
  if (ec != std::errc() || ptr != s.data() + s.size() || secs <= 0) {
    return -EBADMSG;
  }
  token = t->get_data();
  expires_in = secs;
  return 0;
}

namespace {

class Provider final : public rgw::ext_authz::Provider {
  // cached service access token. The mutex only guards the fields and is
  // never held while waiting for the network. Concurrent refreshes are
  // harmless: the last writer wins.
  struct CachedToken {
    std::string key; // endpoint and credential the token was issued for
    std::string token;
    ceph::mono_time expires;
  };
  ceph::mutex lock = ceph::make_mutex("rgw::ext_authz::ibm::Provider");
  CachedToken cached;

  static std::string cache_key(const std::string& endpoint,
                               const std::string& credential) {
    return endpoint + '\n' + credential;
  }

  bool get_cached(const std::string& key, std::string& token) {
    std::lock_guard l{lock};
    if (cached.token.empty() || cached.key != key ||
        ceph::mono_clock::now() >= cached.expires) {
      return false;
    }
    token = cached.token;
    return true;
  }

  void put_cached(const std::string& key, const std::string& token,
                  int64_t expires_in) {
    const int64_t life = expires_in - token_renew_margin_secs;
    std::lock_guard l{lock};
    cached.key = key;
    cached.token = token;
    cached.expires = ceph::mono_clock::now() + std::chrono::seconds(
        life > 0 ? life : 0);
  }

  // Forget the cached token, but only if it is the one that was rejected: a
  // concurrent request may already have replaced it with a good one.
  void drop_cached(const std::string& rejected) {
    std::lock_guard l{lock};
    if (cached.token == rejected) {
      cached = CachedToken{};
    }
  }

  // The HTTP client maps a 401 reply to -EPERM, which the caller reads as an
  // explicit Deny. A failed request is never a policy decision, so report
  // it as an error instead.
  static int transport_error(int r) {
    return r == -EPERM ? -EACCES : r;
  }

  // Log the start of a failed reply so the remote error text is visible.
  // Control characters are replaced so a reply cannot forge log lines.
  static void log_error_body(const DoutPrefixProvider* dpp, const char* what,
                             const bufferlist& resp) {
    constexpr size_t max_logged_bytes = 512;
    if (resp.length() == 0) {
      ldpp_dout(dpp, 1) << "ext_authz: " << what << " reply body is empty"
          << dendl;
      return;
    }
    std::string body = resp.to_str();
    const bool truncated = body.size() > max_logged_bytes;
    body.resize(std::min(body.size(), max_logged_bytes));
    for (auto& c : body) {
      if (static_cast<unsigned char>(c) < 0x20 || c == 0x7f) {
        c = ' ';
      }
    }
    ldpp_dout(dpp, 1) << "ext_authz: " << what << " reply body"
        << (truncated ? " (truncated)" : "") << ": " << body << dendl;
  }

  static void configure(RGWHTTPClient& req, const RGWExtAuthzConfig& cfg) {
    req.set_verify_ssl(true); // never optional
    req.set_req_connect_timeout(cfg.connect_timeout_secs);
    req.set_req_timeout(cfg.request_timeout_secs);
    if (!cfg.tls_ca_bundle.empty()) {
      req.set_ca_blob(cfg.tls_ca_bundle);
    }
  }

  static int load_credential(const DoutPrefixProvider* dpp,
                             const RGWExtAuthzConfig& cfg,
                             std::string& credential) {
    if (!cfg.service_credential_file.empty()) {
      std::ifstream in(cfg.service_credential_file);
      std::stringstream ss;
      if (!in || !(ss << in.rdbuf())) {
        ldpp_dout(dpp, 0) << "ERROR: ext_authz: cannot read "
            "service_credential_file" << dendl;
        return -EIO;
      }
      credential = trim(ss.str());
    } else {
      credential = cfg.service_credential;
    }
    if (credential.empty() || credential == RGWExtAuthzConfig::redacted) {
      ldpp_dout(dpp, 0) << "ERROR: ext_authz: no service credential" << dendl;
      return -EINVAL;
    }
    return 0;
  }

  int fetch_token(const DoutPrefixProvider* dpp, optional_yield y,
                  const RGWExtAuthzConfig& cfg, const std::string& credential,
                  const std::string& key, std::string& token) {
    bufferlist resp;
    RGWHTTPTransceiver req(dpp->get_cct(), "POST",
                           join_url(cfg.endpoint, "/identity/token"), &resp);
    configure(req, cfg);
    req.append_header("Content-Type", "application/x-www-form-urlencoded");
    req.append_header("Accept", "application/json");
    const std::string form =
        "grant_type=" + url_encode("urn:ibm:params:oauth:grant-type:apikey") +
        "&apikey=" + url_encode(credential);
    req.set_post_data(form);
    req.set_send_length(form.size());

    int r = req.process(dpp, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: ext_authz: token request failed r=" << r
          << " http_status=" << req.get_http_status() << dendl;
      log_error_body(dpp, "token", resp);
      return transport_error(r);
    }
    if (resp.length() > max_response_bytes) {
      return -EBADMSG;
    }
    int64_t expires_in = 0;
    r = parse_token_response(std::string_view(resp.c_str(), resp.length()),
                             token, expires_in);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: ext_authz: malformed token response"
          << dendl;
      return r;
    }
    put_cached(key, token, expires_in);
    return 0;
  }

  // One POST to /v2/authz. Returns the HTTP client result and sets 'status'.
  int call_authz(const DoutPrefixProvider* dpp, optional_yield y,
                 const RGWExtAuthzConfig& cfg, const Request& req,
                 const std::string& token, const std::string& body,
                 bufferlist& resp, int& status) {
    RGWHTTPTransceiver http(
        dpp->get_cct(), "POST",
        join_url(cfg.pep_endpoint.empty() ? cfg.endpoint : cfg.pep_endpoint,
                 "/v2/authz"),
        &resp);
    configure(http, cfg);
    http.append_header("Authorization", "Bearer " + token);
    http.append_header("Content-Type", "application/json");
    http.append_header("Accept", "application/json");
    http.append_header("transaction-id", req.correlation_id);
    http.set_post_data(body);
    http.set_send_length(body.size());

    int r = http.process(dpp, y);
    status = http.get_http_status();
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: ext_authz: authz request failed r=" << r
          << " http_status=" << status << dendl;
      log_error_body(dpp, "authz", resp);
    }
    return r;
  }

 public:
  std::string_view name() const override { return provider_name; }

  int validate_config(const RGWExtAuthzConfig& cfg,
                      std::string& err) const override {
    Crn crn;
    if (!parse_crn(cfg.service_instance, crn)) {
      err = "service_instance must be a CRN: "
            "crn:v1:<cname>:<ctype>:<service>:<location>:a/<account>:<guid>::";
      return -EINVAL;
    }
    if (crn.service_name != expected_service_name) {
      err = "service_instance must be a " + std::string(expected_service_name)
          + " CRN";
      return -EINVAL;
    }
    return 0;
  }

  int authorize(const DoutPrefixProvider* dpp, optional_yield y,
                const RGWExtAuthzConfig& cfg, const Request& req,
                std::string& action) override {
    if (req.request_type != "AssumeRoleWithWebIdentity") {
      ldpp_dout(dpp, 0) << "ERROR: ext_authz: unsupported request type"
          << dendl;
      return -ENOTSUP;
    }
    if (req.subject_token.empty() || req.account_id.empty() ||
        req.role_arn.empty() || req.correlation_id.empty()) {
      ldpp_dout(dpp, 0) << "ERROR: ext_authz: incomplete request" << dendl;
      return -EINVAL;
    }
    if (jwt_payload(req.subject_token).empty()) {
      ldpp_dout(dpp, 0) << "ERROR: ext_authz: subject token is not a JWT"
          << dendl;
      return -EINVAL;
    }
    Crn crn;
    if (!parse_crn(cfg.service_instance, crn)) {
      return -EINVAL;
    }
    action = select_action(cfg, req);

    std::string credential;
    if (int r = load_credential(dpp, cfg, credential); r < 0) {
      return r;
    }
    const std::string key = cache_key(cfg.endpoint, credential);
    const std::string body = build_pep_request(crn, action, req,
                                              !is_predefined_role(cfg, req));
    std::string token;
    bool token_was_cached = get_cached(key, token);
    if (!token_was_cached) {
      if (int r = fetch_token(dpp, y, cfg, credential, key, token); r < 0) {
        return r;
      }
    }

    bufferlist resp;
    int status = 0;
    int r = call_authz(dpp, y, cfg, req, token, body, resp, status);
    if (status == 401) {
      drop_cached(token); // revoked or expired earlier than advertised
      // A cached token may have been revoked or rotated before its advertised
      // expiry, so get a new one and ask once more. A token fetched for this
      // request was just issued; a second one would not change the answer.
      if (token_was_cached) {
        ldpp_dout(dpp, 5) << "ext_authz: cached token rejected, "
            "retrying once with a new token" << dendl;
        std::string fresh;
        if (!get_cached(key, fresh)) { // another request may have renewed it
          if (int rt = fetch_token(dpp, y, cfg, credential, key, fresh);
              rt < 0) {
            return rt;
          }
        }
        resp.clear();
        status = 0;
        r = call_authz(dpp, y, cfg, req, fresh, body, resp, status);
        if (status == 401) {
          drop_cached(fresh);
        }
      }
    }
    if (r < 0) {
      return transport_error(r);
    }
    if (resp.length() > max_response_bytes) {
      return -EBADMSG;
    }
    bool permitted = false;
    r = parse_pep_response(std::string_view(resp.c_str(), resp.length()),
                           permitted);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: ext_authz: malformed authz response"
          << dendl;
      return r;
    }
    return permitted ? 0 : -EPERM;
  }
};

} // anonymous namespace

} // namespace rgw::ext_authz::ibm

namespace rgw::ext_authz {

std::unique_ptr<Provider> create_ibm_provider()
{
  return std::make_unique<ibm::Provider>();
}

} // namespace rgw::ext_authz
