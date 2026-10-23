// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <charconv>
#include <cctype>
#include <cstring>
#include <string>
#include <vector>
#include <openssl/crypto.h>
#include "json_spirit/json_spirit_reader.h"
#include "rapidjson/reader.h"
#include "rapidjson/memorystream.h"

#include <errno.h>
#include <fnmatch.h>

#include "rgw_b64.h"

#include "common/errno.h"
#include "common/ceph_json.h"
#include "include/types.h"
#include "include/timegm.h"
#include "include/str_list.h"

#include "rgw_common.h"
#include "rgw_keystone.h"
#include "rgw_keystone_scope.h"
#include "rgw_auth_keystone.h"
#include "rgw_rest_s3.h"
#include "rgw_auth_s3.h"

#include "common/ceph_crypto.h"
#include "common/Cond.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw {
namespace auth {
namespace keystone {

boost::optional<SigningKeyCache::Value>
SigningKeyCache::find(const Key& key, time_t now, mono_time mono_now)
{
  std::lock_guard guard(mutex);
  auto i = entries.find(key);
  if (i == entries.end()) {
    return boost::none;
  }
  const auto& v = i->second.value;
  if (now >= v.wall_expires || now >= v.identity.get_expires() ||
      mono_now >= v.mono_expires) {
    lru.erase(i->second.lru);
    entries.erase(i);
    return boost::none;
  }
  lru.splice(lru.begin(), lru, i->second.lru);
  return i->second.value;
}

bool SigningKeyCache::insert(const Key& key, Value value,
                             time_t now, mono_time mono_now)
{
  std::lock_guard guard(mutex);
  if (!capacity || now >= value.wall_expires ||
      now >= value.identity.get_expires() || mono_now >= value.mono_expires) {
    return false;
  }
  if (auto i = entries.find(key); i != entries.end()) {
    lru.erase(i->second.lru);
    entries.erase(i);
  }
  lru.push_front(key);
  entries.emplace(key, Entry{std::move(value), lru.begin()});
  while (entries.size() > capacity) {
    entries.erase(lru.back());
    lru.pop_back();
  }
  return true;
}

size_t SigningKeyCache::size() const
{
  std::lock_guard guard(mutex);
  return entries.size();
}

namespace {

bool fields_are(JSONObj* obj, std::initializer_list<std::string_view> names)
{
  if (!obj || !obj->is_object()) {
    return false;
  }
  std::set<std::string> seen;
  for (auto i = obj->find_first(); !i.end(); ++i) {
    const auto& name = (*i)->get_name();
    if (std::find(names.begin(), names.end(), name) == names.end() ||
        !seen.insert(name).second) {
      return false;
    }
  }
  return seen.size() == names.size();
}

JSONObj* field(JSONObj* obj, const char* name)
{
  auto i = obj->find_first(name);
  return i.end() ? nullptr : *i;
}

bool string_field(JSONObj* obj, const char* name, std::string& out)
{
  auto f = field(obj, name);
  if (!f || !f->get_data_val().quoted || f->get_data().empty()) {
    return false;
  }
  out = f->get_data();
  return true;
}

bool signed_date(std::string_view input, time_t& value)
{
  if (input.size() != 16 || input[8] != 'T' || input[15] != 'Z') {
    return false;
  }
  for (size_t i = 0; i < input.size(); ++i) {
    if (i != 8 && i != 15 && (input[i] < '0' || input[i] > '9')) {
      return false;
    }
  }
  std::tm t{};
  std::string text(input);
  if (!parse_iso8601(text.c_str(), &t, nullptr, false)) {
    return false;
  }
  value = internal_timegm(&t);
  std::tm canonical{};
  gmtime_r(&value, &canonical);
  char buffer[32];
  return std::strftime(buffer, sizeof(buffer), "%Y%m%dT%H%M%SZ", &canonical) &&
         text == buffer;
}

bool unsigned_seconds(std::string_view text, uint64_t& value)
{
  if (text.empty() || !std::all_of(text.begin(), text.end(),
      [](char c) { return c >= '0' && c <= '9'; })) {
    return false;
  }
  auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  return error == std::errc{} && end == text.data() + text.size();
}

// Bounded and request-local; this transceiver never captures bearer headers.
class S3ProofHTTP : public RGWHTTPTransceiver {
  size_t received = 0;
public:
  S3ProofHTTP(CephContext* cct, const std::string& method,
               const std::string& endpoint, bufferlist* response)
    : RGWHTTPTransceiver(cct, method, endpoint, response,
                        cct->_conf->rgw_keystone_verify_ssl,
                        {"Allow", "Cache-Control", "X-Subject-Token", "Content-Type",
                         "X-Auth-Token", "X-Service-Token", "Authorization"}) {
    set_sensitive_request();
    set_req_connect_timeout(5);
    set_req_timeout(10);
  }
protected:
  int receive_data(void* data, size_t len, bool* pause) override {
    constexpr size_t limit = 64 * 1024;
    if (len > limit - received) {
      return -EFBIG;
    }
    received += len;
    return RGWHTTPTransceiver::receive_data(data, len, pause);
  }
};

bool header_has(const S3ProofHTTP& request, const char* name,
                std::string_view expected)
{
  const auto headers = request.get_headers();
  auto i = headers.find(name);
  if (i == headers.end()) {
    return false;
  }
  std::vector<std::string> values;
  get_str_vec(i->second, values);
  return std::any_of(values.begin(), values.end(), [&](std::string v) {
    std::transform(v.begin(), v.end(), v.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return v == expected;
  });
}

} // anonymous namespace

int validate_s3_v4_proof(std::string_view proof, bool query,
                         std::string_view expires, time_t now,
                         SigningKeyCache::Key& scope)
{
  std::array<std::string_view, 4> lines;
  for (size_t n = 0; n < lines.size(); ++n) {
    auto end = proof.find('\n');
    if ((n < 3 && end == std::string_view::npos) ||
        (n == 3 && end != std::string_view::npos)) {
      return -EINVAL;
    }
    lines[n] = proof.substr(0, end);
    if (n < 3) {
      proof.remove_prefix(end + 1);
    }
  }
  if (lines[0] != "AWS4-HMAC-SHA256" || lines[3].size() != 64 ||
      !std::all_of(lines[3].begin(), lines[3].end(), [](char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
      })) {
    return -EINVAL;
  }
  std::array<std::string_view, 4> parts;
  auto rest = lines[2];
  for (size_t n = 0; n < parts.size(); ++n) {
    auto end = rest.find('/');
    if ((n < 3 && end == std::string_view::npos) ||
        (n == 3 && end != std::string_view::npos)) {
      return -EINVAL;
    }
    parts[n] = rest.substr(0, end);
    if (n < 3) {
      rest.remove_prefix(end + 1);
    }
  }
  if (parts[0].size() != 8 || parts[1].empty() || parts[2] != "s3" ||
      parts[3] != "aws4_request" || lines[1].substr(0, 8) != parts[0] ||
      !std::all_of(parts[1].begin(), parts[1].end(), [](unsigned char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
               (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.';
      })) {
    return -EINVAL;
  }
  time_t stamp;
  constexpr time_t max_age = 7 * 86400;
  if (!signed_date(lines[1], stamp) || now < 0 || stamp > now + 300 ||
      stamp < now - max_age || stamp / 86400 < now / 86400 - 7) {
    return -ERR_REQUEST_TIME_SKEWED;
  }
  if (query) {
    uint64_t seconds = 0;
    if (!unsigned_seconds(expires, seconds) || !seconds || seconds > max_age ||
        now >= stamp + static_cast<time_t>(seconds)) {
      return -ERR_PRESIGNED_URL_EXPIRED;
    }
  } else if (stamp < now - 300) {
    return -ERR_REQUEST_TIME_SKEWED;
  }
  scope.date = parts[0];
  scope.region = parts[1];
  scope.service = parts[2];
  return 0;
}

int decode_s3_proof_response(const std::string& body, bool key_response,
                             const SigningKeyCache::Key& scope,
                             S3ProofResult& result)
{
  result = {};
  if (body.size() > 64 * 1024) {
    return -EFBIG;
  }
  unsigned depth = 0;
  bool quoted = false, escaped = false;
  for (char c : body) {
    if (quoted) {
      if (escaped) escaped = false;
      else if (c == '\\') escaped = true;
      else if (c == '"') quoted = false;
    } else if (c == '"') {
      quoted = true;
    } else if (c == '{' || c == '[') {
      if (++depth > 32) return -EINVAL;
    } else if (c == '}' || c == ']') {
      if (!depth) return -EINVAL;
      --depth;
    }
  }
  if (quoted || depth) return -EINVAL;
  // json_spirit accepts repeated/trailing commas. Validate strict syntax and
  // UTF-8 first, without normalizing the original bytes or losing duplicates
  // and quoted scalar types needed by the schema decoder below.
  rapidjson::MemoryStream input(body.data(), body.size());
  rapidjson::Reader reader;
  rapidjson::BaseReaderHandler<rapidjson::UTF8<>> syntax;
  if (!reader.Parse<rapidjson::kParseDefaultFlags |
                    rapidjson::kParseValidateEncodingFlag>(input, syntax) ||
      input.Tell() != body.size()) {
    return -EINVAL;
  }
  json_spirit::Value value;
  auto begin = body.begin();
  if (!json_spirit::read(begin, body.end(), value) ||
      !std::all_of(begin, body.end(), [](char c) {
        return c == ' ' || c == '\n' || c == '\r' || c == '\t';
      })) {
    return -EINVAL;
  }
  JSONObj root;
  root.init(nullptr, value, "");
  if (key_response ? !fields_are(&root, {"authentication", "scope", "signing_key", "cache_until"})
                   : !fields_are(&root, {"authentication"})) {
    return -EINVAL;
  }
  S3ProofResult decoded;
  auto authentication = field(&root, "authentication");
  if (decoded.identity.decode_s3_authentication(authentication, decoded.credential_id) < 0) {
    return -EINVAL;
  }
  if (key_response) {
    auto scope_obj = field(&root, "scope");
    std::string date, region, service, encoded, deadline, identity_deadline;
    if (!fields_are(scope_obj, {"date", "region", "service"}) ||
        !string_field(scope_obj, "date", date) ||
        !string_field(scope_obj, "region", region) ||
        !string_field(scope_obj, "service", service) ||
        date != scope.date || region != scope.region || service != "s3" ||
        service != scope.service ||
        !string_field(&root, "signing_key", encoded) ||
        !string_field(&root, "cache_until", deadline) ||
        !string_field(authentication, "expires_at", identity_deadline) ||
        deadline != identity_deadline || encoded.size() != 44 || encoded.back() != '=' ||
        !std::all_of(encoded.begin(), encoded.end() - 1, [](unsigned char c) {
          return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                 (c >= '0' && c <= '9') || c == '+' || c == '/';
        })) {
      return -EINVAL;
    }
    try {
      auto bytes = rgw::from_base64(encoded);
      if (bytes.size() != sha256_digest_t::SIZE || rgw::to_base64(bytes) != encoded) {
        return -EINVAL;
      }
      decoded.signing_key.emplace();
      std::memcpy(decoded.signing_key->v, bytes.data(), sha256_digest_t::SIZE);
    } catch (const std::exception&) {
      return -EINVAL;
    }
    decoded.cache_until = decoded.identity.get_expires();
  }
  result = std::move(decoded);
  return 0;
}

EC2Engine::proof_http_result EC2Engine::proof_request(
    const DoutPrefixProvider* dpp, const std::string& method,
    const std::string& endpoint, const std::string& body, optional_yield y) const
{
  for (unsigned attempt = 0; attempt < 2; ++attempt) {
    std::string caller;
    bool cached = false;
    int ret = rgw::keystone::Service::get_admin_token(
        dpp, token_cache, config, y, caller, cached, {5, 10, true});
    if (ret < 0) {
      return {{}, 0, ret};
    }
    bufferlist response;
    S3ProofHTTP request(cct, method, endpoint, &response);
    request.append_header("X-Auth-Token", caller);
    request.append_header("Content-Type", "application/json");
    request.set_post_data(body);
    request.set_send_length(body.size());
    ret = request.process(dpp, y);
    const auto status = request.get_http_status();
    if (status == 401) {
      token_cache.invalidate_admin(dpp);
      if (attempt == 0) {
        continue; // frozen method, endpoint, and proof; fresh transceiver/header
      }
    }
    if (ret < 0 || (method == "OPTIONS" ? status < 200 || status >= 300 : status != 200)) {
      return {{}, status, ret < 0 ? ret : -EACCES};
    }
    const auto headers = request.get_headers();
    if (headers.count("X-Subject-Token") || headers.count("X-Auth-Token") ||
        headers.count("X-Service-Token") || headers.count("Authorization")) {
      return {{}, status, -EINVAL};
    }
    if (method == "OPTIONS") {
      if (!header_has(request, "Allow", "post")) {
        return {{}, status, -EOPNOTSUPP};
      }
    } else if (!header_has(request, "Cache-Control", "private") ||
               !header_has(request, "Cache-Control", "no-store") ||
               !headers.count("Content-Type") ||
               !std::string_view(headers.at("Content-Type")).starts_with("application/json")) {
      return {{}, status, -EINVAL};
    }
    return {response.to_str(), status, 0};
  }
  return {{}, 401, -EACCES};
}

int EC2Engine::check_capability(const DoutPrefixProvider* dpp, optional_yield y) const
{
  if (capability_checked.load()) {
    return 0;
  }
  auto uri = boost::urls::parse_uri(proof_endpoint);
  if (!uri || (uri->scheme() != "http" && uri->scheme() != "https") ||
      !uri->has_authority() || uri->host().empty() || uri->has_userinfo() ||
      uri->has_query() || uri->has_fragment()) {
    return -EINVAL;
  }
  for (const auto suffix : {"signing_key", "validate"}) {
    if (std::string_view(suffix) == "validate" && !allow_legacy_signatures) {
      continue;
    }
    auto response = proof_request(dpp, "OPTIONS", proof_endpoint + "v3/s3tokens/" + suffix, "", y);
    if (response.error) {
      ldpp_dout(dpp, 1) << "Keystone S3 proof capability unavailable, status="
                        << response.status << dendl;
      return response.error;
    }
  }
  capability_checked.store(true);
  return 0;
}

bool
TokenEngine::is_applicable(const std::string& token) const noexcept
{
  return ! token.empty() && ! cct->_conf->rgw_keystone_url.empty();
}

boost::optional<TokenEngine::token_envelope_t>
TokenEngine::get_from_keystone(const DoutPrefixProvider* dpp,
                               const std::string& token,
                               bool allow_expired,
                               optional_yield y) const
{
  /* Unfortunately, we can't use the short form of "using" here. It's because
   * we're aliasing a class' member, not namespace. */
  using RGWValidateKeystoneToken = \
    rgw::keystone::Service::RGWValidateKeystoneToken;

  bool admin_token_retried = false;

admin_token_retry:

  /* The container for plain response obtained from Keystone. It will be
   * parsed token_envelope_t::parse method. */
  ceph::bufferlist token_body_bl;
  RGWValidateKeystoneToken validate(cct, "GET", "", &token_body_bl);

  std::string url = config.get_endpoint_url();
  if (url.empty()) {
    throw -EINVAL;
  }

  url.append("v3/auth/tokens");

  if (allow_expired) {
    url.append("?allow_expired=1");
  }

  validate.append_header("X-Subject-Token", token);

  std::string admin_token;
  bool admin_token_cached = false;
  int ret = rgw::keystone::Service::get_admin_token(dpp, token_cache, config,
                                                    y, admin_token, admin_token_cached);
  if (ret < 0) {
    throw -EINVAL;
  }

  if (allow_expired) {
    validate.append_header("X-Auth-Token", admin_token);
  } else {
    validate.append_header("X-Auth-Token", token);
  }

  validate.set_send_length(0);

  validate.set_url(url);

  ret = validate.process(dpp, y);

  /* NULL terminate for debug output. */
  token_body_bl.append(static_cast<char>(0));

  /* Detect Keystone rejection earlier than during the token parsing.
   * Although failure at the parsing phase doesn't impose a threat,
   * this allows to return proper error code (EACCESS instead of EINVAL
   * or similar) and thus improves logging. */

  /* If admin token is invalid we should expire it from the cache and
     try one last time without the cache. */
  bool admin_token_unauthorized = (validate.get_http_status() ==
    RGWValidateKeystoneToken::HTTP_STATUS_UNAUTHORIZED);

  if (admin_token_unauthorized && admin_token_cached) {
    ldpp_dout(dpp, 20) << "invalidating admin_token cache due to 401" << dendl;
    token_cache.invalidate_admin(dpp);

    if (!admin_token_retried) {
      ldpp_dout(dpp, 20) << "retrying with uncached admin_token" << dendl;
      admin_token_retried = true;
      goto admin_token_retry;
    }
  }

  /* If admin token is invalid or token supplied by client is non-existent. */
  if (admin_token_unauthorized || validate.get_http_status() ==
        RGWValidateKeystoneToken::HTTP_STATUS_NOTFOUND) {
    ldpp_dout(dpp, 5) << "Failed keystone auth from " << url << " with "
                  << validate.get_http_status() << dendl;
    return boost::none;
  }
  // throw any other http or connection errors
  if (ret < 0) {
    throw ret;
  }

  ldpp_dout(dpp, 20) << "received response status=" << validate.get_http_status() << dendl;

  TokenEngine::token_envelope_t token_body;
  ret = token_body.parse(dpp, token, token_body_bl);
  if (ret < 0) {
    throw ret;
  }

  return token_body;
}

TokenEngine::auth_info_t
TokenEngine::get_creds_info(const TokenEngine::token_envelope_t& token
                           ) const noexcept
{
  using acct_privilege_t = rgw::auth::RemoteApplier::AuthInfo::acct_privilege_t;
  std::vector<std::string> role_names;

  /* Check whether the user has an admin status. */
  acct_privilege_t level = acct_privilege_t::IS_PLAIN_ACCT;
  for (const auto& role : token.roles) {
    role_names.push_back(role.name);
    if (role.is_admin && !role.is_system_reader) {
      level = acct_privilege_t::IS_ADMIN_ACCT;
    }
  }

  /* Role-tier cap: read-only when every granting role is a
   * project_reader, no implicit permissions at all when every granting
   * role is implicit_deny, full control otherwise. */
  const uint32_t perm_mask = token.effective_perm_mask();

  /* Build keystone scope info if ops logging is enabled */
  auto keystone_scope = rgw::keystone::build_scope_info(cct, token);

  return auth_info_t {
    /* Suggested account name for the authenticated user. */
    rgw_user(token.get_project_id()),
    /* User's display name (aka real name). */
    token.get_project_name(),
    /* Keystone doesn't support RGW's subuser concept, so perm_mask is FULL_CONTROL for
     * regular users. The project_reader cap above is the sole exception. */
    perm_mask,
    level,
    rgw::auth::RemoteApplier::AuthInfo::NO_ACCESS_KEY,
    rgw::auth::RemoteApplier::AuthInfo::NO_SUBUSER,
    token.get_user_name(),
    TYPE_KEYSTONE,
    std::move(keystone_scope),
    std::move(role_names),
    token.get_user_id()
  };
}

static inline const std::string
make_spec_item(const std::string& tenant, const std::string& id)
{
  return tenant + ":" + id;
}

TokenEngine::acl_strategy_t
TokenEngine::get_acl_strategy(const TokenEngine::token_envelope_t& token) const
{
  /* The primary identity is constructed upon UUIDs. */
  const auto& tenant_uuid = token.get_project_id();
  const auto& user_uuid = token.get_user_id();

  /* For Keystone v2 an alias may be also used. */
  const auto& tenant_name = token.get_project_name();
  const auto& user_name = token.get_user_name();

  /* Construct all possible combinations including Swift's wildcards. */
  const std::array<std::string, 6> allowed_items = {
    make_spec_item(tenant_uuid, user_uuid),
    make_spec_item(tenant_name, user_name),

    /* Wildcards. */
    make_spec_item(tenant_uuid, "*"),
    make_spec_item(tenant_name, "*"),
    make_spec_item("*", user_uuid),
    make_spec_item("*", user_name),
  };

  /* Lambda will obtain a copy of (not a reference to!) allowed_items. */
  return [allowed_items, token_roles=token.roles](const rgw::auth::Identity::aclspec_t& aclspec) {
    uint32_t perm = 0;

    for (const auto& allowed_item : allowed_items) {
      const auto iter = aclspec.find(allowed_item);

      if (std::end(aclspec) != iter) {
        perm |= iter->second;
      }
    }

    for (const auto& r : token_roles) {
      if (r.is_system_reader) {
        if (r.is_admin) {    /* system scope reader persona */
          /*
           * Because system reader defeats permissions,
           * we don't even look at the aclspec.
           */
          perm |= RGW_OP_TYPE_READ;
        }
      }
    }

    return perm;
  };
}

TokenEngine::result_t
TokenEngine::authenticate(const DoutPrefixProvider* dpp,
                          const std::string& token,
                          const std::string& service_token,
                          const req_state* const s,
                          optional_yield y) const
{
  bool allow_expired = false;
  boost::optional<TokenEngine::token_envelope_t> t;

  /* This will be initialized on the first call to this method. In C++11 it's
   * also thread-safe. */
  static const struct RolesCacher {
    explicit RolesCacher(CephContext* const cct) {
      get_str_vec(cct->_conf->rgw_keystone_accepted_roles, plain);
      get_str_vec(cct->_conf->rgw_keystone_accepted_admin_roles, admin);
      get_str_vec(cct->_conf->rgw_keystone_accepted_reader_roles, system_reader);
      get_str_vec(cct->_conf->rgw_keystone_accepted_project_reader_roles, project_reader);
      get_str_vec(cct->_conf->rgw_keystone_implicit_deny_roles, implicit_deny);

      plain.insert(std::end(plain), std::begin(admin), std::end(admin));
      plain.insert(std::end(plain), std::begin(project_reader), std::end(project_reader));
      plain.insert(std::end(plain), std::begin(implicit_deny), std::end(implicit_deny));
    }

    std::vector<std::string> plain;
    std::vector<std::string> admin;
    std::vector<std::string> system_reader;
    std::vector<std::string> project_reader;
    std::vector<std::string> implicit_deny;
  } roles(cct);

  static const struct ServiceTokenRolesCacher {
    explicit ServiceTokenRolesCacher(CephContext* const cct) {
      get_str_vec(cct->_conf->rgw_keystone_service_token_accepted_roles, plain);
    }

    std::vector<std::string> plain;
  } service_token_roles(cct);

  if (! is_applicable(token)) {
    return result_t::deny();
  }

  /* Token ID is a legacy of supporting the service-side validation
   * of PKI/PKIz token type which are already-removed-in-OpenStack.
   * The idea was to bury in cache only a short hash instead of few
   * kilobytes. RadosGW doesn't do the local validation anymore. */
  const auto& token_id = rgw_get_token_id(token);
  ldpp_dout(dpp, 20) << "checking cached user token" << dendl;

  /* Check cache first. */
  t = token_cache.find(token_id);
  if (t) {
    ldpp_dout(dpp, 20) << "cached token.project.id=" << t->get_project_id()
                   << dendl;
    auto apl = apl_factory->create_apl_remote(cct, s, get_acl_strategy(*t),
                                              get_creds_info(*t));
    return result_t::grant(std::move(apl));
  }

  /* We have a service token and a token so we verify the service
   * token and if it's invalid the request is invalid. If it's valid
   * we allow an expired token to be used when doing lookup in Keystone.
   * We never get to this if the token is in the cache. */
  if (g_conf()->rgw_keystone_service_token_enabled && ! service_token.empty()) {
    boost::optional<TokenEngine::token_envelope_t> st;

    const auto& service_token_id = rgw_get_token_id(service_token);
    ldpp_dout(dpp, 20) << "checking cached service token" << dendl;

    /* Check cache for service token first. */
    st = token_cache.find_service(service_token_id);
    if (st) {
      ldpp_dout(dpp, 20) << "cached service_token.project.id=" << st->get_project_id()
                     << dendl;

      /* We found the service token in the cache so we allow using an expired
       * token for this request. */
      allow_expired = true;
      ldpp_dout(dpp, 20) << "allowing expired user token with cached service authority" << dendl;
    } else {
      /* Service token was not found in cache. Go to Keystone for validating
       * the token. The allow_expired here must always be false. */
      ceph_assert(allow_expired == false);
      st = get_from_keystone(dpp, service_token, allow_expired, y);

      if (! st) {
        return result_t::deny(-EACCES);
      }

      /* Verify expiration of service token. */
      if (st->expired()) {
        ldpp_dout(dpp, 0) << "got expired service token: " << st->get_project_name()
                       << ":" << st->get_user_name()
                       << " expired " << st->get_expires() << dendl;
        return result_t::deny(-EPERM);
      }

      /* Check for necessary roles for service token. */
      for (const auto& role : service_token_roles.plain) {
        if (st->has_role(role) == true) {
          /* Service token is valid so we allow using an expired token for
           * this request. */
          ldpp_dout(dpp, 20) << "allowing expired user token with validated service authority" << dendl;
          allow_expired = true;
          token_cache.add_service(service_token_id, *st);
          break;
        }
      }

      if (!allow_expired) {
        ldpp_dout(dpp, 0) << "service token user does not hold a matching role; required roles: "
                  << g_conf()->rgw_keystone_service_token_accepted_roles << dendl;
        return result_t::deny(-EPERM);
      }
    }
  }

  /* Token not in cache. Go to the Keystone for validation. This happens even
   * for the legacy PKI/PKIz token types. That's it, after the PKI/PKIz
   * RadosGW-side validation has been removed, we always ask Keystone. */
  t = get_from_keystone(dpp, token, allow_expired, y);
  if (! t) {
    return result_t::deny(-EACCES);
  }
  t->update_roles(roles.plain, roles.admin, roles.system_reader,
                  roles.project_reader, roles.implicit_deny);

  /* Verify expiration. */
  if (t->expired()) {
    if (allow_expired) {
      ldpp_dout(dpp, 20) << "allowing expired token: " << t->get_project_name()
                    << ":" << t->get_user_name()
                    << " expired: " << t->get_expires()
                    << " because of valid service token" << dendl;
    } else {
      ldpp_dout(dpp, 0) << "got expired token: " << t->get_project_name()
                    << ":" << t->get_user_name()
                    << " expired: " << t->get_expires() << dendl;
      return result_t::deny(-EPERM);
    }
  }


  /**
   * Reject a token that holds no accepted role. Admission was computed in
   * update_roles().
   */
  if(!t->admitted()) {
    ldpp_dout(dpp, 0) << "user does not hold a matching role; required roles: "
                 << g_conf()->rgw_keystone_accepted_roles << dendl;
    return result_t::deny(-EPERM);
  }

  /* If this token was an allowed expired token because we got a
    * service token we need to update the expiration before we cache it. */
  if (allow_expired) {
    time_t now = ceph_clock_now().sec();
    time_t new_expires = now + g_conf()->rgw_keystone_expired_token_cache_expiration;
    ldpp_dout(dpp, 20) << "updating expiration of allowed expired token"
                        << " from old " << t->get_expires() << " to now " << now << " + "
                        << g_conf()->rgw_keystone_expired_token_cache_expiration
                        << " secs = "
                        << new_expires << dendl;
    t->set_expires(new_expires);
  }
  ldpp_dout(dpp, 0) << "validated token: " << t->get_project_name()
            << ":" << t->get_user_name()
            << " expires: " << t->get_expires() << dendl;
  token_cache.add(token_id, *t);
  auto apl = apl_factory->create_apl_remote(cct, s, get_acl_strategy(*t),
                                                get_creds_info(*t));
  return result_t::grant(std::move(apl));
}


/*
 * Try to validate S3 auth against keystone s3token interface
 */
std::pair<boost::optional<rgw::keystone::TokenEnvelope>, int>
EC2Engine::get_from_keystone(const DoutPrefixProvider* dpp, const std::string_view& access_key_id,
                             const std::string& string_to_sign,
                             const std::string_view& signature,
                             optional_yield y) const
{
  /* prepare keystone url */
  std::string keystone_url = config.get_endpoint_url();
  if (keystone_url.empty()) {
    throw -EINVAL;
  }

  keystone_url.append("v3/s3tokens");

  /* get authentication token for Keystone. */
  std::string admin_token;
  bool admin_token_cached = false;
  int ret = rgw::keystone::Service::get_admin_token(dpp, token_cache, config,
                                                    y, admin_token, admin_token_cached);
  if (ret < 0) {
    ldpp_dout(dpp, 2) << "s3 keystone: cannot get token for keystone access"
                  << dendl;
    throw ret;
  }

  using RGWValidateKeystoneToken
    = rgw::keystone::Service::RGWValidateKeystoneToken;

  /* The container for plain response obtained from Keystone. It will be
   * parsed token_envelope_t::parse method. */
  ceph::bufferlist token_body_bl;
  RGWValidateKeystoneToken validate(cct, "POST", keystone_url, &token_body_bl);

  /* set required headers for keystone request */
  validate.append_header("X-Auth-Token", admin_token);
  validate.append_header("Content-Type", "application/json");

  /* check if we want to verify keystone's ssl certs */
  validate.set_verify_ssl(cct->_conf->rgw_keystone_verify_ssl);

  /* create json credentials request body */
  JSONFormatter credentials(false);
  credentials.open_object_section("");
  credentials.open_object_section("credentials");
  credentials.dump_string("access", sview2cstr(access_key_id).data());
  credentials.dump_string("token", rgw::to_base64(string_to_sign));
  credentials.dump_string("signature", sview2cstr(signature).data());
  credentials.close_section();
  credentials.close_section();

  std::stringstream os;
  credentials.flush(os);
  validate.set_post_data(os.str());
  validate.set_send_length(os.str().length());

  /* send request */
  ret = validate.process(dpp, y);

  /* if the supplied signature is wrong, we will get 401 from Keystone */
  if (validate.get_http_status() ==
          decltype(validate)::HTTP_STATUS_UNAUTHORIZED) {
    return std::make_pair(boost::none, -ERR_SIGNATURE_NO_MATCH);
  } else if (validate.get_http_status() ==
          decltype(validate)::HTTP_STATUS_NOTFOUND) {
    return std::make_pair(boost::none, -ERR_INVALID_ACCESS_KEY);
  }
  // throw any other http or connection errors
  if (ret < 0) {
    ldpp_dout(dpp, 2) << "s3 keystone: token validation failed, status="
                      << validate.get_http_status() << dendl;
    throw ret;
  }

  /* now parse response */
  rgw::keystone::TokenEnvelope token_envelope;
  ret = token_envelope.parse(dpp, std::string(), token_body_bl);
  if (ret < 0) {
    ldpp_dout(dpp, 2) << "s3 keystone: token parsing failed, ret=0" << ret
                  << dendl;
    throw ret;
  }

  return std::make_pair(std::move(token_envelope), 0);
}

auto EC2Engine::get_secret_from_keystone(const DoutPrefixProvider* dpp,
                                         const std::string& user_id,
                                         const std::string_view& access_key_id,
                                         optional_yield y) const
    -> std::pair<boost::optional<std::string>, int>
{
  /*  Fetch from /users/{USER_ID}/credentials/OS-EC2/{ACCESS_KEY_ID} */
  /* Should return json with response key "credential" which contains entry "secret"*/

  /* prepare keystone url */
  std::string keystone_url = config.get_endpoint_url();
  if (keystone_url.empty()) {
    return make_pair(boost::none, -EINVAL);
  }

  keystone_url.append("v3/");
  keystone_url.append("users/");
  keystone_url.append(user_id);
  keystone_url.append("/credentials/OS-EC2/");
  keystone_url.append(std::string(access_key_id));

  /* get authentication token for Keystone. */
  std::string admin_token;
  bool admin_token_cached = false;
  int ret = rgw::keystone::Service::get_admin_token(dpp, token_cache, config,
                                                    y, admin_token, admin_token_cached);
  if (ret < 0) {
    ldpp_dout(dpp, 2) << "s3 keystone: cannot get token for keystone access"
                  << dendl;
    return make_pair(boost::none, ret);
  }

  using RGWGetAccessSecret
    = rgw::keystone::Service::RGWKeystoneHTTPTransceiver;

  /* The container for plain response obtained from Keystone.*/
  ceph::bufferlist token_body_bl;
  RGWGetAccessSecret secret(cct, "GET", keystone_url, &token_body_bl);

  /* set required headers for keystone request */
  secret.append_header("X-Auth-Token", admin_token);

  /* check if we want to verify keystone's ssl certs */
  secret.set_verify_ssl(cct->_conf->rgw_keystone_verify_ssl);

  /* send request */
  ret = secret.process(dpp, y);

  /* if the supplied access key isn't found, we will get 404 from Keystone */
  if (secret.get_http_status() ==
          decltype(secret)::HTTP_STATUS_NOTFOUND) {
    return make_pair(boost::none, -ERR_INVALID_ACCESS_KEY);
  }
  // return any other http or connection errors
  if (ret < 0) {
    ldpp_dout(dpp, 2) << "s3 keystone: secret fetching failed, status="
                      << secret.get_http_status() << dendl;
    return make_pair(boost::none, ret);
  }

  /* now parse response */

  JSONParser parser;
  if (! parser.parse(token_body_bl.c_str(), token_body_bl.length())) {
    ldpp_dout(dpp, 0) << "Keystone credential parse error: malformed json" << dendl;
    return make_pair(boost::none, -EINVAL);
  }

  JSONObjIter credential_iter = parser.find_first("credential");
  std::string secret_string;

  try {
    if (!credential_iter.end()) {
      JSONDecoder::decode_json("secret", secret_string, *credential_iter, true);
    } else {
      ldpp_dout(dpp, 0) << "Keystone credential not present in return from server" << dendl;
      return make_pair(boost::none, -EINVAL);
    }
  } catch (const JSONDecoder::err& err) {
    ldpp_dout(dpp, 0) << "Keystone credential parse error: " << err.what() << dendl;
    return make_pair(boost::none, -EINVAL);
  }

  return make_pair(secret_string, 0);
}

/*
 * Try to get a token for S3 authentication, using a secret cache if available
 */
auto EC2Engine::get_access_token(const DoutPrefixProvider* dpp,
                                 const std::string_view& access_key_id,
                                 const std::string& string_to_sign,
                                 const std::string_view& signature,
                                 const signature_factory_t& signature_factory,
                                 bool ignore_signature,
                                 optional_yield y) const
    -> access_token_result
{
  using server_signature_t = VersionAbstractor::server_signature_t;
  boost::optional<rgw::keystone::TokenEnvelope> token;
  boost::optional<std::string> secret;
  int failure_reason;

  /* Get a token from the cache if one has already been stored */
  boost::optional<boost::tuple<rgw::keystone::TokenEnvelope, std::string>>
    t = secret_cache.find(std::string(access_key_id));

  /* Check that credentials can correctly be used to sign data */
  if (t) {
    /* We should ignore checking signature in cache if caller tells us to which
     * means we're handling a HTTP OPTIONS call. */
    if (ignore_signature) {
      ldpp_dout(dpp, 20) << "ignore_signature set and found in cache" << dendl;
      return {t->get<0>(), t->get<1>(), 0};
    } else {
      std::string sig(signature);
      server_signature_t server_signature = signature_factory(cct, t->get<1>(), string_to_sign);
      if (sig.compare(server_signature) == 0) {
        return {t->get<0>(), t->get<1>(), 0};
      } else {
        ldpp_dout(dpp, 0) << "Secret string does not correctly sign payload, cache miss" << dendl;
      }
    }
  } else {
    ldpp_dout(dpp, 0) << "No stored secret string, cache miss" << dendl;
  }

  /* No cached token, token expired, or secret invalid: fall back to keystone */
  std::tie(token, failure_reason) =
      get_from_keystone(dpp, access_key_id, string_to_sign, signature, y);

  if (token) {
    /* Fetch secret from keystone for the access_key_id */
    std::tie(secret, failure_reason) =
        get_secret_from_keystone(dpp, token->get_user_id(), access_key_id, y);

    if (secret) {
      /* Add token, secret pair to cache, and set timeout */
      secret_cache.add(std::string(access_key_id), *token, *secret);
    }
  }

  return {token, secret, failure_reason};
}

EC2Engine::acl_strategy_t
EC2Engine::get_acl_strategy(const EC2Engine::token_envelope_t&) const
{
  /* This is based on the assumption that the default acl strategy in
   * get_perms_from_aclspec, will take care. Extra acl spec is not required. */
  return nullptr;
}

EC2Engine::auth_info_t
EC2Engine::get_creds_info(const EC2Engine::token_envelope_t& token,
                          const std::string& access_key_id
                         ) const noexcept
{
  using acct_privilege_t = \
    rgw::auth::RemoteApplier::AuthInfo::acct_privilege_t;

  /* Check whether the user has an admin status. is_admin is set by
   * update_roles() from the accepted-admin-roles list. */
  acct_privilege_t level = acct_privilege_t::IS_PLAIN_ACCT;
  for (const auto& role : token.roles) {
    if (role.is_admin) {
      level = acct_privilege_t::IS_ADMIN_ACCT;
      break;
    }
  }

  /* Role-tier cap: read-only when every granting role is a
   * project_reader, no implicit permissions at all when every granting
   * role is implicit_deny, full control otherwise. */
  const uint32_t perm_mask = token.effective_perm_mask();

  /* Build keystone scope info if ops logging is enabled */
  auto keystone_scope = rgw::keystone::build_scope_info(cct, token);

  std::vector<std::string> role_names;
  for (const auto& role : token.roles) {
    role_names.push_back(role.name);
  }

  return auth_info_t {
    /* Suggested account name for the authenticated user. */
    rgw_user(token.get_project_id()),
    /* User's display name (aka real name). */
    token.get_project_name(),
    /* Keystone doesn't support RGW's subuser concept,perm_mask is FULL_CONTROL for
     * regular users. The project_reader cap above is the sole exception. */
    perm_mask,
    level,
    access_key_id,
    rgw::auth::RemoteApplier::AuthInfo::NO_SUBUSER,
    token.get_user_name(),
    TYPE_KEYSTONE,
    std::move(keystone_scope),
    std::move(role_names),
    token.get_user_id()
  };
}

rgw::auth::Engine::result_t EC2Engine::authenticate(
    const DoutPrefixProvider* dpp, const VersionAbstractor::auth_data_t& data,
    const req_state* s, optional_yield y) const
{
  using Kind = VersionAbstractor::ProofKind;
  const bool v2 = data.proof_kind == Kind::s3_v2;
  if (!allow_legacy_signatures && (v2 || data.proof_kind == Kind::browser_v2)) {
    return result_t::deny(-EOPNOTSUPP);
  }
  if (!signing_key_mode) {
    return AWSEngine::authenticate(dpp, data, s, y);
  }
  if (!data.session_token.empty()) {
    return result_t::deny(); // independent STS backend, never ignore its token
  }
  if (data.proof_kind == Kind::browser_v2 || data.proof_kind == Kind::browser_v4) {
    ldpp_dout(dpp, 1) << "Keystone signing_key mode does not support browser policy proofs; "
                        "use an explicitly configured supported backend" << dendl;
    return result_t::deny(-EOPNOTSUPP);
  }
  if ((!v2 && data.proof_kind != Kind::s3_v4) || (!v2 && !data.derived_key)) {
    return result_t::deny(-EOPNOTSUPP);
  }

  const auto start_mono = mono_now();
  const time_t start_wall = wall_now();
  SigningKeyCache::Key key;
  key.access = data.access_key_id;
  key.endpoint = proof_endpoint;
  const bool query = !s->info.env->get("HTTP_AUTHORIZATION", "")[0];
  auto check_proof = [&](const VersionAbstractor::auth_data_t& candidate,
                         time_t now) -> int {
    if (!v2) {
      auto ret = validate_s3_v4_proof(candidate.string_to_sign, query,
          s->info.args.get("x-amz-expires"), now, key);
      if (ret < 0) {
        return ret;
      }
      const auto& derived = *candidate.derived_key;
      const auto date_start = candidate.string_to_sign.find('\n') + 1;
      const auto date_end = candidate.string_to_sign.find('\n', date_start);
      if (derived.date != key.date || derived.region != key.region ||
          derived.service != key.service ||
          derived.request_date != std::string_view(candidate.string_to_sign).substr(date_start, date_end - date_start) ||
          derived.credential_scope != key.date + "/" + key.region + "/s3/aws4_request" ||
          !derived.signature_factory || !derived.completer_factory) {
        return -EINVAL;
      }
    } else {
      // Bind the remotely verified V2 proof to the actual request and expiry.
      std::string actual;
      utime_t header_time;
      if (!rgw_create_s3_canonical_header(s, s->op_type, s->info,
                                         &header_time, actual, query) ||
          actual != candidate.string_to_sign) {
        return -EINVAL;
      }
      if (query) {
        uint64_t expiry = 0;
        if (!unsigned_seconds(s->info.args.get("Expires"), expiry) ||
            expiry <= static_cast<uint64_t>(now) ||
            expiry - static_cast<uint64_t>(now) > 7 * 86400) {
          return -ERR_PRESIGNED_URL_EXPIRED;
        }
      } else if (header_time.sec() > now + 300 || header_time.sec() < now - 300) {
        return -ERR_REQUEST_TIME_SKEWED;
      }
    }
    return 0;
  };
  if (auto ret = check_proof(data, start_wall); ret < 0) {
    return result_t::deny(ret);
  }
  if (auto ret = check_capability(dpp, y); ret < 0) {
    return result_t::deny(ret);
  }

  boost::optional<SigningKeyCache::Value> cached;
  if (!v2 && local_cache_ttl) {
    cached = signing_keys.find(key, wall_now(), mono_now());
  }
  S3ProofResult proof;
  bool from_cache = !!cached;
  if (cached) {
    proof.identity = cached->identity;
    proof.credential_id = cached->credential_id;
    proof.signing_key = cached->signing_key;
    proof.cache_until = cached->wall_expires;
  } else {
    JSONFormatter credentials(false);
    credentials.open_object_section("");
    credentials.open_object_section("credentials");
    credentials.dump_string("access", std::string(data.access_key_id));
    auto encoded = rgw::to_base64(data.string_to_sign);
    std::replace(encoded.begin(), encoded.end(), '+', '-');
    std::replace(encoded.begin(), encoded.end(), '/', '_');
    credentials.dump_string("token", encoded);
    credentials.dump_string("signature", std::string(data.client_signature));
    credentials.close_section();
    credentials.close_section();
    std::ostringstream serialized;
    credentials.flush(serialized);
    const auto endpoint = proof_endpoint + "v3/s3tokens/" +
                          (v2 ? "validate" : "signing_key");
    if (v2) {
      const auto count = legacy_validations.fetch_add(1) + 1;
      ldpp_dout(dpp, 1) << "Deprecated signature: protocol=SigV2 consumer=s3 count="
                        << count << dendl;
    }
    auto response = proof_request(dpp, "POST", endpoint, serialized.str(), y);
    if (response.error) {
      // Uniform403 is not credential-existence evidence. Independent local
      // backends remain eligible, but this engine never tries another protocol.
      ldpp_dout(dpp, 5) << "Keystone S3 proof denied or unavailable, status="
                        << response.status << dendl;
      return result_t::deny(response.error);
    }
    if (decode_s3_proof_response(response.body, !v2, key, proof) < 0) {
      return result_t::deny(-EINVAL);
    }
  }
  const time_t now = wall_now();
  const auto mono = mono_now();
  if (auto ret = check_proof(data, now); ret < 0) {
    return result_t::deny(ret);
  }
  if (proof.identity.get_expires() <= now) {
    return result_t::reject(-EACCES);
  }
  boost::optional<VersionAbstractor::auth_data_t> alternative;
  const auto* verified = &data;
  if (!v2) {
    if (!proof.signing_key) {
      return result_t::deny(-EINVAL);
    }
    const auto matches = [&](const VersionAbstractor::auth_data_t& candidate) {
      const auto expected = candidate.derived_key->signature_factory(
          *proof.signing_key, candidate.string_to_sign);
      return candidate.client_signature.size() == expected.size() &&
          CRYPTO_memcmp(candidate.client_signature.data(), expected.c_str(), expected.size()) == 0;
    };
    if (!matches(data)) {
      // A known key remains authoritative. Only the actual Boto2 canonical
      // alternative of this same request may verify locally with that key;
      // never refetch it, downgrade, or loosen REJECTED into backend fallback.
      if (from_cache &&
          dynamic_cast<const rgw::auth::s3::AWSGeneralAbstractor*>(&ver_abstractor) &&
          !dynamic_cast<const rgw::auth::s3::AWSGeneralBoto2Abstractor*>(&ver_abstractor)) {
        try {
          rgw::auth::s3::AWSGeneralBoto2Abstractor boto(cct);
          auto candidate = boto.get_auth_data(s);
          const auto& original = *data.derived_key;
          if (candidate.proof_kind == Kind::s3_v4 && candidate.derived_key &&
              candidate.access_key_id == data.access_key_id &&
              candidate.client_signature == data.client_signature &&
              candidate.session_token.empty() &&
              candidate.derived_key->date == original.date &&
              candidate.derived_key->region == original.region &&
              candidate.derived_key->service == original.service &&
              candidate.derived_key->request_date == original.request_date &&
              candidate.derived_key->credential_scope == original.credential_scope &&
              candidate.string_to_sign != data.string_to_sign &&
              check_proof(candidate, wall_now()) == 0 && matches(candidate)) {
            alternative = std::move(candidate);
            verified = &*alternative;
          }
        } catch (const int) {
          // Malformed actual request is still a known-key signature failure.
        }
      }
      if (!alternative) {
        return result_t::reject(-ERR_SIGNATURE_NO_MATCH);
      }
    }
  }

  std::vector<std::string> plain, admin, reader, project_reader, implicit_deny;
  get_str_vec(cct->_conf->rgw_keystone_accepted_roles, plain);
  get_str_vec(cct->_conf->rgw_keystone_accepted_admin_roles, admin);
  get_str_vec(cct->_conf->rgw_keystone_accepted_reader_roles, reader);
  get_str_vec(cct->_conf->rgw_keystone_accepted_project_reader_roles, project_reader);
  get_str_vec(cct->_conf->rgw_keystone_implicit_deny_roles, implicit_deny);
  proof.identity.update_roles(plain, admin, reader, project_reader, implicit_deny);
  if (!proof.identity.admitted()) {
    return result_t::reject(-EACCES);
  }

  // Admission is independent of cache storage. TTL0 means no storage, not an
  // expired request and not an exemption from the server/identity/hard ceiling.
  const time_t maximum = local_cache_ttl ? std::min<unsigned>(local_cache_ttl, 3600) : 3600;
  const time_t deadline = v2 ? proof.identity.get_expires() :
      std::min(proof.cache_until, proof.identity.get_expires());
  const time_t wall_deadline = std::min(deadline, start_wall + maximum);
  auto mono_deadline = std::min({
      mono + std::chrono::seconds(std::max<time_t>(0, deadline - now)),
      start_mono + std::chrono::seconds(std::max<time_t>(0, deadline - start_wall)),
      start_mono + std::chrono::seconds(maximum)});
  if (cached) {
    mono_deadline = std::min(mono_deadline, cached->mono_expires);
  }
  const time_t admission_wall = wall_now();
  const auto admission_mono = mono_now();
  if (admission_wall >= wall_deadline || admission_mono >= mono_deadline ||
      check_proof(*verified, admission_wall) < 0) {
    return result_t::reject(-EACCES);
  }
  if (!v2 && !from_cache && local_cache_ttl) {
    SigningKeyCache::Value entry;
    entry.identity = proof.identity;
    entry.signing_key = *proof.signing_key;
    entry.credential_id = proof.credential_id;
    entry.wall_expires = wall_deadline;
    entry.mono_expires = mono_deadline;
    signing_keys.insert(key, std::move(entry), admission_wall, admission_mono);
  }
  auto apl = apl_factory->create_apl_remote(cct, s,
      get_acl_strategy(proof.identity),
      get_creds_info(proof.identity, std::string(data.access_key_id)));
  if (v2) {
    return result_t::grant(std::move(apl));
  }
  return result_t::grant(std::move(apl),
      verified->derived_key->completer_factory(*proof.signing_key));
}

rgw::auth::Engine::result_t EC2Engine::authenticate(
  const DoutPrefixProvider* dpp,
  const std::string_view& access_key_id,
  const std::string_view& signature,
  const std::string_view& session_token,
  const string_to_sign_t& string_to_sign,
  const signature_factory_t& signature_factory,
  const completer_factory_t& completer_factory,
  const req_state* s,
  optional_yield y) const
{
  /* This will be initialized on the first call to this method. In C++11 it's
   * also thread-safe. */
  static const struct RolesCacher {
    explicit RolesCacher(CephContext* const cct) {
      get_str_vec(cct->_conf->rgw_keystone_accepted_roles, plain);
      get_str_vec(cct->_conf->rgw_keystone_accepted_admin_roles, admin);
      get_str_vec(cct->_conf->rgw_keystone_accepted_reader_roles, system_reader);
      get_str_vec(cct->_conf->rgw_keystone_accepted_project_reader_roles, project_reader);
      get_str_vec(cct->_conf->rgw_keystone_implicit_deny_roles, implicit_deny);

      /* Admit the project-reader and implicit-deny tiers on their own, the
       * same way an admin role is admitted. A user whose only role is one of
       * these is still accepted, and its role flag caps what it may do.
       * system_reader is not merged here: it grants cross-cluster read and
       * also needs an admin role. */
      plain.insert(std::end(plain), std::begin(admin), std::end(admin));
      plain.insert(std::end(plain), std::begin(project_reader), std::end(project_reader));
      plain.insert(std::end(plain), std::begin(implicit_deny), std::end(implicit_deny));
    }

    std::vector<std::string> plain;
    std::vector<std::string> admin;
    std::vector<std::string> system_reader;
    std::vector<std::string> project_reader;
    std::vector<std::string> implicit_deny;
  } accepted_roles(cct);

  /* When we handle a HTTP OPTIONS call we must ignore the signature */
  bool ignore_signature = (s->op_type == RGW_OP_OPTIONS_CORS);

  auto [t, secret_key, failure_reason] =
    get_access_token(dpp, access_key_id, string_to_sign,
                     signature, signature_factory, ignore_signature, y);
  if (! t) {
    if (failure_reason == -ERR_SIGNATURE_NO_MATCH) {
      // we looked up a secret but it didn't generate the same signature as
      // the client. since we found this access key in keystone, we should
      // reject the request instead of trying other engines
      return result_t::reject(failure_reason);
    }
    return result_t::deny(failure_reason);
  }

  /* Verify expiration. */
  if (t->expired()) {
    ldpp_dout(dpp, 0) << "got expired token: " << t->get_project_name()
                  << ":" << t->get_user_name()
                  << " expired: " << t->get_expires() << dendl;
    return result_t::deny();
  }

  /* Classify roles first, then admit on the result rather than rescanning
   * the config lists. */
  t->update_roles(accepted_roles.plain,
                  accepted_roles.admin,
                  accepted_roles.system_reader,
                  accepted_roles.project_reader,
                  accepted_roles.implicit_deny);

  if (! t->admitted()) {
    ldpp_dout(dpp, 5) << "s3 keystone: user does not hold a matching role;"
                     " required roles: "
                  << cct->_conf->rgw_keystone_accepted_roles << dendl;
    return result_t::deny();
  }

  /* everything seems fine, continue with this user */
  ldpp_dout(dpp, 5) << "s3 keystone: validated token: " << t->get_project_name()
                << ":" << t->get_user_name()
                << " expires: " << t->get_expires() << dendl;

  auto apl = apl_factory->create_apl_remote(cct, s, get_acl_strategy(*t),
                                            get_creds_info(*t, std::string(access_key_id)));
  return result_t::grant(std::move(apl), completer_factory(secret_key));
}

bool SecretCache::find(const std::string& token_id,
                       SecretCache::token_envelope_t& token,
		       std::string &secret)
{
  std::lock_guard<std::mutex> l(lock);

  map<std::string, secret_entry>::iterator iter = secrets.find(token_id);
  if (iter == secrets.end()) {
    return false;
  }

  secret_entry& entry = iter->second;
  secrets_lru.erase(entry.lru_iter);

  const utime_t now = ceph_clock_now();
  if (entry.token.expired() || now > entry.expires) {
    secrets.erase(iter);
    return false;
  }
  token = entry.token;
  secret = entry.secret;

  secrets_lru.push_front(token_id);
  entry.lru_iter = secrets_lru.begin();

  return true;
}

void SecretCache::add(const std::string& token_id,
                      const SecretCache::token_envelope_t& token,
		      const std::string& secret)
{
  std::lock_guard<std::mutex> l(lock);

  map<string, secret_entry>::iterator iter = secrets.find(token_id);
  if (iter != secrets.end()) {
    secret_entry& e = iter->second;
    secrets_lru.erase(e.lru_iter);
  }

  const utime_t now = ceph_clock_now();
  secrets_lru.push_front(token_id);
  secret_entry& entry = secrets[token_id];
  entry.token = token;
  entry.secret = secret;
  entry.expires = now + s3_token_expiry_length;
  entry.lru_iter = secrets_lru.begin();

  while (secrets_lru.size() > max) {
    list<string>::reverse_iterator riter = secrets_lru.rbegin();
    iter = secrets.find(*riter);
    assert(iter != secrets.end());
    secrets.erase(iter);
    secrets_lru.pop_back();
  }
}

}; /* namespace keystone */
}; /* namespace auth */
}; /* namespace rgw */
