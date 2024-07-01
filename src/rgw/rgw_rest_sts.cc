// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
#include <vector>
#include <string>
#include <array>
#include <string_view>
#include <sstream>
#include <memory>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/format.hpp>
#include <boost/optional.hpp>
#include <boost/utility/in_place_factory.hpp>
#include <boost/tokenizer.hpp>



#include "ceph_ver.h"
#include "common/Formatter.h"
#include "common/utf8.h"
#include "common/ceph_json.h"

#include "rgw_rest.h"
#include "rgw_auth.h"
#include "rgw_auth_registry.h"
#include "jwt-cpp/jwt.h"
#include "rgw_rest_sts.h"

#include "rgw_formats.h"
#include "rgw_client_io.h"

#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_iam_policy.h"
#include "rgw_iam_policy_keywords.h"

#include "rgw_sts.h"
#include "rgw_rest_oidc_provider.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::auth::sts {

bool
WebTokenEngine::is_applicable(const std::string& token) const noexcept
{
  return ! token.empty();
}

std::string
WebTokenEngine::get_role_tenant(const string& role_arn) const
{
  string tenant;
  auto r_arn = rgw::ARN::parse(role_arn);
  if (r_arn) {
    tenant = r_arn->account;
  }
  return tenant;
}

std::string
WebTokenEngine::get_role_name(const string& role_arn) const
{
  string role_name;
  auto r_arn = rgw::ARN::parse(role_arn);
  if (r_arn) {
    role_name = r_arn->resource;
  }
  if (!role_name.empty()) {
    auto pos = role_name.find_last_of('/');
    if(pos != string::npos) {
      role_name = role_name.substr(pos + 1);
    }
  }
  return role_name;
}

std::unique_ptr<rgw::sal::RGWOIDCProvider>
WebTokenEngine::get_provider(const DoutPrefixProvider *dpp, const string& role_arn, const string& iss) const
{
  string tenant = get_role_tenant(role_arn);

  string idp_url = iss;
  auto pos = idp_url.find("http://");
  if (pos == std::string::npos) {
    pos = idp_url.find("https://");
    if (pos != std::string::npos) {
      idp_url.erase(pos, 8);
    } else {
      pos = idp_url.find("www.");
      if (pos != std::string::npos) {
        idp_url.erase(pos, 4);
      }
    }
  } else {
    idp_url.erase(pos, 7);
  }
  auto provider_arn = rgw::ARN(idp_url, "oidc-provider", tenant);
  string p_arn = provider_arn.to_string();
  std::unique_ptr<rgw::sal::RGWOIDCProvider> provider = store->get_oidc_provider();
  provider->set_arn(p_arn);
  provider->set_tenant(tenant);
  auto ret = provider->get(dpp);
  if (ret < 0) {
    return nullptr;
  }
  return provider;
}

bool
WebTokenEngine::is_client_id_valid(vector<string>& client_ids, const string& client_id) const
{
  for (auto it : client_ids) {
    if (it == client_id) {
      return true;
    }
  }
  return false;
}

bool
WebTokenEngine::is_cert_valid(const vector<string>& thumbprints, const string& cert) const
{
  //calculate thumbprint of cert
  std::unique_ptr<BIO, decltype(&BIO_free_all)> certbio(BIO_new_mem_buf(cert.data(), cert.size()), BIO_free_all);
  std::unique_ptr<BIO, decltype(&BIO_free_all)> keybio(BIO_new(BIO_s_mem()), BIO_free_all);
  string pw="";
  std::unique_ptr<X509, decltype(&X509_free)> x_509cert(PEM_read_bio_X509(certbio.get(), nullptr, nullptr, const_cast<char*>(pw.c_str())), X509_free);
  const EVP_MD* fprint_type = EVP_sha1();
  unsigned int fprint_size;
  unsigned char fprint[EVP_MAX_MD_SIZE];

  if (!X509_digest(x_509cert.get(), fprint_type, fprint, &fprint_size)) {
    return false;
  }
  stringstream ss;
  for (unsigned int i = 0; i < fprint_size; i++) {
    ss << std::setfill('0') << std::setw(2) << std::hex << (0xFF & (unsigned int)fprint[i]);
  }
  std::string digest = ss.str();

  for (auto& it : thumbprints) {
    if (boost::iequals(it,digest)) {
      return true;
    }
  }
  return false;
}

template <typename T>
void
WebTokenEngine::recurse_and_insert(const string& key, const jwt::claim& c, T& t) const
{
  string s_val;
  jwt::claim::type c_type = c.get_type();
  switch(c_type) {
    case jwt::claim::type::null:
      break;
    case jwt::claim::type::boolean:
    case jwt::claim::type::number:
    case jwt::claim::type::int64:
    {
      s_val = c.to_json().serialize();
      t.emplace(std::make_pair(key, s_val));
      break;
    }
    case jwt::claim::type::string:
    {
      s_val = c.to_json().to_str();
      t.emplace(std::make_pair(key, s_val));
      break;
    }
    case jwt::claim::type::array:
    {
      const picojson::array& arr = c.as_array();
      for (auto& a : arr) {
        recurse_and_insert(key, jwt::claim(a), t);
      }
      break;
    }
    case jwt::claim::type::object:
    {
      const picojson::object& obj = c.as_object();
      for (auto& m : obj) {
        recurse_and_insert(m.first, jwt::claim(m.second), t);
      }
      break;
    }
  }
  return;
}

//Extract all token claims so that they can be later used in the Condition element of Role's trust policy
WebTokenEngine::token_t
WebTokenEngine::get_token_claims(const jwt::decoded_jwt& decoded) const
{
  WebTokenEngine::token_t token;
  const auto& claims = decoded.get_payload_claims();

  for (auto& c : claims) {
    if (c.first == string(princTagsNamespace)) {
      continue;
    }
    recurse_and_insert(c.first, c.second, token);
  }
  return token;
}

//Offline validation of incoming Web Token which is a signed JWT (JSON Web Token)
std::tuple<boost::optional<WebTokenEngine::token_t>, boost::optional<WebTokenEngine::principal_tags_t>>
WebTokenEngine::get_from_jwt(const DoutPrefixProvider* dpp, const std::string& token, const req_state* const s,
			     optional_yield y) const
{
  WebTokenEngine::token_t t;
  WebTokenEngine::principal_tags_t principal_tags;
  try {
    const auto& decoded = jwt::decode(token);

    auto& payload = decoded.get_payload();
    ldpp_dout(dpp, 20) << " payload = " << payload << dendl;

    t = get_token_claims(decoded);

    string iss;
    if (decoded.has_issuer()) {
      iss = decoded.get_issuer();
    }

    set<string> aud;
    if (decoded.has_audience()) {
      aud = decoded.get_audience();
    }

    string client_id;
    if (decoded.has_payload_claim("client_id")) {
      client_id = decoded.get_payload_claim("client_id").as_string();
    }
    if (client_id.empty() && decoded.has_payload_claim("clientId")) {
      client_id = decoded.get_payload_claim("clientId").as_string();
    }
    string azp;
    if (decoded.has_payload_claim("azp")) {
      azp = decoded.get_payload_claim("azp").as_string();
    }

    string role_arn = s->info.args.get("RoleArn");
    auto provider = get_provider(dpp, role_arn, iss);
    if (! provider) {
      ldpp_dout(dpp, 0) << "Couldn't get oidc provider info using input iss" << iss << dendl;
      throw -EACCES;
    }
    if (decoded.has_payload_claim(string(princTagsNamespace))) {
      auto& cl = decoded.get_payload_claim(string(princTagsNamespace));
      if (cl.get_type() == jwt::claim::type::object || cl.get_type() == jwt::claim::type::array) {
        recurse_and_insert("dummy", cl, principal_tags);
        for (auto it : principal_tags) {
          ldpp_dout(dpp, 5) << "Key: " << it.first << " Value: " << it.second << dendl;
        }
      } else {
        ldpp_dout(dpp, 0) << "Malformed principal tags" << cl.as_string() << dendl;
        throw -EINVAL;
      }
    }
    vector<string> client_ids = provider->get_client_ids();
    vector<string> thumbprints = provider->get_thumbprints();
    if (! client_ids.empty()) {
      bool found = false;
      for (auto& it : aud) {
        if (is_client_id_valid(client_ids, it)) {
          found = true;
          break;
        }
      }
      if (! found && ! is_client_id_valid(client_ids, client_id) && ! is_client_id_valid(client_ids, azp)) {
        ldpp_dout(dpp, 0) << "Client id in token doesn't match with that registered with oidc provider" << dendl;
        throw -EACCES;
      }
    }
    //Validate signature
    if (decoded.has_algorithm()) {
      auto& algorithm = decoded.get_algorithm();
      try {
        validate_signature(dpp, decoded, algorithm, iss, thumbprints, y);
      } catch (...) {
        throw -EACCES;
      }
    } else {
      return {boost::none, boost::none};
    }
  } catch (int error) {
    if (error == -EACCES) {
      throw -EACCES;
    }
    ldpp_dout(dpp, 5) << "Invalid JWT token" << dendl;
    return {boost::none, boost::none};
  }
  catch (...) {
    ldpp_dout(dpp, 5) << "Invalid JWT token" << dendl;
    return {boost::none, boost::none};
  }
  return {t, principal_tags};
}

std::string
WebTokenEngine::get_cert_url(const string& iss, const DoutPrefixProvider *dpp, optional_yield y) const
{
  string cert_url;
  string openidc_wellknown_url = iss + "/.well-known/openid-configuration";
  bufferlist openidc_resp;
  RGWHTTPTransceiver openidc_req(cct, "GET", openidc_wellknown_url, &openidc_resp);

  //Headers
  openidc_req.append_header("Content-Type", "application/x-www-form-urlencoded");

  int res = openidc_req.process(y);
  if (res < 0) {
    ldpp_dout(dpp, 10) << "HTTP request res: " << res << dendl;
    throw -EINVAL;
  }

  //Debug only
  ldpp_dout(dpp, 20) << "HTTP status: " << openidc_req.get_http_status() << dendl;
  ldpp_dout(dpp, 20) << "JSON Response is: " << openidc_resp.c_str() << dendl;

  JSONParser parser;
  if (parser.parse(openidc_resp.c_str(), openidc_resp.length())) {
    JSONObj::data_val val;
    if (parser.get_data("jwks_uri", &val)) {
      cert_url = val.str.c_str();
      ldpp_dout(dpp, 20) << "Cert URL is: " << cert_url.c_str() << dendl;
    } else {
      ldpp_dout(dpp, 0) << "Malformed json returned while fetching openidc url" << dendl;
    }
  }
  return cert_url;
}

void
WebTokenEngine::validate_signature(const DoutPrefixProvider* dpp, const jwt::decoded_jwt& decoded, const string& algorithm, const string& iss, const vector<string>& thumbprints, optional_yield y) const
{
  if (algorithm != "HS256" && algorithm != "HS384" && algorithm != "HS512") {
    string cert_url = get_cert_url(iss, dpp, y);
    if (cert_url.empty()) {
      throw -EINVAL;
    }

    // Get certificate
    bufferlist cert_resp;
    RGWHTTPTransceiver cert_req(cct, "GET", cert_url, &cert_resp);
    //Headers
    cert_req.append_header("Content-Type", "application/x-www-form-urlencoded");

    int res = cert_req.process(y);
    if (res < 0) {
      ldpp_dout(dpp, 10) << "HTTP request res: " << res << dendl;
      throw -EINVAL;
    }
    //Debug only
    ldpp_dout(dpp, 20) << "HTTP status: " << cert_req.get_http_status() << dendl;
    ldpp_dout(dpp, 20) << "JSON Response is: " << cert_resp.c_str() << dendl;

    JSONParser parser;
    if (parser.parse(cert_resp.c_str(), cert_resp.length())) {
      JSONObj::data_val val;
      if (parser.get_data("keys", &val)) {
        if (val.str[0] == '[') {
          val.str.erase(0, 1);
        }
        if (val.str[val.str.size() - 1] == ']') {
          val.str = val.str.erase(val.str.size() - 1, 1);
        }
        if (parser.parse(val.str.c_str(), val.str.size())) {
          vector<string> x5c;
          if (JSONDecoder::decode_json("x5c", x5c, &parser)) {
            string cert;
            bool found_valid_cert = false;
            for (auto& it : x5c) {
              cert = "-----BEGIN CERTIFICATE-----\n" + it + "\n-----END CERTIFICATE-----";
              ldpp_dout(dpp, 20) << "Certificate is: " << cert.c_str() << dendl;
              if (is_cert_valid(thumbprints, cert)) {
               found_valid_cert = true;
               break;
              }
              found_valid_cert = true;
            }
            if (! found_valid_cert) {
              ldpp_dout(dpp, 0) << "Cert doesn't match that with the thumbprints registered with oidc provider: " << cert.c_str() << dendl;
              throw -EINVAL;
            }
            try {
              //verify method takes care of expired tokens also
              if (algorithm == "RS256") {
                auto verifier = jwt::verify()
                            .allow_algorithm(jwt::algorithm::rs256{cert});

                verifier.verify(decoded);
              } else if (algorithm == "RS384") {
                auto verifier = jwt::verify()
                            .allow_algorithm(jwt::algorithm::rs384{cert});

                verifier.verify(decoded);
              } else if (algorithm == "RS512") {
                auto verifier = jwt::verify()
                            .allow_algorithm(jwt::algorithm::rs512{cert});

                verifier.verify(decoded);
              } else if (algorithm == "ES256") {
                auto verifier = jwt::verify()
                            .allow_algorithm(jwt::algorithm::es256{cert});

                verifier.verify(decoded);
              } else if (algorithm == "ES384") {
                auto verifier = jwt::verify()
                            .allow_algorithm(jwt::algorithm::es384{cert});

                verifier.verify(decoded);
              } else if (algorithm == "ES512") {
                auto verifier = jwt::verify()
                              .allow_algorithm(jwt::algorithm::es512{cert});

                verifier.verify(decoded);
              } else if (algorithm == "PS256") {
                auto verifier = jwt::verify()
                              .allow_algorithm(jwt::algorithm::ps256{cert});

                verifier.verify(decoded);
              } else if (algorithm == "PS384") {
                auto verifier = jwt::verify()
                              .allow_algorithm(jwt::algorithm::ps384{cert});

                verifier.verify(decoded);
              } else if (algorithm == "PS512") {
                auto verifier = jwt::verify()
                              .allow_algorithm(jwt::algorithm::ps512{cert});

                verifier.verify(decoded);
              }
            } catch (std::runtime_error& e) {
              ldpp_dout(dpp, 0) << "Signature validation failed: " << e.what() << dendl;
              throw;
            }
            catch (...) {
              ldpp_dout(dpp, 0) << "Signature validation failed" << dendl;
              throw;
            }
          } else {
            ldpp_dout(dpp, 0) << "x5c not present" << dendl;
            throw -EINVAL;
          }
        } else {
          ldpp_dout(dpp, 0) << "Malformed JSON object for keys" << dendl;
          throw -EINVAL;
        }
      } else {
        ldpp_dout(dpp, 0) << "keys not present in JSON" << dendl;
        throw -EINVAL;
      } //if-else get-data
    } else {
      ldpp_dout(dpp, 0) << "Malformed json returned while fetching cert" << dendl;
      throw -EINVAL;
    } //if-else parser cert_resp
  } else {
    ldpp_dout(dpp, 0) << "JWT signed by HMAC algos are currently not supported" << dendl;
    throw -EINVAL;
  }
}

WebTokenEngine::result_t
WebTokenEngine::authenticate( const DoutPrefixProvider* dpp,
                              const std::string& token,
                              const req_state* const s,
			      optional_yield y) const
{
  if (! is_applicable(token)) {
    return result_t::deny();
  }

  try {
    auto [t, princ_tags] = get_from_jwt(dpp, token, s, y);
    if (t) {
      string role_session = s->info.args.get("RoleSessionName");
      if (role_session.empty()) {
        ldout(s->cct, 0) << "Role Session Name is empty " << dendl;
        return result_t::deny(-EACCES);
      }
      string role_arn = s->info.args.get("RoleArn");
      string role_tenant = get_role_tenant(role_arn);
      string role_name = get_role_name(role_arn);
      std::unique_ptr<rgw::sal::RGWRole> role = store->get_role(role_name, role_tenant);
      int ret = role->get(dpp, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "Role not found: name:" << role_name << " tenant: " << role_tenant << dendl;
        return result_t::deny(-EACCES);
      }
      boost::optional<multimap<string,string>> role_tags = role->get_tags();
      auto apl = apl_factory->create_apl_web_identity(cct, s, role_session, role_tenant, *t, role_tags, princ_tags);
      return result_t::grant(std::move(apl));
    }
    return result_t::deny(-EACCES);
  }
  catch (...) {
    return result_t::deny(-EACCES);
  }
}

} // namespace rgw::auth::sts

int RGWREST_STS::verify_permission(optional_yield y)
{
  STS::STSService _sts(s->cct, store, s->user->get_id(), s->auth.identity.get());
  sts = std::move(_sts);

  string rArn = s->info.args.get("RoleArn");
  const auto& [ret, role] = sts.getRoleInfo(s, rArn, y);
  if (ret < 0) {
    ldpp_dout(this, 0) << "failed to get role info using role arn: " << rArn << dendl;
    return ret;
  }
  string policy = role->get_assume_role_policy();
  buffer::list bl = buffer::list::static_from_string(policy);

  //Parse the policy
  //TODO - This step should be part of Role Creation
  try {
    const rgw::IAM::Policy p(s->cct, s->user->get_tenant(), bl);
    if (!s->principal_tags.empty()) {
      auto res = p.eval(s->env, *s->auth.identity, rgw::IAM::stsTagSession, boost::none);
      if (res != rgw::IAM::Effect::Allow) {
        ldout(s->cct, 0) << "evaluating policy for stsTagSession returned deny/pass" << dendl;
        return -EPERM;
      }
    }
    uint64_t op;
    if (get_type() == RGW_STS_ASSUME_ROLE_WEB_IDENTITY) {
      op = rgw::IAM::stsAssumeRoleWithWebIdentity;
    } else {
      op = rgw::IAM::stsAssumeRole;
    }

    auto res = p.eval(s->env, *s->auth.identity, op, boost::none);
    if (res != rgw::IAM::Effect::Allow) {
      ldout(s->cct, 0) << "evaluating policy for op: " << op << " returned deny/pass" << dendl;
      return -EPERM;
    }
  } catch (rgw::IAM::PolicyParseException& e) {
    ldpp_dout(this, 0) << "failed to parse policy: " << e.what() << dendl;
    return -EPERM;
  }

  return 0;
}

void RGWREST_STS::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

int RGWSTSGetSessionToken::verify_permission(optional_yield y)
{
  rgw::Partition partition = rgw::Partition::aws;
  rgw::Service service = rgw::Service::s3;
  if (!verify_user_permission(this,
                              s,
                              rgw::ARN(partition, service, "", s->user->get_tenant(), ""),
                              rgw::IAM::stsGetSessionToken)) {
    ldpp_dout(this, 0) << "User does not have permssion to perform GetSessionToken" << dendl;
    return -EACCES;
  }

  return 0;
}

int RGWSTSGetSessionToken::get_params()
{
  duration = s->info.args.get("DurationSeconds");
  serialNumber = s->info.args.get("SerialNumber");
  tokenCode = s->info.args.get("TokenCode");

  if (! duration.empty()) {
    string err;
    uint64_t duration_in_secs = strict_strtoll(duration.c_str(), 10, &err);
    if (!err.empty()) {
      ldpp_dout(this, 0) << "Invalid value of input duration: " << duration << dendl;
      return -EINVAL;
    }

    if (duration_in_secs < STS::GetSessionTokenRequest::getMinDuration() ||
            duration_in_secs > s->cct->_conf->rgw_sts_max_session_duration) {
      ldpp_dout(this, 0) << "Invalid duration in secs: " << duration_in_secs << dendl;
      return -EINVAL;
    }
  }

  return 0;
}

void RGWSTSGetSessionToken::execute(optional_yield y)
{
  if (op_ret = get_params(); op_ret < 0) {
    return;
  }

  STS::STSService sts(s->cct, store, s->user->get_id(), s->auth.identity.get());

  STS::GetSessionTokenRequest req(duration, serialNumber, tokenCode);
  const auto& [ret, creds] = sts.getSessionToken(this, req);
  op_ret = std::move(ret);
  //Dump the output
  if (op_ret == 0) {
    s->formatter->open_object_section("GetSessionTokenResponse");
    s->formatter->open_object_section("GetSessionTokenResult");
    s->formatter->open_object_section("Credentials");
    creds.dump(s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWSTSAssumeRoleWithWebIdentity::get_params()
{
  duration = s->info.args.get("DurationSeconds");
  providerId = s->info.args.get("ProviderId");
  policy = s->info.args.get("Policy");
  roleArn = s->info.args.get("RoleArn");
  roleSessionName = s->info.args.get("RoleSessionName");
  iss = s->info.args.get("provider_id");
  sub = s->info.args.get("sub");
  aud = s->info.args.get("aud");

  if (roleArn.empty() || roleSessionName.empty() || sub.empty() || aud.empty()) {
    ldpp_dout(this, 0) << "ERROR: one of role arn or role session name or token is empty" << dendl;
    return -EINVAL;
  }

  if (! policy.empty()) {
    bufferlist bl = bufferlist::static_from_string(policy);
    try {
      const rgw::IAM::Policy p(s->cct, s->user->get_tenant(), bl);
    }
    catch (rgw::IAM::PolicyParseException& e) {
      ldpp_dout(this, 20) << "failed to parse policy: " << e.what() << "policy" << policy << dendl;
      return -ERR_MALFORMED_DOC;
    }
  }

  return 0;
}

void RGWSTSAssumeRoleWithWebIdentity::execute(optional_yield y)
{
  if (op_ret = get_params(); op_ret < 0) {
    return;
  }

  STS::AssumeRoleWithWebIdentityRequest req(s->cct, duration, providerId, policy, roleArn,
                        roleSessionName, iss, sub, aud, s->principal_tags);
  STS::AssumeRoleWithWebIdentityResponse response = sts.assumeRoleWithWebIdentity(this, req);
  op_ret = std::move(response.assumeRoleResp.retCode);

  //Dump the output
  if (op_ret == 0) {
    s->formatter->open_object_section("AssumeRoleWithWebIdentityResponse");
    s->formatter->open_object_section("AssumeRoleWithWebIdentityResult");
    encode_json("SubjectFromWebIdentityToken", response.sub , s->formatter);
    encode_json("Audience", response.aud , s->formatter);
    s->formatter->open_object_section("AssumedRoleUser");
    response.assumeRoleResp.user.dump(s->formatter);
    s->formatter->close_section();
    s->formatter->open_object_section("Credentials");
    response.assumeRoleResp.creds.dump(s->formatter);
    s->formatter->close_section();
    encode_json("Provider", response.providerId , s->formatter);
    encode_json("PackedPolicySize", response.assumeRoleResp.packedPolicySize , s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWSTSAssumeRole::get_params()
{
  duration = s->info.args.get("DurationSeconds");
  externalId = s->info.args.get("ExternalId");
  policy = s->info.args.get("Policy");
  roleArn = s->info.args.get("RoleArn");
  roleSessionName = s->info.args.get("RoleSessionName");
  serialNumber = s->info.args.get("SerialNumber");
  tokenCode = s->info.args.get("TokenCode");

  if (roleArn.empty() || roleSessionName.empty()) {
    ldpp_dout(this, 0) << "ERROR: one of role arn or role session name is empty" << dendl;
    return -EINVAL;
  }

  if (! policy.empty()) {
    bufferlist bl = bufferlist::static_from_string(policy);
    try {
      const rgw::IAM::Policy p(s->cct, s->user->get_tenant(), bl);
    }
    catch (rgw::IAM::PolicyParseException& e) {
      ldpp_dout(this, 0) << "failed to parse policy: " << e.what() << "policy" << policy << dendl;
      return -ERR_MALFORMED_DOC;
    }
  }

  return 0;
}

void RGWSTSAssumeRole::execute(optional_yield y)
{
  if (op_ret = get_params(); op_ret < 0) {
    return;
  }

  STS::AssumeRoleRequest req(s->cct, duration, externalId, policy, roleArn,
                        roleSessionName, serialNumber, tokenCode);
  STS::AssumeRoleResponse response = sts.assumeRole(s, req, y);
  op_ret = std::move(response.retCode);
  //Dump the output
  if (op_ret == 0) {
    s->formatter->open_object_section("AssumeRoleResponse");
    s->formatter->open_object_section("AssumeRoleResult");
    s->formatter->open_object_section("Credentials");
    response.creds.dump(s->formatter);
    s->formatter->close_section();
    s->formatter->open_object_section("AssumedRoleUser");
    response.user.dump(s->formatter);
    s->formatter->close_section();
    encode_json("PackedPolicySize", response.packedPolicySize , s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGW_Auth_STS::authorize(const DoutPrefixProvider *dpp,
                            rgw::sal::Store* store,
                            const rgw::auth::StrategyRegistry& auth_registry,
                            struct req_state *s, optional_yield y)
{
  return rgw::auth::Strategy::apply(dpp, auth_registry.get_sts(), s, y);
}

void RGWHandler_REST_STS::rgw_sts_parse_input()
{
  if (post_body.size() > 0) {
    ldpp_dout(s, 10) << "Content of POST: " << post_body << dendl;

    if (post_body.find("Action") != string::npos) {
      boost::char_separator<char> sep("&");
      boost::tokenizer<boost::char_separator<char>> tokens(post_body, sep);
      for (const auto& t : tokens) {
        auto pos = t.find("=");
        if (pos != string::npos) {
          constexpr bool in_query = true; // replace '+' with ' '
          s->info.args.append(t.substr(0,pos),
                              url_decode(t.substr(pos+1, t.size() -1), in_query));
        }
      }
    }
  }
  auto payload_hash = rgw::auth::s3::calc_v4_payload_hash(post_body);
  s->info.args.append("PayloadHash", payload_hash);
}

RGWOp *RGWHandler_REST_STS::op_post()
{
  rgw_sts_parse_input();

  if (s->info.args.exists("Action"))    {
    string action = s->info.args.get("Action");
    if (action == "AssumeRole") {
      return new RGWSTSAssumeRole;
    } else if (action == "GetSessionToken") {
      return new RGWSTSGetSessionToken;
    } else if (action == "AssumeRoleWithWebIdentity") {
      return new RGWSTSAssumeRoleWithWebIdentity;
    }
  }

  return nullptr;
}

int RGWHandler_REST_STS::init(rgw::sal::Store* store,
                              struct req_state *s,
                              rgw::io::BasicClient *cio)
{
  s->dialect = "sts";

  if (int ret = RGWHandler_REST_STS::init_from_header(s, RGW_FORMAT_XML, true); ret < 0) {
    ldpp_dout(s, 10) << "init_from_header returned err=" << ret <<  dendl;
    return ret;
  }

  return RGWHandler_REST::init(store, s, cio);
}

int RGWHandler_REST_STS::authorize(const DoutPrefixProvider* dpp, optional_yield y)
{
  if (s->info.args.exists("Action") && s->info.args.get("Action") == "AssumeRoleWithWebIdentity") {
    return RGW_Auth_STS::authorize(dpp, store, auth_registry, s, y);
  }
  return RGW_Auth_S3::authorize(dpp, store, auth_registry, s, y);
}

int RGWHandler_REST_STS::init_from_header(struct req_state* s,
                                          int default_formatter,
                                          bool configurable_format)
{
  string req;
  string first;

  s->prot_flags = RGW_REST_STS;

  const char *p, *req_name;
  if (req_name = s->relative_uri.c_str(); *req_name == '?') {
    p = req_name;
  } else {
    p = s->info.request_params.c_str();
  }

  s->info.args.set(p);
  s->info.args.parse(s);

  /* must be called after the args parsing */
  if (int ret = allocate_formatter(s, default_formatter, configurable_format); ret < 0)
    return ret;

  if (*req_name != '/')
    return 0;

  req_name++;

  if (!*req_name)
    return 0;

  req = req_name;
  int pos = req.find('/');
  if (pos >= 0) {
    first = req.substr(0, pos);
  } else {
    first = req;
  }

  return 0;
}

RGWHandler_REST*
RGWRESTMgr_STS::get_handler(rgw::sal::Store* store,
			    struct req_state* const s,
			    const rgw::auth::StrategyRegistry& auth_registry,
			    const std::string& frontend_prefix)
{
  return new RGWHandler_REST_STS(auth_registry);
}
