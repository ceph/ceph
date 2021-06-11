// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
#include <vector>
#include <string>
#include <array>
#include <string_view>
#include <sstream>
#include <memory>
#include <regex>

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

//Offline validation of incoming Web Token which is a signed JWT (JSON Web Token)
boost::optional<WebTokenEngine::token_t>
WebTokenEngine::get_from_jwt(const DoutPrefixProvider* dpp, const std::string& token, const req_state* const s,
			     optional_yield y) const
{
  WebTokenEngine::token_t t;
  try {
    const auto& decoded = jwt::decode(token);

    auto& payload = decoded.get_payload();
    ldpp_dout(dpp, 20) << " payload = " << payload << dendl;
    if (decoded.has_issuer()) {
      t.iss = decoded.get_issuer();
    }
    if (decoded.has_audience()) {
      auto aud = decoded.get_audience();
      t.aud = *(aud.begin());
    }
    if (decoded.has_subject()) {
      t.sub = decoded.get_subject();
    }
    if (decoded.has_payload_claim("client_id")) {
      t.client_id = decoded.get_payload_claim("client_id").as_string();
    }
    if (t.client_id.empty() && decoded.has_payload_claim("clientId")) {
      t.client_id = decoded.get_payload_claim("clientId").as_string();
    }
    string role_arn = s->info.args.get("RoleArn");
    auto provider = get_provider(dpp, role_arn, t.iss);
    if (! provider) {
      ldpp_dout(dpp, 0) << "Couldn't get oidc provider info using input iss" << t.iss << dendl;
      throw -EACCES;
    }
    vector<string> client_ids = provider->get_client_ids();
    vector<string> thumbprints = provider->get_thumbprints();
    if (! client_ids.empty()) {
      if (! is_client_id_valid(client_ids, t.client_id) && ! is_client_id_valid(client_ids, t.aud)) {
        ldpp_dout(dpp, 0) << "Client id in token doesn't match with that registered with oidc provider" << dendl;
        throw -EACCES;
      }
    }
    //Validate signature
    if (decoded.has_algorithm()) {
      auto& algorithm = decoded.get_algorithm();
      try {
        validate_signature(dpp, decoded, algorithm, t.iss, thumbprints, y);
      } catch (...) {
        throw -EACCES;
      }
    } else {
      return boost::none;
    }
  } catch (int error) {
    if (error == -EACCES) {
      throw -EACCES;
    }
    ldpp_dout(dpp, 5) << "Invalid JWT: " << error << dendl;
    return boost::none;
  }
  catch (const std::exception& e) {
    ldpp_dout(dpp, 5) << "Invalid JWT: " << e.what() << dendl;
    return boost::none;
  }
  return t;
}

std::string
WebTokenEngine::get_jwks_url(const string& iss, const DoutPrefixProvider *dpp, optional_yield y) const
{
  string jwks_url;
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
      jwks_url = val.str.c_str();
      ldpp_dout(dpp, 20) << "JWKS URL is: " << jwks_url.c_str() << dendl;
    } else {
      ldpp_dout(dpp, 0) << "Malformed json returned while fetching jwks url" << dendl;
    }
  }
  return jwks_url;
}

vector<string>
WebTokenEngine::get_x5c_certs_from_x5u_url(const DoutPrefixProvider* dpp, const string& x5u_url, optional_yield y) const
{
  bufferlist x5u_resp;
  RGWHTTPTransceiver x5u_req(cct, "GET", x5u_url, &x5u_resp);
  //Headers
  x5u_req.append_header("Content-Type", "application/x-www-form-urlencoded");

  int res = x5u_req.process(y);
  if (res < 0) {
    ldpp_dout(dpp, 10) << "HTTP request res: " << res << " for getting x5c from x5u" << dendl;
    throw -EINVAL;
  }
  string certs = x5u_resp.c_str();
  vector<string> x5c;
  const regex pattern("(-----BEGIN CERTIFICATE-----(.*?)-----END CERTIFICATE-----)+");
  for(sregex_iterator it = sregex_iterator(
      certs.begin(), certs.end(), pattern);
      it != sregex_iterator(); it++) {
      smatch match;
      match = *it;
      x5c.emplace_back(match.str(1));
      ldpp_dout(dpp, 10) << "Matched: " << match.str(1) << dendl;
  }
  return x5c;
}

void
WebTokenEngine::validate_signature_using_cert(const DoutPrefixProvider* dpp, const jwt::decoded_jwt& decoded, const string& algorithm, const vector<string>& certs, const vector<string>& thumbprints, bool add_pem_str) const
{
  string cert;
  bool found_valid_cert = false;
  for (auto& it : certs) {
    if (add_pem_str) {
      cert = "-----BEGIN CERTIFICATE-----\n" + it + "\n-----END CERTIFICATE-----";
    } else {
      cert = it;
    }
    if (is_cert_valid(thumbprints, cert)) {
      found_valid_cert = true;
      break;
    }
  }
  if (! found_valid_cert) {
    ldpp_dout(dpp, 0) << "Cert doesn't match that with the thumbprints registered with oidc provider: " << cert.c_str() << dendl;
    throw -EINVAL;
  }
  cert.clear();
  if (add_pem_str) {
    cert = "-----BEGIN CERTIFICATE-----\n" + certs[0] + "\n-----END CERTIFICATE-----"; //first cert always contains the public key
  } else {
    cert = certs[0];
  }
  ldpp_dout(dpp, 20) << "Certificate is: " << cert.c_str() << dendl;
  //verify method takes care of expired tokens also
  try {
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
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "Signature validation using x5c failed: " << e.what() << dendl;
    throw;
  }
  catch (...) {
    ldpp_dout(dpp, 0) << "Signature validation using x5c failed" << dendl;
    throw;
  }
  ldpp_dout(dpp, 10) << "Verified signature using x5c certificate "<< dendl;
}

void
WebTokenEngine::validate_signature_using_n_e(const DoutPrefixProvider* dpp, const jwt::decoded_jwt& decoded, const string& n, const string& e, const string &algorithm) const
{
  try {
    if (algorithm == "RS256") {
      auto verifier = jwt::verify()
                  .allow_algorithm(jwt::algorithm::rs256().setModulusAndExponent(n,e));
      verifier.verify(decoded);
    } else if (algorithm == "RS384") {
      auto verifier = jwt::verify()
                  .allow_algorithm(jwt::algorithm::rs384().setModulusAndExponent(n,e));
      verifier.verify(decoded);
    } else if (algorithm == "RS512") {
      auto verifier = jwt::verify()
                  .allow_algorithm(jwt::algorithm::rs512().setModulusAndExponent(n,e));
      verifier.verify(decoded);
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "Signature validation using n, e failed: " << e.what() << dendl;
    throw;
  }
  catch (...) {
    ldpp_dout(dpp, 0) << "Signature validation n, e failed" << dendl;
  }
  ldpp_dout(dpp, 10) << "Verified signature using n and e"<< dendl;
  return;
}

void
WebTokenEngine::validate_signature(const DoutPrefixProvider* dpp, const jwt::decoded_jwt& decoded, const string& algorithm, const string& iss, const vector<string>& thumbprints, optional_yield y) const
{
  if (algorithm != "HS256" && algorithm != "HS384" && algorithm != "HS512") {
    //Get x5c, x5u, kid, x5t from header
    //if x5c present use it
    //if x5u is present, get cert
    //Use kid or x5t to locate a valid cert from JWK
    string kid, x5t, x5t256;
    if (decoded.has_header_claim("x5c")) {
      auto &claim = decoded.get_header_claim("x5c").as_array();
      vector<string> x5c;
      for (auto& it : claim) {
        x5c.emplace_back(it.to_str());
      }
      validate_signature_using_cert(dpp, decoded, algorithm, x5c, thumbprints);
      return;
    }
    if (decoded.has_header_claim("x5u")) {
      string x5u_url = decoded.get_header_claim("x5u").as_string();
      vector<string> x5c = get_x5c_certs_from_x5u_url(dpp, x5u_url, y);
      validate_signature_using_cert(dpp, decoded, algorithm, x5c, thumbprints, false);
      return;
    }
    if (decoded.has_key_id()) {
      kid = decoded.get_key_id();
    }
    if (decoded.has_header_claim("x5t")) {
      x5t = decoded.get_header_claim("x5t").as_string();
    }
    if (decoded.has_header_claim("x5t#256")) {
      x5t256 = decoded.get_header_claim("x5t#256").as_string();
    }

    string jwks_url = get_jwks_url(iss, dpp, y);
    if (jwks_url.empty()) {
      throw -EINVAL;
    }
    // Get certificate
    bufferlist jwks_resp;
    RGWHTTPTransceiver jwks_req(cct, "GET", jwks_url, &jwks_resp);
    //Headers
    jwks_req.append_header("Content-Type", "application/x-www-form-urlencoded");

    int res = jwks_req.process(y);
    if (res < 0) {
      ldpp_dout(dpp, 10) << "HTTP request res: " << res << dendl;
      throw -EINVAL;
    }
    //Debug only
    ldpp_dout(dpp, 20) << "HTTP status for jwks uri is: " << jwks_req.get_http_status() << dendl;
    ldpp_dout(dpp, 20) << "JSON Response for jwks uri is: " << jwks_resp.c_str() << dendl;

    JSONParser parser;
    if (parser.parse(jwks_resp.c_str(), jwks_resp.length())) {
      JSONObj* val = parser.find_obj("keys");
      if (val && val->is_array()) {
          string key_kid, key_x5t, key_x5t256;
          vector<string> keys = val->get_array_elements();
          for (auto &key : keys) {
            JSONParser k_parser;
            if (k_parser.parse(key.c_str(), key.size())) {
              if (JSONDecoder::decode_json("kid", key_kid, &k_parser) ||
                  JSONDecoder::decode_json("x5t", key_x5t, &k_parser) ||
                  JSONDecoder::decode_json("x5t#S256", key_x5t256, &k_parser)) {
                if (kid != key_kid && x5t != key_x5t && x5t256 != key_x5t256)
                  continue; //if none matches then continue to next
              }
              // Check if x5c is present and can be used for signature validation
              vector<string> x5c;
              if (JSONDecoder::decode_json("x5c", x5c, &k_parser)) {
                validate_signature_using_cert(dpp, decoded, algorithm, x5c, thumbprints);
                return;
              } else {
                ldpp_dout(dpp, 0) << "x5c not present" << dendl;
              }

              // If x5u is present, use it to get certs and validate signature
              string x5u;
              if (JSONDecoder::decode_json("x5u", x5u, &k_parser)) {
                vector<string> x5c = get_x5c_certs_from_x5u_url(dpp, x5u, y);
                validate_signature_using_cert(dpp, decoded, algorithm, x5c, thumbprints, false);
                return;
              } else {
                ldpp_dout(dpp, 0) << "x5u not present" << dendl;
              }

              // If x5c is not present, then check if n,e are present for RSA group of algorithms
              if (algorithm == "RS256" || algorithm == "RS384" || algorithm == "RS512") {
                string n, e; //modulus and exponent
                if (JSONDecoder::decode_json("n", n, &k_parser) && JSONDecoder::decode_json("e", e, &k_parser)) {
                  validate_signature_using_n_e(dpp, decoded, algorithm, n, e);
                  return;
                } else {
                  ldpp_dout(dpp, 0) << "n, e not present" << dendl;
                }
              }

              ldpp_dout(dpp, 0) << "Signature can not be validated with the input given in keys: "<< dendl;
              throw -EINVAL;
            }
          } //end for iterate through keys
        } else {
        ldpp_dout(dpp, 0) << "keys not present in JSON" << dendl;
        throw -EINVAL;
      } //if-else get-data
    } else {
      ldpp_dout(dpp, 0) << "Malformed json returned while fetching jwks" << dendl;
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
  boost::optional<WebTokenEngine::token_t> t;

  if (! is_applicable(token)) {
    return result_t::deny();
  }

  try {
    t = get_from_jwt(dpp, token, s, y);
  }
  catch (...) {
    return result_t::deny(-EACCES);
  }

  if (t) {
    string role_session = s->info.args.get("RoleSessionName");
    if (role_session.empty()) {
      ldpp_dout(dpp, 0) << "Role Session Name is empty " << dendl;
      return result_t::deny(-EACCES);
    }
    string role_arn = s->info.args.get("RoleArn");
    string role_tenant = get_role_tenant(role_arn);
    auto apl = apl_factory->create_apl_web_identity(cct, s, role_session, role_tenant, *t);
    return result_t::grant(std::move(apl));
  }
  return result_t::deny(-EACCES);
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
    //Check if the input role arn is there as one of the Principals in the policy,
    // If yes, then return 0, else -EPERM
    auto p_res = p.eval_principal(s->env, *s->auth.identity);
    if (p_res == rgw::IAM::Effect::Deny) {
      ldpp_dout(this, 0) << "evaluating principal returned deny" << dendl;
      return -EPERM;
    }
    auto c_res = p.eval_conditions(s->env);
    if (c_res == rgw::IAM::Effect::Deny) {
      ldpp_dout(this, 0) << "evaluating condition returned deny" << dendl;
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
  const auto& [ret, creds] = sts.getSessionToken(req);
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
                        roleSessionName, iss, sub, aud);
  STS::AssumeRoleWithWebIdentityResponse response = sts.assumeRoleWithWebIdentity(req);
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
          s->info.args.append(t.substr(0,pos),
                              url_decode(t.substr(pos+1, t.size() -1)));
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
