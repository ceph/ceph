// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/pem.h>
#include <openssl/bio.h>

#include <vector>
#include <string>
#include <array>
#include <iomanip>
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
#include "rgw_account.h"
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
#include "rgw_asio_thread.h"


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

int WebTokenEngine::load_provider(const DoutPrefixProvider* dpp, optional_yield y,
                                  const string& role_arn, const string& iss,
                                  RGWOIDCProviderInfo& info) const
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

  return driver->load_oidc_provider(dpp, y, tenant, idp_url, info);
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
    RGWOIDCProviderInfo provider;
    int r = load_provider(dpp, y, role_arn, iss, provider);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "Couldn't get oidc provider info using input iss" << iss << dendl;
      throw std::system_error(EACCES, std::system_category());
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
        throw std::system_error(EINVAL, std::system_category());
      }
    }
    if (! provider.client_ids.empty()) {
      bool found = false;
      for (auto& it : aud) {
        if (is_client_id_valid(provider.client_ids, it)) {
          found = true;
          break;
        }
      }
      if (! found && ! is_client_id_valid(provider.client_ids, client_id) && ! is_client_id_valid(provider.client_ids, azp)) {
        ldpp_dout(dpp, 0) << "Client id in token doesn't match with that registered with oidc provider" << dendl;
        throw std::system_error(EACCES, std::system_category());
      }
    }
    //Validate signature
    if (decoded.has_algorithm()) {
      auto& algorithm = decoded.get_algorithm();
      try {
        validate_signature(dpp, decoded, algorithm, iss, provider.thumbprints, y);
      } catch (const std::exception& e) {
        throw std::system_error(EACCES, std::system_category());
      }
    } else {
      return {boost::none, boost::none};
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 5) << "Invalid JWT token" << dendl;
    return {boost::none, boost::none};
  }
  return {t, principal_tags};
}

std::string
WebTokenEngine::get_cert_url(const string& iss, const DoutPrefixProvider *dpp, optional_yield y) const
{
  string cert_url;
  string openidc_wellknown_url = iss;
  bufferlist openidc_resp;

  if (openidc_wellknown_url.back() == '/') {
    openidc_wellknown_url.pop_back();
  }
  openidc_wellknown_url.append("/.well-known/openid-configuration");

  RGWHTTPTransceiver openidc_req(cct, "GET", openidc_wellknown_url, &openidc_resp);

  //Headers
  openidc_req.append_header("Content-Type", "application/x-www-form-urlencoded");

  int res = openidc_req.process(dpp, y);
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

std::string
WebTokenEngine::get_top_level_domain_from_host(const DoutPrefixProvider* dpp, const std::string& hostname) const
{
  std::string host = hostname;
  //get top level domain only, removing https etc
  auto pos = host.find("http://");
  if (pos == std::string::npos) {
    pos = host.find("https://");
    if (pos != std::string::npos) {
      host.erase(pos, 8);
    } else {
      pos = host.find("www.");
      if (pos != std::string::npos) {
        host.erase(pos, 4);
      }
    }
  } else {
    host.erase(pos, 7);
  }

  pos = host.find("/");
  if (pos != std::string::npos) {
    host.erase(pos, (host.length() - 1));
  }

  ldpp_dout(dpp, 20) << "Top level domain name of the host is: " << host << dendl;
  return host;
}

int
WebTokenEngine::create_connection(const DoutPrefixProvider* dpp, const std::string& hostname, int port) const
{
  struct hostent* host = gethostbyname(hostname.c_str());
  if (!host) {
    ldpp_dout(dpp, 0) << "gethostbyname failed for host: " << hostname << dendl;
    return -1;
  }

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  memcpy(&addr.sin_addr, host->h_addr, host->h_length);

  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    ldpp_dout(dpp, 10) << "creation of socket failed: " << sock << dendl;
    return -1;
  }

  int ret = connect(sock, (struct sockaddr*)&addr, sizeof(addr));
  if (ret != 0) {
    ldpp_dout(dpp, 10) << "connection to socket failed: " << ret << dendl;
    close(sock);
    return -1;
  }

  return sock;
}

std::string
WebTokenEngine::extract_last_certificate(const DoutPrefixProvider* dpp, const std::string& pem_chain) const
{
  const std::string BEGIN_MARKER = "-----BEGIN CERTIFICATE-----";
  const std::string END_MARKER = "-----END CERTIFICATE-----";

  // Find the last occurrence of BEGIN marker
  size_t begin_pos = pem_chain.rfind(BEGIN_MARKER);
  if (begin_pos == std::string::npos) {
    ldpp_dout(dpp, 10) << "No BEGIN marker found in certificate chain" << dendl;
    throw std::runtime_error("No BEGIN marker found in certificate chain");
  }

  // Find the END marker that comes after the last BEGIN marker
  size_t end_pos = pem_chain.find(END_MARKER, begin_pos);
  if (end_pos == std::string::npos) {
    ldpp_dout(dpp, 10) << "No matching END marker found after last BEGIN marker" << dendl;
    throw std::runtime_error("No matching END marker found after last BEGIN marker");
  }

  // Calculate the start and length of the complete certificate (including markers)
  size_t cert_length = (end_pos + END_MARKER.length()) - begin_pos;

  // Extract the complete certificate
  std::string last_cert = pem_chain.substr(begin_pos, cert_length);

  return last_cert;
}

void
WebTokenEngine::shutdown_ssl(const DoutPrefixProvider* dpp, SSL* ssl, SSL_CTX* ctx) const
{
  int status = SSL_shutdown(ssl);
  //status = 0, we have issued shutdown but not acknowledged by remote connection
  //status = 1, remote connection has shutdown
  //status !=1 && != 0, error
  if (status == 0) {
    status = SSL_shutdown(ssl);
  }
  if (status != 1) {
    auto error = SSL_get_error(ssl, status);
    ldpp_dout(dpp, 10) << "SSL shutdown failed with error: "<< error << dendl;
  }
  SSL_free(ssl); // This also frees cert chains
  SSL_CTX_free(ctx);
}

std::string
WebTokenEngine::connect_to_host_get_cert_chain(const DoutPrefixProvider* dpp, const std::string& hostname, int port) const
{
  maybe_warn_about_blocking(dpp);

  // Create SSL context
  SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
  if (!ctx) {
    throw std::runtime_error("Failed to create SSL context");
  }

  // Create SSL connection
  SSL* ssl = SSL_new(ctx);
  if (!ssl) {
    SSL_CTX_free(ctx);
    throw std::runtime_error("Failed to create SSL object");
  }

  // Create socket and connect
  int sock = create_connection(dpp, hostname, port);
  if (sock < 0) {
    SSL_CTX_free(ctx);
    throw std::runtime_error("Failed to connect to host:" + hostname);
  }

  SSL_set_fd(ssl, sock);

  // Set SNI hostname
  SSL_set_tlsext_host_name(ssl, hostname.c_str());

  // Perform handshake
  if (SSL_connect(ssl) != 1) {
    SSL_free(ssl);
    SSL_CTX_free(ctx);
    close(sock);
    throw std::runtime_error("SSL handshake failed");
  }

  std::string chain_pem;

  // Get the peer certificate (server's certificate)
  X509* cert = SSL_get_peer_certificate(ssl);
  if (!cert) {
    shutdown_ssl(dpp, ssl, ctx);
    close(sock);
    throw std::runtime_error("No certificate was presented");
  }

  // Get the chain
  STACK_OF(X509)* chain = SSL_get_peer_cert_chain(ssl);
  if (!chain) {
    X509_free(cert);
    shutdown_ssl(dpp, ssl, ctx);
    close(sock);
    throw std::runtime_error("Failed to get certificate chain");
  }

  // Create BIO for PEM output
  BIO* bio = BIO_new(BIO_s_mem());
  if (!bio) {
    X509_free(cert);
    shutdown_ssl(dpp, ssl, ctx);
    close(sock);
    throw std::runtime_error("Failed to to create BIO for PEM");
  }

  // Write the server's certificate first
  if (!PEM_write_bio_X509(bio, cert)) {
    BIO_free(bio);
    X509_free(cert);
    shutdown_ssl(dpp, ssl, ctx);
    close(sock);
    throw std::runtime_error("Failed to write server certificate to BIO");
  }
  X509_free(cert);

  // Write the rest of the chain
  int chain_length = sk_X509_num(chain);
  for (int i = 0; i < chain_length; i++) {
    X509* chain_cert = sk_X509_value(chain, i);
    if (!chain_cert) {
      BIO_free(bio);
      shutdown_ssl(dpp, ssl, ctx);
      close(sock);
      throw std::runtime_error("NULL certificate encountered in chain at position " + std::to_string(i));
    }
    if (!PEM_write_bio_X509(bio, chain_cert)) {
      BIO_free(bio);
      shutdown_ssl(dpp, ssl, ctx);
      close(sock);
      throw std::runtime_error("Failed to write chain certificate to BIO at position " + std::to_string(i));
    }
  }

  // Get the PEM data
  char* pem_data;
  long pem_size = BIO_get_mem_data(bio, &pem_data);
  chain_pem = std::string(pem_data, pem_size);

  // Cleanup
  BIO_free(bio);
  shutdown_ssl(dpp, ssl, ctx);
  close(sock);

  return chain_pem;
}

void
WebTokenEngine::validate_signature_using_n_e(const DoutPrefixProvider* dpp, const jwt::decoded_jwt& decoded, const std::string &algorithm, const std::string& n, const std::string& e) const
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
    ldpp_dout(dpp, 10) << std::string("Signature validation using n, e failed: ") + e.what() << dendl;
    throw std::system_error(EACCES, std::system_category(), std::string("Signature validation using n, e failed: ") + e.what());
  }
  ldpp_dout(dpp, 10) << "Verified signature using n and e"<< dendl;
  return;
}

void
WebTokenEngine::validate_signature(const DoutPrefixProvider* dpp, const jwt::decoded_jwt& decoded, const string& algorithm, const string& iss, const vector<string>& thumbprints, optional_yield y) const
{
  if (algorithm != "HS256" && algorithm != "HS384" && algorithm != "HS512") {
    string cert_url = get_cert_url(iss, dpp, y);
    if (cert_url.empty()) {
      throw std::system_error(EINVAL, std::system_category());
    }

    // Get certificate
    bufferlist cert_resp;
    RGWHTTPTransceiver cert_req(cct, "GET", cert_url, &cert_resp);
    //Headers
    cert_req.append_header("Content-Type", "application/x-www-form-urlencoded");

    int res = cert_req.process(dpp, y);
    if (res < 0) {
      ldpp_dout(dpp, 10) << "HTTP request res: " << res << dendl;
      throw std::system_error(EINVAL, std::system_category());
    }
    //Debug only
    ldpp_dout(dpp, 20) << "HTTP status: " << cert_req.get_http_status() << dendl;
    ldpp_dout(dpp, 20) << "JSON Response is: " << cert_resp.c_str() << dendl;

    JSONParser parser;
    if (parser.parse(cert_resp.c_str(), cert_resp.length())) {
      JSONObj* val = parser.find_obj("keys");
      if (val && val->is_array()) {
        vector<string> keys = val->get_array_elements();
        for (auto &key : keys) {
          JSONParser k_parser;
          vector<string> x5c;
          std::string use;
          bool skip{false};
          if (k_parser.parse(key.c_str(), key.size())) {
            if (JSONDecoder::decode_json("use", use, &k_parser)) {
              if (use == "enc") { //if key is for encryption then don't use x5c or n and e for signature validation
                skip = true;
              } else if (use == "sig") {
                skip = false;
              }
            }
            if (JSONDecoder::decode_json("x5c", x5c, &k_parser)) {
              if (skip == true) {
                continue;
              }
              string cert;
              bool found_valid_cert = false;
              for (auto& it : x5c) {
                cert = "-----BEGIN CERTIFICATE-----\n" + it + "\n-----END CERTIFICATE-----";
                ldpp_dout(dpp, 20) << "Certificate is: " << cert.c_str() << dendl;
                if (is_cert_valid(thumbprints, cert)) {
                  found_valid_cert = true;
                  break;
                }
              }
              if (! found_valid_cert) {
                ldpp_dout(dpp, 0) << "Cert doesn't match that with the thumbprints registered with oidc provider: " << cert.c_str() << dendl;
                throw std::system_error(EINVAL, std::system_category());
              }
              try {
                //verify method takes care of expired tokens also
                if (algorithm == "RS256") {
                  auto verifier = jwt::verify()
                              .allow_algorithm(jwt::algorithm::rs256{cert});

                  verifier.verify(decoded);
                  return;
                } else if (algorithm == "RS384") {
                  auto verifier = jwt::verify()
                              .allow_algorithm(jwt::algorithm::rs384{cert});

                  verifier.verify(decoded);
                  return;
                } else if (algorithm == "RS512") {
                  auto verifier = jwt::verify()
                              .allow_algorithm(jwt::algorithm::rs512{cert});

                  verifier.verify(decoded);
                  return;
                } else if (algorithm == "ES256") {
                  auto verifier = jwt::verify()
                              .allow_algorithm(jwt::algorithm::es256{cert});

                  verifier.verify(decoded);
                  return;
                } else if (algorithm == "ES384") {
                  auto verifier = jwt::verify()
                              .allow_algorithm(jwt::algorithm::es384{cert});

                  verifier.verify(decoded);
                  return;
                } else if (algorithm == "ES512") {
                  auto verifier = jwt::verify()
                                .allow_algorithm(jwt::algorithm::es512{cert});

                  verifier.verify(decoded);
                  return;
                } else if (algorithm == "PS256") {
                  auto verifier = jwt::verify()
                                .allow_algorithm(jwt::algorithm::ps256{cert});

                  verifier.verify(decoded);
                  return;
                } else if (algorithm == "PS384") {
                  auto verifier = jwt::verify()
                                .allow_algorithm(jwt::algorithm::ps384{cert});

                  verifier.verify(decoded);
                  return;
                } else if (algorithm == "PS512") {
                  auto verifier = jwt::verify()
                                .allow_algorithm(jwt::algorithm::ps512{cert});

                  verifier.verify(decoded);
                  return;
                } else {
                  ldpp_dout(dpp, 0) << "Unsupported algorithm: " << algorithm << dendl;
                  throw std::system_error(EINVAL, std::system_category());
                }
              }
              catch (const std::exception& e) {
                ldpp_dout(dpp, 0) << "Signature validation using x5c failed" << e.what() << dendl;
                throw std::system_error(EACCES, std::system_category());
              }
            } else {
              if (algorithm == "RS256" || algorithm == "RS384" || algorithm == "RS512") {
                string n, e; //modulus and exponent
                if (JSONDecoder::decode_json("n", n, &k_parser) && JSONDecoder::decode_json("e", e, &k_parser)) {
                  if (skip == true) {
                    continue;
                  }
                  //Fetch and verify cert according to https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_create_oidc_verify-thumbprint.html
                  //and the same must be installed as part of create oidc provider in rgw
                  //this can be made common to all types of keys(x5c, n&e), making thumbprint validation similar to
                  //AWS
                  std::string hostname = get_top_level_domain_from_host(dpp, cert_url);
                  //connect to host and get back cert chain from it
                  std::string cert_chain = connect_to_host_get_cert_chain(dpp, hostname, 443);
                  std::string cert;
                  try {
                    cert = extract_last_certificate(dpp, cert_chain);
                    ldpp_dout(dpp, 20) << "last cert: " << cert << dendl;
                  } catch(const std::exception& e) {
                    ldpp_dout(dpp, 20) << "Extracting last cert of jwks uri failed with: " << e.what() << dendl;
                    throw std::system_error(EINVAL, std::system_category());
                  }
                  if (!is_cert_valid(thumbprints, cert)) {
                    ldpp_dout(dpp, 20) << "Cert doesn't match that with the thumbprints registered with oidc provider: " << cert.c_str() << dendl;
                    throw std::system_error(EINVAL, std::system_category());
                  }
                  validate_signature_using_n_e(dpp, decoded, algorithm, n, e);
                  return;
                }
                ldpp_dout(dpp, 0) << "x5c not present or n, e not present" << dendl;
                throw std::system_error(EINVAL, std::system_category());
              } else {
                throw std::system_error(EINVAL, std::system_category(), "Invalid algorithm: " + algorithm);
              }
            }
            ldpp_dout(dpp, 0) << "Signature can not be validated with the input given in keys: "<< dendl;
            throw std::system_error(EINVAL, std::system_category());
          } //end k_parser.parse
        }//end for iterate through keys
      } else { //end val->is_array
        ldpp_dout(dpp, 0) << "keys not present in JSON" << dendl;
        throw std::system_error(EINVAL, std::system_category());
      }
    } else {
      ldpp_dout(dpp, 0) << "Malformed json returned while fetching cert" << dendl;
      throw std::system_error(EINVAL, std::system_category());
    } //if-else get-data
  } else {
    ldpp_dout(dpp, 0) << "JWT signed by HMAC algos are currently not supported" << dendl;
    throw std::system_error(EINVAL, std::system_category());
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

      rgw_account_id role_account;
      if (rgw::account::validate_id(role_tenant)) {
        role_account = std::move(role_tenant);
        role_tenant.clear();
      }

      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, role_tenant, role_account);
      int ret = role->load_by_name(dpp, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "Role not found: name:" << role_name << " tenant: " << role_tenant << dendl;
        return result_t::deny(-EACCES);
      }

      std::optional<RGWAccountInfo> account;
      if (!role_account.empty()) {
        account.emplace();
        rgw::sal::Attrs attrs; // ignored
        RGWObjVersionTracker objv; // ignored
        ret = driver->load_account_by_id(dpp, y, role_account,
                                         *account, attrs, objv);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "Role account " << role_account << " not found" << dendl;
          return result_t::deny(-EACCES);
        }
      }

      boost::optional<multimap<string,string>> role_tags = role->get_tags();
      auto apl = apl_factory->create_apl_web_identity(
          cct, s, role->get_id(), role_session, role_tenant,
          *t, role_tags, princ_tags, std::move(account));
      return result_t::grant(std::move(apl));
    }
    return result_t::deny(-EACCES);
  }
  catch (const std::exception& e) {
    return result_t::deny(-EACCES);
  }
}

} // namespace rgw::auth::sts

int RGWREST_STS::verify_permission(optional_yield y)
{
  STS::STSService _sts(s->cct, driver, s->user->get_id(), s->auth.identity.get());
  sts = std::move(_sts);

  string rArn = s->info.args.get("RoleArn");
  const auto& [ret, role] = sts.getRoleInfo(s, rArn, y);
  if (ret < 0) {
    ldpp_dout(this, 0) << "failed to get role info using role arn: " << rArn << dendl;
    return ret;
  }
  string policy = role->get_assume_role_policy();

  //Parse the policy
  //TODO - This step should be part of Role Creation
  try {
    // resource policy is not restricted to the current tenant
    const std::string* policy_tenant = nullptr;

    const rgw::IAM::Policy p(s->cct, policy_tenant, policy, false);
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
    ldpp_dout(this, 0) << "User does not have permission to perform GetSessionToken" << dendl;
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

  STS::STSService sts(s->cct, driver, s->user->get_id(), s->auth.identity.get());

  STS::GetSessionTokenRequest req(duration, serialNumber, tokenCode);
  const auto& [ret, creds] = sts.getSessionToken(this, req);
  op_ret = std::move(ret);
  //Dump the output
  if (op_ret == 0) {
    s->formatter->open_object_section_in_ns("GetSessionTokenResponse", RGW_REST_STS_XMLNS);
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
    ldpp_dout(this, 0) << "ERROR: one of role arn or role session name or sub or aud is empty" << dendl;
    return -EINVAL;
  }

  if (! policy.empty()) {
    try {
      const rgw::IAM::Policy p(
	s->cct, nullptr, policy,
	s->cct->_conf.get_val<bool>("rgw_policy_reject_invalid_principals"));
    }
    catch (rgw::IAM::PolicyParseException& e) {
      ldpp_dout(this, 5) << "failed to parse policy: " << e.what() << "policy" << policy << dendl;
      s->err.message = e.what();
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
    s->formatter->open_object_section_in_ns("AssumeRoleWithWebIdentityResponse", RGW_REST_STS_XMLNS);
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
    try {
      const rgw::IAM::Policy p(
	s->cct, nullptr, policy,
	s->cct->_conf.get_val<bool>("rgw_policy_reject_invalid_principals"));
    }
    catch (rgw::IAM::PolicyParseException& e) {
      ldpp_dout(this, 0) << "failed to parse policy: " << e.what() << "policy" << policy << dendl;
      s->err.message = e.what();
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
    s->formatter->open_object_section_in_ns("AssumeRoleResponse", RGW_REST_STS_XMLNS);
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
                            rgw::sal::Driver* driver,
                            const rgw::auth::StrategyRegistry& auth_registry,
                            req_state *s, optional_yield y)
{
  return rgw::auth::Strategy::apply(dpp, auth_registry.get_sts(), s, y);
}

using op_generator = RGWOp*(*)();
static const std::unordered_map<std::string_view, op_generator> op_generators = {
  {"AssumeRole", []() -> RGWOp* {return new RGWSTSAssumeRole;}},
  {"GetSessionToken", []() -> RGWOp* {return new RGWSTSGetSessionToken;}},
  {"AssumeRoleWithWebIdentity", []() -> RGWOp* {return new RGWSTSAssumeRoleWithWebIdentity;}}
};

bool RGWHandler_REST_STS::action_exists(const req_state* s)
{
  if (s->info.args.exists("Action")) {
    const std::string action_name = s->info.args.get("Action");
    return op_generators.contains(action_name);
  }
  return false;
}

RGWOp *RGWHandler_REST_STS::op_post()
{
  if (s->info.args.exists("Action")) {
    const std::string action_name = s->info.args.get("Action");
    const auto action_it = op_generators.find(action_name);
    if (action_it != op_generators.end()) {
      return action_it->second();
    }
    ldpp_dout(s, 10) << "unknown action '" << action_name << "' for STS handler" << dendl;
  } else {
    ldpp_dout(s, 10) << "missing action argument in STS handler" << dendl;
  }
  return nullptr;
}

int RGWHandler_REST_STS::init(rgw::sal::Driver* driver,
                              req_state *s,
                              rgw::io::BasicClient *cio)
{
  s->dialect = "sts";
  s->prot_flags = RGW_REST_STS;

  return RGWHandler_REST::init(driver, s, cio);
}

int RGWHandler_REST_STS::authorize(const DoutPrefixProvider* dpp, optional_yield y)
{
  if (s->info.args.exists("Action") && s->info.args.get("Action") == "AssumeRoleWithWebIdentity") {
    return RGW_Auth_STS::authorize(dpp, driver, auth_registry, s, y);
  }
  return RGW_Auth_S3::authorize(dpp, driver, auth_registry, s, y);
}

RGWHandler_REST*
RGWRESTMgr_STS::get_handler(rgw::sal::Driver* driver,
			    req_state* const s,
			    const rgw::auth::StrategyRegistry& auth_registry,
			    const std::string& frontend_prefix)
{
  return new RGWHandler_REST_STS(auth_registry);
}
