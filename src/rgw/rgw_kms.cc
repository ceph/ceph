// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/**
 * Server-side encryption integrations with Key Management Systems (SSE-KMS)
 */

#include <sys/stat.h>
#include "include/str_map.h"
#include "common/safe_io.h"
#include "rgw/rgw_crypt.h"
#include "rgw/rgw_keystone.h"
#include "rgw/rgw_b64.h"
#include "rgw/rgw_kms.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace rgw;


/**
 * Construct a full URL string by concatenating a "base" URL with another path,
 * ensuring there is one and only one forward slash between them. If path is
 * empty, the URL is not changed.
 */
static void concat_url(std::string &url, std::string path) {
  bool url_has_slash = !url.empty() && url.back() == '/';
  if (!path.empty()) {
    if (url_has_slash && path.front() == '/') {
      url.pop_back();
    } else if (!url_has_slash && path.front() != '/') {
      url.push_back('/');
    }
    url.append(path);
  }
}


class VaultSecretEngine: public SecretEngine {

protected:
  CephContext *cct;

  int load_token_from_file(std::string *vault_token)
  {

    int res = 0;
    std::string token_file = cct->_conf->rgw_crypt_vault_token_file;
    if (token_file.empty()) {
      ldout(cct, 0) << "ERROR: Vault token file not set in rgw_crypt_vault_token_file" << dendl;
      return -EINVAL;
    }
    ldout(cct, 20) << "Vault token file: " << token_file << dendl;

    struct stat token_st;
    if (stat(token_file.c_str(), &token_st) != 0) {
      ldout(cct, 0) << "ERROR: Vault token file '" << token_file << "' not found  " << dendl;
      return -ENOENT;
    }

    if (token_st.st_mode & (S_IRWXG | S_IRWXO)) {
      ldout(cct, 0) << "ERROR: Vault token file '" << token_file << "' permissions are "
                    << "too open, it must not be accessible by other users" << dendl;
      return -EACCES;
    }

    char buf[2048];
    res = safe_read_file("", token_file.c_str(), buf, sizeof(buf));
    if (res < 0) {
      if (-EACCES == res) {
        ldout(cct, 0) << "ERROR: Permission denied reading Vault token file" << dendl;
      } else {
        ldout(cct, 0) << "ERROR: Failed to read Vault token file with error " << res << dendl;
      }
      return res;
    }
    // drop trailing newlines
    while (res && isspace(buf[res-1])) {
      --res;
    }
    vault_token->assign(std::string{buf, static_cast<size_t>(res)});
    memset(buf, 0, sizeof(buf));
    ::ceph::crypto::zeroize_for_security(buf, sizeof(buf));
    return res;
  }

  int send_request(std::string_view key_id, JSONParser* parser) override
  {
    bufferlist secret_bl;
    int res;
    string vault_token = "";
    if (RGW_SSE_KMS_VAULT_AUTH_TOKEN == cct->_conf->rgw_crypt_vault_auth){
      ldout(cct, 0) << "Loading Vault Token from filesystem" << dendl;
      res = load_token_from_file(&vault_token);
      if (res < 0){
        return res;
      }
    }

    std::string secret_url = cct->_conf->rgw_crypt_vault_addr;
    if (secret_url.empty()) {
      ldout(cct, 0) << "ERROR: Vault address not set in rgw_crypt_vault_addr" << dendl;
      return -EINVAL;
    }

    concat_url(secret_url, cct->_conf->rgw_crypt_vault_prefix);
    concat_url(secret_url, std::string(key_id));

    RGWHTTPTransceiver secret_req(cct, "GET", secret_url, &secret_bl);

    if (!vault_token.empty()){
      secret_req.append_header("X-Vault-Token", vault_token);
      vault_token.replace(0, vault_token.length(), vault_token.length(), '\000');
    }

    string vault_namespace = cct->_conf->rgw_crypt_vault_namespace;
    if (!vault_namespace.empty()){
      ldout(cct, 20) << "Vault Namespace: " << vault_namespace << dendl;
      secret_req.append_header("X-Vault-Namespace", vault_namespace);
    }

    res = secret_req.process(null_yield);
    if (res < 0) {
      ldout(cct, 0) << "ERROR: Request to Vault failed with error " << res << dendl;
      return res;
    }

    if (secret_req.get_http_status() ==
        RGWHTTPTransceiver::HTTP_STATUS_UNAUTHORIZED) {
      ldout(cct, 0) << "ERROR: Vault request failed authorization" << dendl;
      return -EACCES;
    }

    ldout(cct, 20) << "Request to Vault returned " << res << " and HTTP status "
      << secret_req.get_http_status() << dendl;

    ldout(cct, 20) << "Parse response into JSON Object" << dendl;

    if (!parser->parse(secret_bl.c_str(), secret_bl.length())) {
      ldout(cct, 0) << "ERROR: Failed to parse JSON response from Vault" << dendl;
      return -EINVAL;
    }
    secret_bl.zero();

    return res;
  }

  int decode_secret(JSONObj* json_obj, std::string& actual_key){
    std::string secret;
    try {
      secret = from_base64(json_obj->get_data());
    } catch (std::exception&) {
      ldout(cct, 0) << "ERROR: Failed to base64 decode key retrieved from Vault" << dendl;
      return -EINVAL;
    }

    actual_key.assign(secret.c_str(), secret.length());
    secret.replace(0, secret.length(), secret.length(), '\000');
    return 0;
  }

public:

  VaultSecretEngine(CephContext *cct) {
    this->cct = cct;
  }

  virtual ~VaultSecretEngine(){}
};


class TransitSecretEngine: public VaultSecretEngine {

private:
  int get_key_version(std::string_view key_id, string& version)
  {
    size_t pos = 0;

    pos = key_id.rfind("/");
    if (pos != std::string_view::npos){
      std::string_view token = key_id.substr(pos+1, key_id.length()-pos);
      if (!token.empty() && token.find_first_not_of("0123456789") == std::string_view::npos){
        version.assign(std::string(token));
        return 0;
      }
    }
    return -1;
  }

public:
  TransitSecretEngine(CephContext *cct): VaultSecretEngine(cct){ }

  int get_key(std::string_view key_id, std::string& actual_key)
  {
    JSONParser parser;
    string version;

    if (get_key_version(key_id, version) < 0){
      ldout(cct, 20) << "Missing or invalid key version" << dendl;
      return -EINVAL;
    }

    int res = send_request(key_id, &parser);
    if (res < 0) {
      return res;
    }

    JSONObj* json_obj = &parser;
    std::array<std::string, 3> elements = {"data", "keys", version};
    for(const auto& elem : elements) {
      json_obj = json_obj->find_obj(elem);
      if (!json_obj) {
        ldout(cct, 0) << "ERROR: Key not found in JSON response from Vault using Transit Engine" << dendl;
        return -EINVAL;
      }
    }

    return decode_secret(json_obj, actual_key);
  }

};

class KvSecretEngine: public VaultSecretEngine {

public:

  KvSecretEngine(CephContext *cct): VaultSecretEngine(cct){ }

  virtual ~KvSecretEngine(){}

  int get_key(std::string_view key_id, std::string& actual_key){
    JSONParser parser;
    int res = send_request(key_id, &parser);
    if (res < 0) {
      return res;
    }

    JSONObj *json_obj = &parser;
    std::array<std::string, 3> elements = {"data", "data", "key"};
    for(const auto& elem : elements) {
      json_obj = json_obj->find_obj(elem);
      if (!json_obj) {
        ldout(cct, 0) << "ERROR: Key not found in JSON response from Vault using KV Engine" << dendl;
        return -EINVAL;
      }
    }
    return decode_secret(json_obj, actual_key);
  }

};


static map<string,string> get_str_map(const string &str) {
  map<string,string> m;
  get_str_map(str, &m, ";, \t");
  return m;
}


static int get_actual_key_from_conf(CephContext *cct,
                                    std::string_view key_id,
                                    std::string_view key_selector,
                                    std::string& actual_key)
{
  int res = 0;

  static map<string,string> str_map = get_str_map(
      cct->_conf->rgw_crypt_s3_kms_encryption_keys);

  map<string, string>::iterator it = str_map.find(std::string(key_id));
  if (it == str_map.end())
    return -ERR_INVALID_ACCESS_KEY;

  std::string master_key;
  try {
    master_key = from_base64((*it).second);
  } catch (std::exception&) {
    ldout(cct, 5) << "ERROR: get_actual_key_from_conf invalid encryption key id "
                  << "which contains character that is not base64 encoded."
                  << dendl;
    return -EINVAL;
  }

  if (master_key.length() == AES_256_KEYSIZE) {
    uint8_t _actual_key[AES_256_KEYSIZE];
    if (AES_256_ECB_encrypt(cct,
        reinterpret_cast<const uint8_t*>(master_key.c_str()), AES_256_KEYSIZE,
        reinterpret_cast<const uint8_t*>(key_selector.data()),
        _actual_key, AES_256_KEYSIZE)) {
      actual_key = std::string((char*)&_actual_key[0], AES_256_KEYSIZE);
    } else {
      res = -EIO;
    }
    ::ceph::crypto::zeroize_for_security(_actual_key, sizeof(_actual_key));
  } else {
    ldout(cct, 20) << "Wrong size for key=" << key_id << dendl;
    res = -EIO;
  }

  return res;
}

static int request_key_from_barbican(CephContext *cct,
                                     std::string_view key_id,
                                     const std::string& barbican_token,
                                     std::string& actual_key) {
  int res;

  std::string secret_url = cct->_conf->rgw_barbican_url;
  if (secret_url.empty()) {
    ldout(cct, 0) << "ERROR: conf rgw_barbican_url is not set" << dendl;
    return -EINVAL;
  }
  concat_url(secret_url, "/v1/secrets/");
  concat_url(secret_url, std::string(key_id));

  bufferlist secret_bl;
  RGWHTTPTransceiver secret_req(cct, "GET", secret_url, &secret_bl);
  secret_req.append_header("Accept", "application/octet-stream");
  secret_req.append_header("X-Auth-Token", barbican_token);

  res = secret_req.process(null_yield);
  if (res < 0) {
    return res;
  }
  if (secret_req.get_http_status() ==
      RGWHTTPTransceiver::HTTP_STATUS_UNAUTHORIZED) {
    return -EACCES;
  }

  if (secret_req.get_http_status() >=200 &&
      secret_req.get_http_status() < 300 &&
      secret_bl.length() == AES_256_KEYSIZE) {
    actual_key.assign(secret_bl.c_str(), secret_bl.length());
    secret_bl.zero();
    } else {
      res = -EACCES;
    }
  return res;
}

static int get_actual_key_from_barbican(CephContext *cct,
                                        std::string_view key_id,
                                        std::string& actual_key)
{
  int res = 0;
  std::string token;

  if (rgw::keystone::Service::get_keystone_barbican_token(cct, token) < 0) {
    ldout(cct, 5) << "Failed to retrieve token for Barbican" << dendl;
    return -EINVAL;
  }

  res = request_key_from_barbican(cct, key_id, token, actual_key);
  if (res != 0) {
    ldout(cct, 5) << "Failed to retrieve secret from Barbican:" << key_id << dendl;
  }
  return res;
}


static int get_actual_key_from_vault(CephContext *cct,
                                     std::string_view key_id,
                                     std::string& actual_key)
{
  std::string secret_engine = cct->_conf->rgw_crypt_vault_secret_engine;
  ldout(cct, 20) << "Vault authentication method: " << cct->_conf->rgw_crypt_vault_auth << dendl;
  ldout(cct, 20) << "Vault Secrets Engine: " << secret_engine << dendl;

  if (RGW_SSE_KMS_VAULT_SE_KV == secret_engine){
    KvSecretEngine engine(cct);
    return engine.get_key(key_id, actual_key);
  }
  else if (RGW_SSE_KMS_VAULT_SE_TRANSIT == secret_engine){
    TransitSecretEngine engine(cct);
    return engine.get_key(key_id, actual_key);
  }
  else{
    ldout(cct, 0) << "Missing or invalid secret engine" << dendl;
    return -EINVAL;
  }
}


int get_actual_key_from_kms(CephContext *cct,
                            std::string_view key_id,
                            std::string_view key_selector,
                            std::string& actual_key)
{
  std::string kms_backend;

  kms_backend = cct->_conf->rgw_crypt_s3_kms_backend;
  ldout(cct, 20) << "Getting KMS encryption key for key " << key_id << dendl;
  ldout(cct, 20) << "SSE-KMS backend is " << kms_backend << dendl;

  if (RGW_SSE_KMS_BACKEND_BARBICAN == kms_backend)
    return get_actual_key_from_barbican(cct, key_id, actual_key);

  if (RGW_SSE_KMS_BACKEND_VAULT == kms_backend)
    return get_actual_key_from_vault(cct, key_id, actual_key);

  if (RGW_SSE_KMS_BACKEND_TESTING == kms_backend)
    return get_actual_key_from_conf(cct, key_id, key_selector, actual_key);

  ldout(cct, 0) << "ERROR: Invalid rgw_crypt_s3_kms_backend: " << kms_backend << dendl;
  return -EINVAL;
}
