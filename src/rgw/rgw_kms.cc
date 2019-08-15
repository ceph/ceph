// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/**
 * Server-side encryption integrations with Key Management Systems (SSE-KMS)
 */

#include <rgw/rgw_crypt.h>
#include <rgw/rgw_keystone.h>
#include <rgw/rgw_b64.h>
#include "include/str_map.h"
#include "common/safe_io.h"
#include "rgw/rgw_kms.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace rgw;


map<string,string> get_str_map(const string &str) {
  map<string,string> m;
  get_str_map(str, &m, ";, \t");
  return m;
}

int get_actual_key_from_conf(CephContext *cct,
                             boost::string_view key_id,
                             boost::string_view key_selector,
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
  } catch (...) {
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
    memset(_actual_key, 0, sizeof(_actual_key));
  } else {
    ldout(cct, 20) << "Wrong size for key=" << key_id << dendl;
    res = -EIO;
  }

  return res;
}

int get_barbican_url(CephContext * const cct,
                     std::string& url)
{
  url = cct->_conf->rgw_barbican_url;
  if (url.empty()) {
    ldout(cct, 0) << "ERROR: conf rgw_barbican_url is not set" << dendl;
    return -EINVAL;
  }

  if (url.back() != '/') {
    url.append("/");
  }

  return 0;
}

int request_key_from_barbican(CephContext *cct,
                              boost::string_view key_id,
                              const std::string& barbican_token,
                              std::string& actual_key) {
  std::string secret_url;
  int res;
  res = get_barbican_url(cct, secret_url);
  if (res < 0) {
     return res;
  }
  secret_url += "v1/secrets/" + std::string(key_id);

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
    memset(secret_bl.c_str(), 0, secret_bl.length());
    } else {
      res = -EACCES;
    }
  return res;
}

static int get_actual_key_from_barbican(CephContext *cct,
                                        boost::string_view key_id,
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

int request_key_from_vault_with_agent(CephContext *cct,
                                      boost::string_view key_id,
                                      bufferlist *secret_bl)
{
  // TODO: implement
  return -1;
}

int request_key_from_vault_with_token(CephContext *cct,
                                      boost::string_view key_id,
                                      bufferlist *secret_bl)
{
  // These conf settings are static strings so they're read only once from disk
  static std::string token_file, vault_url;
  // The actual Vault token is read from file on every request
  std::string vault_token;
  int res = 0;

  token_file = cct->_conf->rgw_crypt_s3_kms_vault_token_file;
  if (token_file.empty()) {
    ldout(cct, 0) << "ERROR: Vault token file is not set" << dendl;
    return -EINVAL;
  }
  ldout(cct, 20) << "Vault token file: " << token_file << dendl;

  char buf[2048];
  res = safe_read_file("", token_file.c_str(), buf, sizeof(buf));
  if (res < 0) {
    if (-ENOENT == res) {
      ldout(cct, 0) << "ERROR: Token file '" << token_file << "' not found  " << dendl;
    } else if (-EACCES == res) {
      ldout(cct, 0) << "ERROR: Permission denied reading token file" << dendl;
    } else {
      ldout(cct, 0) << "ERROR: Failed to read token file with error " << res << dendl;
    }
    return res;
  }
  // drop trailing newlines
  while (res && isspace(buf[res-1])) {
    --res;
  }
  vault_token = std::string{buf, static_cast<size_t>(res)};

  vault_url = cct->_conf->rgw_crypt_s3_kms_vault_url;
  if (vault_url.empty()) {
    ldout(cct, 0) << "ERROR: Vault URL is not set" << dendl;
    return -EINVAL;
  }

  std::string secret_url = vault_url + std::string(key_id);
  RGWHTTPTransceiver secret_req(cct, "GET", secret_url, secret_bl);
  secret_req.append_header("X-Vault-Token", vault_token);
  res = secret_req.process(null_yield);
  if (res <= 0) {
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
  return res;
}

int get_actual_key_from_vault(CephContext *cct,
                              boost::string_view key_id,
                              std::string& actual_key)
{
  int res = 0;
  std::string auth;
  bufferlist secret_bl;

  auth = cct->_conf->rgw_crypt_s3_kms_vault_auth;
  ldout(cct, 20) << "Vault auhentication method " << auth << dendl;

  if ("token" == auth) {
    res = request_key_from_vault_with_token(cct, key_id, &secret_bl);
  } else if ("agent" == auth) {
    res = request_key_from_vault_with_agent(cct, key_id, &secret_bl);
  } else {
    ldout(cct, 0) << "ERROR: Unsupported authentication method " << auth << dendl;
    return -EINVAL;
  }

  if (res < 0) {
    return res;
  }

  JSONParser parser;
  if (!parser.parse(secret_bl.c_str(), secret_bl.length())) {
    ldout(cct, 0) << "ERROR: Failed to parse JSON response from Vault" << dendl;
    return -EINVAL;
  }

  // TODO: handle possible missing structures in the JSON response
  JSONObj *data_obj = parser.find_obj("data");
  string secret = from_base64(data_obj->find_obj("data")->find_obj("key")->get_data());

  // TODO: remove this log entry
  ldout(cct, 20) << "Vault secret length: " << secret.length() << dendl;

  actual_key.assign(secret.c_str(), secret.length());
  memset(secret_bl.c_str(), 0, secret_bl.length());

  return res;
}

int get_actual_key_from_kms(CephContext *cct,
                            boost::string_view key_id,
                            boost::string_view key_selector,
                            std::string& actual_key)
{
  std::string kms_backend;

  kms_backend = cct->_conf->rgw_crypt_s3_kms_backend;
  ldout(cct, 20) << "Getting KMS encryption key for key=" << key_id << dendl;
  ldout(cct, 20) << "SSE-KMS backend is " << kms_backend << dendl;

  if (RGW_SSE_KMS_BACKEND_BARBICAN == kms_backend)
    return get_actual_key_from_barbican(cct, key_id, actual_key);

  if (RGW_SSE_KMS_BACKEND_VAULT == kms_backend)
    return get_actual_key_from_vault(cct, key_id, actual_key);

  if (RGW_SSE_KMS_BACKEND_LOCAL != kms_backend)
    ldout(cct, 10) << "Unknown SSE-KMS backend, reverting to local" << dendl;

  return get_actual_key_from_conf(cct, key_id, key_selector, actual_key);
}