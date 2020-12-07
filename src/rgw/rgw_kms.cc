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
#include "rgw/rgw_kmip_client.h"

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

typedef std::map<std::string, std::string> EngineParmMap;

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

  int send_request(std::string_view key_id, JSONParser* parser)
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

//  virtual ~VaultSecretEngine(){}
};


class TransitSecretEngine: public VaultSecretEngine {

private:
  EngineParmMap parms;

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
  TransitSecretEngine(CephContext *cct, EngineParmMap parms): VaultSecretEngine(cct), parms(parms) {
    for (auto& e: parms) {
      lderr(cct) << "ERROR: vault transit secrets engine : parameter "
	<< e.first << "=" << e.second << " ignored" << dendl;
    }
  }

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

  KvSecretEngine(CephContext *cct, EngineParmMap parms): VaultSecretEngine(cct){
    if (!parms.empty()) {
      lderr(cct) << "ERROR: vault kv secrets engine takes no parameters (ignoring them)" << dendl;
    }
  }

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

class KmipSecretEngine;
class KmipGetTheKey {
private:
	CephContext *cct;
	std::string work;
	bool failed = false;
	int ret;
protected:
	KmipGetTheKey(CephContext *cct) : cct(cct) {}
	KmipGetTheKey& keyid_to_keyname(std::string_view key_id);
	KmipGetTheKey& get_uniqueid_for_keyname();
	int get_key_for_uniqueid(std::string &);
	friend KmipSecretEngine;
};

KmipGetTheKey&
KmipGetTheKey::keyid_to_keyname(std::string_view key_id)
{
	work = cct->_conf->rgw_crypt_kmip_kms_key_template;
	std::string keyword = "$keyid";
	std::string replacement = std::string(key_id);
	size_t pos = 0;
	if (work.length() == 0) {
		work = std::move(replacement);
	} else {
		while (pos < work.length()) {
			pos = work.find(keyword, pos);
			if (pos == std::string::npos) break;
			work.replace(pos, keyword.length(), replacement);
			pos += key_id.length();
		}
	}
	return *this;
}

KmipGetTheKey&
KmipGetTheKey::get_uniqueid_for_keyname()
{
	RGWKMIPTransceiver secret_req(cct, RGWKMIPTransceiver::LOCATE);

	secret_req.name = work.data();
	ret = secret_req.process(null_yield);
	if (ret < 0) {
		failed = true;
	} else if (!secret_req.outlist->string_count) {
		ret = -ENOENT;
		lderr(cct) << "error: locate returned no results for "
			<< secret_req.name << dendl;
		failed = true;
	} else if (secret_req.outlist->string_count != 1) {
		ret = -EINVAL;
		lderr(cct) << "error: locate found "
			<< secret_req.outlist->string_count
			<< " results for " << secret_req.name << dendl;
		failed = true;
	} else {
		work = std::string(secret_req.outlist->strings[0]);
	}
	return *this;
}

int
KmipGetTheKey::get_key_for_uniqueid(std::string& actual_key)
{
	if (failed) return ret;
	RGWKMIPTransceiver secret_req(cct, RGWKMIPTransceiver::GET);
	secret_req.unique_id = work.data();
	ret = secret_req.process(null_yield);
	if (ret < 0) {
		failed = true;
	} else {
		actual_key = std::string((char*)(secret_req.outkey->data),
			secret_req.outkey->keylen);
	}
	return ret;
}

class KmipSecretEngine: public SecretEngine {

protected:
  CephContext *cct;

//  int send_request(std::string_view key_id, JSONParser* parser) override
//  {
//    return -EINVAL;
//  }
//
//  int decode_secret(JSONObj* json_obj, std::string& actual_key){
//    return -EINVAL;
//  }

public:

  KmipSecretEngine(CephContext *cct) {
    this->cct = cct;
  }

  int get_key(std::string_view key_id, std::string& actual_key)
  {
	int r;
	r = KmipGetTheKey{cct}
		.keyid_to_keyname(key_id)
		.get_uniqueid_for_keyname()
		.get_key_for_uniqueid(actual_key);
	return r;
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


std::string config_to_engine_and_parms(CephContext *cct,
    const char* which,
    std::string& secret_engine_str,
    EngineParmMap& secret_engine_parms)
{
  std::ostringstream oss;
  std::vector<std::string> secret_engine_v;
  std::string secret_engine;

  get_str_vec(secret_engine_str, " ", secret_engine_v);

  cct->_conf.early_expand_meta(secret_engine_str, &oss);
  auto meta_errors {oss.str()};
  if (meta_errors.length()) {
    meta_errors.erase(meta_errors.find_last_not_of("\n")+1);
    lderr(cct) << "ERROR: while expanding " << which << ": "
	<< meta_errors << dendl;
  }
  for (auto& e: secret_engine_v) {
    if (!secret_engine.length()) {
      secret_engine = std::move(e);
      continue;
    }
    auto p { e.find('=') };
    if (p == std::string::npos) {
      secret_engine_parms.emplace(std::move(e), "");
      continue;
    }
    std::string key{ e.substr(0,p) };
    std::string val{ e.substr(p+1) };
    secret_engine_parms.emplace(std::move(key), std::move(val));
  }
  return secret_engine;
}


static int get_actual_key_from_vault(CephContext *cct,
                                     map<string, bufferlist>& attrs,
                                     std::string& actual_key)
{
  std::string secret_engine_str = cct->_conf->rgw_crypt_vault_secret_engine;
  std::string context = get_str_attribute(attrs, RGW_ATTR_CRYPT_CONTEXT);
  EngineParmMap secret_engine_parms;
  auto secret_engine { config_to_engine_and_parms(
    cct, "rgw_crypt_vault_secret_engine",
    secret_engine_str, secret_engine_parms) };
  ldout(cct, 20) << "Vault authentication method: " << cct->_conf->rgw_crypt_vault_auth << dendl;
  ldout(cct, 20) << "Vault Secrets Engine: " << secret_engine << dendl;
lderr(cct) << "TEMP cooked_context<" << context << ">" << dendl;

  if (RGW_SSE_KMS_VAULT_SE_KV == secret_engine){
    std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
    KvSecretEngine engine(cct, std::move(secret_engine_parms));
    return engine.get_key(key_id, actual_key);
  }
  else if (RGW_SSE_KMS_VAULT_SE_TRANSIT == secret_engine){
    std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
    TransitSecretEngine engine(cct, std::move(secret_engine_parms));
    return engine.get_key(key_id, actual_key);
  }
  else{
    ldout(cct, 0) << "Missing or invalid secret engine" << dendl;
    return -EINVAL;
  }
}


static int make_actual_key_from_vault(CephContext *cct,
                                     map<string, bufferlist>& attrs,
                                     std::string& actual_key)
{
    return get_actual_key_from_vault(cct, attrs, actual_key);
}


static int reconstitute_actual_key_from_vault(CephContext *cct,
                                     map<string, bufferlist>& attrs,
                                     std::string& actual_key)
{
    return get_actual_key_from_vault(cct, attrs, actual_key);
}


static int get_actual_key_from_kmip(CephContext *cct,
                                     std::string_view key_id,
                                     std::string& actual_key)
{
  std::string secret_engine = RGW_SSE_KMS_KMIP_SE_KV;

  if (RGW_SSE_KMS_KMIP_SE_KV == secret_engine){
    KmipSecretEngine engine(cct);
    return engine.get_key(key_id, actual_key);
  }
  else{
    ldout(cct, 0) << "Missing or invalid secret engine" << dendl;
    return -EINVAL;
  }
}


int reconstitute_actual_key_from_kms(CephContext *cct,
                            map<string, bufferlist>& attrs,
                            std::string& actual_key)
{
  std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
  std::string kms_backend { cct->_conf->rgw_crypt_s3_kms_backend };

  ldout(cct, 20) << "Getting KMS encryption key for key " << key_id << dendl;
  ldout(cct, 20) << "SSE-KMS backend is " << kms_backend << dendl;

  if (RGW_SSE_KMS_BACKEND_BARBICAN == kms_backend) {
    return get_actual_key_from_barbican(cct, key_id, actual_key);
  }

  if (RGW_SSE_KMS_BACKEND_VAULT == kms_backend) {
    return reconstitute_actual_key_from_vault(cct, attrs, actual_key);
  }

  if (RGW_SSE_KMS_BACKEND_KMIP == kms_backend) {
    return get_actual_key_from_kmip(cct, key_id, actual_key);
  }

  if (RGW_SSE_KMS_BACKEND_TESTING == kms_backend) {
    std::string key_selector = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYSEL);
    return get_actual_key_from_conf(cct, key_id, key_selector, actual_key);
  }

  ldout(cct, 0) << "ERROR: Invalid rgw_crypt_s3_kms_backend: " << kms_backend << dendl;
  return -EINVAL;
}

int make_actual_key_from_kms(CephContext *cct,
                            map<string, bufferlist>& attrs,
                            std::string& actual_key)
{
  std::string kms_backend { cct->_conf->rgw_crypt_s3_kms_backend };
  if (RGW_SSE_KMS_BACKEND_VAULT == kms_backend)
    return make_actual_key_from_vault(cct, attrs, actual_key);
  return reconstitute_actual_key_from_kms(cct, attrs, actual_key);
}
