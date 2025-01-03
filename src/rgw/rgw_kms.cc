// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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
#include <rapidjson/allocators.h>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include "rapidjson/error/error.h"
#include "rapidjson/error/en.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace rgw;

#ifndef FORTEST_VIRTUAL
#define  FORTEST_VIRTUAL	/**/
#endif

/**
 * Memory pool for use with rapidjson.  This version
 * carefully zeros out all memory before returning it to
 * the system.
 */
#define ALIGNTYPE double
#define MINCHUNKSIZE 4096
class ZeroPoolAllocator {
private:
    struct element {
	struct element *next;
	int size;
	char data[4];
    } *b;
    size_t left;
public:
    static const bool kNeedFree { false };
    ZeroPoolAllocator(){
	b = 0;
	left = 0;
    }
    ~ZeroPoolAllocator(){
        element *p;
	while ((p = b)) {
	    b = p->next;
	    memset(p->data, 0, p->size);
	    free(p);
	}
    }
    void * Malloc(size_t size) {
	void *r;
	if (!size) return 0;
	size = (size + sizeof(ALIGNTYPE)-1)&(-sizeof(ALIGNTYPE));
	if (size > left) {
		size_t ns { size };
		if (ns < MINCHUNKSIZE) ns = MINCHUNKSIZE;
		element *nw { (element *) malloc(sizeof *b + ns) };
		if (!nw) {
// std::cerr << "out of memory" << std::endl;
			return 0;
		}
		left = ns - sizeof *b;
		nw->size = ns;
		nw->next = b;
		b = nw;
	}
	left -= size;
	r = static_cast<void*>(b->data + left);
	return r;
    }
    void* Realloc(void* p, size_t old, size_t nw) {
        if (!nw) return 0;
        void *r = Malloc(nw);
	if (nw > old) nw = old;
	if (r && old) memcpy(r, p, nw);
	return r;
    }
    static void Free(void *p) {
	ceph_assert(0 == "Free should not be called");
    }
private:
    //! Copy constructor is not permitted.
    ZeroPoolAllocator(const ZeroPoolAllocator& rhs) /* = delete */;
    //! Copy assignment operator is not permitted.
    ZeroPoolAllocator& operator=(const ZeroPoolAllocator& rhs) /* = delete */;
};

typedef rapidjson::GenericDocument<rapidjson::UTF8<>,
		ZeroPoolAllocator,
		rapidjson::CrtAllocator
		> ZeroPoolDocument;
typedef rapidjson::GenericValue<rapidjson::UTF8<>, ZeroPoolAllocator> ZeroPoolValue;

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

/**
 * Determine if a string (url) ends with a given suffix.
 * Must deal with (ignore) trailing slashes.
 */
static bool string_ends_maybe_slash(std::string_view hay,
    std::string_view needle)
{
  auto hay_len { hay.size() };
  auto needle_len { needle.size() };
  if (hay_len < needle_len) return false;
  auto hay_suffix_start { hay.data() + (hay_len - needle_len) };
  while (hay_len > needle_len && hay[hay_len-1] == '/') {
    --hay_len;
    --hay_suffix_start;
  }
  std::string_view hay_suffix { hay_suffix_start, needle_len };
  return hay_suffix == needle;
}

template<typename E, typename A = ZeroPoolAllocator>
static inline void
add_name_val_to_obj(std::string &n, std::string &v, rapidjson::GenericValue<E,A> &d,
  A &allocator)
{
  rapidjson::GenericValue<E,A> name, val;
  name.SetString(n.c_str(), n.length(), allocator);
  val.SetString(v.c_str(), v.length(), allocator);
  d.AddMember(name, val, allocator);
}

template<typename E, typename A = ZeroPoolAllocator>
static inline void
add_name_val_to_obj(std::string &n, bool v, rapidjson::GenericValue<E,A> &d,
  A &allocator)
{
  rapidjson::GenericValue<E,A> name, val;
  name.SetString(n.c_str(), n.length(), allocator);
  val.SetBool(v);
  d.AddMember(name, val, allocator);
}

template<typename E, typename A = ZeroPoolAllocator>
static inline void
add_name_val_to_obj(const char *n, std::string &v, rapidjson::GenericValue<E,A> &d,
  A &allocator)
{
  std::string ns{n, strlen(n) };
  add_name_val_to_obj(ns, v, d, allocator);
}

template<typename E, typename A = ZeroPoolAllocator>
static inline void
add_name_val_to_obj(const char *n, bool v, rapidjson::GenericValue<E,A> &d,
  A &allocator)
{
  std::string ns{n, strlen(n) };
  add_name_val_to_obj(ns, v, d, allocator);
}

typedef std::map<std::string, std::string> EngineParmMap;


class SSEContext {
protected:
  virtual ~SSEContext(){};
public:
  virtual const std::string & backend() = 0;
  virtual const std::string & addr() = 0;
  virtual const std::string & auth() = 0;
  virtual const std::string & k_namespace() = 0;
  virtual const std::string & prefix() = 0;
  virtual const std::string & secret_engine() = 0;
  virtual const std::string & ssl_cacert() = 0;
  virtual const std::string & ssl_clientcert() = 0;
  virtual const std::string & ssl_clientkey() = 0;
  virtual const std::string & token_file() = 0;
  virtual const bool verify_ssl() = 0;
};

class VaultSecretEngine: public SecretEngine {

protected:
  CephContext *cct;
  SSEContext & kctx;

  int load_token_from_file(const DoutPrefixProvider *dpp, std::string *vault_token)
  {

    int res = 0;
    std::string token_file = kctx.token_file();
    if (token_file.empty()) {
      ldpp_dout(dpp, 0) << "ERROR: Vault token file not set in rgw_crypt_vault_token_file" << dendl;
      return -EINVAL;
    }
    ldpp_dout(dpp, 20) << "Vault token file: " << token_file << dendl;

    struct stat token_st;
    if (stat(token_file.c_str(), &token_st) != 0) {
      ldpp_dout(dpp, 0) << "ERROR: Vault token file '" << token_file << "' not found  " << dendl;
      return -ENOENT;
    }

    if (token_st.st_mode & (S_IWGRP | S_IXGRP | S_IRWXO)) {
      ldpp_dout(dpp, 0) << "ERROR: Vault token file '" << token_file << "' permissions are "
                    << "too open, the maximum allowed is 0740" << dendl;
      return -EACCES;
    }

    char buf[2048];
    res = safe_read_file("", token_file.c_str(), buf, sizeof(buf));
    if (res < 0) {
      if (-EACCES == res) {
        ldpp_dout(dpp, 0) << "ERROR: Permission denied reading Vault token file" << dendl;
      } else {
        ldpp_dout(dpp, 0) << "ERROR: Failed to read Vault token file with error " << res << dendl;
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

  FORTEST_VIRTUAL
  int send_request(const DoutPrefixProvider *dpp, const char *method, std::string_view infix,
    std::string_view key_id,
    const std::string& postdata,
    optional_yield y,
    bufferlist &secret_bl)
  {
    int res;
    string vault_token = "";
    if (RGW_SSE_KMS_VAULT_AUTH_TOKEN == kctx.auth()){
      ldpp_dout(dpp, 20) << "Loading Vault Token from filesystem" << dendl;
      res = load_token_from_file(dpp, &vault_token);
      if (res < 0){
        return res;
      }
    }

    std::string secret_url = kctx.addr();
    if (secret_url.empty()) {
      ldpp_dout(dpp, 0) << "ERROR: Vault address not set in rgw_crypt_vault_addr" << dendl;
      return -EINVAL;
    }

    concat_url(secret_url, kctx.prefix());
    concat_url(secret_url, std::string(infix));
    concat_url(secret_url, std::string(key_id));

    RGWHTTPTransceiver secret_req(cct, method, secret_url, &secret_bl);

    if (postdata.length()) {
      secret_req.set_post_data(postdata);
      secret_req.set_send_length(postdata.length());
    }

    secret_req.append_header("X-Vault-Token", vault_token);
    if (!vault_token.empty()){
      secret_req.append_header("X-Vault-Token", vault_token);
      vault_token.replace(0, vault_token.length(), vault_token.length(), '\000');
    }

    string vault_namespace = kctx.k_namespace();
    if (!vault_namespace.empty()){
      ldpp_dout(dpp, 20) << "Vault Namespace: " << vault_namespace << dendl;
      secret_req.append_header("X-Vault-Namespace", vault_namespace);
    }

    secret_req.set_verify_ssl(kctx.verify_ssl());

    if (!kctx.ssl_cacert().empty()) {
      secret_req.set_ca_path(kctx.ssl_cacert());
    }

    if (!kctx.ssl_clientcert().empty()) {
      secret_req.set_client_cert(kctx.ssl_clientcert());
    }
    if (!kctx.ssl_clientkey().empty()) {
      secret_req.set_client_key(kctx.ssl_clientkey());
    }

    res = secret_req.process(dpp, y);

    // map 401 to EACCES instead of EPERM
    if (secret_req.get_http_status() ==
        RGWHTTPTransceiver::HTTP_STATUS_UNAUTHORIZED) {
      ldpp_dout(dpp, 0) << "ERROR: Vault request failed authorization" << dendl;
      return -EACCES;
    }
    if (res < 0) {
      ldpp_dout(dpp, 0) << "ERROR: Request to Vault failed with error " << res << dendl;
      return res;
    }

    ldpp_dout(dpp, 20) << "Request to Vault returned " << res << " and HTTP status "
      << secret_req.get_http_status() << dendl;

    return res;
  }

  int send_request(const DoutPrefixProvider *dpp, std::string_view key_id,
                   optional_yield y, bufferlist &secret_bl)
  {
    return send_request(dpp, "GET", "", key_id, string{}, y, secret_bl);
  }

  int decode_secret(const DoutPrefixProvider *dpp, std::string encoded, std::string& actual_key){
    try {
      actual_key = from_base64(encoded);
    } catch (std::exception&) {
      ldpp_dout(dpp, 0) << "ERROR: Failed to base64 decode key retrieved from Vault" << dendl;
      return -EINVAL;
    }
    memset(encoded.data(), 0, encoded.length());
    return 0;
  }

public:

  VaultSecretEngine(CephContext *_c, SSEContext & _k) : cct(_c), kctx(_k) {
  }
};

class TransitSecretEngine: public VaultSecretEngine {
public:
  int compat;
  static const int COMPAT_NEW_ONLY = 0;
  static const int COMPAT_OLD_AND_NEW = 1;
  static const int COMPAT_ONLY_OLD = 2;
  static const int COMPAT_UNSET = -1;

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
  TransitSecretEngine(CephContext *cct, SSEContext & kctx, EngineParmMap parms): VaultSecretEngine(cct, kctx), parms(parms) {
    compat = COMPAT_UNSET;
    for (auto& e: parms) {
      if (e.first == "compat") {
	if (e.second.empty()) {
	  compat = COMPAT_OLD_AND_NEW;
	} else {
	  size_t ep;

	  compat = std::stoi(e.second, &ep);
	  if (ep != e.second.length()) {
	    lderr(cct) << "warning: vault transit secrets engine : compat="
	      << e.second << " trailing junk? (ignored)" << dendl;
	  }
	}
	continue;
      }
      lderr(cct) << "ERROR: vault transit secrets engine : parameter "
	<< e.first << "=" << e.second << " ignored" << dendl;
    }
    if (compat == COMPAT_UNSET) {
      std::string_view v { kctx.prefix() };
      if (string_ends_maybe_slash(v,"/export/encryption-key")) {
	compat = COMPAT_ONLY_OLD;
      } else {
	compat = COMPAT_NEW_ONLY;
      }
    }
  }

  int get_key(const DoutPrefixProvider *dpp, std::string_view key_id,
              optional_yield y, std::string& actual_key) override
  {
    ZeroPoolDocument d;
    ZeroPoolValue *v;
    string version;
    bufferlist secret_bl;

    if (get_key_version(key_id, version) < 0){
      ldpp_dout(dpp, 20) << "Missing or invalid key version" << dendl;
      return -EINVAL;
    }

    int res = send_request(dpp, "GET", compat == COMPAT_ONLY_OLD ? "" : "/export/encryption-key",
                           key_id, string{}, y, secret_bl);
    if (res < 0) {
      return res;
    }

    ldpp_dout(dpp, 20) << "Parse response into JSON Object" << dendl;

    secret_bl.append('\0');
    rapidjson::StringStream isw(secret_bl.c_str());
    d.ParseStream<>(isw);

    if (d.HasParseError()) {
      ldpp_dout(dpp, 0) << "ERROR: Failed to parse JSON response from Vault: "
	 << rapidjson::GetParseError_En(d.GetParseError()) << dendl;
      return -EINVAL;
    }
    secret_bl.zero();

    const char *elements[] = {"data", "keys", version.c_str()};
    v = &d;
    for (auto &elem: elements) {
      if (!v->IsObject()) {
	v = nullptr;
	break;
      }
      auto endr { v->MemberEnd() };
      auto itr { v->FindMember(elem) };
      if (itr == endr) {
	v = nullptr;
	break;
      }
      v = &itr->value;
    }
    if (!v || !v->IsString()) {
      ldpp_dout(dpp, 0) << "ERROR: Key not found in JSON response from Vault using Transit Engine" << dendl;
      return -EINVAL;
    }
    return decode_secret(dpp, v->GetString(), actual_key);
  }

  int make_actual_key(const DoutPrefixProvider *dpp, map<string, bufferlist>& attrs,
                      optional_yield y, std::string& actual_key)
  {
    std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
    if (compat == COMPAT_ONLY_OLD) {
      return get_key(dpp, key_id, y, actual_key);
    }
    if (key_id.find("/") != std::string::npos) {
      ldpp_dout(dpp, 0) << "sorry, can't allow / in keyid" << dendl;
      return -EINVAL;
    }
/*
	data: {context }
	post to prefix + /datakey/plaintext/ + key_id
	jq: .data.plaintext	-> key
	jq: .data.ciphertext	-> (to-be) named attribute
    return decode_secret(json_obj, actual_key)
*/
    std::string context = get_str_attribute(attrs, RGW_ATTR_CRYPT_CONTEXT);
    ZeroPoolDocument d { rapidjson::kObjectType };
    auto &allocator { d.GetAllocator() };
    bufferlist secret_bl;

    add_name_val_to_obj("context", context, d, allocator);
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    if (!d.Accept(writer)) {
      ldpp_dout(dpp, 0) << "ERROR: can't make json for vault" << dendl;
      return -EINVAL;
    }
    std::string post_data { buf.GetString() };

    int res = send_request(dpp, "POST", "/datakey/plaintext/", key_id,
                           post_data, y, secret_bl);
    if (res < 0) {
      return res;
    }

    ldpp_dout(dpp, 20) << "Parse response into JSON Object" << dendl;

    secret_bl.append('\0');
    rapidjson::StringStream isw(secret_bl.c_str());
    d.SetNull();
    d.ParseStream<>(isw);

    if (d.HasParseError()) {
      ldpp_dout(dpp, 0) << "ERROR: Failed to parse JSON response from Vault: "
	 << rapidjson::GetParseError_En(d.GetParseError()) << dendl;
      return -EINVAL;
    }
    secret_bl.zero();

    if (!d.IsObject()) {
      ldpp_dout(dpp, 0) << "ERROR: response from Vault is not an object" << dendl;
      return -EINVAL;
    }
    {
      auto data_itr { d.FindMember("data") };
      if (data_itr == d.MemberEnd()) {
	ldpp_dout(dpp, 0) << "ERROR: no .data in response from Vault" << dendl;
        return -EINVAL;
      }
      auto ciphertext_itr { data_itr->value.FindMember("ciphertext") };
      auto plaintext_itr { data_itr->value.FindMember("plaintext") };
      if (ciphertext_itr == data_itr->value.MemberEnd()) {
	ldpp_dout(dpp, 0) << "ERROR: no .data.ciphertext in response from Vault" << dendl;
	return -EINVAL;
      }
      if (plaintext_itr == data_itr->value.MemberEnd()) {
	ldpp_dout(dpp, 0) << "ERROR: no .data.plaintext in response from Vault" << dendl;
	return -EINVAL;
      }
      auto &ciphertext_v { ciphertext_itr->value };
      auto &plaintext_v { plaintext_itr->value };
      if (!ciphertext_v.IsString()) {
	ldpp_dout(dpp, 0) << "ERROR: .data.ciphertext not a string in response from Vault" << dendl;
	return -EINVAL;
      }
      if (!plaintext_v.IsString()) {
	ldpp_dout(dpp, 0) << "ERROR: .data.plaintext not a string in response from Vault" << dendl;
	return -EINVAL;
      }
      set_attr(attrs, RGW_ATTR_CRYPT_DATAKEY, ciphertext_v.GetString());
      return decode_secret(dpp, plaintext_v.GetString(), actual_key);
    }
  }

  int reconstitute_actual_key(const DoutPrefixProvider *dpp, const map<string, bufferlist>& attrs,
                              optional_yield y, std::string& actual_key)
  {
    std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
    std::string wrapped_key = get_str_attribute(attrs, RGW_ATTR_CRYPT_DATAKEY);
    if (compat == COMPAT_ONLY_OLD || key_id.rfind("/") != std::string::npos) {
      return get_key(dpp, key_id, y, actual_key);
    }
/*
	.data.ciphertext <- (to-be) named attribute
	data: {context ciphertext}
	post to prefix + /decrypt/ + key_id
	jq: .data.plaintext
    return decode_secret(json_obj, actual_key)
*/
    std::string context = get_str_attribute(attrs, RGW_ATTR_CRYPT_CONTEXT);
    ZeroPoolDocument d { rapidjson::kObjectType };
    auto &allocator { d.GetAllocator() };
    bufferlist secret_bl;

    add_name_val_to_obj("context", context, d, allocator);
    add_name_val_to_obj("ciphertext", wrapped_key, d, allocator);
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    if (!d.Accept(writer)) {
      ldpp_dout(dpp, 0) << "ERROR: can't make json for vault" << dendl;
      return -EINVAL;
    }
    std::string post_data { buf.GetString() };

    int res = send_request(dpp, "POST", "/decrypt/", key_id,
                           post_data, y, secret_bl);
    if (res < 0) {
      return res;
    }

    ldpp_dout(dpp, 20) << "Parse response into JSON Object" << dendl;

    secret_bl.append('\0');
    rapidjson::StringStream isw(secret_bl.c_str());
    d.SetNull();
    d.ParseStream<>(isw);

    if (d.HasParseError()) {
      ldpp_dout(dpp, 0) << "ERROR: Failed to parse JSON response from Vault: "
	 << rapidjson::GetParseError_En(d.GetParseError()) << dendl;
      return -EINVAL;
    }
    secret_bl.zero();

    if (!d.IsObject()) {
      ldpp_dout(dpp, 0) << "ERROR: response from Vault is not an object" << dendl;
      return -EINVAL;
    }
    {
      auto data_itr { d.FindMember("data") };
      if (data_itr == d.MemberEnd()) {
	ldpp_dout(dpp, 0) << "ERROR: no .data in response from Vault" << dendl;
        return -EINVAL;
      }
      auto plaintext_itr { data_itr->value.FindMember("plaintext") };
      if (plaintext_itr == data_itr->value.MemberEnd()) {
	ldpp_dout(dpp, 0) << "ERROR: no .data.plaintext in response from Vault" << dendl;
	return -EINVAL;
      }
      auto &plaintext_v { plaintext_itr->value };
      if (!plaintext_v.IsString()) {
	ldpp_dout(dpp, 0) << "ERROR: .data.plaintext not a string in response from Vault" << dendl;
	return -EINVAL;
      }
      return decode_secret(dpp, plaintext_v.GetString(), actual_key);
    }
  }

  int create_bucket_key(const DoutPrefixProvider *dpp,
                        const std::string& key_name, optional_yield y)
  {
/*
	.data.ciphertext <- (to-be) named attribute
	data: {"type": "chacha20-poly1305", "derived": true}
	post to prefix + key_name
	empty output.
*/
    ZeroPoolDocument d { rapidjson::kObjectType };
    auto &allocator { d.GetAllocator() };
    bufferlist dummy_bl;
    std::string chacha20_poly1305 { "chacha20-poly1305" };

    add_name_val_to_obj("type", chacha20_poly1305, d, allocator);
    add_name_val_to_obj("derived", true, d, allocator);
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    if (!d.Accept(writer)) {
      ldpp_dout(dpp, 0) << "ERROR: can't make json for vault" << dendl;
      return -EINVAL;
    }
    std::string post_data { buf.GetString() };

    int res = send_request(dpp, "POST", "/keys/", key_name,
                           post_data, y, dummy_bl);
    if (res < 0) {
      return res;
    }
    if (dummy_bl.length() != 0) {
      ldpp_dout(dpp, 0) << "ERROR: unexpected response from Vault making a key: "
	<< dummy_bl
	<< dendl;
    }
    return 0;
  }

  int delete_bucket_key(const DoutPrefixProvider *dpp,
                        const std::string& key_name, optional_yield y)
  {
/*
	/keys/<keyname>/config
	data: {"deletion_allowed": true}
	post to prefix + key_name
	empty output.
*/
    ZeroPoolDocument d { rapidjson::kObjectType };
    auto &allocator { d.GetAllocator() };
    bufferlist dummy_bl;
    std::ostringstream path_temp;
    path_temp << "/keys/";
    path_temp << key_name;
    std::string delete_path { path_temp.str() };
    path_temp << "/config";
    std::string config_path { path_temp.str() };

    add_name_val_to_obj("deletion_allowed", true, d, allocator);
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    if (!d.Accept(writer)) {
      ldpp_dout(dpp, 0) << "ERROR: can't make json for vault" << dendl;
      return -EINVAL;
    }
    std::string post_data { buf.GetString() };

    int res = send_request(dpp, "POST", "", config_path,
                           post_data, y, dummy_bl);
    if (res < 0) {
      return res;
    }
    if (dummy_bl.length() != 0) {
      ldpp_dout(dpp, 0) << "ERROR: unexpected response from Vault marking key to delete: "
	<< dummy_bl
	<< dendl;
      return -EINVAL;
    }

    res = send_request(dpp, "DELETE", "", delete_path,
                       string{}, y, dummy_bl);
    if (res < 0) {
      return res;
    }
    if (dummy_bl.length() != 0) {
      ldpp_dout(dpp, 0) << "ERROR: unexpected response from Vault deleting key: "
	<< dummy_bl
	<< dendl;
      return -EINVAL;
    }
    return 0;
  }
};

class KvSecretEngine: public VaultSecretEngine {

public:

  KvSecretEngine(CephContext *cct, SSEContext & kctx, EngineParmMap parms): VaultSecretEngine(cct, kctx){
    if (!parms.empty()) {
      lderr(cct) << "ERROR: vault kv secrets engine takes no parameters (ignoring them)" << dendl;
    }
  }

  virtual ~KvSecretEngine(){}

  int get_key(const DoutPrefixProvider *dpp, std::string_view key_id,
              optional_yield y, std::string& actual_key) override {
    ZeroPoolDocument d;
    ZeroPoolValue *v;
    bufferlist secret_bl;

    int res = send_request(dpp, key_id, y, secret_bl);
    if (res < 0) {
      return res;
    }

    ldpp_dout(dpp, 20) << "Parse response into JSON Object" << dendl;

    secret_bl.append('\0');
    rapidjson::StringStream isw(secret_bl.c_str());
    d.ParseStream<>(isw);

    if (d.HasParseError()) {
      ldpp_dout(dpp, 0) << "ERROR: Failed to parse JSON response from Vault: "
	 << rapidjson::GetParseError_En(d.GetParseError()) << dendl;
      return -EINVAL;
    }
    secret_bl.zero();

    static const char *elements[] = {"data", "data", "key"};
    v = &d;
    for (auto &elem: elements) {
      if (!v->IsObject()) {
	v = nullptr;
	break;
      }
      auto endr { v->MemberEnd() };
      auto itr { v->FindMember(elem) };
      if (itr == endr) {
	v = nullptr;
	break;
      }
      v = &itr->value;
    }
    if (!v || !v->IsString()) {
      ldpp_dout(dpp, 0) << "ERROR: Key not found in JSON response from Vault using KV Engine" << dendl;
      return -EINVAL;
    }
    return decode_secret(dpp, v->GetString(), actual_key);
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
	KmipGetTheKey& get_uniqueid_for_keyname(const DoutPrefixProvider* dpp, optional_yield y);
	int get_key_for_uniqueid(const DoutPrefixProvider* dpp, optional_yield y, std::string &);
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
KmipGetTheKey::get_uniqueid_for_keyname(const DoutPrefixProvider* dpp,
                                        optional_yield y)
{
	RGWKMIPTransceiver secret_req(cct, RGWKMIPTransceiver::LOCATE);

	secret_req.name = work.data();
	ret = secret_req.process(dpp, y);
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
KmipGetTheKey::get_key_for_uniqueid(const DoutPrefixProvider* dpp,
                                    optional_yield y, std::string& actual_key)
{
	if (failed) return ret;
	RGWKMIPTransceiver secret_req(cct, RGWKMIPTransceiver::GET);
	secret_req.unique_id = work.data();
	ret = secret_req.process(dpp, y);
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

public:

  KmipSecretEngine(CephContext *cct) {
    this->cct = cct;
  }

  int get_key(const DoutPrefixProvider *dpp, std::string_view key_id,
              optional_yield y, std::string& actual_key) override
  {
	int r;
	r = KmipGetTheKey{cct}
		.keyid_to_keyname(key_id)
		.get_uniqueid_for_keyname(dpp, y)
		.get_key_for_uniqueid(dpp, y, actual_key);
	return r;
  }
};

static int get_actual_key_from_conf(const DoutPrefixProvider* dpp,
                                    std::string_view key_id,
                                    std::string_view key_selector,
                                    std::string& actual_key)
{
  int res = 0;

  CephContext* cct = dpp->get_cct();
  static map<string,string> str_map = get_str_map(
      cct->_conf->rgw_crypt_s3_kms_encryption_keys);

  map<string, string>::iterator it = str_map.find(std::string(key_id));
  if (it == str_map.end())
    return -EINVAL;

  std::string master_key;
  try {
    master_key = from_base64((*it).second);
  } catch (std::exception&) {
    ldpp_dout(dpp, 5) << "ERROR: get_actual_key_from_conf invalid encryption key id "
                  << "which contains character that is not base64 encoded."
                  << dendl;
    return -EINVAL;
  }

  if (master_key.length() == AES_256_KEYSIZE) {
    uint8_t _actual_key[AES_256_KEYSIZE];
    if (AES_256_ECB_encrypt(dpp, cct,
        reinterpret_cast<const uint8_t*>(master_key.c_str()), AES_256_KEYSIZE,
        reinterpret_cast<const uint8_t*>(key_selector.data()),
        _actual_key, AES_256_KEYSIZE)) {
      actual_key = std::string((char*)&_actual_key[0], AES_256_KEYSIZE);
    } else {
      res = -EIO;
    }
    ::ceph::crypto::zeroize_for_security(_actual_key, sizeof(_actual_key));
  } else {
    ldpp_dout(dpp, 20) << "Wrong size for key=" << key_id << dendl;
    res = -EIO;
  }

  return res;
}

static int request_key_from_barbican(const DoutPrefixProvider *dpp,
                                     std::string_view key_id,
                                     const std::string& barbican_token,
                                     optional_yield y,
                                     std::string& actual_key) {
  int res;

  CephContext* cct = dpp->get_cct();
  std::string secret_url = cct->_conf->rgw_barbican_url;
  if (secret_url.empty()) {
    ldpp_dout(dpp, 0) << "ERROR: conf rgw_barbican_url is not set" << dendl;
    return -EINVAL;
  }
  concat_url(secret_url, "/v1/secrets/");
  concat_url(secret_url, std::string(key_id));

  bufferlist secret_bl;
  RGWHTTPTransceiver secret_req(cct, "GET", secret_url, &secret_bl);
  secret_req.append_header("Accept", "application/octet-stream");
  secret_req.append_header("X-Auth-Token", barbican_token);

  res = secret_req.process(dpp, y);
  // map 401 to EACCES instead of EPERM
  if (secret_req.get_http_status() ==
      RGWHTTPTransceiver::HTTP_STATUS_UNAUTHORIZED) {
    return -EACCES;
  }
  if (res < 0) {
    return res;
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

static int get_actual_key_from_barbican(const DoutPrefixProvider *dpp,
                                        std::string_view key_id,
                                        optional_yield y,
                                        std::string& actual_key)
{
  int res = 0;
  std::string token;

  if (rgw::keystone::Service::get_keystone_barbican_token(dpp, y, token) < 0) {
    ldpp_dout(dpp, 5) << "Failed to retrieve token for Barbican" << dendl;
    return -EINVAL;
  }

  res = request_key_from_barbican(dpp, key_id, token, y, actual_key);
  if (res != 0) {
    ldpp_dout(dpp, 5) << "Failed to retrieve secret from Barbican:" << key_id << dendl;
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


static int get_actual_key_from_vault(const DoutPrefixProvider *dpp,
                                     SSEContext & kctx,
                                     map<string, bufferlist>& attrs,
                                     optional_yield y,
                                     std::string& actual_key, bool make_it)
{
  CephContext* cct = dpp->get_cct();
  std::string secret_engine_str = kctx.secret_engine();
  EngineParmMap secret_engine_parms;
  auto secret_engine { config_to_engine_and_parms(
    cct, "rgw_crypt_vault_secret_engine",
    secret_engine_str, secret_engine_parms) };
  ldpp_dout(dpp, 20) << "Vault authentication method: " << kctx.auth() << dendl;
  ldpp_dout(dpp, 20) << "Vault Secrets Engine: " << secret_engine << dendl;

  if (RGW_SSE_KMS_VAULT_SE_KV == secret_engine){
    std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
    KvSecretEngine engine(cct, kctx, std::move(secret_engine_parms));
    return engine.get_key(dpp, key_id, y, actual_key);
  }
  else if (RGW_SSE_KMS_VAULT_SE_TRANSIT == secret_engine){
    TransitSecretEngine engine(cct, kctx, std::move(secret_engine_parms));
    std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
    return make_it
	? engine.make_actual_key(dpp, attrs, y, actual_key)
	: engine.reconstitute_actual_key(dpp, attrs, y, actual_key);
  }
  else {
    ldpp_dout(dpp, 0) << "Missing or invalid secret engine" << dendl;
    return -EINVAL;
  }
}


static int make_actual_key_from_vault(const DoutPrefixProvider *dpp,
                                     SSEContext & kctx,
                                     map<string, bufferlist>& attrs,
                                     optional_yield y,
                                     std::string& actual_key)
{
  return get_actual_key_from_vault(dpp, kctx, attrs, y, actual_key, true);
}


static int reconstitute_actual_key_from_vault(const DoutPrefixProvider *dpp,
                                              SSEContext & kctx,
                                              map<string, bufferlist>& attrs,
                                              optional_yield y,
                                              std::string& actual_key)
{
  return get_actual_key_from_vault(dpp, kctx, attrs, y, actual_key, false);
}


static int get_actual_key_from_kmip(const DoutPrefixProvider *dpp,
                                    std::string_view key_id,
                                    optional_yield y,
                                    std::string& actual_key)
{
  std::string secret_engine = RGW_SSE_KMS_KMIP_SE_KV;

  if (RGW_SSE_KMS_KMIP_SE_KV == secret_engine){
    KmipSecretEngine engine(dpp->get_cct());
    return engine.get_key(dpp, key_id, y, actual_key);
  }
  else{
    ldpp_dout(dpp, 0) << "Missing or invalid secret engine" << dendl;
    return -EINVAL;
  }
}
class KMSContext : public SSEContext {
  CephContext *cct;
public:
  KMSContext(CephContext*_cct) : cct{_cct} {};
  ~KMSContext() override {};
  const std::string & backend() override {
    return cct->_conf->rgw_crypt_s3_kms_backend;
  };
  const std::string & addr() override {
    return cct->_conf->rgw_crypt_vault_addr;
  };
  const std::string & auth() override {
    return cct->_conf->rgw_crypt_vault_auth;
  };
  const std::string & k_namespace() override {
    return cct->_conf->rgw_crypt_vault_namespace;
  };
  const std::string & prefix() override {
    return cct->_conf->rgw_crypt_vault_prefix;
  };
  const std::string & secret_engine() override {
    return cct->_conf->rgw_crypt_vault_secret_engine;
  };
  const std::string & ssl_cacert() override {
    return cct->_conf->rgw_crypt_vault_ssl_cacert;
  };
  const std::string & ssl_clientcert() override {
    return cct->_conf->rgw_crypt_vault_ssl_clientcert;
  };
  const std::string & ssl_clientkey() override {
    return cct->_conf->rgw_crypt_vault_ssl_clientkey;
  };
  const std::string & token_file() override {
    return cct->_conf->rgw_crypt_vault_token_file;
  };
  const bool verify_ssl() override {
    return cct->_conf->rgw_crypt_vault_verify_ssl;
  };
};

class SseS3Context : public SSEContext {
  CephContext *cct;
public:
  static const std::string sse_s3_secret_engine;
  SseS3Context(CephContext*_cct) : cct{_cct} {};
  ~SseS3Context(){};
  const std::string & backend() override {
   return cct->_conf->rgw_crypt_sse_s3_backend;
  };
  const std::string & addr() override {
    return cct->_conf->rgw_crypt_sse_s3_vault_addr;
  };
  const std::string & auth() override {
    return cct->_conf->rgw_crypt_sse_s3_vault_auth;
  };
  const std::string & k_namespace() override {
    return cct->_conf->rgw_crypt_sse_s3_vault_namespace;
  };
  const std::string & prefix() override {
    return cct->_conf->rgw_crypt_sse_s3_vault_prefix;
  };
  const std::string & secret_engine() override {
    return cct->_conf->rgw_crypt_sse_s3_vault_secret_engine;
  };
  const std::string & ssl_cacert() override {
    return cct->_conf->rgw_crypt_sse_s3_vault_ssl_cacert;
  };
  const std::string & ssl_clientcert() override {
    return cct->_conf->rgw_crypt_sse_s3_vault_ssl_clientcert;
  };
  const std::string & ssl_clientkey() override {
    return cct->_conf->rgw_crypt_sse_s3_vault_ssl_clientkey;
  };
  const std::string & token_file() override {
    return cct->_conf->rgw_crypt_sse_s3_vault_token_file;
  };
  const bool verify_ssl() override {
    return cct->_conf->rgw_crypt_sse_s3_vault_verify_ssl;
  };
};

int reconstitute_actual_key_from_kms(const DoutPrefixProvider *dpp,
                                     map<string, bufferlist>& attrs,
                                     optional_yield y,
                                     std::string& actual_key)
{
  std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
  KMSContext kctx { dpp->get_cct() };
  const std::string &kms_backend { kctx.backend() };

  ldpp_dout(dpp, 20) << "Getting KMS encryption key for key " << key_id << dendl;
  ldpp_dout(dpp, 20) << "SSE-KMS backend is " << kms_backend << dendl;

  if (RGW_SSE_KMS_BACKEND_BARBICAN == kms_backend) {
    return get_actual_key_from_barbican(dpp, key_id, y, actual_key);
  }

  if (RGW_SSE_KMS_BACKEND_VAULT == kms_backend) {
    return reconstitute_actual_key_from_vault(dpp, kctx, attrs, y, actual_key);
  }

  if (RGW_SSE_KMS_BACKEND_KMIP == kms_backend) {
    return get_actual_key_from_kmip(dpp, key_id, y, actual_key);
  }

  if (RGW_SSE_KMS_BACKEND_TESTING == kms_backend) {
    std::string key_selector = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYSEL);
    return get_actual_key_from_conf(dpp, key_id, key_selector, actual_key);
  }

  ldpp_dout(dpp, 0) << "ERROR: Invalid rgw_crypt_s3_kms_backend: " << kms_backend << dendl;
  return -EINVAL;
}

int make_actual_key_from_kms(const DoutPrefixProvider *dpp,
                             map<string, bufferlist>& attrs,
                             optional_yield y,
                             std::string& actual_key)
{
  KMSContext kctx { dpp->get_cct() };
  const std::string &kms_backend { kctx.backend() };
  if (RGW_SSE_KMS_BACKEND_VAULT == kms_backend)
    return make_actual_key_from_vault(dpp, kctx, attrs, y, actual_key);
  return reconstitute_actual_key_from_kms(dpp, attrs, y, actual_key);
}

int reconstitute_actual_key_from_sse_s3(const DoutPrefixProvider *dpp,
                                        map<string, bufferlist>& attrs,
                                        optional_yield y,
                                        std::string& actual_key)
{
  std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
  SseS3Context kctx { dpp->get_cct() };
  const std::string &kms_backend { kctx.backend() };

  ldpp_dout(dpp, 20) << "Getting SSE-S3  encryption key for key " << key_id << dendl;
  ldpp_dout(dpp, 20) << "SSE-KMS backend is " << kms_backend << dendl;

  if (RGW_SSE_KMS_BACKEND_VAULT == kms_backend) {
    return reconstitute_actual_key_from_vault(dpp, kctx, attrs, y, actual_key);
  }

  ldpp_dout(dpp, 0) << "ERROR: Invalid rgw_crypt_sse_s3_backend: " << kms_backend << dendl;
  return -EINVAL;
}

int make_actual_key_from_sse_s3(const DoutPrefixProvider *dpp,
                                map<string, bufferlist>& attrs,
                                optional_yield y,
                                std::string& actual_key)
{
  SseS3Context kctx { dpp->get_cct() };
  const std::string kms_backend { kctx.backend() };
  if (RGW_SSE_KMS_BACKEND_VAULT != kms_backend) {
    ldpp_dout(dpp, 0) << "ERROR: Unsupported rgw_crypt_sse_s3_backend: " << kms_backend << dendl;
    return -EINVAL;
  }
  return make_actual_key_from_vault(dpp, kctx, attrs, y, actual_key);
}


int create_sse_s3_bucket_key(const DoutPrefixProvider *dpp,
                             const std::string& bucket_key,
                             optional_yield y)
{
  CephContext* cct = dpp->get_cct();
  SseS3Context kctx { cct };

  const std::string kms_backend { kctx.backend() };
  if (RGW_SSE_KMS_BACKEND_VAULT != kms_backend) {
    ldpp_dout(dpp, 0) << "ERROR: Unsupported rgw_crypt_sse_s3_backend: " << kms_backend << dendl;
    return -EINVAL;
  }

  std::string secret_engine_str = kctx.secret_engine();
  EngineParmMap secret_engine_parms;
  auto secret_engine { config_to_engine_and_parms(
    cct, "rgw_crypt_sse_s3_vault_secret_engine",
    secret_engine_str, secret_engine_parms) };
  if (RGW_SSE_KMS_VAULT_SE_TRANSIT == secret_engine){
    TransitSecretEngine engine(cct, kctx, std::move(secret_engine_parms));
    return engine.create_bucket_key(dpp, bucket_key, y);
  }
  else {
    ldpp_dout(dpp, 0) << "Missing or invalid secret engine" << dendl;
    return -EINVAL;
  }
}

int remove_sse_s3_bucket_key(const DoutPrefixProvider *dpp,
                             const std::string& bucket_key,
                             optional_yield y)
{
  CephContext* cct = dpp->get_cct();
  SseS3Context kctx { cct };
  std::string secret_engine_str = kctx.secret_engine();
  EngineParmMap secret_engine_parms;
  auto secret_engine { config_to_engine_and_parms(
    cct, "rgw_crypt_sse_s3_vault_secret_engine",
    secret_engine_str, secret_engine_parms) };
  if (RGW_SSE_KMS_VAULT_SE_TRANSIT == secret_engine){
    TransitSecretEngine engine(cct, kctx, std::move(secret_engine_parms));
    return engine.delete_bucket_key(dpp, bucket_key, y);
  }
  else {
    ldpp_dout(dpp, 0) << "Missing or invalid secret engine" << dendl;
    return -EINVAL;
  }
}
