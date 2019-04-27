// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <array>

#include <boost/utility/string_view.hpp>
#include <boost/container/static_vector.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string.hpp>

#include "rgw_swift_auth.h"
#include "rgw_rest.h"

#include "common/ceph_crypto.h"
#include "common/Clock.h"

#include "include/random.h"

#include "rgw_client_io.h"
#include "rgw_http_client.h"
#include "include/str_list.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

#define DEFAULT_SWIFT_PREFIX "/swift"

using namespace ceph::crypto;


namespace rgw {
namespace auth {
namespace swift {

/* TempURL: applier */
void TempURLApplier::modify_request_state(const DoutPrefixProvider* dpp, req_state* s) const       /* in/out */
{
  bool inline_exists = false;
  const std::string& filename = s->info.args.get("filename");

  s->info.args.get("inline", &inline_exists);
  if (inline_exists) {
    s->content_disp.override = "inline";
  } else if (!filename.empty()) {
    std::string fenc;
    url_encode(filename, fenc);
    s->content_disp.override = "attachment; filename=\"" + fenc + "\"";
  } else {
    std::string fenc;
    url_encode(s->object.name, fenc);
    s->content_disp.fallback = "attachment; filename=\"" + fenc + "\"";
  }

  ldpp_dout(dpp, 20) << "finished applying changes to req_state for TempURL: "
                    << " content_disp override " << s->content_disp.override
                    << " content_disp fallback " << s->content_disp.fallback
                    << dendl;

}

/* TempURL: engine */
bool TempURLEngine::is_applicable(const req_state* const s) const noexcept
{
  return s->info.args.exists("temp_url_sig") ||
         s->info.args.exists("temp_url_expires");
}

void TempURLEngine::get_owner_info(const DoutPrefixProvider* dpp, const req_state* const s,
                                   RGWUserInfo& owner_info) const
{
  /* We cannot use req_state::bucket_name because it isn't available
   * now. It will be initialized in RGWHandler_REST_SWIFT::postauth_init(). */
  const string& bucket_name = s->init_state.url_bucket;

  /* TempURL requires that bucket and object names are specified. */
  if (bucket_name.empty() || s->object.empty()) {
    throw -EPERM;
  }

  /* TempURL case is completely different than the Keystone auth - you may
   * get account name only through extraction from URL. In turn, knowledge
   * about account is neccessary to obtain its bucket tenant. Without that,
   * the access would be limited to accounts with empty tenant. */
  string bucket_tenant;
  if (!s->account_name.empty()) {
    RGWUserInfo uinfo;
    bool found = false;

    const rgw_user uid(s->account_name);
    if (uid.tenant.empty()) {
      const rgw_user tenanted_uid(uid.id, uid.id);

      if (rgw_get_user_info_by_uid(store, tenanted_uid, uinfo) >= 0) {
        /* Succeeded. */
        bucket_tenant = uinfo.user_id.tenant;
        found = true;
      }
    }

    if (!found && rgw_get_user_info_by_uid(store, uid, uinfo) < 0) {
      throw -EPERM;
    } else {
      bucket_tenant = uinfo.user_id.tenant;
    }
  }

  /* Need to get user info of bucket owner. */
  RGWBucketInfo bucket_info;
  int ret = store->get_bucket_info(*s->sysobj_ctx,
                                   bucket_tenant, bucket_name,
                                   bucket_info, nullptr);
  if (ret < 0) {
    throw ret;
  }

  ldpp_dout(dpp, 20) << "temp url user (bucket owner): " << bucket_info.owner
                 << dendl;

  if (rgw_get_user_info_by_uid(store, bucket_info.owner, owner_info) < 0) {
    throw -EPERM;
  }
}

std::string TempURLEngine::convert_from_iso8601(std::string expires) const
{
  /* Swift's TempURL allows clients to send the expiration as ISO8601-
   * compatible strings. Though, only plain UNIX timestamp are taken
   * for the HMAC calculations. We need to make the conversion. */
  struct tm date_t;
  if (!parse_iso8601(expires.c_str(), &date_t, nullptr, true)) {
    return expires;
  } else {
    return std::to_string(internal_timegm(&date_t));
  }
}

bool TempURLEngine::is_expired(const std::string& expires) const
{
  string err;
  const utime_t now = ceph_clock_now();
  const uint64_t expiration = (uint64_t)strict_strtoll(expires.c_str(),
                                                       10, &err);
  if (!err.empty()) {
    dout(5) << "failed to parse temp_url_expires: " << err << dendl;
    return true;
  }

  if (expiration <= (uint64_t)now.sec()) {
    dout(5) << "temp url expired: " << expiration << " <= " << now.sec() << dendl;
    return true;
  }

  return false;
}

std::string extract_swift_subuser(const std::string& swift_user_name) {
  size_t pos = swift_user_name.find(':');
  if (std::string::npos == pos) {
    return swift_user_name;
  } else {
    return swift_user_name.substr(pos + 1);
  }
}

class TempURLEngine::SignatureHelper
{
private:
  static constexpr uint32_t output_size =
    CEPH_CRYPTO_HMACSHA1_DIGESTSIZE * 2 + 1;

  unsigned char dest[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE]; // 20
  char dest_str[output_size];

public:
  SignatureHelper() = default;

  const char* calc(const std::string& key,
                   const boost::string_view& method,
                   const boost::string_view& path,
                   const std::string& expires) {

    using ceph::crypto::HMACSHA1;
    using UCHARPTR = const unsigned char*;

    HMACSHA1 hmac((UCHARPTR) key.c_str(), key.size());
    hmac.Update((UCHARPTR) method.data(), method.size());
    hmac.Update((UCHARPTR) "\n", 1);
    hmac.Update((UCHARPTR) expires.c_str(), expires.size());
    hmac.Update((UCHARPTR) "\n", 1);
    hmac.Update((UCHARPTR) path.data(), path.size());
    hmac.Final(dest);

    buf_to_hex((UCHARPTR) dest, sizeof(dest), dest_str);

    return dest_str;
  }

  bool is_equal_to(const std::string& rhs) const {
    /* never allow out-of-range exception */
    if (rhs.size() < (output_size - 1)) {
      return false;
    }
    return rhs.compare(0 /* pos */,  output_size, dest_str) == 0;
  }

}; /* TempURLEngine::SignatureHelper */

class TempURLEngine::PrefixableSignatureHelper
    : private TempURLEngine::SignatureHelper {
  using base_t = SignatureHelper;

  const boost::string_view decoded_uri;
  const boost::string_view object_name;
  boost::string_view no_obj_uri;

  const boost::optional<const std::string&> prefix;

public:
  PrefixableSignatureHelper(const std::string& _decoded_uri,
	                    const std::string& object_name,
                            const boost::optional<const std::string&> prefix)
    : decoded_uri(_decoded_uri),
      object_name(object_name),
      prefix(prefix) {
    /* Transform: v1/acct/cont/obj - > v1/acct/cont/
     *
     * NOTE(rzarzynski): we really want to substr() on boost::string_view,
     * not std::string. Otherwise we would end with no_obj_uri referencing
     * a temporary. */
    no_obj_uri = \
      decoded_uri.substr(0, decoded_uri.length() - object_name.length());
  }

  const char* calc(const std::string& key,
                   const boost::string_view& method,
                   const boost::string_view& path,
                   const std::string& expires) {
    if (!prefix) {
      return base_t::calc(key, method, path, expires);
    } else {
      const auto prefixed_path = \
        string_cat_reserve("prefix:", no_obj_uri, *prefix);
      return base_t::calc(key, method, prefixed_path, expires);
    }
  }

  bool is_equal_to(const std::string& rhs) const {
    bool is_auth_ok = base_t::is_equal_to(rhs);

    if (prefix && is_auth_ok) {
      const auto prefix_uri = string_cat_reserve(no_obj_uri, *prefix);
      is_auth_ok = boost::algorithm::starts_with(decoded_uri, prefix_uri);
    }

    return is_auth_ok;
  }
}; /* TempURLEngine::PrefixableSignatureHelper */

TempURLEngine::result_t
TempURLEngine::authenticate(const DoutPrefixProvider* dpp, const req_state* const s) const
{
  if (! is_applicable(s)) {
    return result_t::deny();
  }

  /* NOTE(rzarzynski): RGWHTTPArgs::get(), in contrast to RGWEnv::get(),
   * never returns nullptr. If the requested parameter is absent, we will
   * get the empty string. */
  const std::string& temp_url_sig = s->info.args.get("temp_url_sig");
  const std::string& temp_url_expires = \
    convert_from_iso8601(s->info.args.get("temp_url_expires"));

  if (temp_url_sig.empty() || temp_url_expires.empty()) {
    return result_t::deny();
  }

  /* Though, for prefixed tempurls we need to differentiate between empty
   * prefix and lack of prefix. Empty prefix means allowance for whole
   * container. */
  const boost::optional<const std::string&> temp_url_prefix = \
    s->info.args.get_optional("temp_url_prefix");

  RGWUserInfo owner_info;
  try {
    get_owner_info(dpp, s, owner_info);
  } catch (...) {
    ldpp_dout(dpp, 5) << "cannot get user_info of account's owner" << dendl;
    return result_t::reject();
  }

  if (owner_info.temp_url_keys.empty()) {
    ldpp_dout(dpp, 5) << "user does not have temp url key set, aborting" << dendl;
    return result_t::reject();
  }

  if (is_expired(temp_url_expires)) {
    ldpp_dout(dpp, 5) << "temp url link expired" << dendl;
    return result_t::reject(-EPERM);
  }

  /* We need to verify two paths because of compliance with Swift, Tempest
   * and old versions of RadosGW. The second item will have the prefix
   * of Swift API entry point removed. */

  /* XXX can we search this ONCE? */
  const size_t pos = g_conf()->rgw_swift_url_prefix.find_last_not_of('/') + 1;
  const boost::string_view ref_uri = s->decoded_uri;
  const std::array<boost::string_view, 2> allowed_paths = {
    ref_uri,
    ref_uri.substr(pos + 1)
  };

  /* Account owner calculates the signature also against a HTTP method. */
  boost::container::static_vector<boost::string_view, 3> allowed_methods;
  if (strcmp("HEAD", s->info.method) == 0) {
    /* HEAD requests are specially handled. */
    /* TODO: after getting a newer boost (with static_vector supporting
     * initializers lists), get back to the good notation:
     *   allowed_methods = {"HEAD", "GET", "PUT" };
     * Just for now let's use emplace_back to construct the vector. */
    allowed_methods.emplace_back("HEAD");
    allowed_methods.emplace_back("GET");
    allowed_methods.emplace_back("PUT");
  } else if (strlen(s->info.method) > 0) {
    allowed_methods.emplace_back(s->info.method);
  }

  /* Need to try each combination of keys, allowed path and methods. */
  PrefixableSignatureHelper sig_helper {
    s->decoded_uri,
    s->object.name,
    temp_url_prefix
  };

  for (const auto& kv : owner_info.temp_url_keys) {
    const int temp_url_key_num = kv.first;
    const string& temp_url_key = kv.second;

    if (temp_url_key.empty()) {
      continue;
    }

    for (const auto& path : allowed_paths) {
      for (const auto& method : allowed_methods) {
        const char* const local_sig = sig_helper.calc(temp_url_key, method,
                                                      path, temp_url_expires);

        ldpp_dout(dpp, 20) << "temp url signature [" << temp_url_key_num
                          << "] (calculated): " << local_sig
                          << dendl;

        if (sig_helper.is_equal_to(temp_url_sig)) {
          auto apl = apl_factory->create_apl_turl(cct, s, owner_info);
          return result_t::grant(std::move(apl));
        } else {
          ldpp_dout(dpp,  5) << "temp url signature mismatch: " << local_sig
                            << " != " << temp_url_sig  << dendl;
        }
      }
    }
  }

  return result_t::reject();
}


/* External token */
bool ExternalTokenEngine::is_applicable(const std::string& token) const noexcept
{
  if (token.empty()) {
    return false;
  } else if (g_conf()->rgw_swift_auth_url.empty()) {
    return false;
  } else {
    return true;
  }
}

ExternalTokenEngine::result_t
ExternalTokenEngine::authenticate(const DoutPrefixProvider* dpp,
                                  const std::string& token,
                                  const req_state* const s) const
{
  if (! is_applicable(token)) {
    return result_t::deny();
  }

  std::string auth_url = g_conf()->rgw_swift_auth_url;
  if (auth_url.back() != '/') {
    auth_url.append("/");
  }

  auth_url.append("token");
  char url_buf[auth_url.size() + 1 + token.length() + 1];
  sprintf(url_buf, "%s/%s", auth_url.c_str(), token.c_str());

  RGWHTTPHeadersCollector validator(cct, "GET", url_buf, { "X-Auth-Groups", "X-Auth-Ttl" });

  ldpp_dout(dpp, 10) << "rgw_swift_validate_token url=" << url_buf << dendl;

  int ret = validator.process(null_yield);
  if (ret < 0) {
    throw ret;
  }

  std::string swift_user;
  try {
    std::vector<std::string> swift_groups;
    get_str_vec(validator.get_header_value("X-Auth-Groups"),
                ",", swift_groups);

    if (0 == swift_groups.size()) {
      return result_t::deny(-EPERM);
    } else {
      swift_user = std::move(swift_groups[0]);
    }
  } catch (const std::out_of_range&) {
    /* The X-Auth-Groups header isn't present in the response. */
    return result_t::deny(-EPERM);
  }

  if (swift_user.empty()) {
    return result_t::deny(-EPERM);
  }

  ldpp_dout(dpp, 10) << "swift user=" << swift_user << dendl;

  RGWUserInfo tmp_uinfo;
  ret = rgw_get_user_info_by_swift(store, swift_user, tmp_uinfo);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "NOTICE: couldn't map swift user" << dendl;
    throw ret;
  }

  auto apl = apl_factory->create_apl_local(cct, s, tmp_uinfo,
                                           extract_swift_subuser(swift_user),
                                           boost::none);
  return result_t::grant(std::move(apl));
}

static int build_token(const string& swift_user,
                       const string& key,
                       const uint64_t nonce,
                       const utime_t& expiration,
                       bufferlist& bl)
{
  using ceph::encode;
  encode(swift_user, bl);
  encode(nonce, bl);
  encode(expiration, bl);

  bufferptr p(CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);

  char buf[bl.length() * 2 + 1];
  buf_to_hex((const unsigned char *)bl.c_str(), bl.length(), buf);
  dout(20) << "build_token token=" << buf << dendl;

  char k[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
  memset(k, 0, sizeof(k));
  const char *s = key.c_str();
  for (int i = 0; i < (int)key.length(); i++, s++) {
    k[i % CEPH_CRYPTO_HMACSHA1_DIGESTSIZE] |= *s;
  }
  calc_hmac_sha1(k, sizeof(k), bl.c_str(), bl.length(), p.c_str());

  bl.append(p);

  return 0;

}

static int encode_token(CephContext *cct, string& swift_user, string& key,
			bufferlist& bl)
{
  const auto nonce = ceph::util::generate_random_number<uint64_t>();

  utime_t expiration = ceph_clock_now();
  expiration += cct->_conf->rgw_swift_token_expiration;

  return build_token(swift_user, key, nonce, expiration, bl);
}


/* AUTH_rgwtk (signed token): engine */
bool SignedTokenEngine::is_applicable(const std::string& token) const noexcept
{
  if (token.empty()) {
    return false;
  } else {
    return token.compare(0, 10, "AUTH_rgwtk") == 0;
  }
}

SignedTokenEngine::result_t
SignedTokenEngine::authenticate(const DoutPrefixProvider* dpp,
                                const std::string& token,
                                const req_state* const s) const
{
  if (! is_applicable(token)) {
    return result_t::deny(-EPERM);
  }

  /* Effective token string is the part after the prefix. */
  const std::string etoken = token.substr(strlen("AUTH_rgwtk"));
  const size_t etoken_len = etoken.length();

  if (etoken_len & 1) {
    ldpp_dout(dpp, 0) << "NOTICE: failed to verify token: odd token length="
	          << etoken_len << dendl;
    throw -EINVAL;
  }

  ceph::bufferptr p(etoken_len/2);
  int ret = hex_to_buf(etoken.c_str(), p.c_str(), etoken_len);
  if (ret < 0) {
    throw ret;
  }

  ceph::bufferlist tok_bl;
  tok_bl.append(p);

  uint64_t nonce;
  utime_t expiration;
  std::string swift_user;

  try {
    auto iter = tok_bl.cbegin();

    using ceph::decode;
    decode(swift_user, iter);
    decode(nonce, iter);
    decode(expiration, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "NOTICE: failed to decode token" << dendl;
    throw -EINVAL;
  }

  const utime_t now = ceph_clock_now();
  if (expiration < now) {
    ldpp_dout(dpp, 0) << "NOTICE: old timed out token was used now=" << now
	          << " token.expiration=" << expiration
                  << dendl;
    return result_t::deny(-EPERM);
  }

  RGWUserInfo user_info;
  ret = rgw_get_user_info_by_swift(store, swift_user, user_info);
  if (ret < 0) {
    throw ret;
  }

  ldpp_dout(dpp, 10) << "swift_user=" << swift_user << dendl;

  const auto siter = user_info.swift_keys.find(swift_user);
  if (siter == std::end(user_info.swift_keys)) {
    return result_t::deny(-EPERM);
  }

  const auto swift_key = siter->second;

  bufferlist local_tok_bl;
  ret = build_token(swift_user, swift_key.key, nonce, expiration, local_tok_bl);
  if (ret < 0) {
    throw ret;
  }

  if (local_tok_bl.length() != tok_bl.length()) {
    ldpp_dout(dpp, 0) << "NOTICE: tokens length mismatch:"
                  << " tok_bl.length()=" << tok_bl.length()
	          << " local_tok_bl.length()=" << local_tok_bl.length()
                  << dendl;
    return result_t::deny(-EPERM);
  }

  if (memcmp(local_tok_bl.c_str(), tok_bl.c_str(),
             local_tok_bl.length()) != 0) {
    char buf[local_tok_bl.length() * 2 + 1];

    buf_to_hex(reinterpret_cast<const unsigned char *>(local_tok_bl.c_str()),
               local_tok_bl.length(), buf);

    ldpp_dout(dpp, 0) << "NOTICE: tokens mismatch tok=" << buf << dendl;
    return result_t::deny(-EPERM);
  }

  auto apl = apl_factory->create_apl_local(cct, s, user_info,
                                           extract_swift_subuser(swift_user),
                                           boost::none);
  return result_t::grant(std::move(apl));
}

} /* namespace swift */
} /* namespace auth */
} /* namespace rgw */


void RGW_SWIFT_Auth_Get::execute()
{
  int ret = -EPERM;

  const char *key = s->info.env->get("HTTP_X_AUTH_KEY");
  const char *user = s->info.env->get("HTTP_X_AUTH_USER");

  s->prot_flags |= RGW_REST_SWIFT;

  string user_str;
  RGWUserInfo info;
  bufferlist bl;
  RGWAccessKey *swift_key;
  map<string, RGWAccessKey>::iterator siter;

  string swift_url = g_conf()->rgw_swift_url;
  string swift_prefix = g_conf()->rgw_swift_url_prefix;
  string tenant_path;

  /*
   * We did not allow an empty Swift prefix before, but we want it now.
   * So, we take rgw_swift_url_prefix = "/" to yield the empty prefix.
   * The rgw_swift_url_prefix = "" is the default and yields "/swift"
   * in a backwards-compatible way.
   */
  if (swift_prefix.size() == 0) {
    swift_prefix = DEFAULT_SWIFT_PREFIX;
  } else if (swift_prefix == "/") {
    swift_prefix.clear();
  } else {
    if (swift_prefix[0] != '/') {
      swift_prefix.insert(0, "/");
    }
  }

  if (swift_url.size() == 0) {
    bool add_port = false;
    const char *server_port = s->info.env->get("SERVER_PORT_SECURE");
    const char *protocol;
    if (server_port) {
      add_port = (strcmp(server_port, "443") != 0);
      protocol = "https";
    } else {
      server_port = s->info.env->get("SERVER_PORT");
      add_port = (strcmp(server_port, "80") != 0);
      protocol = "http";
    }
    const char *host = s->info.env->get("HTTP_HOST");
    if (!host) {
      dout(0) << "NOTICE: server is misconfigured, missing rgw_swift_url_prefix or rgw_swift_url, HTTP_HOST is not set" << dendl;
      ret = -EINVAL;
      goto done;
    }
    swift_url = protocol;
    swift_url.append("://");
    swift_url.append(host);
    if (add_port && !strchr(host, ':')) {
      swift_url.append(":");
      swift_url.append(server_port);
    }
  }

  if (!key || !user)
    goto done;

  user_str = user;

  if ((ret = rgw_get_user_info_by_swift(store, user_str, info)) < 0)
  {
    ret = -EACCES;
    goto done;
  }

  siter = info.swift_keys.find(user_str);
  if (siter == info.swift_keys.end()) {
    ret = -EPERM;
    goto done;
  }
  swift_key = &siter->second;

  if (swift_key->key.compare(key) != 0) {
    dout(0) << "NOTICE: RGW_SWIFT_Auth_Get::execute(): bad swift key" << dendl;
    ret = -EPERM;
    goto done;
  }

  if (!g_conf()->rgw_swift_tenant_name.empty()) {
    tenant_path = "/AUTH_";
    tenant_path.append(g_conf()->rgw_swift_tenant_name);
  } else if (g_conf()->rgw_swift_account_in_url) {
    tenant_path = "/AUTH_";
    tenant_path.append(info.user_id.to_str());
  }

  dump_header(s, "X-Storage-Url", swift_url + swift_prefix + "/v1" +
              tenant_path);

  using rgw::auth::swift::encode_token;
  if ((ret = encode_token(s->cct, swift_key->id, swift_key->key, bl)) < 0)
    goto done;

  {
    static constexpr size_t PREFIX_LEN = sizeof("AUTH_rgwtk") - 1;
    char token_val[PREFIX_LEN + bl.length() * 2 + 1];

    snprintf(token_val, PREFIX_LEN + 1, "AUTH_rgwtk");
    buf_to_hex((const unsigned char *)bl.c_str(), bl.length(),
	       token_val + PREFIX_LEN);

    dump_header(s, "X-Storage-Token", token_val);
    dump_header(s, "X-Auth-Token", token_val);
  }

  ret = STATUS_NO_CONTENT;

done:
  set_req_state_err(s, ret);
  dump_errno(s);
  end_header(s);
}

int RGWHandler_SWIFT_Auth::init(RGWRados *store, struct req_state *state,
				rgw::io::BasicClient *cio)
{
  state->dialect = "swift-auth";
  state->formatter = new JSONFormatter;
  state->format = RGW_FORMAT_JSON;

  return RGWHandler::init(store, state, cio);
}

int RGWHandler_SWIFT_Auth::authorize(const DoutPrefixProvider *dpp)
{
  return 0;
}

RGWOp *RGWHandler_SWIFT_Auth::op_get()
{
  return new RGW_SWIFT_Auth_Get;
}

