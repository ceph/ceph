// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <array>
#include <boost/algorithm/string/predicate.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>

#include <boost/algorithm/string.hpp>
#include <boost/container/static_vector.hpp>
#include <boost/container/flat_map.hpp>

#include "common/sstring.hh"
#include "rgw_common.h"
#include "rgw_rest_s3.h"
#include "rgw_auth.h"
#include "rgw_auth_filters.h"
#include "rgw_auth_keystone.h"


namespace rgw {
namespace auth {
namespace s3 {

static constexpr auto RGW_AUTH_GRACE = std::chrono::minutes{15};

// returns true if the request time is within RGW_AUTH_GRACE of the current time
bool is_time_skew_ok(time_t t);

class STSAuthStrategy : public rgw::auth::Strategy,
                        public rgw::auth::RemoteApplier::Factory,
                        public rgw::auth::LocalApplier::Factory,
                        public rgw::auth::RoleApplier::Factory {
  typedef rgw::auth::IdentityApplier::aplptr_t aplptr_t;
  rgw::sal::Driver* driver;
  const rgw::auth::ImplicitTenants& implicit_tenant_context;

  STSEngine  sts_engine;

  aplptr_t create_apl_remote(CephContext* const cct,
                             const req_state* const s,
                             rgw::auth::RemoteApplier::acl_strategy_t&& acl_alg,
                             const rgw::auth::RemoteApplier::AuthInfo &info) const override {
    auto apl = rgw::auth::add_sysreq(cct, driver, s,
      rgw::auth::RemoteApplier(cct, driver, std::move(acl_alg), info,
			       implicit_tenant_context,
                               rgw::auth::ImplicitTenants::IMPLICIT_TENANTS_S3));
    return aplptr_t(new decltype(apl)(std::move(apl)));
  }

  aplptr_t create_apl_local(CephContext* const cct,
                            const req_state* const s,
                            const RGWUserInfo& user_info,
                            const std::string& subuser,
                            const std::optional<uint32_t>& perm_mask,
                            const std::string& access_key_id) const override {
    auto apl = rgw::auth::add_sysreq(cct, driver, s,
      rgw::auth::LocalApplier(cct, user_info, subuser, perm_mask, access_key_id));
    return aplptr_t(new decltype(apl)(std::move(apl)));
  }

  aplptr_t create_apl_role(CephContext* const cct,
                            const req_state* const s,
                            const rgw::auth::RoleApplier::Role& role,
                            const rgw::auth::RoleApplier::TokenAttrs& token_attrs) const override {
    auto apl = rgw::auth::add_sysreq(cct, driver, s,
      rgw::auth::RoleApplier(cct, role, token_attrs));
    return aplptr_t(new decltype(apl)(std::move(apl)));
  }

public:
  STSAuthStrategy(CephContext* const cct,
                       rgw::sal::Driver* driver,
                       const rgw::auth::ImplicitTenants& implicit_tenant_context,
                       AWSEngine::VersionAbstractor* const ver_abstractor)
    : driver(driver),
      implicit_tenant_context(implicit_tenant_context),
      sts_engine(cct, driver, *ver_abstractor,
                  static_cast<rgw::auth::LocalApplier::Factory*>(this),
                  static_cast<rgw::auth::RemoteApplier::Factory*>(this),
                  static_cast<rgw::auth::RoleApplier::Factory*>(this)) {
      if (cct->_conf->rgw_s3_auth_use_sts) {
        add_engine(Control::SUFFICIENT, sts_engine);
      }
    }

  const char* get_name() const noexcept override {
    return "rgw::auth::s3::STSAuthStrategy";
  }
};

class ExternalAuthStrategy : public rgw::auth::Strategy,
                             public rgw::auth::RemoteApplier::Factory {
  typedef rgw::auth::IdentityApplier::aplptr_t aplptr_t;
  rgw::sal::Driver* driver;
  const rgw::auth::ImplicitTenants& implicit_tenant_context;

  using keystone_config_t = rgw::keystone::CephCtxConfig;
  using keystone_cache_t = rgw::keystone::TokenCache;
  using secret_cache_t = rgw::auth::keystone::SecretCache;
  using EC2Engine = rgw::auth::keystone::EC2Engine;

  boost::optional <EC2Engine> keystone_engine;
  LDAPEngine ldap_engine;

  aplptr_t create_apl_remote(CephContext* const cct,
                             const req_state* const s,
                             rgw::auth::RemoteApplier::acl_strategy_t&& acl_alg,
                             const rgw::auth::RemoteApplier::AuthInfo &info) const override {
    auto apl = rgw::auth::add_sysreq(cct, driver, s,
      rgw::auth::RemoteApplier(cct, driver, std::move(acl_alg), info,
                               implicit_tenant_context,
                               rgw::auth::ImplicitTenants::IMPLICIT_TENANTS_S3));
    /* TODO(rzarzynski): replace with static_ptr. */
    return aplptr_t(new decltype(apl)(std::move(apl)));
  }

public:
  ExternalAuthStrategy(CephContext* const cct,
                       rgw::sal::Driver* driver,
                       const rgw::auth::ImplicitTenants& implicit_tenant_context,
                       AWSEngine::VersionAbstractor* const ver_abstractor)
    : driver(driver),
      implicit_tenant_context(implicit_tenant_context),
      ldap_engine(cct, driver, *ver_abstractor,
                  static_cast<rgw::auth::RemoteApplier::Factory*>(this)) {

    if (cct->_conf->rgw_s3_auth_use_keystone &&
        ! cct->_conf->rgw_keystone_url.empty()) {

      keystone_engine.emplace(cct, ver_abstractor,
                              static_cast<rgw::auth::RemoteApplier::Factory*>(this),
                              keystone_config_t::get_instance(),
                              keystone_cache_t::get_instance<keystone_config_t>(),
			      secret_cache_t::get_instance());
      add_engine(Control::SUFFICIENT, *keystone_engine);

    }

    if (ldap_engine.valid()) {
      add_engine(Control::SUFFICIENT, ldap_engine);
    }
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::s3::AWSv2ExternalAuthStrategy";
  }
};


template <class AbstractorT,
          bool AllowAnonAccessT = false>
class AWSAuthStrategy : public rgw::auth::Strategy,
                        public rgw::auth::LocalApplier::Factory {
  typedef rgw::auth::IdentityApplier::aplptr_t aplptr_t;

  static_assert(std::is_base_of<rgw::auth::s3::AWSEngine::VersionAbstractor,
                                AbstractorT>::value,
                "AbstractorT must be a subclass of rgw::auth::s3::VersionAbstractor");

  rgw::sal::Driver* driver;
  AbstractorT ver_abstractor;

  S3AnonymousEngine anonymous_engine;
  ExternalAuthStrategy external_engines;
  STSAuthStrategy sts_engine;
  LocalEngine local_engine;

  aplptr_t create_apl_local(CephContext* const cct,
                            const req_state* const s,
                            const RGWUserInfo& user_info,
                            const std::string& subuser,
                            const std::optional<uint32_t>& perm_mask,
                            const std::string& access_key_id) const override {
    auto apl = rgw::auth::add_sysreq(cct, driver, s,
      rgw::auth::LocalApplier(cct, user_info, subuser, perm_mask, access_key_id));
    /* TODO(rzarzynski): replace with static_ptr. */
    return aplptr_t(new decltype(apl)(std::move(apl)));
  }

public:
  using engine_map_t = std::map <std::string, std::reference_wrapper<const Engine>>;
  void add_engines(const std::vector <std::string>& auth_order,
		   engine_map_t eng_map)
  {
    auto ctrl_flag = Control::SUFFICIENT;
    for (const auto &eng : auth_order) {
      // fallback to the last engine, in case of multiple engines, since ctrl
      // flag is sufficient for others, error from earlier engine is returned
      if (&eng == &auth_order.back() && eng_map.size() > 1) {
        ctrl_flag = Control::FALLBACK;
      }
      if (const auto kv = eng_map.find(eng);
          kv != eng_map.end()) {
        add_engine(ctrl_flag, kv->second);
      }
    }
  }

  auto parse_auth_order(CephContext* const cct)
  {
    std::vector <std::string> result;

    const std::set <std::string_view> allowed_auth = { "sts", "external", "local" };
    std::vector <std::string> default_order = { "sts", "external", "local" };
    // supplied strings may contain a space, so let's bypass that
    boost::split(result, cct->_conf->rgw_s3_auth_order,
		 boost::is_any_of(", "), boost::token_compress_on);

    if (std::any_of(result.begin(), result.end(),
		    [allowed_auth](std::string_view s)
		    { return allowed_auth.find(s) == allowed_auth.end();})){
      return default_order;
    }
    return result;
  }

  AWSAuthStrategy(CephContext* const cct,
                  const rgw::auth::ImplicitTenants& implicit_tenant_context,
                  rgw::sal::Driver* driver)
    : driver(driver),
      ver_abstractor(cct),
      anonymous_engine(cct,
                       static_cast<rgw::auth::LocalApplier::Factory*>(this)),
      external_engines(cct, driver, implicit_tenant_context, &ver_abstractor),
      sts_engine(cct, driver, implicit_tenant_context, &ver_abstractor),
      local_engine(cct, driver, ver_abstractor,
                   static_cast<rgw::auth::LocalApplier::Factory*>(this)) {
    /* The anonymous auth. */
    if (AllowAnonAccessT) {
      add_engine(Control::SUFFICIENT, anonymous_engine);
    }

    auto auth_order = parse_auth_order(cct);
    engine_map_t engine_map;

    /* STS Auth*/
    if (! sts_engine.is_empty()) {
      engine_map.insert(std::make_pair("sts", std::cref(sts_engine)));
    }

    /* The external auth. */
    if (! external_engines.is_empty()) {
      engine_map.insert(std::make_pair("external", std::cref(external_engines)));
    }
    /* The local auth. */
    if (cct->_conf->rgw_s3_auth_use_rados) {
      engine_map.insert(std::make_pair("local", std::cref(local_engine)));
    }

    add_engines(auth_order, engine_map);
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::s3::AWSAuthStrategy";
  }
}; /* AWSAuthstrategy */

class AWSv4ComplMulti : public rgw::auth::Completer,
                        public rgw::io::DecoratedRestfulClient<rgw::io::RestfulClient*>,
                        public std::enable_shared_from_this<AWSv4ComplMulti> {

  using io_base_t = rgw::io::DecoratedRestfulClient<rgw::io::RestfulClient*>;
  using signing_key_t = sha256_digest_t;

  using trailer_map_t = boost::container::flat_map<std::string_view, std::string_view>;

  const req_state* const s;

  const std::string_view date;
  const std::string_view credential_scope;
  const uint32_t flags;
  const signing_key_t signing_key;

  class ChunkMeta {
    size_t data_offset_in_stream = 0;
    size_t data_length = 0;
    std::string signature;
    uint32_t flags{FLAG_NONE};
    uint32_t cnt;

    ChunkMeta(const size_t data_starts_in_stream,
              const size_t data_length,
              const std::string_view signature,
	      uint32_t _flags,
	      uint32_t _cnt)
      : data_offset_in_stream(data_starts_in_stream),
        data_length(data_length),
        signature(std::string(signature)),
	flags(_flags),
	cnt(_cnt)
    {}

    explicit ChunkMeta(const std::string_view& signature, uint32_t _flags,
		       uint32_t _cnt)
      : signature(std::string(signature)), flags(_flags), cnt(_cnt)
    {}

  public:
    static constexpr size_t SIG_SIZE = 64;

    /* Let's suppose the data length fields can't exceed uint64_t. */
    static constexpr size_t META_MAX_SIZE = \
      sarrlen("\r\nffffffffffffffff;chunk-signature=") + SIG_SIZE + sarrlen("\r\n");

    /* The metadata size of for the last, empty chunk. */
    static constexpr size_t META_MIN_SIZE = \
      sarrlen("0;chunk-signature=") + SIG_SIZE + sarrlen("\r\n");

    /* Detect whether a given stream_pos fits in boundaries of a chunk. */
    bool is_new_chunk_in_stream(size_t stream_pos) const;

    /* Get the remaining data size. */
    size_t get_data_size(size_t stream_pos) const;

    const std::string& get_signature() const {
      return signature;
    }

    size_t get_offset() { return data_offset_in_stream; }

    /* Factory: create an object representing metadata of first, initial chunk
     * in a stream. */
    static ChunkMeta create_first(const std::string_view& seed_signature,
				  uint32_t flags,
				  uint32_t cnt) {
      return ChunkMeta(seed_signature, flags, cnt);
    }

    /* Factory: parse a block of META_MAX_SIZE bytes and creates an object
     * representing non-first chunk in a stream. As the process is sequential
     * and depends on the previous chunk, caller must pass it. */
    static std::pair<ChunkMeta, size_t> create_next(CephContext* cct,
                                                    ChunkMeta&& prev,
                                                    const char* metabuf,
                                                    size_t metabuf_len,
						    uint32_t flags);
  } chunk_meta;

  uint16_t lf_bytes;
  size_t stream_pos;
  boost::container::static_vector<char, ChunkMeta::META_MAX_SIZE> parsing_buf;
  boost::optional<std::string_view> x_amz_trailer;
  ceph::crypto::SHA256* sha256_hash;
  std::string prev_chunk_signature;

  bool is_signature_mismatched();
  std::string calc_chunk_signature(const std::string& payload_hash) const;

  struct ReceiveChunkResult {
    size_t received;
    size_t data_offset_in_stream;

    ReceiveChunkResult(size_t x, size_t y)
      : received(x), data_offset_in_stream(y)
    {}
  }; /* ReceiveChunkResult */

  inline CephContext* cct() const {
    return s->cct;
  }

  inline bool expect_trailer_signature() const {
    return flags & AWSv4ComplMulti::FLAG_TRAILER_SIGNATURE;
  }

  inline void put_prop(const std::string_view k, const std::string_view v) {
    /* assume the caller will mangle the key name, if required */
    auto& map = const_cast<env_map_t&>(s->info.env->get_map());
    map.insert(env_map_t::value_type(k, v));
  }

  inline void extract_trailing_headers(std::string_view x_amz_trailer,
				       std::string_view& mut_sv_trailer,
				       trailer_map_t& trailer_map);

  std::string calc_v4_trailer_signature(const trailer_map_t& trailer_map,
					const std::string_view last_chunk_sig);

  ReceiveChunkResult recv_chunk(char* buf, size_t max, uint32_t rc_cnt, bool& eof);

  public:

  static constexpr uint32_t FLAG_NONE =              0x00;
  static constexpr uint32_t FLAG_TRAILING_CHECKSUM = 0x01;
  static constexpr uint32_t FLAG_UNSIGNED_PAYLOAD =  0x02;
  static constexpr uint32_t FLAG_UNSIGNED_CHUNKED =  0x04;
  static constexpr uint32_t FLAG_TRAILER_SIGNATURE = 0x08;

  /* We need the constructor to be public because of the std::make_shared that
   * is employed by the create() method. */
  AWSv4ComplMulti(const req_state* const s,
                  std::string_view date,
                  std::string_view credential_scope,
                  std::string_view seed_signature,
		  uint32_t _flags,
                  const signing_key_t& signing_key)
    : io_base_t(nullptr),
      s(s),
      date(std::move(date)),
      credential_scope(std::move(credential_scope)),
      flags(_flags),
      signing_key(signing_key),

      /* The evolving state. */
      chunk_meta(ChunkMeta::create_first(
		   seed_signature, flags, 0 /* first call in cycle */)),
      lf_bytes(0),
      stream_pos(0),
      sha256_hash(calc_hash_sha256_open_stream()),
      prev_chunk_signature(std::move(seed_signature))
  {
    auto cksum = s->info.env->get("HTTP_X_AMZ_TRAILER");
    if (!! cksum) {
      x_amz_trailer = std::string_view(cksum, std::strlen(cksum));
    }
  } /* AWSv4ComplMulti */

  ~AWSv4ComplMulti() {
    if (sha256_hash) {
      calc_hash_sha256_close_stream(&sha256_hash);
    }
  }

  /* rgw::io::DecoratedRestfulClient. */
  size_t recv_body(char* buf, size_t max) override;

  /* rgw::auth::Completer. */
  void modify_request_state(const DoutPrefixProvider* dpp, req_state* s_rw) override;
  bool complete() override;

  /* Factories. */
  static cmplptr_t create(const req_state* s,
                          std::string_view date,
                          std::string_view credential_scope,
                          std::string_view seed_signature,
			  uint32_t flags,
                          const boost::optional<std::string>& secret_key);

};

class AWSv4ComplSingle : public rgw::auth::Completer,
                         public rgw::io::DecoratedRestfulClient<rgw::io::RestfulClient*>,
                         public std::enable_shared_from_this<AWSv4ComplSingle> {
  using io_base_t = rgw::io::DecoratedRestfulClient<rgw::io::RestfulClient*>;

  CephContext* const cct;
  const char* const expected_request_payload_hash;
  ceph::crypto::SHA256* sha256_hash = nullptr;

public:
  /* Defined in rgw_auth_s3.cc because of get_v4_exp_payload_hash(). We need
   * the constructor to be public because of the std::make_shared employed by
   * the create() method. */
  explicit AWSv4ComplSingle(const req_state* const s);

  ~AWSv4ComplSingle() {
    if (sha256_hash) {
      calc_hash_sha256_close_stream(&sha256_hash);
    }
  }

  /* rgw::io::DecoratedRestfulClient. */
  size_t recv_body(char* buf, size_t max) override;

  /* rgw::auth::Completer. */
  void modify_request_state(const DoutPrefixProvider* dpp, req_state* s_rw) override;
  bool complete() override;

  /* Factories. */
  static cmplptr_t create(const req_state* s,
                          const boost::optional<std::string>&);

};

} /* namespace s3 */
} /* namespace auth */
} /* namespace rgw */

void rgw_create_s3_canonical_header(
  const DoutPrefixProvider *dpp,
  const char *method,
  const char *content_md5,
  const char *content_type,
  const char *date,
  const meta_map_t& meta_map,
  const meta_map_t& qs_map,
  const char *request_uri,
  const std::map<std::string, std::string>& sub_resources,
  std::string& dest_str);
bool rgw_create_s3_canonical_header(const DoutPrefixProvider *dpp,
                                    const req_info& info,
                                    utime_t *header_time,       /* out */
                                    std::string& dest,          /* out */
                                    bool qsr);
static inline std::tuple<bool, std::string, utime_t>
rgw_create_s3_canonical_header(const DoutPrefixProvider *dpp, const req_info& info, const bool qsr) {
  std::string dest;
  utime_t header_time;

  const bool ok = rgw_create_s3_canonical_header(dpp, info, &header_time, dest, qsr);
  return std::make_tuple(ok, dest, header_time);
}

namespace rgw {
namespace auth {
namespace s3 {

static constexpr char AWS4_HMAC_SHA256_STR[] = "AWS4-HMAC-SHA256";
static constexpr char AWS4_HMAC_SHA256_PAYLOAD_STR[] = "AWS4-HMAC-SHA256-PAYLOAD";

static constexpr char AWS4_EMPTY_PAYLOAD_HASH[] = \
  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

static constexpr char AWS4_UNSIGNED_PAYLOAD_HASH[] = "UNSIGNED-PAYLOAD";

static constexpr char AWS4_STREAMING_PAYLOAD_HASH[] = \
  "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

/* trailing header forms */
static constexpr char AWS4_STREAMING_UNSIGNED_PAYLOAD_TRAILER[] = \
  "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

static constexpr char AWS4_STREAMING_HMAC_SHA256_PAYLOAD_TRAILER[] = \
  "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";

bool is_non_s3_op(RGWOpType op_type);

int parse_v4_credentials(const req_info& info,                     /* in */
			 std::string_view& access_key_id,        /* out */
			 std::string_view& credential_scope,     /* out */
			 std::string_view& signedheaders,        /* out */
			 std::string_view& signature,            /* out */
			 std::string_view& date,                 /* out */
			 std::string_view& session_token,        /* out */
			 const bool using_qs,                    /* in  */
                         const DoutPrefixProvider *dpp);         /* in */

string gen_v4_scope(const ceph::real_time& timestamp,
                    const string& region,
                    const string& service);

static inline bool char_needs_aws4_escaping(const char c, bool encode_slash)
{
  if ((c >= 'a' && c <= 'z') ||
      (c >= 'A' && c <= 'Z') ||
      (c >= '0' && c <= '9')) {
    return false;
  }

  switch (c) {
    case '-':
    case '_':
    case '.':
    case '~':
      return false;
  }

  if (c == '/' && !encode_slash)
    return false;

  return true;
}

static inline std::string aws4_uri_encode(const std::string& src, bool encode_slash)
{
  std::string result;

  for (const std::string::value_type c : src) {
    if (char_needs_aws4_escaping(c, encode_slash)) {
      rgw_uri_escape_char(c, result);
    } else {
      result.push_back(c);
    }
  }

  return result;
}

static inline std::string aws4_uri_recode(const std::string_view& src, bool encode_slash)
{
  std::string decoded = url_decode(src);
  return aws4_uri_encode(decoded, encode_slash);
}

static inline std::string get_v4_canonical_uri(const req_info& info) {
  /* The code should normalize according to RFC 3986 but S3 does NOT do path
   * normalization that SigV4 typically does. This code follows the same
   * approach that boto library. See auth.py:canonical_uri(...). */

  std::string canonical_uri = aws4_uri_recode(info.request_uri_aws4, false);

  if (canonical_uri.empty()) {
    canonical_uri = "/";
  } else {
    boost::replace_all(canonical_uri, "+", "%20");
  }

  return canonical_uri;
}

static inline std::string gen_v4_canonical_uri(const req_info& info) {
  /* The code should normalize according to RFC 3986 but S3 does NOT do path
   * normalization that SigV4 typically does. This code follows the same
   * approach that boto library. See auth.py:canonical_uri(...). */

  std::string canonical_uri = aws4_uri_recode(info.request_uri, false);

  if (canonical_uri.empty()) {
    canonical_uri = "/";
  } else {
    boost::replace_all(canonical_uri, "+", "%20");
  }

  return canonical_uri;
}

static inline const string calc_v4_payload_hash(const string& payload)
{
  ceph::crypto::SHA256* sha256_hash = calc_hash_sha256_open_stream();
  calc_hash_sha256_update_stream(sha256_hash, payload.c_str(), payload.length());
  const auto payload_hash = calc_hash_sha256_close_stream(&sha256_hash);
  return payload_hash;
}

static inline const char* get_v4_exp_payload_hash(const req_info& info)
{
  /* In AWSv4 the hash of real, transferred payload IS NOT necessary to form
   * a Canonical Request, and thus verify a Signature. x-amz-content-sha256
   * header lets get the information very early -- before seeing first byte
   * of HTTP body. As a consequence, we can decouple Signature verification
   * from payload's fingerprint check. */
  const char *expected_request_payload_hash = \
    info.env->get("HTTP_X_AMZ_CONTENT_SHA256");

  if (!expected_request_payload_hash) {
    /* An HTTP client MUST send x-amz-content-sha256. The single exception
     * is the case of using the Query Parameters where "UNSIGNED-PAYLOAD"
     * literals are used for crafting Canonical Request:
     *
     *  You don't include a payload hash in the Canonical Request, because
     *  when you create a presigned URL, you don't know the payload content
     *  because the URL is used to upload an arbitrary payload. Instead, you
     *  use a constant string UNSIGNED-PAYLOAD. */
    expected_request_payload_hash = AWS4_UNSIGNED_PAYLOAD_HASH;
  }

  return expected_request_payload_hash;
}

static inline bool is_traditional_v4_unsigned_payload(const char* const exp_payload_hash)
{
  return boost::equals(exp_payload_hash, AWS4_UNSIGNED_PAYLOAD_HASH);
}

static inline bool is_v4_payload_unsigned_chunked(const char* const exp_payload_hash)
{
  return boost::equals(exp_payload_hash, AWS4_STREAMING_UNSIGNED_PAYLOAD_TRAILER);
}

static inline bool is_v4_payload_unsigned(const char* const exp_payload_hash)
{
  return boost::contains(exp_payload_hash, "UNSIGNED-PAYLOAD");
}

static inline bool have_checksum_trailer(const char* const exp_payload_hash)
{
  return boost::algorithm::ends_with(exp_payload_hash, "TRAILER");
}

static inline bool expect_trailer_signature(const char* const exp_payload_hash)
{
  return boost::equals(exp_payload_hash, AWS4_STREAMING_HMAC_SHA256_PAYLOAD_TRAILER);
}

static inline bool is_v4_payload_empty(const req_state* const s)
{
  /* from rfc2616 - 4.3 Message Body
   *
   * "The presence of a message-body in a request is signaled by the inclusion
   * of a Content-Length or Transfer-Encoding header field in the request's
   * message-headers." */
  return s->content_length == 0 &&
         s->info.env->get("HTTP_TRANSFER_ENCODING") == nullptr;
}

static inline bool is_v4_payload_streamed(const char* const exp_payload_hash)
{
  return boost::algorithm::starts_with(exp_payload_hash, "STREAMING-");
}

std::string get_v4_canonical_qs(const req_info& info, bool using_qs);

std::string gen_v4_canonical_qs(const req_info& info, bool is_non_s3_op);

std::string get_v4_canonical_method(const req_state* s);

boost::optional<std::string>
get_v4_canonical_headers(const req_info& info,
                         const std::string_view& signedheaders,
                         bool using_qs,
                         bool force_boto2_compat);

std::string gen_v4_canonical_headers(const req_info& info,
                                     const std::map<std::string, std::string>& extra_headers,
                                     string *signed_hdrs);

extern sha256_digest_t
get_v4_canon_req_hash(CephContext* cct,
                      const std::string_view& http_verb,
                      const std::string& canonical_uri,
                      const std::string& canonical_qs,
                      const std::string& canonical_hdrs,
                      const std::string_view& signed_hdrs,
                      const std::string_view& request_payload_hash,
                      const DoutPrefixProvider *dpp);

AWSEngine::VersionAbstractor::string_to_sign_t
get_v4_string_to_sign(CephContext* cct,
                      const std::string_view& algorithm,
                      const std::string_view& request_date,
                      const std::string_view& credential_scope,
                      const sha256_digest_t& canonreq_hash,
                      const DoutPrefixProvider *dpp);

extern AWSEngine::VersionAbstractor::server_signature_t
get_v4_signature(const std::string_view& credential_scope,
                 CephContext* const cct,
                 const std::string_view& secret_key,
                 const AWSEngine::VersionAbstractor::string_to_sign_t& string_to_sign,
                 const DoutPrefixProvider *dpp);

extern AWSEngine::VersionAbstractor::server_signature_t
get_v2_signature(CephContext*,
                 const std::string& secret_key,
                 const AWSEngine::VersionAbstractor::string_to_sign_t& string_to_sign);
} /* namespace s3 */
} /* namespace auth */
} /* namespace rgw */
