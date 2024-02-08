// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <array>
#include <string.h>
#include <string_view>

#include "common/ceph_crypto.h"
#include "common/split.h"
#include "common/Formatter.h"
#include "common/utf8.h"
#include "common/ceph_json.h"
#include "common/safe_io.h"
#include "common/errno.h"
#include "auth/Crypto.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/tokenizer.hpp>
#define BOOST_BIND_GLOBAL_PLACEHOLDERS
#ifdef HAVE_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wimplicit-const-int-float-conversion"
#endif
#ifdef HAVE_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION
#pragma clang diagnostic pop
#endif
#undef BOOST_BIND_GLOBAL_PLACEHOLDERS

#include <liboath/oath.h>

#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_s3website.h"
#include "rgw_rest_pubsub.h"
#include "rgw_auth_s3.h"
#include "rgw_acl.h"
#include "rgw_policy_s3.h"
#include "rgw_user.h"
#include "rgw_cors.h"
#include "rgw_cors_s3.h"
#include "rgw_tag_s3.h"

#include "rgw_client_io.h"

#include "rgw_keystone.h"
#include "rgw_auth_keystone.h"
#include "rgw_auth_registry.h"

#include "rgw_es_query.h"

#include <typeinfo> // for 'typeid'

#include "rgw_ldap.h"
#include "rgw_token.h"
#include "rgw_rest_role.h"
#include "rgw_crypt.h"
#include "rgw_crypt_sanitize.h"
#include "rgw_rest_user_policy.h"
#include "rgw_zone.h"
#include "rgw_bucket_sync.h"

#include "include/ceph_assert.h"
#include "rgw_role.h"
#include "rgw_rest_sts.h"
#include "rgw_rest_iam.h"
#include "rgw_sts.h"
#include "rgw_sal_rados.h"

#include "rgw_s3select.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace rgw;
using namespace ceph::crypto;

void list_all_buckets_start(req_state *s)
{
  s->formatter->open_array_section_in_ns("ListAllMyBucketsResult", XMLNS_AWS_S3);
}

void list_all_buckets_end(req_state *s)
{
  s->formatter->close_section();
}

void dump_bucket(req_state *s, const RGWBucketEnt& ent)
{
  s->formatter->open_object_section("Bucket");
  s->formatter->dump_string("Name", ent.bucket.name);
  dump_time(s, "CreationDate", ent.creation_time);
  s->formatter->close_section();
}

void rgw_get_errno_s3(rgw_http_error *e , int err_no)
{
  rgw_http_errors::const_iterator r = rgw_http_s3_errors.find(err_no);

  if (r != rgw_http_s3_errors.end()) {
    e->http_ret = r->second.first;
    e->s3_code = r->second.second;
  } else {
    e->http_ret = 500;
    e->s3_code = "UnknownError";
  }
}

static inline std::string get_s3_expiration_header(
  req_state* s,
  const ceph::real_time& mtime)
{
  return rgw::lc::s3_expiration_header(
    s, s->object->get_key(), s->tagset, mtime, s->bucket_attrs);
}

static inline bool get_s3_multipart_abort_header(
  req_state* s, const ceph::real_time& mtime,
  ceph::real_time& date, std::string& rule_id)
{
  return rgw::lc::s3_multipart_abort_header(
          s, s->object->get_key(), mtime, s->bucket_attrs, date, rule_id);
}

struct response_attr_param {
  const char *param;
  const char *http_attr;
};

static struct response_attr_param resp_attr_params[] = {
  {"response-content-type", "Content-Type"},
  {"response-content-language", "Content-Language"},
  {"response-expires", "Expires"},
  {"response-cache-control", "Cache-Control"},
  {"response-content-disposition", "Content-Disposition"},
  {"response-content-encoding", "Content-Encoding"},
  {NULL, NULL},
};

#define SSE_C_GROUP 1
#define KMS_GROUP 2

int get_encryption_defaults(req_state *s)
{
  int meta_sse_group = 0;
  constexpr auto sse_c_prefix = "x-amz-server-side-encryption-customer-";
  constexpr auto encrypt_attr = "x-amz-server-side-encryption";
  constexpr auto context_attr = "x-amz-server-side-encryption-context";
  constexpr auto kms_attr = "x-amz-server-side-encryption-aws-kms-key-id";
  constexpr auto bucket_key_attr = "x-amz-server-side-encryption-bucket-key-enabled";
  bool bucket_configuration_found { false };
  bool rest_only { false };

  for (auto& kv : s->info.crypt_attribute_map) {
    if (kv.first.find(sse_c_prefix) == 0)
      meta_sse_group |= SSE_C_GROUP;
    else if (kv.first.find(encrypt_attr) == 0)
      meta_sse_group |= KMS_GROUP;
  }
  if (meta_sse_group == (SSE_C_GROUP|KMS_GROUP)) {
    s->err.message = "Server side error - can't do sse-c & sse-kms|sse-s3";
    return -EINVAL;
  }

  const auto& buck_attrs = s->bucket_attrs;
  auto aiter = buck_attrs.find(RGW_ATTR_BUCKET_ENCRYPTION_POLICY);
  RGWBucketEncryptionConfig bucket_encryption_conf;
  if (aiter != buck_attrs.end()) {
    ldpp_dout(s, 5) << "Found RGW_ATTR_BUCKET_ENCRYPTION_POLICY on "
	    << s->bucket_name << dendl;

    bufferlist::const_iterator iter{&aiter->second};

    try {
      bucket_encryption_conf.decode(iter);
      bucket_configuration_found = true;
    } catch (const buffer::error& e) {
      s->err.message = "Server side error - can't decode bucket_encryption_conf";
      ldpp_dout(s, 5) << __func__ <<  "decode bucket_encryption_conf failed" << dendl;
      return -EINVAL;
    }
  }
  if (meta_sse_group & SSE_C_GROUP) {
    ldpp_dout(s, 20) << "get_encryption_defaults: no defaults cause sse-c forced"
	<< dendl;
    return 0;			// sse-c: no defaults here
  }
  std::string sse_algorithm { bucket_encryption_conf.sse_algorithm() };
  auto kms_master_key_id { bucket_encryption_conf.kms_master_key_id() };
  bool bucket_key_enabled { bucket_encryption_conf.bucket_key_enabled() };
  bool kms_attr_seen = false;
  if (bucket_configuration_found) {
    ldpp_dout(s, 5) << "RGW_ATTR_BUCKET_ENCRYPTION ALGO: "
	  <<  sse_algorithm << dendl;
  }

  auto iter = s->info.crypt_attribute_map.find(encrypt_attr);
  if (iter != s->info.crypt_attribute_map.end()) {
ldpp_dout(s, 20) << "get_encryption_defaults: found encrypt_attr " << encrypt_attr << " = " << iter->second << ", setting sse_algorithm to that" << dendl;
    rest_only = true;
    sse_algorithm = iter->second;
  } else if (sse_algorithm != "") {
    rgw_set_amz_meta_header(s->info.crypt_attribute_map, encrypt_attr, sse_algorithm, OVERWRITE);
  }

  iter = s->info.crypt_attribute_map.find(kms_attr);
  if (iter != s->info.crypt_attribute_map.end()) {
ldpp_dout(s, 20) << "get_encryption_defaults: found kms_attr " << kms_attr << " = " << iter->second << ", setting kms_attr_seen" << dendl;
    if (!rest_only) {
      s->err.message = std::string("incomplete rest sse parms: ") + kms_attr + " not valid without kms";
      ldpp_dout(s, 5) << __func__ << "argument problem: " << s->err.message << dendl;
      return -EINVAL;
    }
    kms_attr_seen = true;
  } else if (!rest_only && kms_master_key_id != "") {
ldpp_dout(s, 20) << "get_encryption_defaults: no kms_attr, but kms_master_key_id = " << kms_master_key_id << ", setting kms_attr_seen" << dendl;
    kms_attr_seen = true;
    rgw_set_amz_meta_header(s->info.crypt_attribute_map, kms_attr, kms_master_key_id, OVERWRITE);
  }

  iter = s->info.crypt_attribute_map.find(bucket_key_attr);
  if (iter != s->info.crypt_attribute_map.end()) {
ldpp_dout(s, 20) << "get_encryption_defaults: found bucket_key_attr " << bucket_key_attr << " = " << iter->second << ", setting kms_attr_seen" << dendl;
    if (!rest_only) {
      s->err.message = std::string("incomplete rest sse parms: ") + bucket_key_attr + " not valid without kms";
      ldpp_dout(s, 5) << __func__ << "argument problem: " << s->err.message << dendl;
      return -EINVAL;
    }
    kms_attr_seen = true;
  } else if (!rest_only && bucket_key_enabled) {
ldpp_dout(s, 20) << "get_encryption_defaults: no bucket_key_attr, but bucket_key_enabled,  setting kms_attr_seen" << dendl;
    kms_attr_seen = true;
    rgw_set_amz_meta_header(s->info.crypt_attribute_map, bucket_key_attr, "true", OVERWRITE);
  }

  iter = s->info.crypt_attribute_map.find(context_attr);
  if (iter != s->info.crypt_attribute_map.end()) {
ldpp_dout(s, 20) << "get_encryption_defaults: found context_attr " << context_attr << " = " << iter->second << ", setting kms_attr_seen" << dendl;
    if (!rest_only) {
      s->err.message = std::string("incomplete rest sse parms: ") + context_attr + " not valid without kms";
      ldpp_dout(s, 5) << __func__ << "argument problem: " << s->err.message << dendl;
      return -EINVAL;
    }
    kms_attr_seen = true;
  }

  if (kms_attr_seen && sse_algorithm == "") {
ldpp_dout(s, 20) << "get_encryption_defaults: kms_attr but no algorithm, defaulting to aws_kms" << dendl;
    sse_algorithm = "aws:kms";
  }
for (const auto& kv: s->info.crypt_attribute_map) {
ldpp_dout(s, 20) << "get_encryption_defaults:  final map: " << kv.first << " = " << kv.second << dendl;
}
ldpp_dout(s, 20) << "get_encryption_defaults:  kms_attr_seen is " << kms_attr_seen << " and sse_algorithm is " << sse_algorithm << dendl;
  if (kms_attr_seen && sse_algorithm != "aws:kms") {
    s->err.message = "algorithm <" + sse_algorithm + "> but got sse-kms attributes";
    return -EINVAL;
  }

  return 0;
}

int RGWGetObj_ObjStore_S3Website::send_response_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {
  map<string, bufferlist>::iterator iter;
  iter = attrs.find(RGW_ATTR_AMZ_WEBSITE_REDIRECT_LOCATION);
  if (iter != attrs.end()) {
    bufferlist &bl = iter->second;
    s->redirect = bl.c_str();
    s->err.http_ret = 301;
    ldpp_dout(this, 20) << __CEPH_ASSERT_FUNCTION << " redirecting per x-amz-website-redirect-location=" << s->redirect << dendl;
    op_ret = -ERR_WEBSITE_REDIRECT;
    set_req_state_err(s, op_ret);
    dump_errno(s);
    dump_content_length(s, 0);
    dump_redirect(s, s->redirect);
    end_header(s, this);
    return op_ret;
  } else {
    return RGWGetObj_ObjStore_S3::send_response_data(bl, bl_ofs, bl_len);
  }
}

int RGWGetObj_ObjStore_S3Website::send_response_data_error(optional_yield y)
{
  return RGWGetObj_ObjStore_S3::send_response_data_error(y);
}

int RGWGetObj_ObjStore_S3::get_params(optional_yield y)
{
  // for multisite sync requests, only read the slo manifest itself, rather than
  // all of the data from its parts. the parts will sync as separate objects
  skip_manifest = s->info.args.exists(RGW_SYS_PARAM_PREFIX "sync-manifest");

  // multisite sync requests should fetch encrypted data, along with the
  // attributes needed to support decryption on the other zone
  if (s->system_request) {
    skip_decrypt = s->info.args.exists(RGW_SYS_PARAM_PREFIX "skip-decrypt");
  }

  // multisite sync requests should fetch cloudtiered objects
  sync_cloudtiered = s->info.args.exists(RGW_SYS_PARAM_PREFIX "sync-cloudtiered");

  dst_zone_trace = s->info.args.get(RGW_SYS_PARAM_PREFIX "if-not-replicated-to");
  get_torrent = s->info.args.exists("torrent");

  // optional part number
  auto optstr = s->info.args.get_optional("partNumber");
  if (optstr) {
    string err;
    multipart_part_num = strict_strtol(optstr->c_str(), 10, &err);
    if (!err.empty()) {
      s->err.message = "Invalid partNumber: " + err;
      ldpp_dout(s, 10) << "bad part number " << *optstr << ": " << err << dendl;
      return -ERR_INVALID_PART;
    }
  }

  return RGWGetObj_ObjStore::get_params(y);
}

int RGWGetObj_ObjStore_S3::send_response_data_error(optional_yield y)
{
  bufferlist bl;
  return send_response_data(bl, 0 , 0);
}

template <class T>
int decode_attr_bl_single_value(map<string, bufferlist>& attrs, const char *attr_name, T *result, T def_val)
{
  map<string, bufferlist>::iterator iter = attrs.find(attr_name);
  if (iter == attrs.end()) {
    *result = def_val;
    return 0;
  }
  bufferlist& bl = iter->second;
  if (bl.length() == 0) {
    *result = def_val;
    return 0;
  }
  auto bliter = bl.cbegin();
  try {
    decode(*result, bliter);
  } catch (buffer::error& err) {
    return -EIO;
  }
  return 0;
}

inline bool str_has_cntrl(const std::string s) {
  return std::any_of(s.begin(), s.end(), ::iscntrl);
}

inline bool str_has_cntrl(const char* s) {
  std::string _s(s);
  return str_has_cntrl(_s);
}

int RGWGetObj_ObjStore_S3::send_response_data(bufferlist& bl, off_t bl_ofs,
					      off_t bl_len)
{
  const char *content_type = NULL;
  string content_type_str;
  map<string, string> response_attrs;
  map<string, string>::iterator riter;
  bufferlist metadata_bl;

  string expires = get_s3_expiration_header(s, lastmod);

  if (sent_header)
    goto send_data;

  if (custom_http_ret) {
    set_req_state_err(s, 0);
    dump_errno(s, custom_http_ret);
  } else {
    set_req_state_err(s, (partial_content && !op_ret) ? STATUS_PARTIAL_CONTENT
                  : op_ret);
    dump_errno(s);
  }

  if (op_ret)
    goto done;

  if (range_str)
    dump_range(s, start, end, s->obj_size);

  if (s->system_request &&
      s->info.args.exists(RGW_SYS_PARAM_PREFIX "prepend-metadata")) {

    dump_header(s, "Rgwx-Object-Size", (long long)total_len);

    if (rgwx_stat) {
      /*
       * in this case, we're not returning the object's content, only the prepended
       * extra metadata
       */
      total_len = 0;
    }

    /* JSON encode object metadata */
    JSONFormatter jf;
    jf.open_object_section("obj_metadata");
    encode_json("attrs", attrs, &jf);
    utime_t ut(lastmod);
    encode_json("mtime", ut, &jf);
    jf.close_section();
    stringstream ss;
    jf.flush(ss);
    metadata_bl.append(ss.str());
    dump_header(s, "Rgwx-Embedded-Metadata-Len", metadata_bl.length());
    total_len += metadata_bl.length();
  }

  if (s->system_request && !real_clock::is_zero(lastmod)) {
    /* we end up dumping mtime in two different methods, a bit redundant */
    dump_epoch_header(s, "Rgwx-Mtime", lastmod);
    uint64_t pg_ver = 0;
    int r = decode_attr_bl_single_value(attrs, RGW_ATTR_PG_VER, &pg_ver, (uint64_t)0);
    if (r < 0) {
      ldpp_dout(this, 0) << "ERROR: failed to decode pg ver attr, ignoring" << dendl;
    }
    dump_header(s, "Rgwx-Obj-PG-Ver", pg_ver);

    uint32_t source_zone_short_id = 0;
    r = decode_attr_bl_single_value(attrs, RGW_ATTR_SOURCE_ZONE, &source_zone_short_id, (uint32_t)0);
    if (r < 0) {
      ldpp_dout(this, 0) << "ERROR: failed to decode pg ver attr, ignoring" << dendl;
    }
    if (source_zone_short_id != 0) {
      dump_header(s, "Rgwx-Source-Zone-Short-Id", source_zone_short_id);
    }
  }

  for (auto &it : crypt_http_responses)
    dump_header(s, it.first, it.second);

  dump_content_length(s, total_len);
  dump_last_modified(s, lastmod);
  dump_header_if_nonempty(s, "x-amz-version-id", version_id);
  dump_header_if_nonempty(s, "x-amz-expiration", expires);

  if (attrs.find(RGW_ATTR_APPEND_PART_NUM) != attrs.end()) {
    dump_header(s, "x-rgw-object-type", "Appendable");
    dump_header(s, "x-rgw-next-append-position", s->obj_size);
  } else {
    dump_header(s, "x-rgw-object-type", "Normal");
  }
  // replication status
  if (auto i = attrs.find(RGW_ATTR_OBJ_REPLICATION_STATUS);
      i != attrs.end()) {
    dump_header(s, "x-amz-replication-status", i->second);
  }
  if (auto i = attrs.find(RGW_ATTR_OBJ_REPLICATION_TRACE);
      i != attrs.end()) {
    try {
      std::vector<rgw_zone_set_entry> zones;
      auto p = i->second.cbegin();
      decode(zones, p);
      for (const auto& zone : zones) {
        dump_header(s, "x-rgw-replicated-from", zone.to_str());
      }
    } catch (const buffer::error&) {} // omit x-rgw-replicated-from headers
  }
  if (multipart_parts_count) {
    dump_header(s, "x-amz-mp-parts-count", *multipart_parts_count);
  }

  if (! op_ret) {
    if (! lo_etag.empty()) {
      /* Handle etag of Swift API's large objects (DLO/SLO). It's entirely
       * legit to perform GET on them through S3 API. In such situation,
       * a client should receive the composited content with corresponding
       * etag value. */
      dump_etag(s, lo_etag);
    } else {
      auto iter = attrs.find(RGW_ATTR_ETAG);
      if (iter != attrs.end()) {
        dump_etag(s, iter->second.to_str());
      }
    }

    for (struct response_attr_param *p = resp_attr_params; p->param; p++) {
      bool exists;
      string val = s->info.args.get(p->param, &exists);
      if (exists) {
	/* reject unauthenticated response header manipulation, see
	 * https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html */
	if (s->auth.identity->is_anonymous()) {
	  return -ERR_INVALID_REQUEST;
	}
        /* HTTP specification says no control characters should be present in
         * header values: https://tools.ietf.org/html/rfc7230#section-3.2
         *      field-vchar    = VCHAR / obs-text
         *
         * Failure to validate this permits a CRLF injection in HTTP headers,
         * whereas S3 GetObject only permits specific headers.
         */
        if(str_has_cntrl(val)) {
          /* TODO: return a more distinct error in future;
           * stating what the problem is */
          return -ERR_INVALID_REQUEST;
        }

	if (strcmp(p->param, "response-content-type") != 0) {
	  response_attrs[p->http_attr] = val;
	} else {
	  content_type_str = val;
	  content_type = content_type_str.c_str();
	}
      }
    }

    for (auto iter = attrs.begin(); iter != attrs.end(); ++iter) {
      const char *name = iter->first.c_str();
      map<string, string>::iterator aiter = rgw_to_http_attrs.find(name);
      if (aiter != rgw_to_http_attrs.end()) {
        if (response_attrs.count(aiter->second) == 0) {
          /* Was not already overridden by a response param. */

          size_t len = iter->second.length();
          string s(iter->second.c_str(), len);
          while (len && !s[len - 1]) {
            --len;
            s.resize(len);
          }
          response_attrs[aiter->second] = s;
        }
      } else if (iter->first.compare(RGW_ATTR_CONTENT_TYPE) == 0) {
        /* Special handling for content_type. */
        if (!content_type) {
          content_type_str = rgw_bl_str(iter->second);
          content_type = content_type_str.c_str();
        }
      } else if (strcmp(name, RGW_ATTR_SLO_UINDICATOR) == 0) {
        // this attr has an extra length prefix from encode() in prior versions
        dump_header(s, "X-Object-Meta-Static-Large-Object", "True");
      } else if (strncmp(name, RGW_ATTR_META_PREFIX,
			 sizeof(RGW_ATTR_META_PREFIX)-1) == 0) {
        /* User custom metadata. */
        name += sizeof(RGW_ATTR_PREFIX) - 1;
        dump_header(s, name, iter->second);
      } else if (iter->first.compare(RGW_ATTR_TAGS) == 0) {
        RGWObjTags obj_tags;
        try{
          auto it = iter->second.cbegin();
          obj_tags.decode(it);
        } catch (buffer::error &err) {
          ldpp_dout(this,0) << "Error caught buffer::error couldn't decode TagSet " << dendl;
        }
        dump_header(s, RGW_AMZ_TAG_COUNT, obj_tags.count());
      } else if (iter->first.compare(RGW_ATTR_OBJECT_RETENTION) == 0 && get_retention){
        RGWObjectRetention retention;
        try {
          decode(retention, iter->second);
          dump_header(s, "x-amz-object-lock-mode", retention.get_mode());
          string date = ceph::to_iso_8601(retention.get_retain_until_date());
          dump_header(s, "x-amz-object-lock-retain-until-date", date.c_str());
        } catch (buffer::error& err) {
          ldpp_dout(this, 0) << "ERROR: failed to decode RGWObjectRetention" << dendl;
        }
      } else if (iter->first.compare(RGW_ATTR_OBJECT_LEGAL_HOLD) == 0 && get_legal_hold) {
        RGWObjectLegalHold legal_hold;
        try {
          decode(legal_hold, iter->second);
          dump_header(s, "x-amz-object-lock-legal-hold",legal_hold.get_status());
        } catch (buffer::error& err) {
          ldpp_dout(this, 0) << "ERROR: failed to decode RGWObjectLegalHold" << dendl;
        }
      }
    }
  }

done:
  for (riter = response_attrs.begin(); riter != response_attrs.end();
       ++riter) {
    dump_header(s, riter->first, riter->second);
  }

  if (op_ret == -ERR_NOT_MODIFIED) {
      dump_last_modified(s, lastmod);

      auto iter = attrs.find(RGW_ATTR_ETAG);
      if (iter != attrs.end())
        dump_etag(s, iter->second.to_str());

      iter = attrs.find(RGW_ATTR_CACHE_CONTROL);
      if (iter != attrs.end())
        dump_header(s, rgw_to_http_attrs[RGW_ATTR_CACHE_CONTROL], iter->second);

      iter = attrs.find(RGW_ATTR_EXPIRES);
      if (iter != attrs.end())
        dump_header(s, rgw_to_http_attrs[RGW_ATTR_EXPIRES], iter->second);

      end_header(s, this);
  } else {
      if (!content_type)
          content_type = "binary/octet-stream";

      end_header(s, this, content_type);
  }

  if (metadata_bl.length()) {
    dump_body(s, metadata_bl);
  }
  sent_header = true;

send_data:
  if (get_data && !op_ret) {
    int r = dump_body(s, bl.c_str() + bl_ofs, bl_len);
    if (r < 0)
      return r;
  }

  return 0;
}

int RGWGetObj_ObjStore_S3::get_decrypt_filter(std::unique_ptr<RGWGetObj_Filter> *filter, RGWGetObj_Filter* cb, bufferlist* manifest_bl)
{
  if (skip_decrypt) { // bypass decryption for multisite sync requests
    return 0;
  }

  std::unique_ptr<BlockCrypt> block_crypt;
  int res = rgw_s3_prepare_decrypt(s, s->yield, attrs, &block_crypt,
                                   crypt_http_responses);
  if (res < 0) {
    return res;
  }
  if (block_crypt == nullptr) {
    return 0;
  }

  // in case of a multipart upload, we need to know the part lengths to
  // correctly decrypt across part boundaries
  std::vector<size_t> parts_len;

  // for replicated objects, the original part lengths are preserved in an xattr
  if (auto i = attrs.find(RGW_ATTR_CRYPT_PARTS); i != attrs.end()) {
    try {
      auto p = i->second.cbegin();
      using ceph::decode;
      decode(parts_len, p);
    } catch (const buffer::error&) {
      ldpp_dout(this, 1) << "failed to decode RGW_ATTR_CRYPT_PARTS" << dendl;
      return -EIO;
    }
  } else if (manifest_bl) {
    // otherwise, we read the part lengths from the manifest
    res = RGWGetObj_BlockDecrypt::read_manifest_parts(this, *manifest_bl,
                                                      parts_len);
    if (res < 0) {
      return res;
    }
  }

  *filter = std::make_unique<RGWGetObj_BlockDecrypt>(
      s, s->cct, cb, std::move(block_crypt),
      std::move(parts_len), s->yield);
  return 0;
}
int RGWGetObj_ObjStore_S3::verify_requester(const rgw::auth::StrategyRegistry& auth_registry, optional_yield y) 
{
  int ret = -EINVAL;
  ret = RGWOp::verify_requester(auth_registry, y);
  if(!s->user->get_caps().check_cap("amz-cache", RGW_CAP_READ) && !ret && s->info.env->exists("HTTP_X_AMZ_CACHE"))
    ret = override_range_hdr(auth_registry, y);
  return ret;
}

int RGWGetObj_ObjStore_S3::override_range_hdr(const rgw::auth::StrategyRegistry& auth_registry, optional_yield y)
{
  int ret = -EINVAL;
  ldpp_dout(this, 10) << "cache override headers" << dendl;
  RGWEnv* rgw_env = const_cast<RGWEnv *>(s->info.env);
  const char* backup_range = rgw_env->get("HTTP_RANGE");
  const char hdrs_split[2] = {(char)178,'\0'};
  const char kv_split[2] = {(char)177,'\0'};
  const char* cache_hdr = rgw_env->get("HTTP_X_AMZ_CACHE");
  for (std::string_view hdr : ceph::split(cache_hdr, hdrs_split)) {
    auto kv = ceph::split(hdr, kv_split);
    auto k = kv.begin();
    if (std::distance(k, kv.end()) != 2) {
      return -EINVAL;
    }
    auto v = std::next(k);
    std::string key = "HTTP_";
    key.append(*k);
    boost::replace_all(key, "-", "_");
    ldpp_dout(this, 10) << "after splitting cache kv key: " << key  << " " << *v << dendl;
    rgw_env->set(std::move(key), std::string(*v));
  }
  ret = RGWOp::verify_requester(auth_registry, y);
  if(!ret && backup_range) {
    rgw_env->set("HTTP_RANGE",backup_range);
  } else {
    rgw_env->remove("HTTP_RANGE");
  }
  return ret;
}


void RGWGetObjTags_ObjStore_S3::send_response_data(bufferlist& bl)
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

  if (!op_ret){
    s->formatter->open_object_section_in_ns("Tagging", XMLNS_AWS_S3);
    s->formatter->open_object_section("TagSet");
    if (has_tags){
      RGWObjTagSet_S3 tagset;
      auto iter = bl.cbegin();
      try {
        tagset.decode(iter);
      } catch (buffer::error& err) {
        ldpp_dout(this,0) << "ERROR: caught buffer::error, couldn't decode TagSet" << dendl;
        op_ret= -EIO;
        return;
      }
      tagset.dump_xml(s->formatter);
    }
    s->formatter->close_section();
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}


int RGWPutObjTags_ObjStore_S3::get_params(optional_yield y)
{
  RGWXMLParser parser;

  if (!parser.init()){
    return -EINVAL;
  }

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;

  int r = 0;
  bufferlist data;
  std::tie(r, data) = read_all_input(s, max_size, false);

  if (r < 0)
    return r;

  if (!parser.parse(data.c_str(), data.length(), 1)) {
    return -ERR_MALFORMED_XML;
  }

  RGWObjTagging_S3 tagging;

  try {
    RGWXMLDecoder::decode_xml("Tagging", tagging, &parser);
  } catch (RGWXMLDecoder::err& err) {
    ldpp_dout(this, 5) << "Malformed tagging request: " << err << dendl;
    return -ERR_MALFORMED_XML;
  }

  RGWObjTags obj_tags;
  r = tagging.rebuild(obj_tags);
  if (r < 0)
    return r;

  obj_tags.encode(tags_bl);
  ldpp_dout(this, 20) << "Read " << obj_tags.count() << "tags" << dendl;

  return 0;
}

void RGWPutObjTags_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

}

void RGWDeleteObjTags_ObjStore_S3::send_response()
{
  if (op_ret == 0){
    op_ret = STATUS_NO_CONTENT;
  }
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);
}

void RGWGetBucketTags_ObjStore_S3::send_response_data(bufferlist& bl)
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

  if (!op_ret) {
  s->formatter->open_object_section_in_ns("Tagging", XMLNS_AWS_S3);
  s->formatter->open_object_section("TagSet");
  if (has_tags){
    RGWObjTagSet_S3 tagset;
    auto iter = bl.cbegin();
    try {
      tagset.decode(iter);
    } catch (buffer::error& err) {
      ldpp_dout(this,0) << "ERROR: caught buffer::error, couldn't decode TagSet" << dendl;
      op_ret= -EIO;
      return;
    }
    tagset.dump_xml(s->formatter);
  }
  s->formatter->close_section();
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

int RGWPutBucketTags_ObjStore_S3::get_params(const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWXMLParser parser;

  if (!parser.init()){
    return -EINVAL;
  }

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  int r = 0;
  bufferlist data;

  std::tie(r, data) = read_all_input(s, max_size, false);

  if (r < 0)
    return r;

  if (!parser.parse(data.c_str(), data.length(), 1)) {
    return -ERR_MALFORMED_XML;
  }

  RGWObjTagging_S3 tagging;
  try {
    RGWXMLDecoder::decode_xml("Tagging", tagging, &parser);
  } catch (RGWXMLDecoder::err& err) {

    ldpp_dout(dpp, 5) << "Malformed tagging request: " << err << dendl;
    return -ERR_MALFORMED_XML;
  }

  RGWObjTags obj_tags(50); // A tag set can contain as many as 50 tags, or it can be empty.
  r = tagging.rebuild(obj_tags);
  if (r < 0)
    return r;

  obj_tags.encode(tags_bl);
  ldpp_dout(dpp, 20) << "Read " << obj_tags.count() << "tags" << dendl;

  // forward bucket tags requests to meta master zone
  if (!driver->is_meta_master()) {
    /* only need to keep this data around if we're not meta master */
    in_data = std::move(data);
  }

  return 0;
}

void RGWPutBucketTags_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);
}

void RGWDeleteBucketTags_ObjStore_S3::send_response()
{
  // A successful DeleteBucketTagging should
  // return a 204 status code.
  if (op_ret == 0)
    op_ret = STATUS_NO_CONTENT;
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);
}

namespace {

bool is_valid_status(const string& s) {
  return (s == "Enabled" ||
          s == "Disabled");
}

static string enabled_group_id = "s3-bucket-replication:enabled";
static string disabled_group_id = "s3-bucket-replication:disabled";

struct ReplicationConfiguration {
  string role;

  struct Rule {
    struct DeleteMarkerReplication {
      string status;

      void decode_xml(XMLObj *obj) {
        RGWXMLDecoder::decode_xml("Status", status, obj);
      }

      void dump_xml(Formatter *f) const {
        encode_xml("Status", status, f);
      }

      bool is_valid(CephContext *cct) const {
        bool result = is_valid_status(status);
        if (!result) {
          ldout(cct, 5) << "NOTICE: bad status provided in DeleteMarkerReplication element (status=" << status << ")" << dendl;
        }
        return result;
      }
    };

    struct Source { /* rgw extension */
      std::vector<string> zone_names;

      void decode_xml(XMLObj *obj) {
        RGWXMLDecoder::decode_xml("Zone", zone_names, obj);
      }

      void dump_xml(Formatter *f) const {
        encode_xml("Zone", zone_names, f);
      }
    };

    struct Destination {
      struct AccessControlTranslation {
        string owner;

        void decode_xml(XMLObj *obj) {
          RGWXMLDecoder::decode_xml("Owner", owner, obj);
        }
        void dump_xml(Formatter *f) const {
          encode_xml("Owner", owner, f);
        }
      };

      std::optional<AccessControlTranslation> acl_translation;
      std::optional<string> account;
      string bucket;
      std::optional<string> storage_class;
      std::vector<string> zone_names;

      void decode_xml(XMLObj *obj) {
        RGWXMLDecoder::decode_xml("AccessControlTranslation", acl_translation, obj);
        RGWXMLDecoder::decode_xml("Account", account, obj);
        if (account && account->empty()) {
          account.reset();
        }
        RGWXMLDecoder::decode_xml("Bucket", bucket, obj);
        RGWXMLDecoder::decode_xml("StorageClass", storage_class, obj);
        if (storage_class && storage_class->empty()) {
          storage_class.reset();
        }
        RGWXMLDecoder::decode_xml("Zone", zone_names, obj); /* rgw extension */
      }

      void dump_xml(Formatter *f) const {
        encode_xml("AccessControlTranslation", acl_translation, f);
        encode_xml("Account", account, f);
        encode_xml("Bucket", bucket, f);
        encode_xml("StorageClass", storage_class, f);
        encode_xml("Zone", zone_names, f);
      }
    };

    struct Filter {
      struct Tag {
        string key;
        string value;

        bool empty() const {
          return key.empty() && value.empty();
        }

        void decode_xml(XMLObj *obj) {
          RGWXMLDecoder::decode_xml("Key", key, obj);
          RGWXMLDecoder::decode_xml("Value", value, obj);
        };

        void dump_xml(Formatter *f) const {
          encode_xml("Key", key, f);
          encode_xml("Value", value, f);
        }
      };

      struct AndElements {
        std::optional<string> prefix;
        std::vector<Tag> tags;

        bool empty() const {
          return !prefix &&
            (tags.size() == 0);
        }

        void decode_xml(XMLObj *obj) {
          std::vector<Tag> _tags;
          RGWXMLDecoder::decode_xml("Prefix", prefix, obj);
          if (prefix && prefix->empty()) {
            prefix.reset();
          }
          RGWXMLDecoder::decode_xml("Tag", _tags, obj);
          for (auto& t : _tags) {
            if (!t.empty()) {
              tags.push_back(std::move(t));
            }
          }
        };

        void dump_xml(Formatter *f) const {
          encode_xml("Prefix", prefix, f);
          encode_xml("Tag", tags, f);
        }
      };

      std::optional<string> prefix;
      std::optional<Tag> tag;
      std::optional<AndElements> and_elements;

      bool empty() const {
        return (!prefix && !tag && !and_elements);
      }

      void decode_xml(XMLObj *obj) {
        RGWXMLDecoder::decode_xml("Prefix", prefix, obj);
        if (prefix && prefix->empty()) {
          prefix.reset();
        }
        RGWXMLDecoder::decode_xml("Tag", tag, obj);
        if (tag && tag->empty()) {
          tag.reset();
        }
        RGWXMLDecoder::decode_xml("And", and_elements, obj);
        if (and_elements && and_elements->empty()) {
          and_elements.reset();
        }
      };

      void dump_xml(Formatter *f) const {
        encode_xml("Prefix", prefix, f);
        encode_xml("Tag", tag, f);
        encode_xml("And", and_elements, f);
      }

      bool is_valid(CephContext *cct) const {
        if (tag && prefix) {
          ldout(cct, 5) << "NOTICE: both tag and prefix were provided in replication filter rule" << dendl;
          return false;
        }

        if (and_elements) {
          if (prefix && and_elements->prefix) {
            ldout(cct, 5) << "NOTICE: too many prefixes were provided in re" << dendl;
            return false;
          }
        }
        return true;
      };

      int to_sync_pipe_filter(CephContext *cct,
                              rgw_sync_pipe_filter *f) const {
        if (!is_valid(cct)) {
          return -EINVAL;
        }
        if (prefix) {
          f->prefix = *prefix;
        }
        if (tag) {
          f->tags.insert(rgw_sync_pipe_filter_tag(tag->key, tag->value));
        }

        if (and_elements) {
          if (and_elements->prefix) {
            f->prefix = *and_elements->prefix;
          }
          for (auto& t : and_elements->tags) {
            f->tags.insert(rgw_sync_pipe_filter_tag(t.key, t.value));
          }
        }
        return 0;
      }

      void from_sync_pipe_filter(const rgw_sync_pipe_filter& f) {
        if (f.prefix && f.tags.empty()) {
          prefix = f.prefix;
          return;
        }
        if (f.prefix) {
          and_elements.emplace();
          and_elements->prefix = f.prefix;
        } else if (f.tags.size() == 1) {
          auto iter = f.tags.begin();
          if (iter == f.tags.end()) {
            /* should never happen */
            return;
          }
          auto& t = *iter;
          tag.emplace();
          tag->key = t.key;
          tag->value = t.value;
          return;
        }

        if (f.tags.empty()) {
          return;
        }

        if (!and_elements) {
          and_elements.emplace();
        }

        for (auto& t : f.tags) {
          auto& tag = and_elements->tags.emplace_back();
          tag.key = t.key;
          tag.value = t.value;
        }
      }
    };

    set<rgw_zone_id> get_zone_ids_from_names(rgw::sal::Driver* driver,
                                             const vector<string>& zone_names) const {
      set<rgw_zone_id> ids;

      for (auto& name : zone_names) {
	std::unique_ptr<rgw::sal::Zone> zone;
	int ret = driver->get_zone()->get_zonegroup().get_zone_by_name(name, &zone);
	if (ret >= 0) {
	  rgw_zone_id id = zone->get_id();
	  ids.insert(std::move(id));
        }
      }

      return ids;
    }

    vector<string> get_zone_names_from_ids(rgw::sal::Driver* driver,
                                           const set<rgw_zone_id>& zone_ids) const {
      vector<string> names;

      for (auto& id : zone_ids) {
	std::unique_ptr<rgw::sal::Zone> zone;
	int ret = driver->get_zone()->get_zonegroup().get_zone_by_id(id.id, &zone);
	if (ret >= 0) {
	  names.emplace_back(zone->get_name());
	}
      }

      return names;
    }

    std::optional<DeleteMarkerReplication> delete_marker_replication;
    std::optional<Source> source;
    Destination destination;
    std::optional<Filter> filter;
    string id;
    int32_t priority;
    string status;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("DeleteMarkerReplication", delete_marker_replication, obj);
      RGWXMLDecoder::decode_xml("Source", source, obj);
      RGWXMLDecoder::decode_xml("Destination", destination, obj);
      RGWXMLDecoder::decode_xml("ID", id, obj);

      std::optional<string> prefix;
      RGWXMLDecoder::decode_xml("Prefix", prefix, obj);
      if (prefix) {
        filter.emplace();
        filter->prefix = prefix;
      }

      if (!filter) {
        RGWXMLDecoder::decode_xml("Filter", filter, obj);
      } else {
        /* don't want to have filter reset because it might have been initialized
         * when decoding prefix
         */
        RGWXMLDecoder::decode_xml("Filter", *filter, obj);
      }

      RGWXMLDecoder::decode_xml("Priority", priority, obj);
      RGWXMLDecoder::decode_xml("Status", status, obj);
    }

    void dump_xml(Formatter *f) const {
      encode_xml("DeleteMarkerReplication", delete_marker_replication, f);
      encode_xml("Source", source, f);
      encode_xml("Destination", destination, f);
      encode_xml("Filter", filter, f);
      encode_xml("ID", id, f);
      encode_xml("Priority", priority, f);
      encode_xml("Status", status, f);
    }

    bool is_valid(CephContext *cct) const {
      if (!is_valid_status(status)) {
        ldout(cct, 5) << "NOTICE: bad status provided in rule (status=" << status << ")" << dendl;
        return false;
      }
      if ((filter && !filter->is_valid(cct)) ||
          (delete_marker_replication && !delete_marker_replication->is_valid(cct))) {
        return false;
      }
      return true;
    }

    int to_sync_policy_pipe(req_state *s, rgw::sal::Driver* driver,
                            rgw_sync_bucket_pipes *pipe,
                            bool *enabled) const {
      if (!is_valid(s->cct)) {
        return -EINVAL;
      }

      pipe->id = id;
      pipe->params.priority = priority;

      const auto& user_id = s->user->get_id();

      rgw_bucket_key dest_bk(user_id.tenant,
                             destination.bucket);

      if (source && !source->zone_names.empty()) {
        pipe->source.zones = get_zone_ids_from_names(driver, source->zone_names);
      } else {
        pipe->source.set_all_zones(true);
      }
      if (!destination.zone_names.empty()) {
        pipe->dest.zones = get_zone_ids_from_names(driver, destination.zone_names);
      } else {
        pipe->dest.set_all_zones(true);
      }
      pipe->dest.bucket.emplace(dest_bk);

      if (filter) {
        int r = filter->to_sync_pipe_filter(s->cct, &pipe->params.source.filter);
        if (r < 0) {
          return r;
        }
      }
      if (destination.acl_translation) {
        rgw_user u;
        u.tenant = user_id.tenant;
        u.from_str(destination.acl_translation->owner); /* explicit tenant will override tenant,
                                                           otherwise will inherit it from s->user */
        pipe->params.dest.acl_translation.emplace();
        pipe->params.dest.acl_translation->owner = u;
      }
      pipe->params.dest.storage_class = destination.storage_class;

      *enabled = (status == "Enabled");

      pipe->params.mode = rgw_sync_pipe_params::Mode::MODE_USER;
      pipe->params.user = user_id.to_str();

      return 0;
    }

    void from_sync_policy_pipe(rgw::sal::Driver* driver,
                              const rgw_sync_bucket_pipes& pipe,
                              bool enabled) {
      id = pipe.id;
      status = (enabled ? "Enabled" : "Disabled");
      priority = pipe.params.priority;

      if (pipe.source.all_zones) {
        source.reset();
      } else if (pipe.source.zones) {
        source.emplace();
        source->zone_names = get_zone_names_from_ids(driver, *pipe.source.zones);
      }

      if (!pipe.dest.all_zones &&
          pipe.dest.zones) {
        destination.zone_names = get_zone_names_from_ids(driver, *pipe.dest.zones);
      }

      if (pipe.params.dest.acl_translation) {
        destination.acl_translation.emplace();
        destination.acl_translation->owner = pipe.params.dest.acl_translation->owner.to_str();
      }

      if (pipe.params.dest.storage_class) {
        destination.storage_class = *pipe.params.dest.storage_class;
      }

      if (pipe.dest.bucket) {
        destination.bucket = pipe.dest.bucket->get_key();
      }

      filter.emplace();
      filter->from_sync_pipe_filter(pipe.params.source.filter);

      if (filter->empty()) {
        filter.reset();
      }
    }
  };

  std::vector<Rule> rules;

  void decode_xml(XMLObj *obj) {
    RGWXMLDecoder::decode_xml("Role", role, obj);
    RGWXMLDecoder::decode_xml("Rule", rules, obj);
  }

  void dump_xml(Formatter *f) const {
    encode_xml("Role", role, f);
    encode_xml("Rule", rules, f);
  }

  int to_sync_policy_groups(req_state *s, rgw::sal::Driver* driver,
                            vector<rgw_sync_policy_group> *result) const {
    result->resize(2);

    rgw_sync_policy_group& enabled_group = (*result)[0];
    rgw_sync_policy_group& disabled_group = (*result)[1];

    enabled_group.id = enabled_group_id;
    enabled_group.status = rgw_sync_policy_group::Status::ENABLED;
    disabled_group.id = disabled_group_id;
    disabled_group.status = rgw_sync_policy_group::Status::ALLOWED; /* not enabled, not forbidden */

    for (auto& rule : rules) {
      rgw_sync_bucket_pipes pipe;
      bool enabled;
      int r = rule.to_sync_policy_pipe(s, driver, &pipe, &enabled);
      if (r < 0) {
        ldpp_dout(s, 5) << "NOTICE: failed to convert replication configuration into sync policy pipe (rule.id=" << rule.id << "): " << cpp_strerror(-r) << dendl;
        return r;
      }

      if (enabled) {
        enabled_group.pipes.emplace_back(std::move(pipe));
      } else {
        disabled_group.pipes.emplace_back(std::move(pipe));
      }
    }
    return 0;
  }

  void from_sync_policy_group(rgw::sal::Driver* driver,
                              const rgw_sync_policy_group& group) {

    bool enabled = (group.status == rgw_sync_policy_group::Status::ENABLED);

    for (auto& pipe : group.pipes) {
      auto& rule = rules.emplace_back();
      rule.from_sync_policy_pipe(driver, pipe, enabled);
    }
  }
};

}

void RGWGetBucketReplication_ObjStore_S3::send_response_data()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

  ReplicationConfiguration conf;

  if (s->bucket->get_info().sync_policy) {
    auto policy = s->bucket->get_info().sync_policy;

    auto iter = policy->groups.find(enabled_group_id);
    if (iter != policy->groups.end()) {
      conf.from_sync_policy_group(driver, iter->second);
    }
    iter = policy->groups.find(disabled_group_id);
    if (iter != policy->groups.end()) {
      conf.from_sync_policy_group(driver, iter->second);
    }
  }

  if (!op_ret) {
  s->formatter->open_object_section_in_ns("ReplicationConfiguration", XMLNS_AWS_S3);
  conf.dump_xml(s->formatter);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

int RGWPutBucketReplication_ObjStore_S3::get_params(optional_yield y)
{
  RGWXMLParser parser;

  if (!parser.init()){
    return -EINVAL;
  }

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  int r = 0;
  bufferlist data;

  std::tie(r, data) = read_all_input(s, max_size, false);

  if (r < 0)
    return r;

  if (!parser.parse(data.c_str(), data.length(), 1)) {
    return -ERR_MALFORMED_XML;
  }

  ReplicationConfiguration conf;
  try {
    RGWXMLDecoder::decode_xml("ReplicationConfiguration", conf, &parser);
  } catch (RGWXMLDecoder::err& err) {

    ldpp_dout(this, 5) << "Malformed tagging request: " << err << dendl;
    return -ERR_MALFORMED_XML;
  }

  r = conf.to_sync_policy_groups(s, driver, &sync_policy_groups);
  if (r < 0) {
    return r;
  }

  // forward requests to meta master zone
  if (!driver->is_meta_master()) {
    /* only need to keep this data around if we're not meta master */
    in_data = std::move(data);
  }

  return 0;
}

void RGWPutBucketReplication_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);
}

void RGWDeleteBucketReplication_ObjStore_S3::update_sync_policy(rgw_sync_policy_info *policy)
{
  policy->groups.erase(enabled_group_id);
  policy->groups.erase(disabled_group_id);
}

void RGWDeleteBucketReplication_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);
}

void RGWListBuckets_ObjStore_S3::send_response_begin(bool has_buckets)
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  dump_start(s);
  // Explicitly use chunked transfer encoding so that we can stream the result
  // to the user without having to wait for the full length of it.
  end_header(s, NULL, to_mime_type(s->format), CHUNKED_TRANSFER_ENCODING);

  if (! op_ret) {
    list_all_buckets_start(s);
    dump_owner(s, s->user->get_id(), s->user->get_display_name());
    s->formatter->open_array_section("Buckets");
    sent_data = true;
  }
}

void RGWListBuckets_ObjStore_S3::send_response_data(std::span<const RGWBucketEnt> buckets)
{
  if (!sent_data)
    return;

  for (const auto& ent : buckets) {
    dump_bucket(s, ent);
  }
  rgw_flush_formatter(s, s->formatter);
}

void RGWListBuckets_ObjStore_S3::send_response_end()
{
  if (sent_data) {
    s->formatter->close_section();
    list_all_buckets_end(s);
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

int RGWGetUsage_ObjStore_S3::get_params(optional_yield y)
{
  start_date = s->info.args.get("start-date");
  end_date = s->info.args.get("end-date");
  return 0;
}

static void dump_usage_categories_info(Formatter *formatter, const rgw_usage_log_entry& entry, map<string, bool> *categories)
{
  formatter->open_array_section("categories");
  map<string, rgw_usage_data>::const_iterator uiter;
  for (uiter = entry.usage_map.begin(); uiter != entry.usage_map.end(); ++uiter) {
    if (categories && !categories->empty() && !categories->count(uiter->first))
      continue;
    const rgw_usage_data& usage = uiter->second;
    formatter->open_object_section("Entry");
    encode_json("Category", uiter->first, formatter);
    encode_json("BytesSent", usage.bytes_sent, formatter);
    encode_json("BytesReceived", usage.bytes_received, formatter);
    encode_json("Ops", usage.ops, formatter);
    encode_json("SuccessfulOps", usage.successful_ops, formatter);
    formatter->close_section(); // Entry
  }
  formatter->close_section(); // Category
}

static void dump_usage_bucket_info(Formatter *formatter, const std::string& name, const bucket_meta_entry& entry)
{
  formatter->open_object_section("Entry");
  encode_json("Bucket", name, formatter);
  encode_json("Bytes", entry.size, formatter);
  encode_json("Bytes_Rounded", entry.size_rounded, formatter);
  formatter->close_section(); // entry
}

void RGWGetUsage_ObjStore_S3::send_response()
{
  if (op_ret < 0)
    set_req_state_err(s, op_ret);
  dump_errno(s);

  // Explicitly use chunked transfer encoding so that we can stream the result
  // to the user without having to wait for the full length of it.
  end_header(s, this, to_mime_type(s->format), CHUNKED_TRANSFER_ENCODING);
  dump_start(s);
  if (op_ret < 0)
    return;

  Formatter *formatter = s->formatter;
  string last_owner;
  bool user_section_open = false;

  formatter->open_object_section("Usage");
  if (show_log_entries) {
    formatter->open_array_section("Entries");
  }
  map<rgw_user_bucket, rgw_usage_log_entry>::iterator iter;
  for (iter = usage.begin(); iter != usage.end(); ++iter) {
    const rgw_user_bucket& ub = iter->first;
    const rgw_usage_log_entry& entry = iter->second;

    if (show_log_entries) {
      if (ub.user.compare(last_owner) != 0) {
        if (user_section_open) {
          formatter->close_section();
          formatter->close_section();
        }
        formatter->open_object_section("User");
        formatter->dump_string("Owner", ub.user);
        formatter->open_array_section("Buckets");
        user_section_open = true;
        last_owner = ub.user;
      }
      formatter->open_object_section("Bucket");
      formatter->dump_string("Bucket", ub.bucket);
      utime_t ut(entry.epoch, 0);
      ut.gmtime(formatter->dump_stream("Time"));
      formatter->dump_int("Epoch", entry.epoch);
      dump_usage_categories_info(formatter, entry, &categories);
      formatter->close_section(); // bucket
    }

    summary_map[ub.user].aggregate(entry, &categories);
  }

  if (show_log_entries) {
     if (user_section_open) {
       formatter->close_section(); // buckets
       formatter->close_section(); //user
     }
     formatter->close_section(); // entries
   }

   if (show_log_sum) {
     formatter->open_array_section("Summary");
     map<string, rgw_usage_log_entry>::iterator siter;
     for (siter = summary_map.begin(); siter != summary_map.end(); ++siter) {
       const rgw_usage_log_entry& entry = siter->second;
       formatter->open_object_section("User");
       formatter->dump_string("User", siter->first);
       dump_usage_categories_info(formatter, entry, &categories);
       rgw_usage_data total_usage;
       entry.sum(total_usage, categories);
       formatter->open_object_section("Total");
       encode_json("BytesSent", total_usage.bytes_sent, formatter);
       encode_json("BytesReceived", total_usage.bytes_received, formatter);
       encode_json("Ops", total_usage.ops, formatter);
       encode_json("SuccessfulOps", total_usage.successful_ops, formatter);
       formatter->close_section(); // total
       formatter->close_section(); // user
     }

     if (s->cct->_conf->rgw_rest_getusage_op_compat) {
       formatter->open_object_section("Stats");
     }

     // send info about quota config
     auto user_info = s->user->get_info();
     encode_json("QuotaMaxBytes", user_info.quota.user_quota.max_size, formatter);
     encode_json("QuotaMaxBuckets", user_info.max_buckets, formatter);
     encode_json("QuotaMaxObjCount", user_info.quota.user_quota.max_objects, formatter);
     encode_json("QuotaMaxBytesPerBucket", user_info.quota.bucket_quota.max_objects, formatter);
     encode_json("QuotaMaxObjCountPerBucket", user_info.quota.bucket_quota.max_size, formatter);
     // send info about user's capacity utilization
     encode_json("TotalBytes", stats.size, formatter);
     encode_json("TotalBytesRounded", stats.size_rounded, formatter);
     encode_json("TotalEntries", stats.num_objects, formatter);

     if (s->cct->_conf->rgw_rest_getusage_op_compat) {
       formatter->close_section(); //Stats
     }

     formatter->close_section(); // summary
   }

  formatter->open_array_section("CapacityUsed");
  formatter->open_object_section("User");
  formatter->open_array_section("Buckets");
  for (const auto& biter : buckets_usage) {
    const bucket_meta_entry& entry = biter.second;
    dump_usage_bucket_info(formatter, biter.first, entry);
  }
  formatter->close_section(); // Buckets
  formatter->close_section(); // User
  formatter->close_section(); // CapacityUsed

  formatter->close_section(); // usage
  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWListBucket_ObjStore_S3::get_common_params()
{
  list_versions = s->info.args.exists("versions");
  prefix = s->info.args.get("prefix");

  // non-standard
  s->info.args.get_bool("allow-unordered", &allow_unordered, false);
  delimiter = s->info.args.get("delimiter");
  max_keys = s->info.args.get("max-keys");
  op_ret = parse_max_keys();
  if (op_ret < 0) {
   return op_ret;
  }
  encoding_type = s->info.args.get("encoding-type");
  if (s->system_request) {
    s->info.args.get_bool("objs-container", &objs_container, false);
    const char *shard_id_str = s->info.env->get("HTTP_RGWX_SHARD_ID");
    if (shard_id_str) {
      string err;
      shard_id = strict_strtol(shard_id_str, 10, &err);
      if (!err.empty()) {
        ldpp_dout(this, 5) << "bad shard id specified: " << shard_id_str << dendl;
        return -EINVAL;
      }
    } else {
     shard_id = s->bucket_instance_shard_id;
    }
  }
  return 0;
}

int RGWListBucket_ObjStore_S3::get_params(optional_yield y)
{
  int ret = get_common_params();
  if (ret < 0) {
    return ret;
  }
  if (!list_versions) {
    marker = s->info.args.get("marker");
  } else {
    marker.name = s->info.args.get("key-marker");
    marker.instance = s->info.args.get("version-id-marker");
  }
  return 0;
}

int RGWListBucket_ObjStore_S3v2::get_params(optional_yield y)
{
int ret = get_common_params();
if (ret < 0) {
  return ret;
}
s->info.args.get_bool("fetch-owner", &fetchOwner, false);
startAfter = s->info.args.get("start-after", &start_after_exist);
continuation_token = s->info.args.get("continuation-token", &continuation_token_exist);
if(!continuation_token_exist) {
  marker = startAfter;
} else {
  marker = continuation_token;
}
return 0;
}

void RGWListBucket_ObjStore_S3::send_common_versioned_response()
{
  if (!s->bucket_tenant.empty()) {
    s->formatter->dump_string("Tenant", s->bucket_tenant);
  }
  s->formatter->dump_string("Name", s->bucket_name);
  s->formatter->dump_string("Prefix", prefix);
  s->formatter->dump_int("MaxKeys", max);
  if (!delimiter.empty()) {
    s->formatter->dump_string("Delimiter", delimiter);
  }
  s->formatter->dump_string("IsTruncated", (max && is_truncated ? "true"
              : "false"));

  if (!common_prefixes.empty()) {
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin();
      pref_iter != common_prefixes.end(); ++pref_iter) {
      s->formatter->open_array_section("CommonPrefixes");
      dump_urlsafe(s, encode_key, "Prefix", pref_iter->first, false);

      s->formatter->close_section();
      }
    }
  }

void RGWListBucket_ObjStore_S3::send_versioned_response()
{
  s->formatter->open_object_section_in_ns("ListVersionsResult", XMLNS_AWS_S3);
  if (strcasecmp(encoding_type.c_str(), "url") == 0) {
    s->formatter->dump_string("EncodingType", "url");
    encode_key = true;
  }
  RGWListBucket_ObjStore_S3::send_common_versioned_response();
  s->formatter->dump_string("KeyMarker", marker.name);
  s->formatter->dump_string("VersionIdMarker", marker.instance);
  if (is_truncated && !next_marker.empty()) {
    dump_urlsafe(s ,encode_key, "NextKeyMarker", next_marker.name);
    if (next_marker.instance.empty()) {
      s->formatter->dump_string("NextVersionIdMarker", "null");
    }
    else {
      s->formatter->dump_string("NextVersionIdMarker", next_marker.instance);
    }
  }

  if (op_ret >= 0) {
    if (objs_container) {
      s->formatter->open_array_section("Entries");
    }

    vector<rgw_bucket_dir_entry>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      const char *section_name = (iter->is_delete_marker() ? "DeleteMarker"
          : "Version");
      s->formatter->open_object_section(section_name);
      if (objs_container) {
        s->formatter->dump_bool("IsDeleteMarker", iter->is_delete_marker());
      }
      rgw_obj_key key(iter->key);
      dump_urlsafe(s ,encode_key, "Key", key.name);
      string version_id = key.instance;
      if (version_id.empty()) {
        version_id = "null";
      }
      if (s->system_request) {
        if (iter->versioned_epoch > 0) {
          s->formatter->dump_int("VersionedEpoch", iter->versioned_epoch);
        }
        s->formatter->dump_string("RgwxTag", iter->tag);
        utime_t ut(iter->meta.mtime);
        ut.gmtime_nsec(s->formatter->dump_stream("RgwxMtime"));
      }
      s->formatter->dump_string("VersionId", version_id);
      s->formatter->dump_bool("IsLatest", iter->is_current());
      dump_time(s, "LastModified", iter->meta.mtime);
      if (!iter->is_delete_marker()) {
        s->formatter->dump_format("ETag", "\"%s\"", iter->meta.etag.c_str());
        s->formatter->dump_int("Size", iter->meta.accounted_size);
        auto& storage_class = rgw_placement_rule::get_canonical_storage_class(iter->meta.storage_class);
        s->formatter->dump_string("StorageClass", storage_class.c_str());
      }
      dump_owner(s, rgw_user(iter->meta.owner), iter->meta.owner_display_name);
      if (iter->meta.appendable) {
        s->formatter->dump_string("Type", "Appendable");
      } else {
        s->formatter->dump_string("Type", "Normal");
      }
      s->formatter->close_section(); // Version/DeleteMarker
    }
    if (objs_container) {
      s->formatter->close_section(); // Entries
    }
    s->formatter->close_section(); // ListVersionsResult
  }
  rgw_flush_formatter_and_reset(s, s->formatter);
}


void RGWListBucket_ObjStore_S3::send_common_response()
{
  if (!s->bucket_tenant.empty()) {
    s->formatter->dump_string("Tenant", s->bucket_tenant);
  }
  s->formatter->dump_string("Name", s->bucket_name);
  s->formatter->dump_string("Prefix", prefix);
  s->formatter->dump_int("MaxKeys", max);
  if (!delimiter.empty()) {
    dump_urlsafe(s, encode_key, "Delimiter", delimiter, false);
  }
  s->formatter->dump_string("IsTruncated", (max && is_truncated ? "true"
              : "false"));

    if (!common_prefixes.empty()) {
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin();
      pref_iter != common_prefixes.end(); ++pref_iter) {
      s->formatter->open_array_section("CommonPrefixes");
      dump_urlsafe(s, encode_key, "Prefix", pref_iter->first, false);
      s->formatter->close_section();
      }
    }
  }

void RGWListBucket_ObjStore_S3::send_response()
{
  if (op_ret < 0) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);

  // Explicitly use chunked transfer encoding so that we can stream the result
  // to the user without having to wait for the full length of it.
  end_header(s, this, to_mime_type(s->format), CHUNKED_TRANSFER_ENCODING);
  dump_start(s);
  if (op_ret < 0) {
    return;
  }
  if (list_versions) {
    send_versioned_response();
    return;
  }

  s->formatter->open_object_section_in_ns("ListBucketResult", XMLNS_AWS_S3);
  if (strcasecmp(encoding_type.c_str(), "url") == 0) {
    s->formatter->dump_string("EncodingType", "url");
    encode_key = true;
  }
  
  RGWListBucket_ObjStore_S3::send_common_response();
  
  if (op_ret >= 0) {
    if (s->format == RGWFormat::JSON) {
      s->formatter->open_array_section("Contents");
    }
    vector<rgw_bucket_dir_entry>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {

      rgw_obj_key key(iter->key);
      /* conditionally format JSON in the obvious way--I'm unsure if
       * AWS actually does this */
      if (s->format == RGWFormat::XML) {
	s->formatter->open_array_section("Contents");
      } else {
	// json
	s->formatter->open_object_section("dummy");
      }
      dump_urlsafe(s ,encode_key, "Key", key.name);
      dump_time(s, "LastModified", iter->meta.mtime);
      s->formatter->dump_format("ETag", "\"%s\"", iter->meta.etag.c_str());
      s->formatter->dump_int("Size", iter->meta.accounted_size);
      auto& storage_class = rgw_placement_rule::get_canonical_storage_class(iter->meta.storage_class);
      s->formatter->dump_string("StorageClass", storage_class.c_str());
      dump_owner(s, rgw_user(iter->meta.owner), iter->meta.owner_display_name);
      if (s->system_request) {
	s->formatter->dump_string("RgwxTag", iter->tag);
      }
      if (iter->meta.appendable) {
	s->formatter->dump_string("Type", "Appendable");
	} else {
	s->formatter->dump_string("Type", "Normal");
      }
      // JSON has one extra section per element
      s->formatter->close_section();
    } // foreach obj
    if (s->format == RGWFormat::JSON) {
      s->formatter->close_section();
    }
  }
  s->formatter->dump_string("Marker", marker.name);
  if (is_truncated && !next_marker.empty()) {
    dump_urlsafe(s, encode_key, "NextMarker", next_marker.name);
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
} /* RGWListBucket_ObjStore_S3::send_response() */

void RGWListBucket_ObjStore_S3v2::send_versioned_response()
{
  s->formatter->open_object_section_in_ns("ListVersionsResult", XMLNS_AWS_S3);
  RGWListBucket_ObjStore_S3v2::send_common_versioned_response();
  s->formatter->dump_string("KeyContinuationToken", marker.name);
  s->formatter->dump_string("VersionIdContinuationToken", marker.instance);
  if (is_truncated && !next_marker.empty()) {
    s->formatter->dump_string("NextKeyContinuationToken", next_marker.name);
    s->formatter->dump_string("NextVersionIdContinuationToken", next_marker.instance);
  }

  if (strcasecmp(encoding_type.c_str(), "url") == 0) {
    s->formatter->dump_string("EncodingType", "url");
    encode_key = true;
  }

  if (op_ret >= 0) {
    if (objs_container) {
      s->formatter->open_array_section("Entries");
    }

    vector<rgw_bucket_dir_entry>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      const char *section_name = (iter->is_delete_marker() ? "DeleteContinuationToken"
          : "Version");
      s->formatter->open_object_section(section_name);
      if (objs_container) {
        s->formatter->dump_bool("IsDeleteContinuationToken", iter->is_delete_marker());
      }
      rgw_obj_key key(iter->key);
      dump_urlsafe(s, encode_key, "Key", key.name);
      string version_id = key.instance;
      if (version_id.empty()) {
        version_id = "null";
      }
      if (s->system_request) {
        if (iter->versioned_epoch > 0) {
          s->formatter->dump_int("VersionedEpoch", iter->versioned_epoch);
        }
        s->formatter->dump_string("RgwxTag", iter->tag);
        utime_t ut(iter->meta.mtime);
        ut.gmtime_nsec(s->formatter->dump_stream("RgwxMtime"));
      }
      s->formatter->dump_string("VersionId", version_id);
      s->formatter->dump_bool("IsLatest", iter->is_current());
      dump_time(s, "LastModified", iter->meta.mtime);
      if (!iter->is_delete_marker()) {
        s->formatter->dump_format("ETag", "\"%s\"", iter->meta.etag.c_str());
        s->formatter->dump_int("Size", iter->meta.accounted_size);
        auto& storage_class = rgw_placement_rule::get_canonical_storage_class(iter->meta.storage_class);
        s->formatter->dump_string("StorageClass", storage_class.c_str());
      }
      if (fetchOwner == true) {
        dump_owner(s, rgw_user(iter->meta.owner), iter->meta.owner_display_name);
      }
      s->formatter->close_section();
    }


    if (objs_container) {
      s->formatter->close_section();
    }

     if (!common_prefixes.empty()) {
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin();
      pref_iter != common_prefixes.end(); ++pref_iter) {
      s->formatter->open_array_section("CommonPrefixes");
      dump_urlsafe(s, encode_key, "Prefix", pref_iter->first, false);

      s->formatter->dump_int("KeyCount",objs.size());
      if (start_after_exist) {
        s->formatter->dump_string("StartAfter", startAfter);
      }
      s->formatter->close_section();
      }
    }

    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

void RGWListBucket_ObjStore_S3v2::send_response()
{
  if (op_ret < 0) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);

  // Explicitly use chunked transfer encoding so that we can stream the result
  // to the user without having to wait for the full length of it.
  end_header(s, this, to_mime_type(s->format), CHUNKED_TRANSFER_ENCODING);
  dump_start(s);
  if (op_ret < 0) {
    return;
  }
  if (list_versions) {
    send_versioned_response();
    return;
  }

  s->formatter->open_object_section_in_ns("ListBucketResult", XMLNS_AWS_S3);
  if (strcasecmp(encoding_type.c_str(), "url") == 0) {
    s->formatter->dump_string("EncodingType", "url");
    encode_key = true;
  }

  RGWListBucket_ObjStore_S3::send_common_response();
  if (op_ret >= 0) {
    vector<rgw_bucket_dir_entry>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      rgw_obj_key key(iter->key);
      s->formatter->open_array_section("Contents");
      dump_urlsafe(s, encode_key, "Key", key.name);
      dump_time(s, "LastModified", iter->meta.mtime);
      s->formatter->dump_format("ETag", "\"%s\"", iter->meta.etag.c_str());
      s->formatter->dump_int("Size", iter->meta.accounted_size);
      auto& storage_class = rgw_placement_rule::get_canonical_storage_class(iter->meta.storage_class);
      s->formatter->dump_string("StorageClass", storage_class.c_str());
      if (fetchOwner == true) {
        dump_owner(s, rgw_user(iter->meta.owner), iter->meta.owner_display_name);
      }
      if (s->system_request) {
        s->formatter->dump_string("RgwxTag", iter->tag);
      }
      if (iter->meta.appendable) {
        s->formatter->dump_string("Type", "Appendable");
      } else {
        s->formatter->dump_string("Type", "Normal");
      }
      s->formatter->close_section();
    }
  }
  if (continuation_token_exist) {
    s->formatter->dump_string("ContinuationToken", continuation_token);
  }
  if (is_truncated && !next_marker.empty()) {
    s->formatter->dump_string("NextContinuationToken", next_marker.name);
  }
  s->formatter->dump_int("KeyCount", objs.size() + common_prefixes.size());
  if (start_after_exist) {
    s->formatter->dump_string("StartAfter", startAfter);
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWGetBucketLogging_ObjStore_S3::send_response()
{
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

  s->formatter->open_object_section_in_ns("BucketLoggingStatus", XMLNS_AWS_S3);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWGetBucketLocation_ObjStore_S3::send_response()
{
  dump_errno(s);
  end_header(s, this);
  dump_start(s);

  std::unique_ptr<rgw::sal::ZoneGroup> zonegroup;
  string api_name;

  int ret = driver->get_zonegroup(s->bucket->get_info().zonegroup, &zonegroup);
  if (ret >= 0) {
    api_name = zonegroup->get_api_name();
  } else  {
    if (s->bucket->get_info().zonegroup != "default") {
      api_name = s->bucket->get_info().zonegroup;
    }
  }

  s->formatter->dump_format_ns("LocationConstraint", XMLNS_AWS_S3,
			       "%s", api_name.c_str());
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWGetBucketVersioning_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

  s->formatter->open_object_section_in_ns("VersioningConfiguration", XMLNS_AWS_S3);
  if (versioned) {
    const char *status = (versioning_enabled ? "Enabled" : "Suspended");
    s->formatter->dump_string("Status", status);
    const char *mfa_status = (mfa_enabled ? "Enabled" : "Disabled");
    s->formatter->dump_string("MfaDelete", mfa_status);
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

struct ver_config_status {
  int status{VersioningSuspended};

  enum MFAStatus {
    MFA_UNKNOWN,
    MFA_DISABLED,
    MFA_ENABLED,
  } mfa_status{MFA_UNKNOWN};
  int retcode{0};

  void decode_xml(XMLObj *obj) {
    string status_str;
    string mfa_str;
    RGWXMLDecoder::decode_xml("Status", status_str, obj);
    if (status_str == "Enabled") {
      status = VersioningEnabled;
    } else if (status_str != "Suspended") {
      status = VersioningStatusInvalid;
    }


    if (RGWXMLDecoder::decode_xml("MfaDelete", mfa_str, obj)) {
      if (mfa_str == "Enabled") {
        mfa_status = MFA_ENABLED;
      } else if (mfa_str == "Disabled") {
        mfa_status = MFA_DISABLED;
      } else {
        retcode = -EINVAL;
      }
    }
  }
};

int RGWSetBucketVersioning_ObjStore_S3::get_params(optional_yield y)
{
  int r = 0;
  bufferlist data;
  std::tie(r, data) =
    read_all_input(s, s->cct->_conf->rgw_max_put_param_size, false);
  if (r < 0) {
    return r;
  }

  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize parser" << dendl;
    return -EIO;
  }

  char* buf = data.c_str();
  if (!parser.parse(buf, data.length(), 1)) {
    ldpp_dout(this, 10) << "NOTICE: failed to parse data: " << buf << dendl;
    r = -EINVAL;
    return r;
  }

  ver_config_status status_conf;

  if (!RGWXMLDecoder::decode_xml("VersioningConfiguration", status_conf, &parser)) {
    ldpp_dout(this, 10) << "NOTICE: bad versioning config input" << dendl;
    return -EINVAL;
  }

  if (!driver->is_meta_master()) {
    /* only need to keep this data around if we're not meta master */
    in_data.append(data);
  }

  versioning_status = status_conf.status;
  if (versioning_status == VersioningStatusInvalid) {
    r = -EINVAL;
  }

  if (status_conf.mfa_status != ver_config_status::MFA_UNKNOWN) {
    mfa_set_status = true;
    switch (status_conf.mfa_status) {
      case ver_config_status::MFA_DISABLED:
        mfa_status = false;
        break;
      case ver_config_status::MFA_ENABLED:
        mfa_status = true;
        break;
      default:
        ldpp_dout(this, 0) << "ERROR: RGWSetBucketVersioning_ObjStore_S3::get_params(optional_yield y): unexpected switch case mfa_status=" << status_conf.mfa_status << dendl;
        r = -EIO;
    }
  } else if (status_conf.retcode < 0) {
    r = status_conf.retcode;
  }
  return r;
}

void RGWSetBucketVersioning_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
}

int RGWSetBucketWebsite_ObjStore_S3::get_params(optional_yield y)
{
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;

  int r = 0;
  bufferlist data;
  std::tie(r, data) = read_all_input(s, max_size, false);

  if (r < 0) {
    return r;
  }

  in_data.append(data);

  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize parser" << dendl;
    return -EIO;
  }

  char* buf = data.c_str();
  if (!parser.parse(buf, data.length(), 1)) {
    ldpp_dout(this, 5) << "failed to parse xml: " << buf << dendl;
    return -EINVAL;
  }

  try {
    RGWXMLDecoder::decode_xml("WebsiteConfiguration", website_conf, &parser, true);
  } catch (RGWXMLDecoder::err& err) {
    ldpp_dout(this, 5) << "unexpected xml: " << buf << dendl;
    return -EINVAL;
  }

  if (website_conf.is_redirect_all && website_conf.redirect_all.hostname.empty()) {
    s->err.message = "A host name must be provided to redirect all requests (e.g. \"example.com\").";
    ldpp_dout(this, 5) << s->err.message << dendl;
    return -EINVAL;
  } else if (!website_conf.is_redirect_all && !website_conf.is_set_index_doc) {
    s->err.message = "A value for IndexDocument Suffix must be provided if RedirectAllRequestsTo is empty";
    ldpp_dout(this, 5) << s->err.message << dendl;
    return -EINVAL;
  } else if (!website_conf.is_redirect_all && website_conf.is_set_index_doc &&
             website_conf.index_doc_suffix.empty()) {
    s->err.message = "The IndexDocument Suffix is not well formed";
    ldpp_dout(this, 5) << s->err.message << dendl;
    return -EINVAL;
  }

#define WEBSITE_ROUTING_RULES_MAX_NUM      50
  int max_num = s->cct->_conf->rgw_website_routing_rules_max_num;
  if (max_num < 0) {
    max_num = WEBSITE_ROUTING_RULES_MAX_NUM;
  }
  int routing_rules_num = website_conf.routing_rules.rules.size();
  if (routing_rules_num > max_num) {
    ldpp_dout(this, 4) << "An website routing config can have up to "
                     << max_num
                     << " rules, request website routing rules num: "
                     << routing_rules_num << dendl;
    s->err.message = std::to_string(routing_rules_num) +" routing rules provided, the number of routing rules in a website configuration is limited to "
                     + std::to_string(max_num)
                     + ".";
    return -ERR_INVALID_WEBSITE_ROUTING_RULES_ERROR;
  }

  return 0;
}

void RGWSetBucketWebsite_ObjStore_S3::send_response()
{
  if (op_ret < 0)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
}

void RGWDeleteBucketWebsite_ObjStore_S3::send_response()
{
  if (op_ret == 0) {
    op_ret = STATUS_NO_CONTENT;
  }
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
}

void RGWGetBucketWebsite_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

  if (op_ret < 0) {
    return;
  }

  RGWBucketWebsiteConf& conf = s->bucket->get_info().website_conf;

  s->formatter->open_object_section_in_ns("WebsiteConfiguration", XMLNS_AWS_S3);
  conf.dump_xml(s->formatter);
  s->formatter->close_section(); // WebsiteConfiguration
  rgw_flush_formatter_and_reset(s, s->formatter);
}

static void dump_bucket_metadata(req_state *s, rgw::sal::Bucket* bucket,
                                 RGWStorageStats& stats)
{
  dump_header(s, "X-RGW-Object-Count", static_cast<long long>(stats.num_objects));
  dump_header(s, "X-RGW-Bytes-Used", static_cast<long long>(stats.size));

  // only bucket's owner is allowed to get the quota settings of the account
  if (bucket->get_owner() == s->user->get_id()) {
    auto user_info = s->user->get_info();
    auto bucket_quota = s->bucket->get_info().quota; // bucket quota
    dump_header(s, "X-RGW-Quota-User-Size", static_cast<long long>(user_info.quota.user_quota.max_size));
    dump_header(s, "X-RGW-Quota-User-Objects", static_cast<long long>(user_info.quota.user_quota.max_objects));
    dump_header(s, "X-RGW-Quota-Max-Buckets", static_cast<long long>(user_info.max_buckets));
    dump_header(s, "X-RGW-Quota-Bucket-Size", static_cast<long long>(bucket_quota.max_size));
    dump_header(s, "X-RGW-Quota-Bucket-Objects", static_cast<long long>(bucket_quota.max_objects));
  }
}

void RGWStatBucket_ObjStore_S3::send_response()
{
  if (op_ret >= 0) {
    dump_bucket_metadata(s, bucket.get(), stats);
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);

  end_header(s, this);
  dump_start(s);
}

static int create_s3_policy(req_state *s, rgw::sal::Driver* driver,
			    RGWAccessControlPolicy& policy,
			    const ACLOwner& owner)
{
  if (s->has_acl_header) {
    if (!s->canned_acl.empty())
      return -ERR_INVALID_REQUEST;

    return rgw::s3::create_policy_from_headers(s, driver, owner,
                                               *s->info.env, policy);
  }

  return rgw::s3::create_canned_acl(owner, s->bucket_owner,
                                    s->canned_acl, policy);
}

class RGWLocationConstraint : public XMLObj
{
public:
  RGWLocationConstraint() {}
  ~RGWLocationConstraint() override {}
  bool xml_end(const char *el) override {
    if (!el)
      return false;

    location_constraint = get_data();

    return true;
  }

  string location_constraint;
};

class RGWCreateBucketConfig : public XMLObj
{
public:
  RGWCreateBucketConfig() {}
  ~RGWCreateBucketConfig() override {}
};

class RGWCreateBucketParser : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el) override {
    return new XMLObj;
  }

public:
  RGWCreateBucketParser() {}
  ~RGWCreateBucketParser() override {}

  bool get_location_constraint(string& zone_group) {
    XMLObj *config = find_first("CreateBucketConfiguration");
    if (!config)
      return false;

    XMLObj *constraint = config->find_first("LocationConstraint");
    if (!constraint)
      return false;

    zone_group = constraint->get_data();

    return true;
  }
};

int RGWCreateBucket_ObjStore_S3::get_params(optional_yield y)
{
  bool relaxed_names = s->cct->_conf->rgw_relaxed_s3_bucket_names;

  int r;
  if (!s->system_request) {
    r = valid_s3_bucket_name(s->bucket_name, relaxed_names);
    if (r) return r;
  }

  r = create_s3_policy(s, driver, policy, s->owner);
  if (r < 0)
    return r;

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;

  int op_ret = 0;
  bufferlist data;
  std::tie(op_ret, data) = read_all_input(s, max_size, false);

  if ((op_ret < 0) && (op_ret != -ERR_LENGTH_REQUIRED))
    return op_ret;

  if (data.length()) {
    RGWCreateBucketParser parser;

    if (!parser.init()) {
      ldpp_dout(this, 0) << "ERROR: failed to initialize parser" << dendl;
      return -EIO;
    }

    char* buf = data.c_str();
    bool success = parser.parse(buf, data.length(), 1);
    ldpp_dout(this, 20) << "create bucket input data=" << buf << dendl;

    if (!success) {
      ldpp_dout(this, 0) << "failed to parse input: " << buf << dendl;
      return -EINVAL;
    }

    if (!parser.get_location_constraint(location_constraint)) {
      ldpp_dout(this, 0) << "provided input did not specify location constraint correctly" << dendl;
      return -EINVAL;
    }

    ldpp_dout(this, 10) << "create bucket location constraint: "
		      << location_constraint << dendl;
  }

  size_t pos = location_constraint.find(':');
  if (pos != string::npos) {
    createparams.placement_rule.init(location_constraint.substr(pos + 1),
                                     s->info.storage_class);
    location_constraint = location_constraint.substr(0, pos);
  } else {
    createparams.placement_rule.storage_class = s->info.storage_class;
  }
  auto iter = s->info.x_meta_map.find("x-amz-bucket-object-lock-enabled");
  if (iter != s->info.x_meta_map.end()) {
    if (!boost::algorithm::iequals(iter->second, "true") && !boost::algorithm::iequals(iter->second, "false")) {
      return -EINVAL;
    }
    createparams.obj_lock_enabled = boost::algorithm::iequals(iter->second, "true");
  }
  return 0;
}

void RGWCreateBucket_ObjStore_S3::send_response()
{
  if (op_ret == -ERR_BUCKET_EXISTS)
    op_ret = 0;
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s);

  if (op_ret < 0)
    return;

  if (s->system_request) {
    JSONFormatter f; /* use json formatter for system requests output */

    const RGWBucketInfo& info = s->bucket->get_info();
    const obj_version& ep_objv = s->bucket->get_version();
    f.open_object_section("info");
    encode_json("entry_point_object_ver", ep_objv, &f);
    encode_json("object_ver", info.objv_tracker.read_version, &f);
    encode_json("bucket_info", info, &f);
    f.close_section();
    rgw_flush_formatter_and_reset(s, &f);
  }
}

void RGWDeleteBucket_ObjStore_S3::send_response()
{
  int r = op_ret;
  if (!r)
    r = STATUS_NO_CONTENT;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s, this);
}

static inline void map_qs_metadata(req_state* s, bool crypto_too)
{
  /* merge S3 valid user metadata from the query-string into
   * x_meta_map, which maps them to attributes */
  const auto& params = const_cast<RGWHTTPArgs&>(s->info.args).get_params();
  for (const auto& elt : params) {
    std::string k = boost::algorithm::to_lower_copy(elt.first);
    if (k.find("x-amz-meta-") == /* offset */ 0) {
      rgw_add_amz_meta_header(s->info.x_meta_map, k, elt.second);
    }
    if (crypto_too && k.find("x-amz-server-side-encryption") == /* offset */ 0) {
      rgw_set_amz_meta_header(s->info.crypt_attribute_map, k, elt.second, OVERWRITE);
    }
  }
}

int RGWPutObj_ObjStore_S3::get_params(optional_yield y)
{
  if (!s->length) {
    const char *encoding = s->info.env->get("HTTP_TRANSFER_ENCODING");
    if (!encoding || strcmp(encoding, "chunked") != 0) {
      ldout(s->cct, 20) << "neither length nor chunked encoding" << dendl;
      return -ERR_LENGTH_REQUIRED;
    }

    chunked_upload = true;
  }

  int ret;

  map_qs_metadata(s, true);
  ret = get_encryption_defaults(s);
  if (ret < 0) {
    ldpp_dout(this, 5) << __func__ << "(): get_encryption_defaults() returned ret=" << ret << dendl;
    return ret;
  }

  ret = create_s3_policy(s, driver, policy, s->owner);
  if (ret < 0)
    return ret;

  if_match = s->info.env->get("HTTP_IF_MATCH");
  if_nomatch = s->info.env->get("HTTP_IF_NONE_MATCH");

  /* handle object tagging */
  auto tag_str = s->info.env->get("HTTP_X_AMZ_TAGGING");
  if (tag_str){
    obj_tags = std::make_unique<RGWObjTags>();
    ret = obj_tags->set_from_string(tag_str);
    if (ret < 0){
      ldpp_dout(this,0) << "setting obj tags failed with " << ret << dendl;
      if (ret == -ERR_INVALID_TAG){
        ret = -EINVAL; //s3 returns only -EINVAL for PUT requests
      }

      return ret;
    }
  }

  //handle object lock
  auto obj_lock_mode_str = s->info.env->get("HTTP_X_AMZ_OBJECT_LOCK_MODE");
  auto obj_lock_date_str = s->info.env->get("HTTP_X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE");
  auto obj_legal_hold_str = s->info.env->get("HTTP_X_AMZ_OBJECT_LOCK_LEGAL_HOLD");
  if (obj_lock_mode_str && obj_lock_date_str) {
    boost::optional<ceph::real_time> date = ceph::from_iso_8601(obj_lock_date_str);
    if (boost::none == date || ceph::real_clock::to_time_t(*date) <= ceph_clock_now()) {
        ret = -EINVAL;
        ldpp_dout(this,0) << "invalid x-amz-object-lock-retain-until-date value" << dendl;
        return ret;
    }
    if (strcmp(obj_lock_mode_str, "GOVERNANCE") != 0 && strcmp(obj_lock_mode_str, "COMPLIANCE") != 0) {
        ret = -EINVAL;
        ldpp_dout(this,0) << "invalid x-amz-object-lock-mode value" << dendl;
        return ret;
    }
    obj_retention = new RGWObjectRetention(obj_lock_mode_str, *date);
  } else if ((obj_lock_mode_str && !obj_lock_date_str) || (!obj_lock_mode_str && obj_lock_date_str)) {
    ret = -EINVAL;
    ldpp_dout(this,0) << "need both x-amz-object-lock-mode and x-amz-object-lock-retain-until-date " << dendl;
    return ret;
  }
  if (obj_legal_hold_str) {
    if (strcmp(obj_legal_hold_str, "ON") != 0 && strcmp(obj_legal_hold_str, "OFF") != 0) {
        ret = -EINVAL;
        ldpp_dout(this,0) << "invalid x-amz-object-lock-legal-hold value" << dendl;
        return ret;
    }
    obj_legal_hold = new RGWObjectLegalHold(obj_legal_hold_str);
  }
  if (!s->bucket->get_info().obj_lock_enabled() && (obj_retention || obj_legal_hold)) {
    ldpp_dout(this, 0) << "ERROR: object retention or legal hold can't be set if bucket object lock not configured" << dendl;
    ret = -ERR_INVALID_REQUEST;
    return ret;
  }
  multipart_upload_id = s->info.args.get("uploadId");
  multipart_part_str = s->info.args.get("partNumber");
  if (!multipart_part_str.empty()) {
    string err;
    multipart_part_num = strict_strtol(multipart_part_str.c_str(), 10, &err);
    if (!err.empty()) {
      ldpp_dout(s, 10) << "bad part number: " << multipart_part_str << ": " << err << dendl;
      return -EINVAL;
    }
  } else if (!multipart_upload_id.empty()) {
    ldpp_dout(s, 10) << "part number with no multipart upload id" << dendl;
    return -EINVAL;
  }

  append = s->info.args.exists("append");
  if (append) {
    string pos_str = s->info.args.get("position");
    string err;
    long long pos_tmp = strict_strtoll(pos_str.c_str(), 10, &err);
    if (!err.empty()) {
      ldpp_dout(s, 10) << "bad position: " << pos_str << ": " << err << dendl;
      return -EINVAL;
    } else if (pos_tmp < 0) {
      ldpp_dout(s, 10) << "bad position: " << pos_str << ": " << "position shouldn't be negative" << dendl;
      return -EINVAL;
    }
    position = uint64_t(pos_tmp);
  }

  return RGWPutObj_ObjStore::get_params(y);
}

int RGWPutObj_ObjStore_S3::get_data(bufferlist& bl)
{
  const int ret = RGWPutObj_ObjStore::get_data(bl);
  if (ret == 0) {
    const int ret_auth = do_aws4_auth_completion();
    if (ret_auth < 0) {
      return ret_auth;
    }
  }

  return ret;
}

static int get_success_retcode(int code)
{
  switch (code) {
    case 201:
      return STATUS_CREATED;
    case 204:
      return STATUS_NO_CONTENT;
  }
  return 0;
}

void RGWPutObj_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
    dump_errno(s);
  } else {
    if (s->cct->_conf->rgw_s3_success_create_obj_status) {
      op_ret = get_success_retcode(
	s->cct->_conf->rgw_s3_success_create_obj_status);
      set_req_state_err(s, op_ret);
    }

    string expires = get_s3_expiration_header(s, mtime);

    if (copy_source.empty()) {
      dump_errno(s);
      dump_etag(s, etag);
      dump_content_length(s, 0);
      dump_header_if_nonempty(s, "x-amz-version-id", version_id);
      dump_header_if_nonempty(s, "x-amz-expiration", expires);
      for (auto &it : crypt_http_responses)
        dump_header(s, it.first, it.second);
    } else {
      dump_errno(s);
      dump_header_if_nonempty(s, "x-amz-version-id", version_id);
      dump_header_if_nonempty(s, "x-amz-expiration", expires);
      end_header(s, this, to_mime_type(s->format));
      dump_start(s);
      struct tm tmp;
      utime_t ut(mtime);
      time_t secs = (time_t)ut.sec();
      gmtime_r(&secs, &tmp);
      char buf[TIME_BUF_SIZE];
      s->formatter->open_object_section_in_ns("CopyPartResult",
          "http://s3.amazonaws.com/doc/2006-03-01/");
      if (strftime(buf, sizeof(buf), "%Y-%m-%dT%T.000Z", &tmp) > 0) {
        s->formatter->dump_string("LastModified", buf);
      }
      s->formatter->dump_string("ETag", etag);
      s->formatter->close_section();
      rgw_flush_formatter_and_reset(s, s->formatter);
      return;
    }
  }
  if (append) {
    if (op_ret == 0 || op_ret == -ERR_POSITION_NOT_EQUAL_TO_LENGTH) {
      dump_header(s, "x-rgw-next-append-position", cur_accounted_size);
    }
  }
  if (s->system_request && !real_clock::is_zero(mtime)) {
    dump_epoch_header(s, "Rgwx-Mtime", mtime);
  }
  end_header(s, this);
}

static inline void set_attr(map<string, bufferlist>& attrs, const char* key, const std::string& value)
{
  bufferlist bl;
  encode(value,bl);
  attrs.emplace(key, std::move(bl));
}

static inline void set_attr(map<string, bufferlist>& attrs, const char* key, const char* value)
{
  bufferlist bl;
  encode(value,bl);
  attrs.emplace(key, std::move(bl));
}

int RGWPutObj_ObjStore_S3::get_decrypt_filter(
    std::unique_ptr<RGWGetObj_Filter>* filter,
    RGWGetObj_Filter* cb,
    map<string, bufferlist>& attrs,
    bufferlist* manifest_bl)
{
  std::map<std::string, std::string> crypt_http_responses_unused;

  std::unique_ptr<BlockCrypt> block_crypt;
  int res = rgw_s3_prepare_decrypt(s, s->yield, attrs, &block_crypt,
                                   crypt_http_responses_unused);
  if (res < 0) {
    return res;
  }
  if (block_crypt == nullptr) {
    return 0;
  }

  // in case of a multipart upload, we need to know the part lengths to
  // correctly decrypt across part boundaries
  std::vector<size_t> parts_len;

  // for replicated objects, the original part lengths are preserved in an xattr
  if (auto i = attrs.find(RGW_ATTR_CRYPT_PARTS); i != attrs.end()) {
    try {
      auto p = i->second.cbegin();
      using ceph::decode;
      decode(parts_len, p);
    } catch (const buffer::error&) {
      ldpp_dout(this, 1) << "failed to decode RGW_ATTR_CRYPT_PARTS" << dendl;
      return -EIO;
    }
  } else if (manifest_bl) {
    // otherwise, we read the part lengths from the manifest
    res = RGWGetObj_BlockDecrypt::read_manifest_parts(this, *manifest_bl,
                                                      parts_len);
    if (res < 0) {
      return res;
    }
  }

  *filter = std::make_unique<RGWGetObj_BlockDecrypt>(
      s, s->cct, cb, std::move(block_crypt),
      std::move(parts_len), s->yield);
  return 0;
}

int RGWPutObj_ObjStore_S3::get_encrypt_filter(
    std::unique_ptr<rgw::sal::DataProcessor> *filter,
    rgw::sal::DataProcessor *cb)
{
  int res = 0;
  if (!multipart_upload_id.empty()) {
    std::unique_ptr<rgw::sal::MultipartUpload> upload =
      s->bucket->get_multipart_upload(s->object->get_name(),
				  multipart_upload_id);
    std::unique_ptr<rgw::sal::Object> obj = upload->get_meta_obj();
    obj->set_in_extra_data(true);
    res = obj->get_obj_attrs(s->yield, this);
    if (res == 0) {
      std::unique_ptr<BlockCrypt> block_crypt;
      /* We are adding to existing object.
       * We use crypto mode that configured as if we were decrypting. */
      res = rgw_s3_prepare_decrypt(s, s->yield, obj->get_attrs(),
                                   &block_crypt, crypt_http_responses);
      if (res == 0 && block_crypt != nullptr)
        filter->reset(new RGWPutObj_BlockEncrypt(s, s->cct, cb, std::move(block_crypt), s->yield));
    }
    /* it is ok, to not have encryption at all */
  }
  else
  {
    std::unique_ptr<BlockCrypt> block_crypt;
    res = rgw_s3_prepare_encrypt(s, s->yield, attrs, &block_crypt,
                                 crypt_http_responses);
    if (res == 0 && block_crypt != nullptr) {
      filter->reset(new RGWPutObj_BlockEncrypt(s, s->cct, cb, std::move(block_crypt), s->yield));
    }
  }
  return res;
}

void RGWPostObj_ObjStore_S3::rebuild_key(rgw::sal::Object* obj)
{
  string key = obj->get_name();
  static string var = "${filename}";
  int pos = key.find(var);
  if (pos < 0)
    return;

  string new_key = key.substr(0, pos);
  new_key.append(filename);
  new_key.append(key.substr(pos + var.size()));

  obj->set_key(new_key);
}

std::string RGWPostObj_ObjStore_S3::get_current_filename() const
{
  return s->object->get_name();
}

std::string RGWPostObj_ObjStore_S3::get_current_content_type() const
{
  return content_type;
}

int RGWPostObj_ObjStore_S3::get_params(optional_yield y)
{
  op_ret = RGWPostObj_ObjStore::get_params(y);
  if (op_ret < 0) {
    return op_ret;
  }

  map_qs_metadata(s, false);

  bool done;
  do {
    struct post_form_part part;
    int r = read_form_part_header(&part, done);
    if (r < 0)
      return r;

    if (s->cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
      ldpp_dout(this, 20) << "read part header -- part.name="
                        << part.name << dendl;

      for (const auto& pair : part.fields) {
        ldpp_dout(this, 20) << "field.name=" << pair.first << dendl;
        ldpp_dout(this, 20) << "field.val=" << pair.second.val << dendl;
        ldpp_dout(this, 20) << "field.params:" << dendl;

        for (const auto& param_pair : pair.second.params) {
          ldpp_dout(this, 20) << " " << param_pair.first
                            << " -> " << param_pair.second << dendl;
        }
      }
    }

    if (done) { /* unexpected here */
      err_msg = "Malformed request";
      return -EINVAL;
    }

    if (stringcasecmp(part.name, "file") == 0) { /* beginning of data transfer */
      struct post_part_field& field = part.fields["Content-Disposition"];
      map<string, string>::iterator iter = field.params.find("filename");
      if (iter != field.params.end()) {
	filename = iter->second;
      }
      parts[part.name] = part;
      break;
    }

    bool boundary;
    uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
    r = read_data(part.data, chunk_size, boundary, done);
    if (r < 0 || !boundary) {
      err_msg = "Couldn't find boundary";
      return -EINVAL;
    }
    parts[part.name] = part;
    string part_str(part.data.c_str(), part.data.length());
    env.add_var(part.name, part_str);
  } while (!done);

  for (auto &p: parts) {
    if (! boost::istarts_with(p.first, "x-amz-server-side-encryption")) {
      continue;
    }
    bufferlist &d { p.second.data };
    std::string v { rgw_trim_whitespace(std::string_view(d.c_str(), d.length())) };
    rgw_set_amz_meta_header(s->info.crypt_attribute_map, p.first, v, OVERWRITE);
  }
  int r = get_encryption_defaults(s);
  if (r < 0) {
    ldpp_dout(this, 5) << __func__ << "(): get_encryption_defaults() returned ret=" << r << dendl;
    return r;
  }

  ldpp_dout(this, 20) << "adding bucket to policy env: " << s->bucket->get_name()
		    << dendl;
  env.add_var("bucket", s->bucket->get_name());

  string object_str;
  if (!part_str(parts, "key", &object_str)) {
    err_msg = "Key not specified";
    return -EINVAL;
  }

  s->object = s->bucket->get_object(rgw_obj_key(object_str));

  rebuild_key(s->object.get());

  if (rgw::sal::Object::empty(s->object.get())) {
    err_msg = "Empty object name";
    return -EINVAL;
  }

  env.add_var("key", s->object->get_name());

  part_str(parts, "Content-Type", &content_type);

  /* AWS permits POST without Content-Type: http://tracker.ceph.com/issues/20201 */
  if (! content_type.empty()) {
    env.add_var("Content-Type", content_type);
  }

  std::string storage_class;
  part_str(parts, "x-amz-storage-class", &storage_class);

  if (! storage_class.empty()) {
    s->dest_placement.storage_class = storage_class;
    if (!driver->valid_placement(s->dest_placement)) {
      ldpp_dout(this, 0) << "NOTICE: invalid dest placement: " << s->dest_placement.to_str() << dendl;
      err_msg = "The storage class you specified is not valid";
      return -EINVAL;
    }
  }

  map<string, struct post_form_part, ltstr_nocase>::iterator piter =
    parts.upper_bound(RGW_AMZ_META_PREFIX);
  for (; piter != parts.end(); ++piter) {
    string n = piter->first;
    if (strncasecmp(n.c_str(), RGW_AMZ_META_PREFIX,
		    sizeof(RGW_AMZ_META_PREFIX) - 1) != 0)
      break;

    string attr_name = RGW_ATTR_PREFIX;
    attr_name.append(n);

    /* need to null terminate it */
    bufferlist& data = piter->second.data;
    string str = string(data.c_str(), data.length());

    bufferlist attr_bl;
    attr_bl.append(str.c_str(), str.size() + 1);

    attrs[attr_name] = attr_bl;
  }
  // TODO: refactor this and the above loop to share code
  piter = parts.find(RGW_AMZ_WEBSITE_REDIRECT_LOCATION);
  if (piter != parts.end()) {
    string n = piter->first;
    string attr_name = RGW_ATTR_PREFIX;
    attr_name.append(n);
    /* need to null terminate it */
    bufferlist& data = piter->second.data;
    string str = string(data.c_str(), data.length());

    bufferlist attr_bl;
    attr_bl.append(str.c_str(), str.size() + 1);

    attrs[attr_name] = attr_bl;
  }

  r = get_policy(y);
  if (r < 0)
    return r;

  r = get_tags();
  if (r < 0)
    return r;


  min_len = post_policy.min_length;
  max_len = post_policy.max_length;



  return 0;
}

int RGWPostObj_ObjStore_S3::get_tags()
{
  string tags_str;
  if (part_str(parts, "tagging", &tags_str)) {
    RGWXMLParser parser;
    if (!parser.init()){
      ldpp_dout(this, 0) << "Couldn't init RGWObjTags XML parser" << dendl;
      err_msg = "Server couldn't process the request";
      return -EINVAL; // TODO: This class of errors in rgw code should be a 5XX error
    }
    if (!parser.parse(tags_str.c_str(), tags_str.size(), 1)) {
      ldpp_dout(this,0 ) << "Invalid Tagging XML" << dendl;
      err_msg = "Invalid Tagging XML";
      return -EINVAL;
    }

    RGWObjTagging_S3 tagging;

    try {
      RGWXMLDecoder::decode_xml("Tagging", tagging, &parser);
    } catch (RGWXMLDecoder::err& err) {
      ldpp_dout(this, 5) << "Malformed tagging request: " << err << dendl;
      return -EINVAL;
    }

    RGWObjTags obj_tags;
    int r = tagging.rebuild(obj_tags);
    if (r < 0)
      return r;

    bufferlist tags_bl;
    obj_tags.encode(tags_bl);
    ldpp_dout(this, 20) << "Read " << obj_tags.count() << "tags" << dendl;
    attrs[RGW_ATTR_TAGS] = tags_bl;
  }


  return 0;
}

int RGWPostObj_ObjStore_S3::get_policy(optional_yield y)
{
  if (part_bl(parts, "policy", &s->auth.s3_postobj_creds.encoded_policy)) {
    bool aws4_auth = false;

    /* x-amz-algorithm handling */
    using rgw::auth::s3::AWS4_HMAC_SHA256_STR;
    if ((part_str(parts, "x-amz-algorithm", &s->auth.s3_postobj_creds.x_amz_algorithm)) &&
        (s->auth.s3_postobj_creds.x_amz_algorithm == AWS4_HMAC_SHA256_STR)) {
      ldpp_dout(this, 0) << "Signature verification algorithm AWS v4 (AWS4-HMAC-SHA256)" << dendl;
      aws4_auth = true;
    } else {
      ldpp_dout(this, 0) << "Signature verification algorithm AWS v2" << dendl;
    }

    // check that the signature matches the encoded policy
    if (aws4_auth) {
      /* AWS4 */

      /* x-amz-credential handling */
      if (!part_str(parts, "x-amz-credential",
                    &s->auth.s3_postobj_creds.x_amz_credential)) {
        ldpp_dout(this, 0) << "No S3 aws4 credential found!" << dendl;
        err_msg = "Missing aws4 credential";
        return -EINVAL;
      }

      /* x-amz-signature handling */
      if (!part_str(parts, "x-amz-signature",
                    &s->auth.s3_postobj_creds.signature)) {
        ldpp_dout(this, 0) << "No aws4 signature found!" << dendl;
        err_msg = "Missing aws4 signature";
        return -EINVAL;
      }

      /* x-amz-date handling */
      std::string received_date_str;
      if (!part_str(parts, "x-amz-date", &received_date_str)) {
        ldpp_dout(this, 0) << "No aws4 date found!" << dendl;
        err_msg = "Missing aws4 date";
        return -EINVAL;
      }
    } else {
      /* AWS2 */

      // check that the signature matches the encoded policy
      if (!part_str(parts, "AWSAccessKeyId",
                    &s->auth.s3_postobj_creds.access_key)) {
        ldpp_dout(this, 0) << "No S3 aws2 access key found!" << dendl;
        err_msg = "Missing aws2 access key";
        return -EINVAL;
      }

      if (!part_str(parts, "signature", &s->auth.s3_postobj_creds.signature)) {
        ldpp_dout(this, 0) << "No aws2 signature found!" << dendl;
        err_msg = "Missing aws2 signature";
        return -EINVAL;
      }
    }

    if (part_str(parts, "x-amz-security-token", &s->auth.s3_postobj_creds.x_amz_security_token)) {
      if (s->auth.s3_postobj_creds.x_amz_security_token.size() == 0) {
        err_msg = "Invalid token";
        return -EINVAL;
      }
    }

    /* FIXME: this is a makeshift solution. The browser upload authentication will be
     * handled by an instance of rgw::auth::Completer spawned in Handler's authorize()
     * method. */
    const int ret = rgw::auth::Strategy::apply(this, auth_registry_ptr->get_s3_post(), s, y);
    if (ret != 0) {
      return -EACCES;
    } else {
      /* Populate the owner info. */
      s->owner.id = s->user->get_id();
      s->owner.display_name = s->user->get_display_name();
      ldpp_dout(this, 20) << "Successful Signature Verification!" << dendl;
    }

    ceph::bufferlist decoded_policy;
    try {
      decoded_policy.decode_base64(s->auth.s3_postobj_creds.encoded_policy);
    } catch (buffer::error& err) {
      ldpp_dout(this, 0) << "failed to decode_base64 policy" << dendl;
      err_msg = "Could not decode policy";
      return -EINVAL;
    }

    decoded_policy.append('\0'); // NULL terminate
    ldpp_dout(this, 20) << "POST policy: " << decoded_policy.c_str() << dendl;


    int r = post_policy.from_json(decoded_policy, err_msg);
    if (r < 0) {
      if (err_msg.empty()) {
	err_msg = "Failed to parse policy";
      }
      ldpp_dout(this, 0) << "failed to parse policy" << dendl;
      return -EINVAL;
    }

    if (aws4_auth) {
      /* AWS4 */
      post_policy.set_var_checked("x-amz-signature");
    } else {
      /* AWS2 */
      post_policy.set_var_checked("AWSAccessKeyId");
      post_policy.set_var_checked("signature");
    }
    post_policy.set_var_checked("policy");

    r = post_policy.check(&env, err_msg);
    if (r < 0) {
      if (err_msg.empty()) {
	err_msg = "Policy check failed";
      }
      ldpp_dout(this, 0) << "policy check failed" << dendl;
      return r;
    }

  } else {
    ldpp_dout(this, 0) << "No attached policy found!" << dendl;
  }

  string canned_acl;
  part_str(parts, "acl", &canned_acl);

  ldpp_dout(this, 20) << "canned_acl=" << canned_acl << dendl;
  int r = rgw::s3::create_canned_acl(s->owner, s->bucket_owner,
                                     canned_acl, policy);
  if (r < 0) {
    err_msg = "Bad canned ACLs";
    return r;
  }

  return 0;
}

int RGWPostObj_ObjStore_S3::complete_get_params()
{
  bool done;
  do {
    struct post_form_part part;
    int r = read_form_part_header(&part, done);
    if (r < 0) {
      return r;
    }

    ceph::bufferlist part_data;
    bool boundary;
    uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
    r = read_data(part.data, chunk_size, boundary, done);
    if (r < 0 || !boundary) {
      return -EINVAL;
    }

    /* Just reading the data but not storing any results of that. */
  } while (!done);

  return 0;
}

int RGWPostObj_ObjStore_S3::get_data(ceph::bufferlist& bl, bool& again)
{
  bool boundary;
  bool done;

  const uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
  int r = read_data(bl, chunk_size, boundary, done);
  if (r < 0) {
    return r;
  }

  if (boundary) {
    if (!done) {
      /* Reached end of data, let's drain the rest of the params */
      r = complete_get_params();
      if (r < 0) {
       return r;
      }
    }
  }

  again = !boundary;
  return bl.length();
}

void RGWPostObj_ObjStore_S3::send_response()
{
  if (op_ret == 0 && parts.count("success_action_redirect")) {
    string redirect;

    part_str(parts, "success_action_redirect", &redirect);

    string tenant;
    string bucket;
    string key;
    string etag_str = "\"";

    etag_str.append(etag);
    etag_str.append("\"");

    string etag_url;

    url_encode(s->bucket_tenant, tenant); /* surely overkill, but cheap */
    url_encode(s->bucket_name, bucket);
    url_encode(s->object->get_name(), key);
    url_encode(etag_str, etag_url);

    if (!s->bucket_tenant.empty()) {
      /*
       * What we really would like is to quaily the bucket name, so
       * that the client could simply copy it and paste into next request.
       * Unfortunately, in S3 we cannot know if the client will decide
       * to come through DNS, with "bucket.tenant" syntax, or through
       * URL with "tenant\bucket" syntax. Therefore, we provide the
       * tenant separately.
       */
      redirect.append("?tenant=");
      redirect.append(tenant);
      redirect.append("&bucket=");
      redirect.append(bucket);
    } else {
      redirect.append("?bucket=");
      redirect.append(bucket);
    }
    redirect.append("&key=");
    redirect.append(key);
    redirect.append("&etag=");
    redirect.append(etag_url);

    int r = check_utf8(redirect.c_str(), redirect.size());
    if (r < 0) {
      op_ret = r;
      goto done;
    }
    dump_redirect(s, redirect);
    op_ret = STATUS_REDIRECT;
  } else if (op_ret == 0 && parts.count("success_action_status")) {
    string status_string;
    uint32_t status_int;

    part_str(parts, "success_action_status", &status_string);

    int r = stringtoul(status_string, &status_int);
    if (r < 0) {
      op_ret = r;
      goto done;
    }

    switch (status_int) {
      case 200:
	break;
      case 201:
	op_ret = STATUS_CREATED;
	break;
      default:
	op_ret = STATUS_NO_CONTENT;
	break;
    }
  } else if (! op_ret) {
    op_ret = STATUS_NO_CONTENT;
  }

done:
  if (op_ret == STATUS_CREATED) {
    for (auto &it : crypt_http_responses)
      dump_header(s, it.first, it.second);
    s->formatter->open_object_section("PostResponse");
    std::string base_uri = compute_domain_uri(s);
    if (!s->bucket_tenant.empty()){
      s->formatter->dump_format("Location", "%s/%s:%s/%s",
                                base_uri.c_str(),
                                url_encode(s->bucket_tenant).c_str(),
                                url_encode(s->bucket_name).c_str(),
                                url_encode(s->object->get_name()).c_str());
      s->formatter->dump_string("Tenant", s->bucket_tenant);
    } else {
      s->formatter->dump_format("Location", "%s/%s/%s",
                                base_uri.c_str(),
                                url_encode(s->bucket_name).c_str(),
                                url_encode(s->object->get_name()).c_str());
    }
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object->get_name());
    s->formatter->dump_string("ETag", etag);
    s->formatter->close_section();
  }
  s->err.message = err_msg;
  set_req_state_err(s, op_ret);
  dump_errno(s);
  if (op_ret >= 0) {
    dump_content_length(s, s->formatter->get_len());
  }
  if (op_ret == STATUS_NO_CONTENT) {
    dump_etag(s, etag);
  }
  end_header(s, this);
  if (op_ret != STATUS_CREATED)
    return;

  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWPostObj_ObjStore_S3::get_encrypt_filter(
    std::unique_ptr<rgw::sal::DataProcessor> *filter,
    rgw::sal::DataProcessor *cb)
{
  std::unique_ptr<BlockCrypt> block_crypt;
  int res = rgw_s3_prepare_encrypt(s, s->yield, attrs, &block_crypt,
                                   crypt_http_responses);
  if (res == 0 && block_crypt != nullptr) {
    filter->reset(new RGWPutObj_BlockEncrypt(s, s->cct, cb, std::move(block_crypt), s->yield));
  }
  return res;
}

int RGWDeleteObj_ObjStore_S3::get_params(optional_yield y)
{
  const char *if_unmod = s->info.env->get("HTTP_X_AMZ_DELETE_IF_UNMODIFIED_SINCE");

  if (s->system_request) {
    s->info.args.get_bool(RGW_SYS_PARAM_PREFIX "no-precondition-error", &no_precondition_error, false);
  }

  if (if_unmod) {
    std::string if_unmod_decoded = url_decode(if_unmod);
    uint64_t epoch;
    uint64_t nsec;
    if (utime_t::parse_date(if_unmod_decoded, &epoch, &nsec) < 0) {
      ldpp_dout(this, 10) << "failed to parse time: " << if_unmod_decoded << dendl;
      return -EINVAL;
    }
    unmod_since = utime_t(epoch, nsec).to_real_time();
  }

  const char *bypass_gov_header = s->info.env->get("HTTP_X_AMZ_BYPASS_GOVERNANCE_RETENTION");
  if (bypass_gov_header) {
    std::string bypass_gov_decoded = url_decode(bypass_gov_header);
    bypass_governance_mode = boost::algorithm::iequals(bypass_gov_decoded, "true");
  }

  return 0;
}

void RGWDeleteObj_ObjStore_S3::send_response()
{
  int r = op_ret;
  if (r == -ENOENT)
    r = 0;
  if (!r)
    r = STATUS_NO_CONTENT;

  set_req_state_err(s, r);
  dump_errno(s);
  dump_header_if_nonempty(s, "x-amz-version-id", version_id);
  if (delete_marker) {
    dump_header(s, "x-amz-delete-marker", "true");
  }
  end_header(s, this);
}

int RGWCopyObj_ObjStore_S3::init_dest_policy()
{
  /* build a policy for the target object */
  return create_s3_policy(s, driver, dest_policy, s->owner);
}

int RGWCopyObj_ObjStore_S3::get_params(optional_yield y)
{
  //handle object lock
  auto obj_lock_mode_str = s->info.env->get("HTTP_X_AMZ_OBJECT_LOCK_MODE");
  auto obj_lock_date_str = s->info.env->get("HTTP_X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE");
  auto obj_legal_hold_str = s->info.env->get("HTTP_X_AMZ_OBJECT_LOCK_LEGAL_HOLD");
  if (obj_lock_mode_str && obj_lock_date_str) {
    boost::optional<ceph::real_time> date = ceph::from_iso_8601(obj_lock_date_str);
    if (boost::none == date || ceph::real_clock::to_time_t(*date) <= ceph_clock_now()) {
      s->err.message = "invalid x-amz-object-lock-retain-until-date value";
      ldpp_dout(this,0) << s->err.message << dendl;
      return -EINVAL;
    }
    if (strcmp(obj_lock_mode_str, "GOVERNANCE") != 0 && strcmp(obj_lock_mode_str, "COMPLIANCE") != 0) {
      s->err.message = "invalid x-amz-object-lock-mode value";
      ldpp_dout(this,0) << s->err.message << dendl;
      return -EINVAL;
    }
    obj_retention = new RGWObjectRetention(obj_lock_mode_str, *date);
  } else if (obj_lock_mode_str || obj_lock_date_str) {
    s->err.message = "need both x-amz-object-lock-mode and x-amz-object-lock-retain-until-date ";
    ldpp_dout(this,0) << s->err.message << dendl;
    return -EINVAL;
  }
  if (obj_legal_hold_str) {
    if (strcmp(obj_legal_hold_str, "ON") != 0 && strcmp(obj_legal_hold_str, "OFF") != 0) {
      s->err.message = "invalid x-amz-object-lock-legal-hold value";
      ldpp_dout(this,0) << s->err.message << dendl;
      return -EINVAL;
    }
    obj_legal_hold = new RGWObjectLegalHold(obj_legal_hold_str);
  }

  if_mod = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE");
  if_unmod = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE");
  if_match = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE_IF_MATCH");
  if_nomatch = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE_IF_NONE_MATCH");

  if (s->system_request) {
    source_zone = s->info.args.get(RGW_SYS_PARAM_PREFIX "source-zone");
    s->info.args.get_bool(RGW_SYS_PARAM_PREFIX "copy-if-newer", &copy_if_newer, false);
  }

  const char *copy_source_temp = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE");
  if (copy_source_temp) {
    copy_source = copy_source_temp;
  }
  auto tmp_md_d = s->info.env->get("HTTP_X_AMZ_METADATA_DIRECTIVE");
  if (tmp_md_d) {
    if (strcasecmp(tmp_md_d, "COPY") == 0) {
      attrs_mod = rgw::sal::ATTRSMOD_NONE;
    } else if (strcasecmp(tmp_md_d, "REPLACE") == 0) {
      attrs_mod = rgw::sal::ATTRSMOD_REPLACE;
    } else if (!source_zone.empty()) {
      attrs_mod = rgw::sal::ATTRSMOD_NONE; // default for intra-zone_group copy
    } else {
      s->err.message = "Unknown metadata directive.";
      ldpp_dout(this, 0) << s->err.message << dendl;
      return -EINVAL;
    }
    md_directive = tmp_md_d;
  }

  if (source_zone.empty() &&
      (s->bucket->get_tenant() == s->src_tenant_name) &&
      (s->bucket->get_name() == s->src_bucket_name) &&
      (s->object->get_name() == s->src_object->get_name()) &&
      s->src_object->get_instance().empty() &&
      (attrs_mod != rgw::sal::ATTRSMOD_REPLACE)) {
    need_to_check_storage_class = true;
  }

  return 0;
}

int RGWCopyObj_ObjStore_S3::check_storage_class(const rgw_placement_rule& src_placement)
{
  if (src_placement == s->dest_placement) {
    /* can only copy object into itself if replacing attrs */
    s->err.message = "This copy request is illegal because it is trying to copy "
      "an object to itself without changing the object's metadata, "
      "storage class, website redirect location or encryption attributes.";
    ldpp_dout(this, 0) << s->err.message << dendl;
    return -ERR_INVALID_REQUEST;
  }
  return 0;
}

void RGWCopyObj_ObjStore_S3::send_partial_response(off_t ofs)
{
  if (! sent_header) {
    if (op_ret)
    set_req_state_err(s, op_ret);
    dump_errno(s);

    // Explicitly use chunked transfer encoding so that we can stream the result
    // to the user without having to wait for the full length of it.
    end_header(s, this, to_mime_type(s->format), CHUNKED_TRANSFER_ENCODING);
    dump_start(s);
    if (op_ret == 0) {
      s->formatter->open_object_section_in_ns("CopyObjectResult", XMLNS_AWS_S3);
    }
    sent_header = true;
  } else {
    /* Send progress field. Note that this diverge from the original S3
     * spec. We do this in order to keep connection alive.
     */
    s->formatter->dump_int("Progress", (uint64_t)ofs);
  }
  rgw_flush_formatter(s, s->formatter);
}

void RGWCopyObj_ObjStore_S3::send_response()
{
  if (!sent_header)
    send_partial_response(0);

  if (op_ret == 0) {
    dump_time(s, "LastModified", mtime);
    if (!etag.empty()) {
      s->formatter->dump_format("ETag", "\"%s\"",etag.c_str());
    }
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

void RGWGetACLs_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);
  rgw_flush_formatter(s, s->formatter);
  dump_body(s, acls);
}

int RGWPutACLs_ObjStore_S3::get_params(optional_yield y)
{
  int ret =  RGWPutACLs_ObjStore::get_params(y);
  if (ret >= 0) {
    const int ret_auth = do_aws4_auth_completion();
    if (ret_auth < 0) {
      return ret_auth;
    }
  } else {
    /* a request body is not required an S3 PutACLs request--n.b.,
     * s->length is non-null iff a content length was parsed (the
     * ACP or canned ACL could be in any of 3 headers, don't worry
     * about that here) */
    if ((ret == -ERR_LENGTH_REQUIRED) &&
	!!(s->length)) {
      return 0;
    }
  }
  return ret;
}

int RGWPutACLs_ObjStore_S3::get_policy_from_state(const ACLOwner& owner,
                                                  RGWAccessControlPolicy& policy)
{
  // bucket-* canned acls do not apply to bucket
  if (rgw::sal::Object::empty(s->object.get())) {
    if (s->canned_acl.find("bucket") != string::npos)
      s->canned_acl.clear();
  }

  return create_s3_policy(s, driver, policy, owner);
}

void RGWPutACLs_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);
}

void RGWGetLC_ObjStore_S3::execute(optional_yield y)
{
  config.set_ctx(s->cct);

  map<string, bufferlist>::iterator aiter = s->bucket_attrs.find(RGW_ATTR_LC);
  if (aiter == s->bucket_attrs.end()) {
    op_ret = -ENOENT;
    return;
  }

  bufferlist::const_iterator iter{&aiter->second};
  try {
      config.decode(iter);
    } catch (const buffer::error& e) {
      ldpp_dout(this, 0) << __func__ <<  "decode life cycle config failed" << dendl;
      op_ret = -EIO;
      return;
    }
}

void RGWGetLC_ObjStore_S3::send_response()
{
  if (op_ret) {
    if (op_ret == -ENOENT) {
      set_req_state_err(s, ERR_NO_SUCH_LC);
    } else {
      set_req_state_err(s, op_ret);
    }
  }
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

  if (op_ret < 0)
    return;

  encode_xml("LifecycleConfiguration", XMLNS_AWS_S3, config, s->formatter);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWPutLC_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);
}

void RGWDeleteLC_ObjStore_S3::send_response()
{
  if (op_ret == 0)
      op_ret = STATUS_NO_CONTENT;
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);
}

void RGWGetCORS_ObjStore_S3::send_response()
{
  if (op_ret) {
    if (op_ret == -ENOENT)
      set_req_state_err(s, ERR_NO_SUCH_CORS_CONFIGURATION);
    else
      set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, NULL, to_mime_type(s->format));
  dump_start(s);
  if (! op_ret) {
    string cors;
    RGWCORSConfiguration_S3 *s3cors =
      static_cast<RGWCORSConfiguration_S3 *>(&bucket_cors);
    stringstream ss;

    s3cors->to_xml(ss);
    cors = ss.str();
    dump_body(s, cors);
  }
}

int RGWPutCORS_ObjStore_S3::get_params(optional_yield y)
{
  RGWCORSXMLParser_S3 parser(this, s->cct);
  RGWCORSConfiguration_S3 *cors_config;

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;

  int r = 0;
  bufferlist data;
  std::tie(r, data) = read_all_input(s, max_size, false);
  if (r < 0) {
    return r;
  }

  if (!parser.init()) {
    return -EINVAL;
  }

  char* buf = data.c_str();
  if (!buf || !parser.parse(buf, data.length(), 1)) {
    return -ERR_MALFORMED_XML;
  }
  cors_config =
    static_cast<RGWCORSConfiguration_S3 *>(parser.find_first(
					     "CORSConfiguration"));
  if (!cors_config) {
    return -ERR_MALFORMED_XML;
  }

#define CORS_RULES_MAX_NUM      100
  int max_num = s->cct->_conf->rgw_cors_rules_max_num;
  if (max_num < 0) {
    max_num = CORS_RULES_MAX_NUM;
  }
  int cors_rules_num = cors_config->get_rules().size();
  if (cors_rules_num > max_num) {
    ldpp_dout(this, 4) << "An cors config can have up to "
                     << max_num
                     << " rules, request cors rules num: "
                     << cors_rules_num << dendl;
    s->err.message = "The number of CORS rules should not exceed allowed limit of "
                     + std::to_string(max_num) + " rules.";
    return -ERR_INVALID_CORS_RULES_ERROR;
  }

  // forward bucket cors requests to meta master zone
  if (!driver->is_meta_master()) {
    /* only need to keep this data around if we're not meta master */
    in_data.append(data);
  }

  if (s->cct->_conf->subsys.should_gather<ceph_subsys_rgw, 15>()) {
    ldpp_dout(this, 15) << "CORSConfiguration";
    cors_config->to_xml(*_dout);
    *_dout << dendl;
  }

  cors_config->encode(cors_bl);

  return 0;
}

void RGWPutCORS_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, NULL, to_mime_type(s->format));
  dump_start(s);
}

void RGWDeleteCORS_ObjStore_S3::send_response()
{
  int r = op_ret;
  if (!r || r == -ENOENT)
    r = STATUS_NO_CONTENT;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s, NULL);
}

void RGWOptionsCORS_ObjStore_S3::send_response()
{
  string hdrs, exp_hdrs;
  uint32_t max_age = CORS_MAX_AGE_INVALID;
  /*EACCES means, there is no CORS registered yet for the bucket
   *ENOENT means, there is no match of the Origin in the list of CORSRule
   */
  if (op_ret == -ENOENT)
    op_ret = -EACCES;
  if (op_ret < 0) {
    set_req_state_err(s, op_ret);
    dump_errno(s);
    end_header(s, NULL);
    return;
  }
  get_response_params(hdrs, exp_hdrs, &max_age);

  dump_errno(s);
  dump_access_control(s, origin, req_meth, hdrs.c_str(), exp_hdrs.c_str(),
		      max_age);
  end_header(s, NULL);
}

void RGWPutBucketEncryption_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

void RGWGetBucketEncryption_ObjStore_S3::send_response()
{
  if (op_ret) {
    if (op_ret == -ENOENT)
      set_req_state_err(s, ERR_NO_SUCH_BUCKET_ENCRYPTION_CONFIGURATION);
    else
      set_req_state_err(s, op_ret);
  }

  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

  if (!op_ret) {
    encode_xml("ServerSideEncryptionConfiguration", XMLNS_AWS_S3,
      bucket_encryption_conf, s->formatter);
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

void RGWDeleteBucketEncryption_ObjStore_S3::send_response()
{
  if (op_ret == 0) {
    op_ret = STATUS_NO_CONTENT;
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s);
}

void RGWGetRequestPayment_ObjStore_S3::send_response()
{
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

  s->formatter->open_object_section_in_ns("RequestPaymentConfiguration", XMLNS_AWS_S3);
  const char *payer = requester_pays ? "Requester" :  "BucketOwner";
  s->formatter->dump_string("Payer", payer);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

class RGWSetRequestPaymentParser : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el) override {
    return new XMLObj;
  }

public:
  RGWSetRequestPaymentParser() {}
  ~RGWSetRequestPaymentParser() override {}

  int get_request_payment_payer(bool *requester_pays) {
    XMLObj *config = find_first("RequestPaymentConfiguration");
    if (!config)
      return -EINVAL;

    *requester_pays = false;

    XMLObj *field = config->find_first("Payer");
    if (!field)
      return 0;

    auto& s = field->get_data();

    if (stringcasecmp(s, "Requester") == 0) {
      *requester_pays = true;
    } else if (stringcasecmp(s, "BucketOwner") != 0) {
      return -EINVAL;
    }

    return 0;
  }
};

int RGWSetRequestPayment_ObjStore_S3::get_params(optional_yield y)
{
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;

  int r = 0;
  std::tie(r, in_data) = read_all_input(s, max_size, false);

  if (r < 0) {
    return r;
  }


  RGWSetRequestPaymentParser parser;

  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize parser" << dendl;
    return -EIO;
  }

  char* buf = in_data.c_str();
  if (!parser.parse(buf, in_data.length(), 1)) {
    ldpp_dout(this, 10) << "failed to parse data: " << buf << dendl;
    return -EINVAL;
  }

  return parser.get_request_payment_payer(&requester_pays);
}

void RGWSetRequestPayment_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s);
}

int RGWInitMultipart_ObjStore_S3::get_params(optional_yield y)
{
  int ret;

  ret = get_encryption_defaults(s);
  if (ret < 0) {
    ldpp_dout(this, 5) << __func__ << "(): get_encryption_defaults() returned ret=" << ret << dendl;
    return ret;
  }

  return create_s3_policy(s, driver, policy, s->owner);
}

void RGWInitMultipart_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  for (auto &it : crypt_http_responses)
     dump_header(s, it.first, it.second);
  ceph::real_time abort_date;
  string rule_id;
  bool exist_multipart_abort = get_s3_multipart_abort_header(s, mtime, abort_date, rule_id);
  if (exist_multipart_abort) {
    dump_time_header(s, "x-amz-abort-date", abort_date);
    dump_header_if_nonempty(s, "x-amz-abort-rule-id", rule_id);
  }
  end_header(s, this, to_mime_type(s->format));
  if (op_ret == 0) {
    dump_start(s);
    s->formatter->open_object_section_in_ns("InitiateMultipartUploadResult", XMLNS_AWS_S3);
    if (!s->bucket_tenant.empty())
      s->formatter->dump_string("Tenant", s->bucket_tenant);
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object->get_name());
    s->formatter->dump_string("UploadId", upload_id);
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

int RGWInitMultipart_ObjStore_S3::prepare_encryption(map<string, bufferlist>& attrs)
{
  int res = 0;
  res = rgw_s3_prepare_encrypt(s, s->yield, attrs, nullptr, crypt_http_responses);
  return res;
}

int RGWCompleteMultipart_ObjStore_S3::get_params(optional_yield y)
{
  int ret = RGWCompleteMultipart_ObjStore::get_params(y);
  if (ret < 0) {
    return ret;
  }

  map_qs_metadata(s, true);

  return do_aws4_auth_completion();
}

void RGWCompleteMultipart_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  dump_header_if_nonempty(s, "x-amz-version-id", version_id);
  end_header(s, this, to_mime_type(s->format));
  if (op_ret == 0) {
    dump_start(s);
    s->formatter->open_object_section_in_ns("CompleteMultipartUploadResult", XMLNS_AWS_S3);
    std::string base_uri = compute_domain_uri(s);
    if (!s->bucket_tenant.empty()) {
      s->formatter->dump_format("Location", "%s/%s:%s/%s",
	  base_uri.c_str(),
	  s->bucket_tenant.c_str(),
	  s->bucket_name.c_str(),
	  s->object->get_name().c_str()
          );
      s->formatter->dump_string("Tenant", s->bucket_tenant);
    } else {
      s->formatter->dump_format("Location", "%s/%s/%s",
	  base_uri.c_str(),
	  s->bucket_name.c_str(),
	  s->object->get_name().c_str()
          );
    }
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object->get_name());
    s->formatter->dump_string("ETag", etag);
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

void RGWAbortMultipart_ObjStore_S3::send_response()
{
  int r = op_ret;
  if (!r)
    r = STATUS_NO_CONTENT;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s, this);
}

void RGWListMultipart_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  // Explicitly use chunked transfer encoding so that we can stream the result
  // to the user without having to wait for the full length of it.
  end_header(s, this, to_mime_type(s->format), CHUNKED_TRANSFER_ENCODING);

  if (op_ret == 0) {
    dump_start(s);
    s->formatter->open_object_section_in_ns("ListPartsResult", XMLNS_AWS_S3);
    map<uint32_t, std::unique_ptr<rgw::sal::MultipartPart>>::iterator iter;
    map<uint32_t, std::unique_ptr<rgw::sal::MultipartPart>>::reverse_iterator test_iter;
    int cur_max = 0;

    iter = upload->get_parts().begin();
    test_iter = upload->get_parts().rbegin();
    if (test_iter != upload->get_parts().rend()) {
      cur_max = test_iter->first;
    }
    if (!s->bucket_tenant.empty())
      s->formatter->dump_string("Tenant", s->bucket_tenant);
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object->get_name());
    s->formatter->dump_string("UploadId", upload_id);
    s->formatter->dump_string("StorageClass", placement->get_storage_class());
    s->formatter->dump_int("PartNumberMarker", marker);
    s->formatter->dump_int("NextPartNumberMarker", cur_max);
    s->formatter->dump_int("MaxParts", max_parts);
    s->formatter->dump_string("IsTruncated", (truncated ? "true" : "false"));

    ACLOwner& owner = policy.get_owner();
    dump_owner(s, owner.id, owner.display_name);

    for (; iter != upload->get_parts().end(); ++iter) {
      rgw::sal::MultipartPart* part = iter->second.get();

      s->formatter->open_object_section("Part");

      dump_time(s, "LastModified", part->get_mtime());

      s->formatter->dump_unsigned("PartNumber", part->get_num());
      s->formatter->dump_format("ETag", "\"%s\"", part->get_etag().c_str());
      s->formatter->dump_unsigned("Size", part->get_size());
      s->formatter->close_section();
    }
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

void RGWListBucketMultiparts_ObjStore_S3::send_response()
{
  if (op_ret < 0)
    set_req_state_err(s, op_ret);
  dump_errno(s);

  // Explicitly use chunked transfer encoding so that we can stream the result
  // to the user without having to wait for the full length of it.
  end_header(s, this, to_mime_type(s->format), CHUNKED_TRANSFER_ENCODING);
  dump_start(s);
  if (op_ret < 0)
    return;

  s->formatter->open_object_section_in_ns("ListMultipartUploadsResult", XMLNS_AWS_S3);
  if (!s->bucket_tenant.empty())
    s->formatter->dump_string("Tenant", s->bucket_tenant);
  s->formatter->dump_string("Bucket", s->bucket_name);
  if (!prefix.empty())
    s->formatter->dump_string("Prefix", prefix);
  if (!marker_key.empty())
    s->formatter->dump_string("KeyMarker", marker_key);
  if (!marker_upload_id.empty())
    s->formatter->dump_string("UploadIdMarker", marker_upload_id);
  if (!next_marker_key.empty())
    s->formatter->dump_string("NextKeyMarker", next_marker_key);
  if (!next_marker_upload_id.empty())
    s->formatter->dump_string("NextUploadIdMarker", next_marker_upload_id);
  s->formatter->dump_int("MaxUploads", max_uploads);
  if (!delimiter.empty())
    s->formatter->dump_string("Delimiter", delimiter);
  s->formatter->dump_string("IsTruncated", (is_truncated ? "true" : "false"));

  if (op_ret >= 0) {
    vector<std::unique_ptr<rgw::sal::MultipartUpload>>::iterator iter;
    for (iter = uploads.begin(); iter != uploads.end(); ++iter) {
      rgw::sal::MultipartUpload* upload = iter->get();
      s->formatter->open_array_section("Upload");
      dump_urlsafe(s, encode_url, "Key", upload->get_key(), false);
      s->formatter->dump_string("UploadId", upload->get_upload_id());
      const ACLOwner& owner = upload->get_owner();
      dump_owner(s, owner.id, owner.display_name, "Initiator");
      dump_owner(s, owner.id, owner.display_name); // Owner
      s->formatter->dump_string("StorageClass", "STANDARD");
      dump_time(s, "Initiated", upload->get_mtime());
      s->formatter->close_section();
    }
    if (!common_prefixes.empty()) {
      s->formatter->open_array_section("CommonPrefixes");
      for (const auto& kv : common_prefixes) {
        dump_urlsafe(s, encode_url, "Prefix", kv.first, false);
      }
      s->formatter->close_section();
    }
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWDeleteMultiObj_ObjStore_S3::get_params(optional_yield y)
{
  int ret = RGWDeleteMultiObj_ObjStore::get_params(y);
  if (ret < 0) {
    return ret;
  }

  const char *bypass_gov_header = s->info.env->get("HTTP_X_AMZ_BYPASS_GOVERNANCE_RETENTION");
  if (bypass_gov_header) {
    std::string bypass_gov_decoded = url_decode(bypass_gov_header);
    bypass_governance_mode = boost::algorithm::iequals(bypass_gov_decoded, "true");
  }

  return do_aws4_auth_completion();
}

void RGWDeleteMultiObj_ObjStore_S3::send_status()
{
  if (! status_dumped) {
    if (op_ret < 0)
      set_req_state_err(s, op_ret);
    dump_errno(s);
    status_dumped = true;
  }
}

void RGWDeleteMultiObj_ObjStore_S3::begin_response()
{

  if (!status_dumped) {
    send_status();
  }

  dump_start(s);
  // Explicitly use chunked transfer encoding so that we can stream the result
  // to the user without having to wait for the full length of it.
  end_header(s, this, to_mime_type(s->format), CHUNKED_TRANSFER_ENCODING);
  s->formatter->open_object_section_in_ns("DeleteResult", XMLNS_AWS_S3);

  rgw_flush_formatter(s, s->formatter);
}

void RGWDeleteMultiObj_ObjStore_S3::send_partial_response(const rgw_obj_key& key,
							  bool delete_marker,
							  const string& marker_version_id,
                                                          int ret,
                                                          boost::asio::deadline_timer *formatter_flush_cond)
{
  if (!key.empty()) {
    delete_multi_obj_entry ops_log_entry;
    ops_log_entry.key = key.name;
    ops_log_entry.version_id = key.instance;
    if (ret == 0) {
      ops_log_entry.error = false;
      ops_log_entry.http_status = 200;
      ops_log_entry.delete_marker = delete_marker;
      if (delete_marker) {
        ops_log_entry.marker_version_id = marker_version_id;
      }
      if (!quiet) {
        s->formatter->open_object_section("Deleted");
        s->formatter->dump_string("Key", key.name);
        if (!key.instance.empty()) {
            s->formatter->dump_string("VersionId", key.instance);
        }
        if (delete_marker) {
            s->formatter->dump_bool("DeleteMarker", true);
            s->formatter->dump_string("DeleteMarkerVersionId", marker_version_id);
        }
        s->formatter->close_section();
      }
    } else if (ret < 0) {
      struct rgw_http_error r;
      int err_no;

      s->formatter->open_object_section("Error");

      err_no = -ret;
      rgw_get_errno_s3(&r, err_no);

      ops_log_entry.error = true;
      ops_log_entry.http_status = r.http_ret;
      ops_log_entry.error_message = r.s3_code;

      s->formatter->dump_string("Key", key.name);
      s->formatter->dump_string("VersionId", key.instance);
      s->formatter->dump_string("Code", r.s3_code);
      s->formatter->dump_string("Message", r.s3_code);
      s->formatter->close_section();
    }

    ops_log_entries.push_back(std::move(ops_log_entry));
    if (formatter_flush_cond) {
      formatter_flush_cond->cancel();
    } else {
      rgw_flush_formatter(s, s->formatter);
    }
  }
}

void RGWDeleteMultiObj_ObjStore_S3::end_response()
{

  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWGetObjLayout_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/json");

  JSONFormatter f;

  if (op_ret < 0) {
    return;
  }

  f.open_object_section("result");
  s->object->dump_obj_layout(this, s->yield, &f);
  f.close_section();
  rgw_flush_formatter(s, &f);
}

int RGWConfigBucketMetaSearch_ObjStore_S3::get_params(optional_yield y)
{
  auto iter = s->info.x_meta_map.find("x-amz-meta-search");
  if (iter == s->info.x_meta_map.end()) {
    s->err.message = "X-Rgw-Meta-Search header not provided";
    ldpp_dout(this, 5) << s->err.message << dendl;
    return -EINVAL;
  }

  list<string> expressions;
  get_str_list(iter->second, ",", expressions);

  for (auto& expression : expressions) {
    vector<string> args;
    get_str_vec(expression, ";", args);

    if (args.empty()) {
      s->err.message = "invalid empty expression";
      ldpp_dout(this, 5) << s->err.message << dendl;
      return -EINVAL;
    }
    if (args.size() > 2) {
      s->err.message = string("invalid expression: ") + expression;
      ldpp_dout(this, 5) << s->err.message << dendl;
      return -EINVAL;
    }

    string key = boost::algorithm::to_lower_copy(rgw_trim_whitespace(args[0]));
    string val;
    if (args.size() > 1) {
      val = boost::algorithm::to_lower_copy(rgw_trim_whitespace(args[1]));
    }

    if (!boost::algorithm::starts_with(key, RGW_AMZ_META_PREFIX)) {
      s->err.message = string("invalid expression, key must start with '" RGW_AMZ_META_PREFIX "' : ") + expression;
      ldpp_dout(this, 5) << s->err.message << dendl;
      return -EINVAL;
    }

    key = key.substr(sizeof(RGW_AMZ_META_PREFIX) - 1);

    ESEntityTypeMap::EntityType entity_type;

    if (val.empty() || val == "str" || val == "string") {
      entity_type = ESEntityTypeMap::ES_ENTITY_STR;
    } else if (val == "int" || val == "integer") {
      entity_type = ESEntityTypeMap::ES_ENTITY_INT;
    } else if (val == "date" || val == "datetime") {
      entity_type = ESEntityTypeMap::ES_ENTITY_DATE;
    } else {
      s->err.message = string("invalid entity type: ") + val;
      ldpp_dout(this, 5) << s->err.message << dendl;
      return -EINVAL;
    }

    mdsearch_config[key] = entity_type;
  }

  return 0;
}

void RGWConfigBucketMetaSearch_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}

void RGWGetBucketMetaSearch_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, NULL, to_mime_type(s->format));

  Formatter *f = s->formatter;
  f->open_array_section("GetBucketMetaSearchResult");
  for (auto& e : s->bucket->get_info().mdsearch_config) {
    f->open_object_section("Entry");
    string k = string("x-amz-meta-") + e.first;
    f->dump_string("Key", k.c_str());
    const char *type;
    switch (e.second) {
      case ESEntityTypeMap::ES_ENTITY_INT:
        type = "int";
        break;
      case ESEntityTypeMap::ES_ENTITY_DATE:
        type = "date";
        break;
      default:
        type = "str";
    }
    f->dump_string("Type", type);
    f->close_section();
  }
  f->close_section();
  rgw_flush_formatter(s, f);
}

void RGWDelBucketMetaSearch_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}

void RGWPutBucketObjectLock_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

void RGWGetBucketObjectLock_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

  if (op_ret) {
    return;
  }
  encode_xml("ObjectLockConfiguration", s->bucket->get_info().obj_lock, s->formatter);
  rgw_flush_formatter_and_reset(s, s->formatter);
}


int RGWPutObjRetention_ObjStore_S3::get_params(optional_yield y)
{
  const char *bypass_gov_header = s->info.env->get("HTTP_X_AMZ_BYPASS_GOVERNANCE_RETENTION");
  if (bypass_gov_header) {
    std::string bypass_gov_decoded = url_decode(bypass_gov_header);
    bypass_governance_mode = boost::algorithm::iequals(bypass_gov_decoded, "true");
  }

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  std::tie(op_ret, data) = read_all_input(s, max_size, false);
  return op_ret;
}

void RGWPutObjRetention_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

void RGWGetObjRetention_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

  if (op_ret) {
    return;
  }
  encode_xml("Retention", obj_retention, s->formatter);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWPutObjLegalHold_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

void RGWGetObjLegalHold_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

  if (op_ret) {
    return;
  }
  encode_xml("LegalHold", obj_legal_hold, s->formatter);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWGetBucketPolicyStatus_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

  s->formatter->open_object_section_in_ns("PolicyStatus", XMLNS_AWS_S3);
  // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETPolicyStatus.html
  // mentions TRUE and FALSE, but boto/aws official clients seem to want lower
  // case which is returned by AWS as well; so let's be bug to bug compatible
  // with the API
  s->formatter->dump_bool("IsPublic", isPublic);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);

}

void RGWPutBucketPublicAccessBlock_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

void RGWGetBucketPublicAccessBlock_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this, to_mime_type(s->format));
  dump_start(s);

  access_conf.dump_xml(s->formatter);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

RGWOp *RGWHandler_REST_Service_S3::op_get()
{
  if (is_usage_op()) {
    return new RGWGetUsage_ObjStore_S3;
  } else {
    return new RGWListBuckets_ObjStore_S3;
  }
}

RGWOp *RGWHandler_REST_Service_S3::op_head()
{
  return new RGWListBuckets_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Bucket_S3::get_obj_op(bool get_data) const
{
  // Non-website mode
  if (get_data) {
    int list_type = 1;
    s->info.args.get_int("list-type", &list_type, 1);
    switch (list_type) {
      case 1:
        return new RGWListBucket_ObjStore_S3;
      case 2:
        return new RGWListBucket_ObjStore_S3v2;
      default:
        ldpp_dout(s, 5) << __func__ << ": unsupported list-type " << list_type << dendl;
        return new RGWListBucket_ObjStore_S3;
    }
  } else {
    return new RGWStatBucket_ObjStore_S3;
  }
}

RGWOp *RGWHandler_REST_Bucket_S3::op_get()
{
  if (s->info.args.sub_resource_exists("encryption"))
    return nullptr;

  if (s->info.args.sub_resource_exists("logging"))
    return new RGWGetBucketLogging_ObjStore_S3;

  if (s->info.args.sub_resource_exists("location"))
    return new RGWGetBucketLocation_ObjStore_S3;

  if (s->info.args.sub_resource_exists("versioning"))
    return new RGWGetBucketVersioning_ObjStore_S3;

  if (s->info.args.sub_resource_exists("website")) {
    if (!s->cct->_conf->rgw_enable_static_website) {
      return NULL;
    }
    return new RGWGetBucketWebsite_ObjStore_S3;
  }

  if (s->info.args.exists("mdsearch")) {
    return new RGWGetBucketMetaSearch_ObjStore_S3;
  }

  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_S3;
  } else if (is_cors_op()) {
    return new RGWGetCORS_ObjStore_S3;
  } else if (is_request_payment_op()) {
    return new RGWGetRequestPayment_ObjStore_S3;
  } else if (s->info.args.exists("uploads")) {
    return new RGWListBucketMultiparts_ObjStore_S3;
  } else if(is_lc_op()) {
    return new RGWGetLC_ObjStore_S3;
  } else if(is_policy_op()) {
    return new RGWGetBucketPolicy;
  } else if (is_tagging_op()) {
    return new RGWGetBucketTags_ObjStore_S3;
  } else if (is_object_lock_op()) {
    return new RGWGetBucketObjectLock_ObjStore_S3;
  } else if (is_notification_op()) {
    return RGWHandler_REST_PSNotifs_S3::create_get_op();
  } else if (is_replication_op()) {
    return new RGWGetBucketReplication_ObjStore_S3;
  } else if (is_policy_status_op()) {
    return new RGWGetBucketPolicyStatus_ObjStore_S3;
  } else if (is_block_public_access_op()) {
    return new RGWGetBucketPublicAccessBlock_ObjStore_S3;
  } else if (is_bucket_encryption_op()) {
    return new RGWGetBucketEncryption_ObjStore_S3;
  }
  return get_obj_op(true);
}

RGWOp *RGWHandler_REST_Bucket_S3::op_head()
{
  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_S3;
  } else if (s->info.args.exists("uploads")) {
    return new RGWListBucketMultiparts_ObjStore_S3;
  }
  return get_obj_op(false);
}

RGWOp *RGWHandler_REST_Bucket_S3::op_put()
{
  if (s->info.args.sub_resource_exists("logging") ||
      s->info.args.sub_resource_exists("encryption"))
    return nullptr;
  if (s->info.args.sub_resource_exists("versioning"))
    return new RGWSetBucketVersioning_ObjStore_S3;
  if (s->info.args.sub_resource_exists("website")) {
    if (!s->cct->_conf->rgw_enable_static_website) {
      return NULL;
    }
    return new RGWSetBucketWebsite_ObjStore_S3;
  }
  if (is_tagging_op()) {
    return new RGWPutBucketTags_ObjStore_S3;
  } else if (is_acl_op()) {
    return new RGWPutACLs_ObjStore_S3;
  } else if (is_cors_op()) {
    return new RGWPutCORS_ObjStore_S3;
  } else if (is_request_payment_op()) {
    return new RGWSetRequestPayment_ObjStore_S3;
  } else if(is_lc_op()) {
    return new RGWPutLC_ObjStore_S3;
  } else if(is_policy_op()) {
    return new RGWPutBucketPolicy;
  } else if (is_object_lock_op()) {
    return new RGWPutBucketObjectLock_ObjStore_S3;
  } else if (is_notification_op()) {
    return RGWHandler_REST_PSNotifs_S3::create_put_op();
  } else if (is_replication_op()) {
    RGWBucketSyncPolicyHandlerRef sync_policy_handler;
    int ret = driver->get_sync_policy_handler(s, nullopt, nullopt,
					     &sync_policy_handler, null_yield);
    if (ret < 0 || !sync_policy_handler ||
        sync_policy_handler->is_legacy_config()) {
      return nullptr;
    }

    return new RGWPutBucketReplication_ObjStore_S3;
  } else if (is_block_public_access_op()) {
    return new RGWPutBucketPublicAccessBlock_ObjStore_S3;
  } else if (is_bucket_encryption_op()) {
    return new RGWPutBucketEncryption_ObjStore_S3;
  }
  return new RGWCreateBucket_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Bucket_S3::op_delete()
{
  if (s->info.args.sub_resource_exists("logging") ||
      s->info.args.sub_resource_exists("encryption"))
    return nullptr;

  if (is_tagging_op()) {
    return new RGWDeleteBucketTags_ObjStore_S3;
  } else if (is_cors_op()) {
    return new RGWDeleteCORS_ObjStore_S3;
  } else if(is_lc_op()) {
    return new RGWDeleteLC_ObjStore_S3;
  } else if(is_policy_op()) {
    return new RGWDeleteBucketPolicy;
  } else if (is_notification_op()) {
    return RGWHandler_REST_PSNotifs_S3::create_delete_op();
  } else if (is_replication_op()) {
    return new RGWDeleteBucketReplication_ObjStore_S3;
  } else if (is_block_public_access_op()) {
    return new RGWDeleteBucketPublicAccessBlock;
  } else if (is_bucket_encryption_op()) {
    return new RGWDeleteBucketEncryption_ObjStore_S3;
  }

  if (s->info.args.sub_resource_exists("website")) {
    if (!s->cct->_conf->rgw_enable_static_website) {
      return NULL;
    }
    return new RGWDeleteBucketWebsite_ObjStore_S3;
  }

  if (s->info.args.exists("mdsearch")) {
    return new RGWDelBucketMetaSearch_ObjStore_S3;
  }

  return new RGWDeleteBucket_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Bucket_S3::op_post()
{
  if (s->info.args.exists("delete")) {
    return new RGWDeleteMultiObj_ObjStore_S3;
  }

  if (s->info.args.exists("mdsearch")) {
    return new RGWConfigBucketMetaSearch_ObjStore_S3;
  }

  return new RGWPostObj_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Bucket_S3::op_options()
{
  return new RGWOptionsCORS_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Obj_S3::get_obj_op(bool get_data)
{
  RGWGetObj_ObjStore_S3 *get_obj_op = new RGWGetObj_ObjStore_S3;
  get_obj_op->set_get_data(get_data);
  return get_obj_op;
}

RGWOp *RGWHandler_REST_Obj_S3::op_get()
{
  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_S3;
  } else if (s->info.args.exists("uploadId")) {
    return new RGWListMultipart_ObjStore_S3;
  } else if (s->info.args.exists("layout")) {
    return new RGWGetObjLayout_ObjStore_S3;
  } else if (is_tagging_op()) {
    return new RGWGetObjTags_ObjStore_S3;
  } else if (is_obj_retention_op()) {
    return new RGWGetObjRetention_ObjStore_S3;
  } else if (is_obj_legal_hold_op()) {
    return new RGWGetObjLegalHold_ObjStore_S3;
  }
  return get_obj_op(true);
}

RGWOp *RGWHandler_REST_Obj_S3::op_head()
{
  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_S3;
  } else if (s->info.args.exists("uploadId")) {
    return new RGWListMultipart_ObjStore_S3;
  }
  return get_obj_op(false);
}

RGWOp *RGWHandler_REST_Obj_S3::op_put()
{
  if (is_acl_op()) {
    return new RGWPutACLs_ObjStore_S3;
  } else if (is_tagging_op()) {
    return new RGWPutObjTags_ObjStore_S3;
  } else if (is_obj_retention_op()) {
    return new RGWPutObjRetention_ObjStore_S3;
  } else if (is_obj_legal_hold_op()) {
    return new RGWPutObjLegalHold_ObjStore_S3;
  }

  if (s->init_state.src_bucket.empty())
    return new RGWPutObj_ObjStore_S3;
  else
    return new RGWCopyObj_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Obj_S3::op_delete()
{
  if (is_tagging_op()) {
    return new RGWDeleteObjTags_ObjStore_S3;
  }
  string upload_id = s->info.args.get("uploadId");

  if (upload_id.empty())
    return new RGWDeleteObj_ObjStore_S3;
  else
    return new RGWAbortMultipart_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Obj_S3::op_post()
{
  if (s->info.args.exists("uploadId"))
    return new RGWCompleteMultipart_ObjStore_S3;

  if (s->info.args.exists("uploads"))
    return new RGWInitMultipart_ObjStore_S3;
  
  if (is_select_op())
    return rgw::s3select::create_s3select_op();

  return new RGWPostObj_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Obj_S3::op_options()
{
  return new RGWOptionsCORS_ObjStore_S3;
}

int RGWHandler_REST_S3::init_from_header(rgw::sal::Driver* driver,
					 req_state* s,
					 RGWFormat default_formatter,
					 bool configurable_format)
{
  string req;
  string first;

  const char *req_name = s->relative_uri.c_str();
  const char *p;

  if (*req_name == '?') {
    p = req_name;
  } else {
    p = s->info.request_params.c_str();
  }

  s->info.args.set(p);
  s->info.args.parse(s);

  /* must be called after the args parsing */
  int ret = allocate_formatter(s, default_formatter, configurable_format);
  if (ret < 0)
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

  /*
   * XXX The intent of the check for empty is apparently to let the bucket
   * name from DNS to be set ahead. However, we currently take the DNS
   * bucket and re-insert it into URL in rgw_rest.cc:RGWREST::preprocess().
   * So, this check is meaningless.
   *
   * Rather than dropping this, the code needs to be changed into putting
   * the bucket (and its tenant) from DNS and Host: header (HTTP_HOST)
   * into req_status.bucket_name directly.
   */
  if (s->init_state.url_bucket.empty()) {
    // Save bucket to tide us over until token is parsed.
    s->init_state.url_bucket = first;
    string encoded_obj_str;
    if (pos >= 0) {
      encoded_obj_str = req.substr(pos+1);
    }

    /* dang: s->bucket is never set here, since it's created with permissions.
     * These calls will always create an object with no bucket. */
    if (!encoded_obj_str.empty()) {
      if (s->bucket) {
	s->object = s->bucket->get_object(rgw_obj_key(encoded_obj_str, s->info.args.get("versionId")));
      } else {
	s->object = driver->get_object(rgw_obj_key(encoded_obj_str, s->info.args.get("versionId")));
      }
    }
  } else {
    if (s->bucket) {
      s->object = s->bucket->get_object(rgw_obj_key(req_name, s->info.args.get("versionId")));
    } else {
      s->object = driver->get_object(rgw_obj_key(req_name, s->info.args.get("versionId")));
    }
  }
  return 0;
}

int RGWHandler_REST_S3::postauth_init(optional_yield y)
{
  struct req_init_state *t = &s->init_state;

  int ret = rgw_parse_url_bucket(t->url_bucket, s->user->get_tenant(),
                                 s->bucket_tenant, s->bucket_name);
  if (ret) {
    return ret;
  }
  if (s->auth.identity->get_identity_type() == TYPE_ROLE) {
    s->bucket_tenant = s->auth.identity->get_role_tenant();
  }

  ldpp_dout(s, 10) << "s->object=" << s->object
           << " s->bucket=" << rgw_make_bucket_entry_name(s->bucket_tenant, s->bucket_name) << dendl;

  ret = rgw_validate_tenant_name(s->bucket_tenant);
  if (ret)
    return ret;
  if (!s->bucket_name.empty() && !rgw::sal::Object::empty(s->object.get())) {
    ret = validate_object_name(s->object->get_name());
    if (ret)
      return ret;
  }

  if (!t->src_bucket.empty()) {
    string auth_tenant;
    if (s->auth.identity->get_identity_type() == TYPE_ROLE) {
      auth_tenant = s->auth.identity->get_role_tenant();
    } else {
      auth_tenant = s->user->get_tenant();
    }
    ret = rgw_parse_url_bucket(t->src_bucket, auth_tenant,
                               s->src_tenant_name, s->src_bucket_name);
    if (ret) {
      return ret;
    }
    ret = rgw_validate_tenant_name(s->src_tenant_name);
    if (ret)
      return ret;
  }

  const char *mfa = s->info.env->get("HTTP_X_AMZ_MFA");
  if (mfa) {
    ret = s->user->verify_mfa(string(mfa), &s->mfa_verified, s, y);
  }

  return 0;
}

int RGWHandler_REST_S3::init(rgw::sal::Driver* driver, req_state *s,
                             rgw::io::BasicClient *cio)
{
  int ret;

  s->dialect = "s3";

  ret = rgw_validate_tenant_name(s->bucket_tenant);
  if (ret)
    return ret;
  if (!s->bucket_name.empty()) {
    ret = validate_object_name(s->object->get_name());
    if (ret)
      return ret;
  }

  const char *cacl = s->info.env->get("HTTP_X_AMZ_ACL");
  if (cacl)
    s->canned_acl = cacl;

  s->has_acl_header = s->info.env->exists_prefix("HTTP_X_AMZ_GRANT");

  const char *copy_source = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE");
  if (copy_source &&
      (! s->info.env->get("HTTP_X_AMZ_COPY_SOURCE_RANGE")) &&
      (! s->info.args.exists("uploadId"))) {
    rgw_obj_key key;

    ret = RGWCopyObj::parse_copy_location(copy_source,
                                          s->init_state.src_bucket,
                                          key,
                                          s);
    if (!ret) {
      ldpp_dout(s, 0) << "failed to parse copy location" << dendl;
      return -EINVAL; // XXX why not -ERR_INVALID_BUCKET_NAME or -ERR_BAD_URL?
    }
    s->src_object = driver->get_object(key);
  }

  const char *sc = s->info.env->get("HTTP_X_AMZ_STORAGE_CLASS");
  if (sc) {
    s->info.storage_class = sc;
  }

  return RGWHandler_REST::init(driver, s, cio);
}

int RGWHandler_REST_S3::authorize(const DoutPrefixProvider *dpp, optional_yield y)
{
  if (s->info.args.exists("Action") && s->info.args.get("Action") == "AssumeRoleWithWebIdentity") {
    return RGW_Auth_STS::authorize(dpp, driver, auth_registry, s, y);
  }
  return RGW_Auth_S3::authorize(dpp, driver, auth_registry, s, y);
}

enum class AwsVersion {
  UNKNOWN,
  V2,
  V4
};

enum class AwsRoute {
  UNKNOWN,
  QUERY_STRING,
  HEADERS
};

static inline std::pair<AwsVersion, AwsRoute>
discover_aws_flavour(const req_info& info)
{
  using rgw::auth::s3::AWS4_HMAC_SHA256_STR;

  AwsVersion version = AwsVersion::UNKNOWN;
  AwsRoute route = AwsRoute::UNKNOWN;

  const char* http_auth = info.env->get("HTTP_AUTHORIZATION");
  if (http_auth && http_auth[0]) {
    /* Authorization in Header */
    route = AwsRoute::HEADERS;

    if (!strncmp(http_auth, AWS4_HMAC_SHA256_STR,
                 strlen(AWS4_HMAC_SHA256_STR))) {
      /* AWS v4 */
      version = AwsVersion::V4;
    } else if (!strncmp(http_auth, "AWS ", 4)) {
      /* AWS v2 */
      version = AwsVersion::V2;
    }
  } else {
    route = AwsRoute::QUERY_STRING;

    if (info.args.get("x-amz-algorithm") == AWS4_HMAC_SHA256_STR) {
      /* AWS v4 */
      version = AwsVersion::V4;
    } else if (!info.args.get("AWSAccessKeyId").empty()) {
      /* AWS v2 */
      version = AwsVersion::V2;
    }
  }

  return std::make_pair(version, route);
}

/*
 * verify that a signed request comes from the keyholder
 * by checking the signature against our locally-computed version
 *
 * it tries AWS v4 before AWS v2
 */
int RGW_Auth_S3::authorize(const DoutPrefixProvider *dpp,
                           rgw::sal::Driver* const driver,
                           const rgw::auth::StrategyRegistry& auth_registry,
                           req_state* const s, optional_yield y)
{

  /* neither keystone and rados enabled; warn and exit! */
  if (!driver->ctx()->_conf->rgw_s3_auth_use_rados &&
      !driver->ctx()->_conf->rgw_s3_auth_use_keystone &&
      !driver->ctx()->_conf->rgw_s3_auth_use_ldap) {
    ldpp_dout(dpp, 0) << "WARNING: no authorization backend enabled! Users will never authenticate." << dendl;
    return -EPERM;
  }

  const auto ret = rgw::auth::Strategy::apply(dpp, auth_registry.get_s3_main(), s, y);
  if (ret == 0) {
    /* Populate the owner info. */
    s->owner.id = s->user->get_id();
    s->owner.display_name = s->user->get_display_name();
  }
  return ret;
}

int RGWHandler_Auth_S3::init(rgw::sal::Driver* driver, req_state *state,
                             rgw::io::BasicClient *cio)
{
  int ret = RGWHandler_REST_S3::init_from_header(driver, state, RGWFormat::JSON, true);
  if (ret < 0)
    return ret;

  return RGWHandler_REST::init(driver, state, cio);
}

namespace {
// utility classes and functions for handling parameters with the following format:
// Attributes.entry.{N}.{key|value}={VALUE}
// N - any unsigned number
// VALUE - url encoded string

// and Attribute is holding key and value
// ctor and set are done according to the "type" argument
// if type is not "key" or "value" its a no-op
class Attribute {
  std::string key;
  std::string value;
public:
  Attribute(const std::string& type, const std::string& key_or_value) {
    set(type, key_or_value);
  }
  void set(const std::string& type, const std::string& key_or_value) {
    if (type == "key") {
      key = key_or_value;
    } else if (type == "value") {
      value = key_or_value;
    }
  }
  const std::string& get_key() const { return key; }
  const std::string& get_value() const { return value; }
};

using AttributeMap = std::map<unsigned, Attribute>;

// aggregate the attributes into a map
// the key and value are associated by the index (N)
// no assumptions are made on the order in which these parameters are added
void update_attribute_map(const std::string& input, AttributeMap& map) {
  const boost::char_separator<char> sep(".");
  const boost::tokenizer tokens(input, sep);
  auto token = tokens.begin();
  if (*token != "Attributes") {
      return;
  }
  ++token;

  if (*token != "entry") {
      return;
  }
  ++token;

  unsigned idx;
  try {
    idx = std::stoul(*token);
  } catch (const std::invalid_argument&) {
    return;
  }
  ++token;

  std::string key_or_value = "";
  // get the rest of the string regardless of dots
  // this is to allow dots in the value
  while (token != tokens.end()) {
    key_or_value.append(*token+".");
    ++token;
  }
  // remove last separator
  key_or_value.pop_back();

  auto pos = key_or_value.find("=");
  if (pos != std::string::npos) {
    const auto key_or_value_lhs = key_or_value.substr(0, pos);
    constexpr bool in_query = true; // replace '+' with ' '
    const auto key_or_value_rhs = url_decode(key_or_value.substr(pos + 1, key_or_value.size() - 1), in_query);
    const auto map_it = map.find(idx);
    if (map_it == map.end()) {
      // new entry
      map.emplace(std::make_pair(idx, Attribute(key_or_value_lhs, key_or_value_rhs)));
    } else {
      // existing entry
      map_it->second.set(key_or_value_lhs, key_or_value_rhs);
    }
  }
}
}

void parse_post_action(const std::string& post_body, req_state* s)
{
  if (post_body.size() > 0) {
    ldpp_dout(s, 10) << "Content of POST: " << post_body << dendl;

    if (post_body.find("Action") != string::npos) {
      const boost::char_separator<char> sep("&");
      const boost::tokenizer<boost::char_separator<char>> tokens(post_body, sep);
      AttributeMap map;
      for (const auto& t : tokens) {
        const auto pos = t.find("=");
        if (pos != string::npos) {
          const auto key = t.substr(0, pos);
          if (boost::starts_with(key, "Attributes.")) {
            update_attribute_map(t, map);
          } else {
            constexpr bool in_query = true; // replace '+' with ' '
            s->info.args.append(t.substr(0, pos),
                              url_decode(t.substr(pos+1, t.size() -1), in_query));
          }
        }
      }
      // update the regular args with the content of the attribute map
      for (const auto& attr : map) {
          s->info.args.append(attr.second.get_key(), attr.second.get_value());
      }
    }
  }
  const auto payload_hash = rgw::auth::s3::calc_v4_payload_hash(post_body);
  s->info.args.append("PayloadHash", payload_hash);
}

RGWHandler_REST* RGWRESTMgr_S3::get_handler(rgw::sal::Driver* driver,
					    req_state* const s,
                                            const rgw::auth::StrategyRegistry& auth_registry,
                                            const std::string& frontend_prefix)
{
  bool is_s3website = enable_s3website && (s->prot_flags & RGW_REST_WEBSITE);
  int ret =
    RGWHandler_REST_S3::init_from_header(driver, s,
					is_s3website ? RGWFormat::HTML :
					RGWFormat::XML, true);
  if (ret < 0) {
    return nullptr;
  }

  if (is_s3website) {
    if (s->init_state.url_bucket.empty()) {
      return new RGWHandler_REST_Service_S3Website(auth_registry);
    }
    if (rgw::sal::Object::empty(s->object.get())) {
      return new RGWHandler_REST_Bucket_S3Website(auth_registry);
    }
    return new RGWHandler_REST_Obj_S3Website(auth_registry);
  }

  if (s->init_state.url_bucket.empty()) {
    // no bucket
    if (s->op == OP_POST) {
      // POST will be one of: IAM, STS or topic service
      const auto max_size = s->cct->_conf->rgw_max_put_param_size;
      int ret;
      bufferlist data;
      std::tie(ret, data) = rgw_rest_read_all_input(s, max_size, false);
      if (ret < 0) {
        return nullptr;
      }
      parse_post_action(data.to_str(), s);
      if (enable_sts && RGWHandler_REST_STS::action_exists(s)) {
        return new RGWHandler_REST_STS(auth_registry);
      }
      if (enable_iam && RGWHandler_REST_IAM::action_exists(s)) {
        return new RGWHandler_REST_IAM(auth_registry, data);
      }
      if (enable_pubsub && RGWHandler_REST_PSTopic_AWS::action_exists(s)) {
        return new RGWHandler_REST_PSTopic_AWS(auth_registry); 
      }
      return nullptr;
    }
    // non-POST S3 service without a bucket
    return new RGWHandler_REST_Service_S3(auth_registry);
  }
  if (!rgw::sal::Object::empty(s->object.get())) {
    // has object
    return new RGWHandler_REST_Obj_S3(auth_registry);
  }
  if (s->info.args.exist_obj_excl_sub_resource()) {
    return nullptr;
  }
  // has bucket
  return new RGWHandler_REST_Bucket_S3(auth_registry, enable_pubsub);
}

bool RGWHandler_REST_S3Website::web_dir() const {
  std::string subdir_name;
  if (!rgw::sal::Object::empty(s->object.get())) {
    subdir_name = url_decode(s->object->get_name());
  }

  if (subdir_name.empty()) {
    return false;
  } else if (subdir_name.back() == '/' && subdir_name.size() > 1) {
    subdir_name.pop_back();
  }

  std::unique_ptr<rgw::sal::Object> obj = s->bucket->get_object(rgw_obj_key(subdir_name));

  obj->set_atomic();

  RGWObjState* state = nullptr;
  if (obj->get_obj_state(s, &state, s->yield) < 0) {
    return false;
  }
  if (! state->exists) {
    return false;
  }
  return state->exists;
}

int RGWHandler_REST_S3Website::init(rgw::sal::Driver* driver, req_state *s,
                                    rgw::io::BasicClient* cio)
{
  // save the original object name before retarget() replaces it with the
  // result of get_effective_key(). the error_handler() needs the original
  // object name for redirect handling
  if (!rgw::sal::Object::empty(s->object.get())) {
    original_object_name = s->object->get_name();
  } else {
    original_object_name = "";
  }

  return RGWHandler_REST_S3::init(driver, s, cio);
}

int RGWHandler_REST_S3Website::retarget(RGWOp* op, RGWOp** new_op, optional_yield y) {
  *new_op = op;
  ldpp_dout(s, 10) << __func__ << " Starting retarget" << dendl;

  if (!(s->prot_flags & RGW_REST_WEBSITE))
    return 0;

  if (rgw::sal::Bucket::empty(s->bucket.get())) {
    // TODO-FUTURE: if the bucket does not exist, maybe expose it here?
    return -ERR_NO_SUCH_BUCKET;
  }

  if (!s->bucket->get_info().has_website) {
    // TODO-FUTURE: if the bucket has no WebsiteConfig, expose it here
    return -ERR_NO_SUCH_WEBSITE_CONFIGURATION;
  }

  rgw_obj_key new_obj;
  string key_name;
  if (!rgw::sal::Object::empty(s->object.get())) {
    key_name = s->object->get_name();
  }
  bool get_res = s->bucket->get_info().website_conf.get_effective_key(key_name, &new_obj.name, web_dir());
  if (!get_res) {
    s->err.message = "The IndexDocument Suffix is not configurated or not well formed!";
    ldpp_dout(s, 5) << s->err.message << dendl;
    return -EINVAL;
  }

  ldpp_dout(s, 10) << "retarget get_effective_key " << s->object << " -> "
		    << new_obj << dendl;

  RGWBWRoutingRule rrule;
  bool should_redirect =
    s->bucket->get_info().website_conf.should_redirect(new_obj.name, 0, &rrule);

  if (should_redirect) {
    const string& hostname = s->info.env->get("HTTP_HOST", "");
    const string& protocol =
      (s->info.env->get("SERVER_PORT_SECURE") ? "https" : "http");
    int redirect_code = 0;
    rrule.apply_rule(protocol, hostname, key_name, &s->redirect,
		    &redirect_code);
    // APply a custom HTTP response code
    if (redirect_code > 0)
      s->err.http_ret = redirect_code; // Apply a custom HTTP response code
    ldpp_dout(s, 10) << "retarget redirect code=" << redirect_code
		      << " proto+host:" << protocol << "://" << hostname
		      << " -> " << s->redirect << dendl;
    return -ERR_WEBSITE_REDIRECT;
  }

  /*
   * FIXME: if s->object != new_obj, drop op and create a new op to handle
   * operation. Or remove this comment if it's not applicable anymore
   * dang: This could be problematic, since we're not actually replacing op, but
   * we are replacing s->object.  Something might have a pointer to it.
   */
  s->object = s->bucket->get_object(new_obj);

  return 0;
}

RGWOp* RGWHandler_REST_S3Website::op_get()
{
  return get_obj_op(true);
}

RGWOp* RGWHandler_REST_S3Website::op_head()
{
  return get_obj_op(false);
}

int RGWHandler_REST_S3Website::serve_errordoc(const DoutPrefixProvider *dpp, int http_ret, const string& errordoc_key, optional_yield y) {
  int ret = 0;
  s->formatter->reset(); /* Try to throw it all away */

  std::shared_ptr<RGWGetObj_ObjStore_S3Website> getop( static_cast<RGWGetObj_ObjStore_S3Website*>(op_get()));
  if (getop.get() == NULL) {
    return -1; // Trigger double error handler
  }
  getop->init(driver, s, this);
  getop->range_str = NULL;
  getop->if_mod = NULL;
  getop->if_unmod = NULL;
  getop->if_match = NULL;
  getop->if_nomatch = NULL;
  /* This is okay.  It's an error, so nothing will run after this, and it can be
   * called by abort_early(), which can be called before s->object or s->bucket
   * are set up. Note, it won't have bucket. */
  s->object = driver->get_object(errordoc_key);

  ret = init_permissions(getop.get(), y);
  if (ret < 0) {
    ldpp_dout(s, 20) << "serve_errordoc failed, init_permissions ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  ret = read_permissions(getop.get(), y);
  if (ret < 0) {
    ldpp_dout(s, 20) << "serve_errordoc failed, read_permissions ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  if (http_ret) {
     getop->set_custom_http_response(http_ret);
  }

  ret = getop->init_processing(y);
  if (ret < 0) {
    ldpp_dout(s, 20) << "serve_errordoc failed, init_processing ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  ret = getop->verify_op_mask();
  if (ret < 0) {
    ldpp_dout(s, 20) << "serve_errordoc failed, verify_op_mask ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  ret = getop->verify_permission(y);
  if (ret < 0) {
    ldpp_dout(s, 20) << "serve_errordoc failed, verify_permission ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  ret = getop->verify_params();
  if (ret < 0) {
    ldpp_dout(s, 20) << "serve_errordoc failed, verify_params ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  // No going back now
  getop->pre_exec();
  /*
   * FIXME Missing headers:
   * With a working errordoc, the s3 error fields are rendered as HTTP headers,
   *   x-amz-error-code: NoSuchKey
   *   x-amz-error-message: The specified key does not exist.
   *   x-amz-error-detail-Key: foo
   */
  getop->execute(y);
  getop->complete();
  return 0;
}

int RGWHandler_REST_S3Website::error_handler(int err_no,
					     string* error_content,
					     optional_yield y) {
  int new_err_no = -1;
  rgw_http_errors::const_iterator r = rgw_http_s3_errors.find(err_no > 0 ? err_no : -err_no);
  int http_error_code = -1;

  if (r != rgw_http_s3_errors.end()) {
    http_error_code = r->second.first;
  }
  ldpp_dout(s, 10) << "RGWHandler_REST_S3Website::error_handler err_no=" << err_no << " http_ret=" << http_error_code << dendl;

  RGWBWRoutingRule rrule;
  bool have_bucket = !rgw::sal::Bucket::empty(s->bucket.get());
  bool should_redirect = false;
  if (have_bucket) {
    should_redirect =
      s->bucket->get_info().website_conf.should_redirect(original_object_name,
							 http_error_code, &rrule);
  }

  if (should_redirect) {
    const string& hostname = s->info.env->get("HTTP_HOST", "");
    const string& protocol =
      (s->info.env->get("SERVER_PORT_SECURE") ? "https" : "http");
    int redirect_code = 0;
    rrule.apply_rule(protocol, hostname, original_object_name,
                     &s->redirect, &redirect_code);
    // Apply a custom HTTP response code
    if (redirect_code > 0)
      s->err.http_ret = redirect_code; // Apply a custom HTTP response code
    ldpp_dout(s, 10) << "error handler redirect code=" << redirect_code
		      << " proto+host:" << protocol << "://" << hostname
		      << " -> " << s->redirect << dendl;
    return -ERR_WEBSITE_REDIRECT;
  } else if (err_no == -ERR_WEBSITE_REDIRECT) {
    // Do nothing here, this redirect will be handled in abort_early's ERR_WEBSITE_REDIRECT block
    // Do NOT fire the ErrorDoc handler
  } else if (have_bucket && !s->bucket->get_info().website_conf.error_doc.empty()) {
    /* This serves an entire page!
       On success, it will return zero, and no further content should be sent to the socket
       On failure, we need the double-error handler
     */
    new_err_no = RGWHandler_REST_S3Website::serve_errordoc(s, http_error_code, s->bucket->get_info().website_conf.error_doc, y);
    if (new_err_no != -1) {
      err_no = new_err_no;
    }
  } else {
    ldpp_dout(s, 20) << "No special error handling today!" << dendl;
  }

  return err_no;
}

RGWOp* RGWHandler_REST_Obj_S3Website::get_obj_op(bool get_data)
{
  /** If we are in website mode, then it is explicitly impossible to run GET or
   * HEAD on the actual directory. We must convert the request to run on the
   * suffix object instead!
   */
  RGWGetObj_ObjStore_S3Website* op = new RGWGetObj_ObjStore_S3Website;
  op->set_get_data(get_data);
  return op;
}

RGWOp* RGWHandler_REST_Bucket_S3Website::get_obj_op(bool get_data)
{
  /** If we are in website mode, then it is explicitly impossible to run GET or
   * HEAD on the actual directory. We must convert the request to run on the
   * suffix object instead!
   */
  RGWGetObj_ObjStore_S3Website* op = new RGWGetObj_ObjStore_S3Website;
  op->set_get_data(get_data);
  return op;
}

RGWOp* RGWHandler_REST_Service_S3Website::get_obj_op(bool get_data)
{
  /** If we are in website mode, then it is explicitly impossible to run GET or
   * HEAD on the actual directory. We must convert the request to run on the
   * suffix object instead!
   */
  RGWGetObj_ObjStore_S3Website* op = new RGWGetObj_ObjStore_S3Website;
  op->set_get_data(get_data);
  return op;
}


namespace rgw::auth::s3 {

static rgw::auth::Completer::cmplptr_t
null_completer_factory(const boost::optional<std::string>& secret_key)
{
  return nullptr;
}


AWSEngine::VersionAbstractor::auth_data_t
AWSGeneralAbstractor::get_auth_data(const req_state* const s) const
{
  AwsVersion version;
  AwsRoute route;
  std::tie(version, route) = discover_aws_flavour(s->info);

  if (version == AwsVersion::V2) {
    return get_auth_data_v2(s);
  } else if (version == AwsVersion::V4) {
    return get_auth_data_v4(s, route == AwsRoute::QUERY_STRING);
  } else {
    /* FIXME(rzarzynski): handle anon user. */
    throw -EINVAL;
  }
}

boost::optional<std::string>
AWSGeneralAbstractor::get_v4_canonical_headers(
  const req_info& info,
  const std::string_view& signedheaders,
  const bool using_qs) const
{
  return rgw::auth::s3::get_v4_canonical_headers(info, signedheaders,
                                                 using_qs, false);
}

AWSSignerV4::prepare_result_t
AWSSignerV4::prepare(const DoutPrefixProvider *dpp,
                     const std::string& access_key_id,
                     const string& region,
                     const string& service,
                     const req_info& info,
                     const bufferlist *opt_content,
                     bool s3_op)
{
  std::string signed_hdrs;

  ceph::real_time timestamp = ceph::real_clock::now();

  map<string, string> extra_headers;

  std::string date = ceph::to_iso_8601_no_separators(timestamp, ceph::iso_8601_format::YMDhms);

  std::string credential_scope = gen_v4_scope(timestamp, region, service);

  extra_headers["x-amz-date"] = date;

  string content_hash;

  if (opt_content) {
    content_hash = rgw::auth::s3::calc_v4_payload_hash(opt_content->to_str());
    extra_headers["x-amz-content-sha256"] = content_hash;

  }

  /* craft canonical headers */
  std::string canonical_headers = \
    gen_v4_canonical_headers(info, extra_headers, &signed_hdrs);

  using sanitize = rgw::crypt_sanitize::log_content;
  ldpp_dout(dpp, 10) << "canonical headers format = "
                     << sanitize{canonical_headers} << dendl;

  bool is_non_s3_op = !s3_op;

  const char* exp_payload_hash = nullptr;
  string payload_hash;
  if (is_non_s3_op) {
    //For non s3 ops, we need to calculate the payload hash
    payload_hash = info.args.get("PayloadHash");
    exp_payload_hash = payload_hash.c_str();
  } else {
    /* Get the expected hash. */
    if (content_hash.empty()) {
      exp_payload_hash = rgw::auth::s3::get_v4_exp_payload_hash(info);
    } else {
      exp_payload_hash = content_hash.c_str();
    }
  }

  /* Craft canonical URI. Using std::move later so let it be non-const. */
  auto canonical_uri = rgw::auth::s3::gen_v4_canonical_uri(info);


  /* Craft canonical query string. std::moving later so non-const here. */
  auto canonical_qs = rgw::auth::s3::gen_v4_canonical_qs(info, is_non_s3_op);

  auto cct = dpp->get_cct();

  /* Craft canonical request. */
  auto canonical_req_hash = \
    rgw::auth::s3::get_v4_canon_req_hash(cct,
                                         info.method,
                                         std::move(canonical_uri),
                                         std::move(canonical_qs),
                                         std::move(canonical_headers),
                                         signed_hdrs,
                                         exp_payload_hash,
                                         dpp);

  auto string_to_sign = \
    rgw::auth::s3::get_v4_string_to_sign(cct,
                                         AWS4_HMAC_SHA256_STR,
                                         date,
                                         credential_scope,
                                         std::move(canonical_req_hash),
                                         dpp);

  const auto sig_factory = gen_v4_signature;

  /* Requests authenticated with the Query Parameters are treated as unsigned.
   * From "Authenticating Requests: Using Query Parameters (AWS Signature
   * Version 4)":
   *
   *   You don't include a payload hash in the Canonical Request, because
   *   when you create a presigned URL, you don't know the payload content
   *   because the URL is used to upload an arbitrary payload. Instead, you
   *   use a constant string UNSIGNED-PAYLOAD.
   *
   * This means we have absolutely no business in spawning completer. Both
   * aws4_auth_needs_complete and aws4_auth_streaming_mode are set to false
   * by default. We don't need to change that. */
  return {
    access_key_id,
    date,
    credential_scope,
    std::move(signed_hdrs),
    std::move(string_to_sign),
    std::move(extra_headers),
    sig_factory,
  };
}

AWSSignerV4::signature_headers_t
gen_v4_signature(const DoutPrefixProvider *dpp,
                 const std::string_view& secret_key,
                 const AWSSignerV4::prepare_result_t& sig_info)
{
  auto signature = rgw::auth::s3::get_v4_signature(sig_info.scope,
                                                   dpp->get_cct(),
                                                   secret_key,
                                                   sig_info.string_to_sign,
                                                   dpp);
  AWSSignerV4::signature_headers_t result;

  for (auto& entry : sig_info.extra_headers) {
    result[entry.first] = entry.second;
  }
  auto& payload_hash = result["x-amz-content-sha256"];
  if (payload_hash.empty()) {
    payload_hash = AWS4_UNSIGNED_PAYLOAD_HASH;
  }
  string auth_header = string("AWS4-HMAC-SHA256 Credential=").append(sig_info.access_key_id) + "/";
  auth_header.append(sig_info.scope + ",SignedHeaders=")
             .append(sig_info.signed_headers + ",Signature=")
             .append(signature);
  result["Authorization"] = auth_header;

  return result;
}


AWSEngine::VersionAbstractor::auth_data_t
AWSGeneralAbstractor::get_auth_data_v4(const req_state* const s,
                                       const bool using_qs) const
{
  std::string_view access_key_id;
  std::string_view signed_hdrs;

  std::string_view date;
  std::string_view credential_scope;
  std::string_view client_signature;
  std::string_view session_token;

  int ret = rgw::auth::s3::parse_v4_credentials(s->info,
						access_key_id,
						credential_scope,
						signed_hdrs,
						client_signature,
						date,
						session_token,
						using_qs,
                                                s);
  if (ret < 0) {
    throw ret;
  }

  /* craft canonical headers */
  boost::optional<std::string> canonical_headers = \
    get_v4_canonical_headers(s->info, signed_hdrs, using_qs);
  if (canonical_headers) {
    using sanitize = rgw::crypt_sanitize::log_content;
    ldpp_dout(s, 10) << "canonical headers format = "
                      << sanitize{*canonical_headers} << dendl;
  } else {
    throw -EPERM;
  }

  bool is_non_s3_op = rgw::auth::s3::is_non_s3_op(s->op_type);

  const char* exp_payload_hash = nullptr;
  string payload_hash;
  if (is_non_s3_op) {
    //For non s3 ops, we need to calculate the payload hash
    payload_hash = s->info.args.get("PayloadHash");
    exp_payload_hash = payload_hash.c_str();
  } else {
    /* Get the expected hash. */
    exp_payload_hash = rgw::auth::s3::get_v4_exp_payload_hash(s->info);
  }

  /* Craft canonical URI. Using std::move later so let it be non-const. */
  auto canonical_uri = rgw::auth::s3::get_v4_canonical_uri(s->info);

  /* Craft canonical query string. std::moving later so non-const here. */
  auto canonical_qs = rgw::auth::s3::get_v4_canonical_qs(s->info, using_qs);

  /* Craft canonical method. */
  auto canonical_method = rgw::auth::s3::get_v4_canonical_method(s);

  /* Craft canonical request. */
  auto canonical_req_hash = \
    rgw::auth::s3::get_v4_canon_req_hash(s->cct,
                                         std::move(canonical_method),
                                         std::move(canonical_uri),
                                         std::move(canonical_qs),
                                         std::move(*canonical_headers),
                                         signed_hdrs,
                                         exp_payload_hash,
                                         s);

  auto string_to_sign = \
    rgw::auth::s3::get_v4_string_to_sign(s->cct,
                                         AWS4_HMAC_SHA256_STR,
                                         date,
                                         credential_scope,
                                         std::move(canonical_req_hash),
                                         s);

  const auto sig_factory = std::bind(rgw::auth::s3::get_v4_signature,
                                     credential_scope,
                                     std::placeholders::_1,
                                     std::placeholders::_2,
                                     std::placeholders::_3,
                                     s);

  /* Requests authenticated with the Query Parameters are treated as unsigned.
   * From "Authenticating Requests: Using Query Parameters (AWS Signature
   * Version 4)":
   *
   *   You don't include a payload hash in the Canonical Request, because
   *   when you create a presigned URL, you don't know the payload content
   *   because the URL is used to upload an arbitrary payload. Instead, you
   *   use a constant string UNSIGNED-PAYLOAD.
   *
   * This means we have absolutely no business in spawning completer. Both
   * aws4_auth_needs_complete and aws4_auth_streaming_mode are set to false
   * by default. We don't need to change that. */
  if (is_v4_payload_unsigned(exp_payload_hash) || is_v4_payload_empty(s) || is_non_s3_op) {
    return {
      access_key_id,
      client_signature,
      session_token,
      std::move(string_to_sign),
      sig_factory,
      null_completer_factory
    };
  } else {
    /* We're going to handle a signed payload. Be aware that even empty HTTP
     * body (no payload) requires verification:
     *
     *   The x-amz-content-sha256 header is required for all AWS Signature
     *   Version 4 requests. It provides a hash of the request payload. If
     *   there is no payload, you must provide the hash of an empty string. */
    if (!is_v4_payload_streamed(exp_payload_hash)) {
      ldpp_dout(s, 10) << "delaying v4 auth" << dendl;

      /* payload in a single chunk */
      switch (s->op_type)
      {
        case RGW_OP_CREATE_BUCKET:
        case RGW_OP_PUT_OBJ:
        case RGW_OP_PUT_ACLS:
        case RGW_OP_PUT_CORS:
        case RGW_OP_PUT_BUCKET_ENCRYPTION:
        case RGW_OP_GET_BUCKET_ENCRYPTION:
        case RGW_OP_DELETE_BUCKET_ENCRYPTION:
        case RGW_OP_INIT_MULTIPART: // in case that Init Multipart uses CHUNK encoding
        case RGW_OP_COMPLETE_MULTIPART:
        case RGW_OP_SET_BUCKET_VERSIONING:
        case RGW_OP_DELETE_MULTI_OBJ:
        case RGW_OP_ADMIN_SET_METADATA:
        case RGW_OP_SYNC_DATALOG_NOTIFY:
        case RGW_OP_SYNC_DATALOG_NOTIFY2:
        case RGW_OP_SYNC_MDLOG_NOTIFY:
        case RGW_OP_PERIOD_POST:
        case RGW_OP_SET_BUCKET_WEBSITE:
        case RGW_OP_PUT_BUCKET_POLICY:
        case RGW_OP_PUT_OBJ_TAGGING:
	case RGW_OP_PUT_BUCKET_TAGGING:
	case RGW_OP_PUT_BUCKET_REPLICATION:
        case RGW_OP_PUT_LC:
        case RGW_OP_SET_REQUEST_PAYMENT:
        case RGW_OP_PUBSUB_NOTIF_CREATE:
        case RGW_OP_PUBSUB_NOTIF_DELETE:
        case RGW_OP_PUBSUB_NOTIF_LIST:
        case RGW_OP_PUT_BUCKET_OBJ_LOCK:
        case RGW_OP_PUT_OBJ_RETENTION:
        case RGW_OP_PUT_OBJ_LEGAL_HOLD:
        case RGW_STS_GET_SESSION_TOKEN:
        case RGW_STS_ASSUME_ROLE:
        case RGW_OP_PUT_BUCKET_PUBLIC_ACCESS_BLOCK:
        case RGW_OP_GET_BUCKET_PUBLIC_ACCESS_BLOCK:
        case RGW_OP_DELETE_BUCKET_PUBLIC_ACCESS_BLOCK:
	case RGW_OP_GET_OBJ://s3select its post-method(payload contain the query) , the request is get-object
          break;
        default:
          ldpp_dout(s, 10) << "ERROR: AWS4 completion for operation: " << s->op_type << ", NOT IMPLEMENTED" << dendl;
          throw -ERR_NOT_IMPLEMENTED;
      }

      const auto cmpl_factory = std::bind(AWSv4ComplSingle::create,
                                          s,
                                          std::placeholders::_1);
      return {
        access_key_id,
        client_signature,
        session_token,
        std::move(string_to_sign),
        sig_factory,
        cmpl_factory
      };
    } else {
      /* IMHO "streamed" doesn't fit too good here. I would prefer to call
       * it "chunked" but let's be coherent with Amazon's terminology. */

      ldpp_dout(s, 10) << "body content detected in multiple chunks" << dendl;

      /* payload in multiple chunks */

      switch(s->op_type)
      {
        case RGW_OP_PUT_OBJ:
          break;
        default:
          ldpp_dout(s, 10) << "ERROR: AWS4 completion for this operation NOT IMPLEMENTED (streaming mode)" << dendl;
          throw -ERR_NOT_IMPLEMENTED;
      }

      ldpp_dout(s, 10) << "aws4 seed signature ok... delaying v4 auth" << dendl;

      /* In the case of streamed payload client sets the x-amz-content-sha256
       * to "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" but uses "UNSIGNED-PAYLOAD"
       * when constructing the Canonical Request. */

      /* In the case of single-chunk upload client set the header's value is
       * coherent with the one used for Canonical Request crafting. */

      /* In the case of query string-based authentication there should be no
       * x-amz-content-sha256 header and the value "UNSIGNED-PAYLOAD" is used
       * for CanonReq. */
      const auto cmpl_factory = std::bind(AWSv4ComplMulti::create,
                                          s,
                                          date,
                                          credential_scope,
                                          client_signature,
                                          std::placeholders::_1);
      return {
        access_key_id,
        client_signature,
        session_token,
        std::move(string_to_sign),
        sig_factory,
        cmpl_factory
      };
    }
  }
}


boost::optional<std::string>
AWSGeneralBoto2Abstractor::get_v4_canonical_headers(
  const req_info& info,
  const std::string_view& signedheaders,
  const bool using_qs) const
{
  return rgw::auth::s3::get_v4_canonical_headers(info, signedheaders,
                                                 using_qs, true);
}


AWSEngine::VersionAbstractor::auth_data_t
AWSGeneralAbstractor::get_auth_data_v2(const req_state* const s) const
{
  std::string_view access_key_id;
  std::string_view signature;
  std::string_view session_token;
  bool qsr = false;

  const char* http_auth = s->info.env->get("HTTP_AUTHORIZATION");
  if (! http_auth || http_auth[0] == '\0') {
    /* Credentials are provided in query string. We also need to verify
     * the "Expires" parameter now. */
    access_key_id = s->info.args.get("AWSAccessKeyId");
    signature = s->info.args.get("Signature");
    qsr = true;

    std::string_view expires = s->info.args.get("Expires");
    if (expires.empty()) {
      throw -EPERM;
    }

    /* It looks we have the guarantee that expires is a null-terminated,
     * and thus string_view::data() can be safely used. */
    const time_t exp = atoll(expires.data());
    time_t now;
    time(&now);

    if (now >= exp) {
      throw -EPERM;
    }
    if (s->info.args.exists("x-amz-security-token")) {
      session_token = s->info.args.get("x-amz-security-token");
      if (session_token.size() == 0) {
        throw -EPERM;
      }
    }

  } else {
    /* The "Authorization" HTTP header is being used. */
    const std::string_view auth_str(http_auth + strlen("AWS "));
    const size_t pos = auth_str.rfind(':');
    if (pos != std::string_view::npos) {
      access_key_id = auth_str.substr(0, pos);
      signature = auth_str.substr(pos + 1);
    }

    auto token = s->info.env->get_optional("HTTP_X_AMZ_SECURITY_TOKEN");
    if (token) {
      session_token = *token;
      if (session_token.size() == 0) {
        throw -EPERM;
      }
    }
  }

  /* Let's canonize the HTTP headers that are covered by the AWS auth v2. */
  std::string string_to_sign;
  utime_t header_time;
  if (! rgw_create_s3_canonical_header(s, s->info, &header_time, string_to_sign,
        qsr)) {
    ldpp_dout(s, 10) << "failed to create the canonized auth header\n"
                   << rgw::crypt_sanitize::auth{s,string_to_sign} << dendl;
    throw -EPERM;
  }

  ldpp_dout(s, 10) << "string_to_sign:\n"
                 << rgw::crypt_sanitize::auth{s,string_to_sign} << dendl;

  if (!qsr && !is_time_skew_ok(header_time)) {
    throw -ERR_REQUEST_TIME_SKEWED;
  }

  return {
    std::move(access_key_id),
    std::move(signature),
    std::move(session_token),
    std::move(string_to_sign),
    rgw::auth::s3::get_v2_signature,
    null_completer_factory
  };
}


AWSEngine::VersionAbstractor::auth_data_t
AWSBrowserUploadAbstractor::get_auth_data_v2(const req_state* const s) const
{
  return {
    s->auth.s3_postobj_creds.access_key,
    s->auth.s3_postobj_creds.signature,
    s->auth.s3_postobj_creds.x_amz_security_token,
    s->auth.s3_postobj_creds.encoded_policy.to_str(),
    rgw::auth::s3::get_v2_signature,
    null_completer_factory
  };
}

AWSEngine::VersionAbstractor::auth_data_t
AWSBrowserUploadAbstractor::get_auth_data_v4(const req_state* const s) const
{
  const std::string_view credential = s->auth.s3_postobj_creds.x_amz_credential;

  /* grab access key id */
  const size_t pos = credential.find("/");
  const std::string_view access_key_id = credential.substr(0, pos);
  ldpp_dout(s, 10) << "access key id = " << access_key_id << dendl;

  /* grab credential scope */
  const std::string_view credential_scope = credential.substr(pos + 1);
  ldpp_dout(s, 10) << "credential scope = " << credential_scope << dendl;

  const auto sig_factory = std::bind(rgw::auth::s3::get_v4_signature,
                                     credential_scope,
                                     std::placeholders::_1,
                                     std::placeholders::_2,
                                     std::placeholders::_3,
                                     s);

  return {
    access_key_id,
    s->auth.s3_postobj_creds.signature,
    s->auth.s3_postobj_creds.x_amz_security_token,
    s->auth.s3_postobj_creds.encoded_policy.to_str(),
    sig_factory,
    null_completer_factory
  };
}

AWSEngine::VersionAbstractor::auth_data_t
AWSBrowserUploadAbstractor::get_auth_data(const req_state* const s) const
{
  if (s->auth.s3_postobj_creds.x_amz_algorithm == AWS4_HMAC_SHA256_STR) {
    ldpp_dout(s, 0) << "Signature verification algorithm AWS v4"
                     << " (AWS4-HMAC-SHA256)" << dendl;
    return get_auth_data_v4(s);
  } else {
    ldpp_dout(s, 0) << "Signature verification algorithm AWS v2" << dendl;
    return get_auth_data_v2(s);
  }
}

AWSEngine::result_t
AWSEngine::authenticate(const DoutPrefixProvider* dpp, const req_state* const s, optional_yield y) const
{
  /* Small reminder: an ver_abstractor is allowed to throw! */
  const auto auth_data = ver_abstractor.get_auth_data(s);

  if (auth_data.access_key_id.empty() || auth_data.client_signature.empty()) {
    return result_t::deny(-EINVAL);
  } else {
    return authenticate(dpp,
                        auth_data.access_key_id,
		        auth_data.client_signature,
			auth_data.session_token,
			auth_data.string_to_sign,
                        auth_data.signature_factory,
			auth_data.completer_factory,
			s, y);
  }
}

} // namespace rgw::auth::s3

rgw::LDAPHelper* rgw::auth::s3::LDAPEngine::ldh = nullptr;
std::mutex rgw::auth::s3::LDAPEngine::mtx;

void rgw::auth::s3::LDAPEngine::init(CephContext* const cct)
{
  if (! cct->_conf->rgw_s3_auth_use_ldap ||
      cct->_conf->rgw_ldap_uri.empty()) {
    return;
  }

  if (! ldh) {
    std::lock_guard<std::mutex> lck(mtx);
    if (! ldh) {
      const string& ldap_uri = cct->_conf->rgw_ldap_uri;
      const string& ldap_binddn = cct->_conf->rgw_ldap_binddn;
      const string& ldap_searchdn = cct->_conf->rgw_ldap_searchdn;
      const string& ldap_searchfilter = cct->_conf->rgw_ldap_searchfilter;
      const string& ldap_dnattr = cct->_conf->rgw_ldap_dnattr;
      std::string ldap_bindpw = parse_rgw_ldap_bindpw(cct);

      ldh = new rgw::LDAPHelper(ldap_uri, ldap_binddn, ldap_bindpw,
                                ldap_searchdn, ldap_searchfilter, ldap_dnattr);

      ldh->init();
      ldh->bind();
    }
  }
}

bool rgw::auth::s3::LDAPEngine::valid() {
  std::lock_guard<std::mutex> lck(mtx);
  return (!!ldh);
}

rgw::auth::RemoteApplier::acl_strategy_t
rgw::auth::s3::LDAPEngine::get_acl_strategy() const
{
  //This is based on the assumption that the default acl strategy in
  // get_perms_from_aclspec, will take care. Extra acl spec is not required.
  return nullptr;
}

rgw::auth::RemoteApplier::AuthInfo
rgw::auth::s3::LDAPEngine::get_creds_info(const rgw::RGWToken& token) const noexcept
{
  /* The short form of "using" can't be used here -- we're aliasing a class'
   * member. */
  using acct_privilege_t = \
    rgw::auth::RemoteApplier::AuthInfo::acct_privilege_t;

  return rgw::auth::RemoteApplier::AuthInfo {
    rgw_user(token.id),
    token.id,
    RGW_PERM_FULL_CONTROL,
    acct_privilege_t::IS_PLAIN_ACCT,
    rgw::auth::RemoteApplier::AuthInfo::NO_ACCESS_KEY,
    rgw::auth::RemoteApplier::AuthInfo::NO_SUBUSER,
    TYPE_LDAP
  };
}

rgw::auth::Engine::result_t
rgw::auth::s3::LDAPEngine::authenticate(
  const DoutPrefixProvider* dpp,
  const std::string_view& access_key_id,
  const std::string_view& signature,
  const std::string_view& session_token,
  const string_to_sign_t& string_to_sign,
  const signature_factory_t&,
  const completer_factory_t& completer_factory,
  const req_state* const s,
  optional_yield y) const
{
  /* boost filters and/or string_ref may throw on invalid input */
  rgw::RGWToken base64_token;
  try {
    base64_token = rgw::from_base64(access_key_id);
  } catch (...) {
    base64_token = std::string("");
  }

  if (! base64_token.valid()) {
    return result_t::deny();
  }

  //TODO: Uncomment, when we have a migration plan in place.
  //Check if a user of type other than 'ldap' is already present, if yes, then
  //return error.
  /*RGWUserInfo user_info;
  user_info.user_id = base64_token.id;
  if (rgw_get_user_info_by_uid(driver, user_info.user_id, user_info) >= 0) {
    if (user_info.type != TYPE_LDAP) {
      ldpp_dout(dpp, 10) << "ERROR: User id of type: " << user_info.type << " is already present" << dendl;
      return nullptr;
    }
  }*/

  if (ldh->auth(base64_token.id, base64_token.key) != 0) {
    return result_t::deny(-ERR_INVALID_ACCESS_KEY);
  }

  auto apl = apl_factory->create_apl_remote(cct, s, get_acl_strategy(),
                                            get_creds_info(base64_token));
  return result_t::grant(std::move(apl), completer_factory(boost::none));
} /* rgw::auth::s3::LDAPEngine::authenticate */

void rgw::auth::s3::LDAPEngine::shutdown() {
  if (ldh) {
    delete ldh;
    ldh = nullptr;
  }
}

/* LocalEngine */
rgw::auth::Engine::result_t
rgw::auth::s3::LocalEngine::authenticate(
  const DoutPrefixProvider* dpp,
  const std::string_view& _access_key_id,
  const std::string_view& signature,
  const std::string_view& session_token,
  const string_to_sign_t& string_to_sign,
  const signature_factory_t& signature_factory,
  const completer_factory_t& completer_factory,
  const req_state* const s,
  optional_yield y) const
{
  /* get the user info */
  std::unique_ptr<rgw::sal::User> user;
  const std::string access_key_id(_access_key_id);
  /* TODO(rzarzynski): we need to have string-view taking variant. */
  if (driver->get_user_by_access_key(dpp, access_key_id, y, &user) < 0) {
      ldpp_dout(dpp, 5) << "error reading user info, uid=" << access_key_id
              << " can't authenticate" << dendl;
      return result_t::reject(-ERR_INVALID_ACCESS_KEY);
  }
  //TODO: Uncomment, when we have a migration plan in place.
  /*else {
    if (s->user->type != TYPE_RGW) {
      ldpp_dout(dpp, 10) << "ERROR: User id of type: " << s->user->type
                     << " is present" << dendl;
      throw -EPERM;
    }
  }*/

  const auto iter = user->get_info().access_keys.find(access_key_id);
  if (iter == std::end(user->get_info().access_keys)) {
    ldpp_dout(dpp, 0) << "ERROR: access key not encoded in user info" << dendl;
    return result_t::reject(-EPERM);
  }
  const RGWAccessKey& k = iter->second;

  const VersionAbstractor::server_signature_t server_signature = \
    signature_factory(cct, k.key, string_to_sign);
  auto compare = signature.compare(server_signature);

  ldpp_dout(dpp, 15) << "string_to_sign="
                 << rgw::crypt_sanitize::log_content{string_to_sign}
                 << dendl;
  ldpp_dout(dpp, 15) << "server signature=" << server_signature << dendl;
  ldpp_dout(dpp, 15) << "client signature=" << signature << dendl;
  ldpp_dout(dpp, 15) << "compare=" << compare << dendl;

  if (compare != 0) {
    return result_t::reject(-ERR_SIGNATURE_NO_MATCH);
  }

  auto apl = apl_factory->create_apl_local(cct, s, user->get_info(),
                                           k.subuser, std::nullopt, access_key_id);
  return result_t::grant(std::move(apl), completer_factory(k.key));
}

rgw::auth::RemoteApplier::AuthInfo
rgw::auth::s3::STSEngine::get_creds_info(const STS::SessionToken& token) const noexcept
{
  using acct_privilege_t = \
    rgw::auth::RemoteApplier::AuthInfo::acct_privilege_t;

  return rgw::auth::RemoteApplier::AuthInfo {
    token.user,
    token.acct_name,
    token.perm_mask,
    (token.is_admin) ? acct_privilege_t::IS_ADMIN_ACCT: acct_privilege_t::IS_PLAIN_ACCT,
    token.access_key_id,
    rgw::auth::RemoteApplier::AuthInfo::NO_SUBUSER,
    token.acct_type
  };
}

int
rgw::auth::s3::STSEngine::get_session_token(const DoutPrefixProvider* dpp, const std::string_view& session_token,
                                            STS::SessionToken& token) const
{
  string decodedSessionToken;
  try {
    decodedSessionToken = rgw::from_base64(session_token);
  } catch (...) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid session token, not base64 encoded." << dendl;
    return -EINVAL;
  }

  auto* cryptohandler = cct->get_crypto_handler(CEPH_CRYPTO_AES);
  if (! cryptohandler) {
    return -EINVAL;
  }
  string secret_s = cct->_conf->rgw_sts_key;
  buffer::ptr secret(secret_s.c_str(), secret_s.length());
  int ret = 0;
  if (ret = cryptohandler->validate_secret(secret); ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid secret key" << dendl;
    return -EINVAL;
  }
  string error;
  std::unique_ptr<CryptoKeyHandler> keyhandler(cryptohandler->get_key_handler(secret, error));
  if (! keyhandler) {
    return -EINVAL;
  }
  error.clear();

  string decrypted_str;
  buffer::list en_input, dec_output;
  en_input = buffer::list::static_from_string(decodedSessionToken);

  ret = keyhandler->decrypt(en_input, dec_output, &error);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: Decryption failed: " << error << dendl;
    return -EPERM;
  } else {
    try {
      dec_output.append('\0');
      auto iter = dec_output.cbegin();
      decode(token, iter);
    } catch (const buffer::error& e) {
      ldpp_dout(dpp, 0) << "ERROR: decode SessionToken failed: " << error << dendl;
      return -EINVAL;
    }
  }
  return 0;
}

rgw::auth::Engine::result_t
rgw::auth::s3::STSEngine::authenticate(
  const DoutPrefixProvider* dpp,
  const std::string_view& _access_key_id,
  const std::string_view& signature,
  const std::string_view& session_token,
  const string_to_sign_t& string_to_sign,
  const signature_factory_t& signature_factory,
  const completer_factory_t& completer_factory,
  const req_state* const s,
  optional_yield y) const
{
  if (! s->info.args.exists("x-amz-security-token") &&
      ! s->info.env->exists("HTTP_X_AMZ_SECURITY_TOKEN") &&
      s->auth.s3_postobj_creds.x_amz_security_token.empty()) {
    return result_t::deny();
  }

  STS::SessionToken token;
  if (int ret = get_session_token(dpp, session_token, token); ret < 0) {
    return result_t::reject(ret);
  }
  //Authentication
  //Check if access key is not the same passed in by client
  if (token.access_key_id != _access_key_id) {
    ldpp_dout(dpp, 0) << "Invalid access key" << dendl;
    return result_t::reject(-EPERM);
  }
  //Check if the token has expired
  if (! token.expiration.empty()) {
    std::string expiration = token.expiration;
    if (! expiration.empty()) {
      boost::optional<real_clock::time_point> exp = ceph::from_iso_8601(expiration, false);
      if (exp) {
        real_clock::time_point now = real_clock::now();
        if (now >= *exp) {
          ldpp_dout(dpp, 0) << "ERROR: Token expired" << dendl;
          return result_t::reject(-EPERM);
        }
      } else {
        ldpp_dout(dpp, 0) << "ERROR: Invalid expiration: " << expiration << dendl;
        return result_t::reject(-EPERM);
      }
    }
  }
  //Check for signature mismatch
  const VersionAbstractor::server_signature_t server_signature = \
    signature_factory(cct, token.secret_access_key, string_to_sign);
  auto compare = signature.compare(server_signature);

  ldpp_dout(dpp, 15) << "string_to_sign="
                 << rgw::crypt_sanitize::log_content{string_to_sign}
                 << dendl;
  ldpp_dout(dpp, 15) << "server signature=" << server_signature << dendl;
  ldpp_dout(dpp, 15) << "client signature=" << signature << dendl;
  ldpp_dout(dpp, 15) << "compare=" << compare << dendl;

  if (compare != 0) {
    return result_t::reject(-ERR_SIGNATURE_NO_MATCH);
  }

  // Get all the authorization info
  std::unique_ptr<rgw::sal::User> user;
  rgw_user user_id;
  string role_id;
  rgw::auth::RoleApplier::Role r;
  rgw::auth::RoleApplier::TokenAttrs t_attrs;
  if (! token.roleId.empty()) {
    std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(token.roleId);
    if (role->get_by_id(dpp, y) < 0) {
      return result_t::deny(-EPERM);
    }
    r.id = token.roleId;
    r.name = role->get_name();
    r.tenant = role->get_tenant();

    vector<string> role_policy_names = role->get_role_policy_names();
    for (auto& policy_name : role_policy_names) {
      string perm_policy;
      if (int ret = role->get_role_policy(dpp, policy_name, perm_policy); ret == 0) {
        r.role_policies.push_back(std::move(perm_policy));
      }
    }
  }

  user = driver->get_user(token.user);
  if (! token.user.empty() && token.acct_type != TYPE_ROLE) {
    // get user info
    int ret = user->load_user(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 5) << "ERROR: failed reading user info: uid=" << token.user << dendl;
      return result_t::reject(-EPERM);
    }
  }

  if (token.acct_type == TYPE_KEYSTONE || token.acct_type == TYPE_LDAP) {
    auto apl = remote_apl_factory->create_apl_remote(cct, s, get_acl_strategy(),
                                            get_creds_info(token));
    return result_t::grant(std::move(apl), completer_factory(token.secret_access_key));
  } else if (token.acct_type == TYPE_ROLE) {
    t_attrs.user_id = std::move(token.user); // This is mostly needed to assign the owner of a bucket during its creation
    t_attrs.token_policy = std::move(token.policy);
    t_attrs.role_session_name = std::move(token.role_session);
    t_attrs.token_claims = std::move(token.token_claims);
    t_attrs.token_issued_at = std::move(token.issued_at);
    t_attrs.principal_tags = std::move(token.principal_tags);
    auto apl = role_apl_factory->create_apl_role(cct, s, r, t_attrs);
    return result_t::grant(std::move(apl), completer_factory(token.secret_access_key));
  } else { // This is for all local users of type TYPE_RGW or TYPE_NONE
    string subuser;
    auto apl = local_apl_factory->create_apl_local(cct, s, user->get_info(), subuser, token.perm_mask, std::string(_access_key_id));
    return result_t::grant(std::move(apl), completer_factory(token.secret_access_key));
  }
}

bool rgw::auth::s3::S3AnonymousEngine::is_applicable(
  const req_state* s
) const noexcept {
  AwsVersion version;
  AwsRoute route;
  std::tie(version, route) = discover_aws_flavour(s->info);

  /* If HTTP OPTIONS and no authentication provided using the
   * anonymous engine is applicable */
  if (s->op == OP_OPTIONS && version == AwsVersion::UNKNOWN) {
    return true;
  }

  return route == AwsRoute::QUERY_STRING && version == AwsVersion::UNKNOWN;
}
