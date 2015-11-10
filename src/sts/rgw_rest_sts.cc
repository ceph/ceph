// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <string.h>

#include "common/ceph_crypto.h"
#include "common/Formatter.h"
#include "common/utf8.h"
#include "common/armor.h"
#include "common/ceph_json.h"

#include "rgw_rest.h"
#include "rgw_rest_sts.h"
// #include "rgw_rest_s3.h"
// #include "rgw_auth_s3.h"
// #include "rgw_acl.h"
// #include "rgw_policy_s3.h"
#include "rgw_user.h"

#include "rgw_client_io.h"

#define dout_subsys ceph_subsys_sts

using namespace ceph::crypto;

// indexed by tsErrorType
string error_types[] = {"Unknown", "Sender", "Receiver"};

void encode_base64_thing(string s, bufferlist &out)
{
  int est_encoded_len = 9 + (41 * s.length() / 30);
  char temp[est_encoded_len];
  const char *src = s.c_str();
// fixme..  how to do line wrapping?
  int olen = ceph_armor(temp, temp + est_encoded_len,
	src, src + s.length());
  out.append(temp, olen);
}

void assume_role_response::dump(Formatter *f)
{
  bufferlist encoded_token;
  bufferlist encoded_key;
  char encoded_uuid[38];
  char encoded_time[30];

  encode_base64_thing(credentials.session_token, encoded_token);
  encoded_token.append('\0');
  encode_base64_thing(credentials.secret_access_key, encoded_key);
  encoded_key.append('\0');
  request_id.print(encoded_uuid);
  encoded_uuid[37] = 0;
  time_t uv;
  struct tm res[1];
  uv = credentials.expiration.sec();
  struct tm *tmp = gmtime_r(&uv, res);
  strftime(encoded_time, sizeof encoded_time, "%Y-%m-%dT%T.000Z", tmp);

  f->open_object_section_in_ns("AssumeRoleResponse",
			"http://s3.amazonaws.com/doc/2011-06-15/");
    f->open_object_section("AssumeRoleResult");
      f->open_object_section("Credentials");
	f->dump_string_linewrap("SessionToken", encoded_token.c_str());
	f->dump_string_linewrap("SecretAccessKey", encoded_key.c_str());
	f->dump_string("Expiration", encoded_time);
	f->dump_string("AccessKeyId", credentials.access_key_id);
      f->close_section();
      f->open_object_section("AssumedRoleUser");
	f->dump_string("Arn", assumed_role_user.arn);
	f->dump_string("AssumedRoleId", assumed_role_user.assumed_role_id);
      f->close_section();
      f->dump_int("PackedPolicySize", packed_policy_size);
    f->close_section();
    f->open_object_section("ResponseMetadata");
      f->dump_string("RequestId", encoded_uuid);
    f->close_section();
  f->close_section();
}

rgw_http_errors rgw_http_sts_errors({
    { ERR_IDP_REJECTED_CLAIM, {403, "IDPRejectedClaim" }},
    { ERR_UNKNOWN_ERROR, {500, "Unknown" }},
    { ERR_INVALID_ACTION, {400, "InvalidAction" }},
    { ERR_INVALID_TOKEN, {403, "InvalidClientTokenId" }},
    { ERR_MALFORMED_INPUT, {400, "MalformedInput" }},
    { ERR_MISSING_AUTH, {403, "MissingAuthenticationToken" }},
});

sts_err::sts_err() : type(Unknown)
{
  request_id.generate_random();
}


void sts_err::dump(Formatter *f) const
{
  char encoded_uuid[38];

  request_id.print(encoded_uuid);
  encoded_uuid[37] = 0;

  f->open_object_section_in_ns("ErrorResponse",
			"http://s3.amazonaws.com/doc/2011-06-15/");
    f->open_object_section("Error");
      f->dump_string("Type", error_types[type]);
      if (!s3_code_E.empty())
        f->dump_string("Code", s3_code_E);
      if (!message_E.empty())
        f->dump_string("Message", message_E);
    f->close_section();
    f->dump_string("RequestId", encoded_uuid);
  f->close_section();
}

bool sts_err::set_rgw_err(int err_no) {
  rgw_http_errors::const_iterator r;

  r = rgw_http_sts_errors.find(err_no);
  if (r != rgw_http_sts_errors.end()) {
    http_ret_E = r->second.first;
    s3_code_E = r->second.second;
    return true;
  }
  return rgw_err::set_rgw_err(err_no);
}

#if 0
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
#endif

class RGWAssumeRole_STS : public RGWRESTOp {

public:
  RGWAssumeRole_STS() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("sts", RGW_CAP_READ);
  }
  void execute();

  virtual const string name() { return "get_usage"; }
};

#if 0
void RGWAssumeRole_STS::send_response()
{
  if (ret)
    s->set_req_state_err(ret);
  dump_errno(s);
  dump_start(s);
  end_header(s, NULL, "application/xml");

  if (!ret) {
    list_all_buckets_start(s);
    dump_owner(s, s->user.user_id, s->user.display_name);
    s->formatter->open_array_section("Buckets");
    sent_data = true;
  }
  rgw_flush_formatter(s, s->formatter);
}

void RGWAssumeRole_STS::send_response_end()
{
  if (sent_data) {
    s->formatter->close_section();
    list_all_buckets_end(s);
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}
#endif

#if 0
int RGWPostObj_STS::get_params()
{
  // get the part boundary
  string req_content_type_str = s->info.env->get("CONTENT_TYPE", "");
  string req_content_type;
  map<string, string> params;

  if (s->expect_cont) {
    /* ok, here it really gets ugly. With POST, the params are embedded in the
     * request body, so we need to continue before being able to actually look
     * at them. This diverts from the usual request flow.
     */
    dump_continue(s);
    s->expect_cont = false;
  }

  parse_params(req_content_type_str, req_content_type, params);

  if (req_content_type.compare("multipart/form-data") != 0) {
    err_msg = "Request Content-Type is not multipart/form-data";
    return -EINVAL;
  }

  if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
    ldout(s->cct, 20) << "request content_type_str=" << req_content_type_str << dendl;
    ldout(s->cct, 20) << "request content_type params:" << dendl;
    map<string, string>::iterator iter;
    for (iter = params.begin(); iter != params.end(); ++iter) {
      ldout(s->cct, 20) << " " << iter->first << " -> " << iter->second << dendl;
    }
  }

  ldout(s->cct, 20) << "adding bucket to policy env: " << s->bucket.name << dendl;
  env.add_var("bucket", s->bucket.name);

  map<string, string>::iterator iter = params.find("boundary");
  if (iter == params.end()) {
    err_msg = "Missing multipart boundary specification";
    return -EINVAL;
  }

  // create the boundary
  boundary = "--";
  boundary.append(iter->second);

  bool done;
  do {
    struct post_form_part part;
    int r = read_form_part_header(&part, &done);
    if (r < 0)
      return r;
    
    if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
      map<string, struct post_part_field, ltstr_nocase>::iterator piter;
      for (piter = part.fields.begin(); piter != part.fields.end(); ++piter) {
        ldout(s->cct, 20) << "read part header: name=" << part.name << " content_type=" << part.content_type << dendl;
        ldout(s->cct, 20) << "name=" << piter->first << dendl;
        ldout(s->cct, 20) << "val=" << piter->second.val << dendl;
        ldout(s->cct, 20) << "params:" << dendl;
        map<string, string>& params = piter->second.params;
        for (iter = params.begin(); iter != params.end(); ++iter) {
          ldout(s->cct, 20) << " " << iter->first << " -> " << iter->second << dendl;
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
      data_pending = true;
      break;
    }

    bool boundary;
    uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
    r = read_data(part.data, chunk_size, &boundary, &done);
    if (!boundary) {
      err_msg = "Couldn't find boundary";
      return -EINVAL;
    }
    parts[part.name] = part;
    string part_str(part.data.c_str(), part.data.length());
    env.add_var(part.name, part_str);
  } while (!done);

  string object_str;
  if (!part_str("key", &object_str)) {
    err_msg = "Key not specified";
    return -EINVAL;
  }

  s->object = rgw_obj_key(object_str);

  rebuild_key(s->object.name);

  if (s->object.empty()) {
    err_msg = "Empty object name";
    return -EINVAL;
  }

  env.add_var("key", s->object.name);

  part_str("Content-Type", &content_type);
  env.add_var("Content-Type", content_type);

  map<string, struct post_form_part, ltstr_nocase>::iterator piter = parts.upper_bound(RGW_AMZ_META_PREFIX);
  for (; piter != parts.end(); ++piter) {
    string n = piter->first;
    if (strncasecmp(n.c_str(), RGW_AMZ_META_PREFIX, sizeof(RGW_AMZ_META_PREFIX) - 1) != 0)
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

  int r = get_policy();
  if (r < 0)
    return r;

  min_len = post_policy.min_length;
  max_len = post_policy.max_length;

  return 0;
}

int RGWPostObj_STS::get_policy()
{
  bufferlist encoded_policy;

  if (part_bl("policy", &encoded_policy)) {

    // check that the signature matches the encoded policy
    string s3_access_key;
    if (!part_str("AWSAccessKeyId", &s3_access_key)) {
      ldout(s->cct, 0) << "No S3 access key found!" << dendl;
      err_msg = "Missing access key";
      return -EINVAL;
    }
    string received_signature_str;
    if (!part_str("signature", &received_signature_str)) {
      ldout(s->cct, 0) << "No signature found!" << dendl;
      err_msg = "Missing signature";
      return -EINVAL;
    }

    RGWUserInfo user_info;

    ret = rgw_get_user_info_by_access_key(store, s3_access_key, user_info);
    if (ret < 0) {
      // Try keystone authentication as well
      int keystone_result = -EINVAL;
      if (!store->ctx()->_conf->rgw_s3_auth_use_keystone ||
	  store->ctx()->_conf->rgw_keystone_url.empty()) {
        return -EACCES;
      }
      dout(20) << "s3 keystone: trying keystone auth" << dendl;

      RGW_Auth_STS_Keystone_ValidateToken keystone_validator(store->ctx());
      keystone_result = keystone_validator.validate_s3token(s3_access_key,string(encoded_policy.c_str(),encoded_policy.length()),received_signature_str);

      if (keystone_result < 0) {
        ldout(s->cct, 0) << "User lookup failed!" << dendl;
        err_msg = "Bad access key / signature";
        return -EACCES;
      }

      user_info.user_id = keystone_validator.response.token.tenant.id;
      user_info.display_name = keystone_validator.response.token.tenant.name;

      /* try to store user if it not already exists */
      if (rgw_get_user_info_by_uid(store, keystone_validator.response.token.tenant.id, user_info) < 0) {
        int ret = rgw_store_user_info(store, user_info, NULL, NULL, 0, true);
        if (ret < 0) {
          dout(10) << "NOTICE: failed to store new user's info: ret=" << ret << dendl;
        }

        s->perm_mask = RGW_PERM_FULL_CONTROL;
      }
    } else {
      map<string, RGWAccessKey> access_keys  = user_info.access_keys;

      map<string, RGWAccessKey>::const_iterator iter = access_keys.find(s3_access_key);
      // We know the key must exist, since the user was returned by
      // rgw_get_user_info_by_access_key, but it doesn't hurt to check!
      if (iter == access_keys.end()) {
	ldout(s->cct, 0) << "Secret key lookup failed!" << dendl;
	err_msg = "No secret key for matching access key";
	return -EACCES;
      }
      string s3_secret_key = (iter->second).key;

      char expected_signature_char[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];

      calc_hmac_sha1(s3_secret_key.c_str(), s3_secret_key.size(), encoded_policy.c_str(), encoded_policy.length(), expected_signature_char);
      bufferlist expected_signature_hmac_raw;
      bufferlist expected_signature_hmac_encoded;
      expected_signature_hmac_raw.append(expected_signature_char, CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);
      expected_signature_hmac_raw.encode_base64(expected_signature_hmac_encoded);
      expected_signature_hmac_encoded.append((char)0); /* null terminate */

      if (received_signature_str.compare(expected_signature_hmac_encoded.c_str()) != 0) {
	ldout(s->cct, 0) << "Signature verification failed!" << dendl;
	ldout(s->cct, 0) << "received: " << received_signature_str.c_str() << dendl;
	ldout(s->cct, 0) << "expected: " << expected_signature_hmac_encoded.c_str() << dendl;
	err_msg = "Bad access key / signature";
	return -EACCES;
      }
    }

    ldout(s->cct, 0) << "Successful Signature Verification!" << dendl;
    bufferlist decoded_policy;
    try {
      decoded_policy.decode_base64(encoded_policy);
    } catch (buffer::error& err) {
      ldout(s->cct, 0) << "failed to decode_base64 policy" << dendl;
      err_msg = "Could not decode policy";
      return -EINVAL;
    }

    decoded_policy.append('\0'); // NULL terminate

    ldout(s->cct, 0) << "POST policy: " << decoded_policy.c_str() << dendl;

    int r = post_policy.from_json(decoded_policy, err_msg);
    if (r < 0) {
      if (err_msg.empty()) {
	err_msg = "Failed to parse policy";
      }
      ldout(s->cct, 0) << "failed to parse policy" << dendl;
      return -EINVAL;
    }

    post_policy.set_var_checked("AWSAccessKeyId");
    post_policy.set_var_checked("policy");
    post_policy.set_var_checked("signature");

    r = post_policy.check(&env, err_msg);
    if (r < 0) {
      if (err_msg.empty()) {
	err_msg = "Policy check failed";
      }
      ldout(s->cct, 0) << "policy check failed" << dendl;
      return r;
    }

    s->user = user_info;
    s->owner.set_id(user_info.user_id);
    s->owner.set_name(user_info.display_name);
  } else {
    ldout(s->cct, 0) << "No attached policy found!" << dendl;
  }

  string canned_acl;
  part_str("acl", &canned_acl);

  RGWAccessControlPolicy_STS s3policy(s->cct);
  ldout(s->cct, 20) << "canned_acl=" << canned_acl << dendl;
  if (s3policy.create_canned(s->owner, s->bucket_owner, canned_acl) < 0) {
    err_msg = "Bad canned ACLs";
    return -EINVAL;
  }

  policy = s3policy;

  return 0;
}
#endif

class RGWGetPost_STS : public RGWOp {
protected:
  bool get_flag;
public:
  RGWGetPost_STS(bool _gf) : get_flag(_gf) {}
  virtual int verify_permission();
  virtual void execute();
  virtual const string name() { return get_flag ? "get_sts" : "post_sts"; }
};

int RGWGetPost_STS::verify_permission()
{
	// XXX doubt it's this simple.
	return 0;
}

void make_fake_response(assume_role_response &response)
{
  unsigned char x_session[] = {
    0x1, 0xa, 0x3, 0x61, 0x77, 0x73, 0x10, 0xf4, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0x1, 0x17, 0x0, 0xc3, 0xcb, 0x12, 0xd7, 0x3b,
    0xeb, 0x86, 0xcd, 0xac, 0x2f, 0x52, 0x0, 0xf0, 0x52, 0x33, 0x6d, 0xb0,
    0xc, 0xe9, 0x38, 0xc7, 0x81, 0xc8, 0x67, 0xc8, 0xf8, 0x15, 0x94, 0xf0,
    0x75, 0x5, 0x8b, 0x5a, 0xc2, 0x96, 0x1c, 0x60, 0x6e, 0x16, 0xac, 0x0,
    0x78, 0xc8, 0x9c, 0x45, 0x79, 0xb1, 0x7e, 0x94, 0x8f, 0x7c, 0x87, 0xa8,
    0x21, 0x84, 0x6a, 0x4d, 0xf9, 0x5f, 0x28, 0x3f, 0x18, 0x52, 0xec, 0x2d,
    0x84, 0xc, 0x7b, 0x99, 0x21, 0x8, 0xfe, 0xa9, 0xf, 0xa4, 0xa3, 0xe2,
    0xfe, 0x43, 0x1c, 0x19, 0xd4, 0x2b, 0x98, 0x67, 0x5e, 0x7a, 0x13, 0x38,
    0x20, 0x2d, 0x4d, 0xb4, 0x19, 0x94, 0xa6, 0x9d, 0xb0, 0x50, 0x4f, 0x29,
    0x85, 0x46, 0x5a, 0x9a, 0x92, 0xac, 0x6e, 0xea, 0xc4, 0xf, 0x2d, 0xa,
    0x3e, 0x4c, 0x90, 0xd, 0x8c, 0x13, 0xed, 0x66, 0x74, 0xc2, 0xae, 0x55,
    0x49, 0x70, 0xef, 0xa7, 0xbe, 0x58, 0x53, 0xd1, 0xc5, 0xbe, 0x54, 0x5d,
    0xf1, 0x3c, 0x7a, 0xab, 0xa7, 0xc4, 0xf1, 0x84, 0x1c, 0x1c, 0xd5, 0x57,
    0x2, 0x48, 0x98, 0xf6, 0xae, 0x9d, 0xfb, 0x1a, 0x34, 0xac, 0xac, 0x13,
    0xdf, 0xcc, 0x55, 0xaa, 0xbe, 0xd9, 0xf, 0x4b, 0xb4, 0x88, 0xf3, 0xe4,
    0x50, 0xbe, 0xb8, 0x94, 0x86, 0x5b, 0xa8, 0x10, 0x33, 0xfa, 0xc7, 0x2a,
    0x2a, 0x69, 0x73, 0x9b, 0xc1, 0x43, 0xaf, 0x2a, 0x4d, 0xb, 0xd6, 0x23,
    0x73, 0xc7, 0xcf, 0x38, 0xb9, 0xfd, 0x15, 0x7f, 0x4a, 0x49, 0x8b, 0xca,
    0x4e, 0xbe, 0x2b, 0xbf, 0x1d, 0xe2, 0x48, 0x89, 0x53, 0x25, 0xa6, 0xc8,
    0x43, 0x8, 0xf6, 0x20, 0x20, 0x91, 0xfe, 0x82, 0xf1, 0x4,

  };
  unsigned char x_key[] = {
    0xc0, 0x96, 0xa5, 0xad, 0x75, 0x2d, 0x9c, 0x51, 0xc, 0x23, 0xf2,
    0xbb, 0x30, 0x31, 0xd, 0x1b, 0xf6, 0xcf, 0xc5, 0x17, 0xe2, 0x9,
    0x8c, 0xc4, 0x5c, 0x3, 0xf, 0x2c, 0x42, 0x84,
  };


  response.credentials.session_token = string((char*)x_session, sizeof x_session);
  response.credentials.secret_access_key = string((char*)x_key, sizeof x_key);
  struct timeval tv;
  tv.tv_sec = 1310772513;	// 2011-07-15 23:28:33 Z
  tv.tv_usec = 0;
  response.credentials.expiration.set_from_timeval(&tv);
  response.credentials.access_key_id = "AKIAIOSFODNN7EXAMPLE";
  response.assumed_role_user.arn = "arn:aws:sts::123456789012:assumed-role/demo/Bob";
  response.assumed_role_user.assumed_role_id = "ARO123EXAMPLE123:Bob";
  response.packed_policy_size = 6;
//  response.request_id.generate_random();
}

void RGWGetPost_STS::execute()
{
  assume_role_response response(dynamic_cast<sts_err*>(s->err)->request_id);
//  int ret = 0;
//  ret = ERR_INVALID_ACTION;
  s->formatter = new XMLFormatter(true);
//  tsErrorResponse err(Unknown, InvalidAction, "Why I don't know"); MAKE RESPONSE
  make_fake_response(response);
//  s->set_req_state_err(ret, "Why I don't know");
  dump_errno(s);
  end_header(s, dump_access_control_f(), "application/xml");
  response.dump(s->formatter);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

RGWOp *RGWHandler_STS::get_obj_op(bool get_data)
{
  RGWGetPost_STS *get_obj_op = new RGWGetPost_STS(get_data);
  return get_obj_op;
}

RGWOp *RGWHandler_STS::op_get()
{
  return get_obj_op(true);
}

RGWOp *RGWHandler_STS::op_head()
{
  return NULL;
}

RGWOp *RGWHandler_STS::op_put()
{
  return NULL;
}

RGWOp *RGWHandler_STS::op_delete()
{
  return NULL;
}

RGWOp *RGWHandler_STS::op_post()
{
  return get_obj_op(false);
}

RGWOp *RGWHandler_STS::op_options()
{
  return NULL;
}

int RGWHandler_STS::init_from_header(struct req_state *s, int default_formatter, bool configurable_format)
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
  s->info.args.parse();

#if 0
  /* must be called after the args parsing */
  int ret = allocate_formatter(s, default_formatter, configurable_format);
  if (ret < 0)
    return ret;
#endif

  if (*req_name != '/')
    return 0;

  req_name++;

  if (!*req_name)
    return 0;

// XXX make error here?
  return 0;
}

int RGWHandler_STS::init(RGWRados *store, struct req_state *s, RGWClientIO *cio)
{
  const char *cacl = s->info.env->get("HTTP_X_AMZ_ACL");
  if (cacl)
    s->canned_acl = cacl;

  s->has_acl_header = s->info.env->exists_prefix("HTTP_X_AMZ_GRANT");

  s->dialect = "sts";

  return RGWHandler::init(store, s, cio);
}


#if 0
/*
 * Try to validate S3 auth against keystone s3token interface
 */
int RGW_Auth_STS_Keystone_ValidateToken::validate_s3token(const string& auth_id, const string& auth_token, const string& auth_sign) {
  /* prepare keystone url */
  string keystone_url = cct->_conf->rgw_keystone_url;
  if (keystone_url[keystone_url.size() - 1] != '/')
    keystone_url.append("/");
  keystone_url.append("v2.0/s3tokens");

  /* set required headers for keystone request */
  append_header("X-Auth-Token", cct->_conf->rgw_keystone_admin_token);
  append_header("Content-Type", "application/json");

  /* encode token */
  bufferlist token_buff;
  bufferlist token_encoded;
  token_buff.append(auth_token);
  token_buff.encode_base64(token_encoded);
  token_encoded.append((char)0);

  /* create json credentials request body */
  JSONFormatter credentials(false);
  credentials.open_object_section("");
  credentials.open_object_section("credentials");
  credentials.dump_string("access", auth_id);
  credentials.dump_string("token", token_encoded.c_str());
  credentials.dump_string("signature", auth_sign);
  credentials.close_section();
  credentials.close_section();

  std::stringstream os;
  credentials.flush(os);
  set_tx_buffer(os.str());

  /* send request */
  int ret = process("POST", keystone_url.c_str());
  if (ret < 0) {
    dout(2) << "s3 keystone: token validation ERROR: " << rx_buffer.c_str() << dendl;
    return -EPERM;
  }

  /* now parse response */
  if (response.parse(cct, rx_buffer) < 0) {
    dout(2) << "s3 keystone: token parsing failed" << dendl;
    return -EPERM;
  }

  /* check if we have a valid role */
  bool found = false;
  list<string>::iterator iter;
  for (iter = roles_list.begin(); iter != roles_list.end(); ++iter) {
    if ((found=response.user.has_role(*iter))==true)
      break;
  }

  if (!found) {
    ldout(cct, 5) << "s3 keystone: user does not hold a matching role; required roles: " << cct->_conf->rgw_keystone_accepted_roles << dendl;
    return -EPERM;
  }

  /* everything seems fine, continue with this user */
  ldout(cct, 5) << "s3 keystone: validated token: " << response.token.tenant.name << ":" << response.user.name << " expires: " << response.token.expires << dendl;
  return 0;
}

static void init_anon_user(struct req_state *s)
{
  rgw_get_anon_user(s->user);
  s->perm_mask = RGW_PERM_FULL_CONTROL;
}
#endif

/*
 * verify that a signed request comes from the keyholder
 * by checking the signature against our locally-computed version
 */
int RGWHandler_STS::authorize()
{
#if 0
  bool qsr = false;
  string auth_id;
  string auth_sign;

  time_t now;
  time(&now);

  /* neither keystone and rados enabled; warn and exit! */
  if (!store->ctx()->_conf->rgw_s3_auth_use_rados
      && !store->ctx()->_conf->rgw_s3_auth_use_keystone) {
    dout(0) << "WARNING: no authorization backend enabled! Users will never authenticate." << dendl;
    return -EPERM;
  }

  if (s->op == OP_OPTIONS) {
    init_anon_user(s);
    return 0;
  }

  if (!s->http_auth || !(*s->http_auth)) {
    auth_id = s->info.args.get("AWSAccessKeyId");
    if (auth_id.size()) {
      auth_sign = s->info.args.get("Signature");

      string date = s->info.args.get("Expires");
      time_t exp = atoll(date.c_str());
      if (now >= exp)
        return -EPERM;

      qsr = true;
    } else {
      /* anonymous access */
      init_anon_user(s);
      return 0;
    }
  } else {
    if (strncmp(s->http_auth, "AWS ", 4))
      return -EINVAL;
    string auth_str(s->http_auth + 4);
    int pos = auth_str.rfind(':');
    if (pos < 0)
      return -EINVAL;

    auth_id = auth_str.substr(0, pos);
    auth_sign = auth_str.substr(pos + 1);
  }

  /* try keystone auth first */
  int keystone_result = -EINVAL;
  if (store->ctx()->_conf->rgw_s3_auth_use_keystone
      && !store->ctx()->_conf->rgw_keystone_url.empty()) {
    dout(20) << "s3 keystone: trying keystone auth" << dendl;

    RGW_Auth_STS_Keystone_ValidateToken keystone_validator(store->ctx());
    string token;

    if (!rgw_create_s3_canonical_header(s->info, &s->header_time, token, qsr)) {
        dout(10) << "failed to create auth header\n" << token << dendl;
    } else {
      keystone_result = keystone_validator.validate_s3token(auth_id, token, auth_sign);
      if (keystone_result == 0) {
	// Check for time skew first
	time_t req_sec = s->header_time.sec();

	if ((req_sec < now - RGW_AUTH_GRACE_MINS * 60 ||
	     req_sec > now + RGW_AUTH_GRACE_MINS * 60) && !qsr) {
	  dout(10) << "req_sec=" << req_sec << " now=" << now << "; now - RGW_AUTH_GRACE_MINS=" << now - RGW_AUTH_GRACE_MINS * 60 << "; now + RGW_AUTH_GRACE_MINS=" << now + RGW_AUTH_GRACE_MINS * 60 << dendl;
	  dout(0) << "NOTICE: request time skew too big now=" << utime_t(now, 0) << " req_time=" << s->header_time << dendl;
	  return -ERR_REQUEST_TIME_SKEWED;
	}


	s->user.user_id = keystone_validator.response.token.tenant.id;
        s->user.display_name = keystone_validator.response.token.tenant.name; // wow.

        /* try to store user if it not already exists */
        if (rgw_get_user_info_by_uid(store, keystone_validator.response.token.tenant.id, s->user) < 0) {
          int ret = rgw_store_user_info(store, s->user, NULL, NULL, 0, true);
          if (ret < 0)
            dout(10) << "NOTICE: failed to store new user's info: ret=" << ret << dendl;
        }

        s->perm_mask = RGW_PERM_FULL_CONTROL;
      }
    }
  }

  /* keystone failed (or not enabled); check if we want to use rados backend */
  if (!store->ctx()->_conf->rgw_s3_auth_use_rados
      && keystone_result < 0)
    return keystone_result;

  /* now try rados backend, but only if keystone did not succeed */
  if (keystone_result < 0) {
    /* get the user info */
    if (rgw_get_user_info_by_access_key(store, auth_id, s->user) < 0) {
      dout(5) << "error reading user info, uid=" << auth_id << " can't authenticate" << dendl;
      return -ERR_INVALID_ACCESS_KEY;
    }

    /* now verify signature */

    string auth_hdr;
    if (!rgw_create_s3_canonical_header(s->info, &s->header_time, auth_hdr, qsr)) {
      dout(10) << "failed to create auth header\n" << auth_hdr << dendl;
      return -EPERM;
    }
    dout(10) << "auth_hdr:\n" << auth_hdr << dendl;

    time_t req_sec = s->header_time.sec();
    if ((req_sec < now - RGW_AUTH_GRACE_MINS * 60 ||
        req_sec > now + RGW_AUTH_GRACE_MINS * 60) && !qsr) {
      dout(10) << "req_sec=" << req_sec << " now=" << now << "; now - RGW_AUTH_GRACE_MINS=" << now - RGW_AUTH_GRACE_MINS * 60 << "; now + RGW_AUTH_GRACE_MINS=" << now + RGW_AUTH_GRACE_MINS * 60 << dendl;
      dout(0) << "NOTICE: request time skew too big now=" << utime_t(now, 0) << " req_time=" << s->header_time << dendl;
      return -ERR_REQUEST_TIME_SKEWED;
    }

    map<string, RGWAccessKey>::iterator iter = s->user.access_keys.find(auth_id);
    if (iter == s->user.access_keys.end()) {
      dout(0) << "ERROR: access key not encoded in user info" << dendl;
      return -EPERM;
    }
    RGWAccessKey& k = iter->second;

    if (!k.subuser.empty()) {
      map<string, RGWSubUser>::iterator uiter = s->user.subusers.find(k.subuser);
      if (uiter == s->user.subusers.end()) {
        dout(0) << "NOTICE: could not find subuser: " << k.subuser << dendl;
        return -EPERM;
      }
      RGWSubUser& subuser = uiter->second;
      s->perm_mask = subuser.perm_mask;
    } else
      s->perm_mask = RGW_PERM_FULL_CONTROL;

    string digest;
    int ret = rgw_get_s3_header_digest(auth_hdr, k.key, digest);
    if (ret < 0) {
      return -EPERM;
    }

    dout(15) << "calculated digest=" << digest << dendl;
    dout(15) << "auth_sign=" << auth_sign << dendl;
    dout(15) << "compare=" << auth_sign.compare(digest) << dendl;

    if (auth_sign != digest) {
      return -ERR_SIGNATURE_NO_MATCH;
    }

    if (s->user.system) {
      s->system_request = true;
      dout(20) << "system request" << dendl;
      s->info.args.set_system();
      string effective_uid = s->info.args.get(RGW_SYS_PARAM_PREFIX "uid");
      RGWUserInfo effective_user;
      if (!effective_uid.empty()) {
        ret = rgw_get_user_info_by_uid(store, effective_uid, effective_user);
        if (ret < 0) {
          ldout(s->cct, 0) << "User lookup failed!" << dendl;
          return -ENOENT;
        }
        s->user = effective_user;
      }
    }

  } /* if keystone_result < 0 */

  // populate the owner info
  s->owner.set_id(s->user.user_id);
  s->owner.set_name(s->user.display_name);


  return  0;
#else
// XXX do something smarter here...
  return 0 * -EPERM;
#endif
}

RGWHandler *RGWRESTMgr_STS::get_handler(struct req_state *s)
{
  int ret = RGWHandler_STS::init_from_header(s, RGW_FORMAT_XML, false);
  if (ret < 0)
    return NULL;

  return new RGWHandler_STS;
}
