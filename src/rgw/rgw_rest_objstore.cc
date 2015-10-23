// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <limits.h>

#include "common/Formatter.h"
#include "common/HTMLFormatter.h"
#include "common/utf8.h"
#include "include/str_list.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_formats.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_swift.h"
#include "rgw_rest_s3.h"
#include "rgw_swift_auth.h"
#include "rgw_cors_s3.h"
#include "rgw_http_errors.h"
#include "rgw_lib.h"

#include "rgw_client_io.h"
#include "rgw_resolve.h"

#include <numeric>

#define dout_subsys ceph_subsys_rgw

void dump_bucket_from_state(struct req_state *s)
{
  int expose_bucket = g_conf->rgw_expose_bucket;
  if (expose_bucket) {
    if (!s->bucket_name.empty()) {
      string b;
      if (!s->bucket_tenant.empty()) {
        string g = s->bucket_tenant + "/" + s->bucket_name;
        url_encode(g, b);
      } else {
        url_encode(s->bucket_name, b);
      }
      STREAM_IO(s)->print("Bucket: %s\r\n", b.c_str());
    }
  }
}

void dump_uri_from_state(struct req_state *s)
{
  if (strcmp(s->info.request_uri.c_str(), "/") == 0) {

    string location = "http://";
    string server = s->info.env->get("SERVER_NAME", "<SERVER_NAME>");
    location.append(server);
    location += "/";
    if (!s->bucket_name.empty()) {
      if (!s->bucket_tenant.empty()) {
        location += s->bucket_tenant;
        location += ":";
      }
      location += s->bucket_name;
      location += "/";
      if (!s->object.empty()) {
	location += s->object.name;
	STREAM_IO(s)->print("Location: %s\r\n", location.c_str());
      }
    }
  }
  else {
    STREAM_IO(s)->print("Location: \"%s\"\r\n", s->info.request_uri.c_str());
  }
}

void dump_owner(struct req_state *s, rgw_user& id, string& name,
		const char *section)
{
  if (!section)
    section = "Owner";
  s->formatter->open_object_section(section);
  s->formatter->dump_string("ID", id.to_str());
  s->formatter->dump_string("DisplayName", name);
  s->formatter->close_section();
}

void dump_access_control(struct req_state *s, const char *origin,
			 const char *meth,
			 const char *hdr, const char *exp_hdr,
			 uint32_t max_age) {
  if (origin && (origin[0] != '\0')) {
    STREAM_IO(s)->print("Access-Control-Allow-Origin: %s\r\n", origin);
    if (meth && (meth[0] != '\0'))
      STREAM_IO(s)->print("Access-Control-Allow-Methods: %s\r\n", meth);
    if (hdr && (hdr[0] != '\0'))
      STREAM_IO(s)->print("Access-Control-Allow-Headers: %s\r\n", hdr);
    if (exp_hdr && (exp_hdr[0] != '\0')) {
      STREAM_IO(s)->print("Access-Control-Expose-Headers: %s\r\n", exp_hdr);
    }
    if (max_age != CORS_MAX_AGE_INVALID) {
      STREAM_IO(s)->print("Access-Control-Max-Age: %d\r\n", max_age);
    }
  }
}

void dump_access_control(req_state *s, RGWOp *op)
{
  string origin;
  string method;
  string header;
  string exp_header;
  unsigned max_age = CORS_MAX_AGE_INVALID;

  if (!op->generate_cors_headers(origin, method, header, exp_header, &max_age))
    return;

  dump_access_control(s, origin.c_str(), method.c_str(), header.c_str(),
		      exp_header.c_str(), max_age);
}
int RGWGetObj_ObjStore::get_params()
{
  range_str = s->info.env->get("HTTP_RANGE");
  if_mod = s->info.env->get("HTTP_IF_MODIFIED_SINCE");
  if_unmod = s->info.env->get("HTTP_IF_UNMODIFIED_SINCE");
  if_match = s->info.env->get("HTTP_IF_MATCH");
  if_nomatch = s->info.env->get("HTTP_IF_NONE_MATCH");

  if (s->system_request) {
    mod_zone_id = s->info.env->get_int("HTTP_DEST_ZONE_SHORT_ID", 0);
    mod_pg_ver = s->info.env->get_int("HTTP_DEST_PG_VER", 0);
  }

  return 0;
}

int RGWPutObj_ObjStore::verify_params()
{
  if (s->length) {
    off_t len = atoll(s->length);
    if (len > (off_t)(s->cct->_conf->rgw_max_put_size)) {
      return -ERR_TOO_LARGE;
    }
  }

  return 0;
}

int RGWPutObj_ObjStore::get_params()
{
  supplied_md5_b64 = s->info.env->get("HTTP_CONTENT_MD5");

  return 0;
}

int RGWPutObj_ObjStore::get_data(bufferlist& bl)
{
  size_t cl;
  uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
  if (s->length) {
    cl = atoll(s->length) - ofs;
    if (cl > chunk_size)
      cl = chunk_size;
  } else {
    cl = chunk_size;
  }

  int len = 0;
  if (cl) {
    bufferptr bp(cl);

    int read_len; /* cio->read() expects int * */
    int r = STREAM_IO(s)->read(bp.c_str(), cl, &read_len,
			       s->aws4_auth_needs_complete);
    if (r < 0) {
      return r;
    }

    len = read_len;
    bl.append(bp, 0, len);
  }

  if ((uint64_t)ofs + len > s->cct->_conf->rgw_max_put_size) {
    return -ERR_TOO_LARGE;
  }

  if (!ofs)
    supplied_md5_b64 = s->info.env->get("HTTP_CONTENT_MD5");

  return len;
}

int RGWPostObj_ObjStore::verify_params()
{
  /*  check that we have enough memory to store the object
  note that this test isn't exact and may fail unintentionally
  for large requests is */
  if (!s->length) {
    return -ERR_LENGTH_REQUIRED;
  }
  off_t len = atoll(s->length);
  if (len > (off_t)(s->cct->_conf->rgw_max_put_size)) {
    return -ERR_TOO_LARGE;
  }

  return 0;
}

int RGWPutACLs_ObjStore::get_params()
{
  size_t cl = 0;
  if (s->length)
    cl = atoll(s->length);
  if (cl) {
    data = (char *)malloc(cl + 1);
    if (!data) {
       op_ret = -ENOMEM;
       return op_ret;
    }
    int read_len;
    int r = STREAM_IO(s)->read(data, cl, &read_len, s->aws4_auth_needs_complete);
    len = read_len;
    if (r < 0)
      return r;
    data[len] = '\0';
  } else {
    len = 0;
  }

  return op_ret;
}

int RGWCompleteMultipart_ObjStore::get_params()
{
  upload_id = s->info.args.get("uploadId");

  if (upload_id.empty()) {
    op_ret = -ENOTSUP;
    return op_ret;
  }

#define COMPLETE_MULTIPART_MAX_LEN (1024 * 1024) /* api defines max 10,000 parts, this should be enough */
  op_ret = rgw_rest_read_all_input(s, &data, &len, COMPLETE_MULTIPART_MAX_LEN);
  if (op_ret < 0)
    return op_ret;

  return 0;
}

int RGWListMultipart_ObjStore::get_params()
{
  upload_id = s->info.args.get("uploadId");

  if (upload_id.empty()) {
    op_ret = -ENOTSUP;
  }
  string marker_str = s->info.args.get("part-number-marker");

  if (!marker_str.empty()) {
    string err;
    marker = strict_strtol(marker_str.c_str(), 10, &err);
    if (!err.empty()) {
      ldout(s->cct, 20) << "bad marker: "  << marker << dendl;
      op_ret = -EINVAL;
      return op_ret;
    }
  }
  
  string str = s->info.args.get("max-parts");
  if (!str.empty())
    max_parts = atoi(str.c_str());

  return op_ret;
}

int RGWListBucketMultiparts_ObjStore::get_params()
{
  delimiter = s->info.args.get("delimiter");
  prefix = s->info.args.get("prefix");
  string str = s->info.args.get("max-parts");
  if (!str.empty())
    max_uploads = atoi(str.c_str());
  else
    max_uploads = default_max;

  string key_marker = s->info.args.get("key-marker");
  string upload_id_marker = s->info.args.get("upload-id-marker");
  if (!key_marker.empty())
    marker.init(key_marker, upload_id_marker);

  return 0;
}

int RGWDeleteMultiObj_ObjStore::get_params()
{

  if (s->bucket_name.empty()) {
    op_ret = -EINVAL;
    return op_ret;
  }

  // everything is probably fine, set the bucket
  bucket = s->bucket;

  size_t cl = 0;

  if (s->length)
    cl = atoll(s->length);
  if (cl) {
    data = (char *)malloc(cl + 1);
    if (!data) {
      op_ret = -ENOMEM;
      return op_ret;
    }
    int read_len;
    op_ret = STREAM_IO(s)->read(data, cl, &read_len, s->aws4_auth_needs_complete);
    len = read_len;
    if (op_ret < 0)
      return op_ret;
    data[len] = '\0';
  } else {
    return -EINVAL;
  }

  return op_ret;
}

int RGWRESTOp::verify_permission()
{
  return check_caps(s->user->caps);
}

RGWOp* RGWHandler_REST::get_op(RGWRados* store)
{
  RGWOp *op;
  switch (s->op) {
   case OP_GET:
     op = op_get();
     break;
   case OP_PUT:
     op = op_put();
     break;
   case OP_DELETE:
     op = op_delete();
     break;
   case OP_HEAD:
     op = op_head();
     break;
   case OP_POST:
     op = op_post();
     break;
   case OP_COPY:
     op = op_copy();
     break;
   case OP_OPTIONS:
     op = op_options();
     break;
   default:
     return NULL;
  }

  if (op) {
    op->init(store, s, this);
  }
  return op;
} /* get_op */

void RGWHandler_REST::put_op(RGWOp* op)
{
  delete op;
} /* put_op */

int RGWHandler_REST::allocate_formatter(struct req_state *s,
					int default_type,
					bool configurable)
{
  s->format = default_type;
  if (configurable) {
    string format_str = s->info.args.get("format");
    if (format_str.compare("xml") == 0) {
      s->format = RGW_FORMAT_XML;
    } else if (format_str.compare("json") == 0) {
      s->format = RGW_FORMAT_JSON;
    } else if (format_str.compare("html") == 0) {
      s->format = RGW_FORMAT_HTML;
    } else {
      const char *accept = s->info.env->get("HTTP_ACCEPT");
      if (accept) {
        char format_buf[64];
        unsigned int i = 0;
        for (; i < sizeof(format_buf) - 1 && accept[i] && accept[i] != ';'; ++i) {
          format_buf[i] = accept[i];
        }
        format_buf[i] = 0;
        if ((strcmp(format_buf, "text/xml") == 0) || (strcmp(format_buf, "application/xml") == 0)) {
          s->format = RGW_FORMAT_XML;
        } else if (strcmp(format_buf, "application/json") == 0) {
          s->format = RGW_FORMAT_JSON;
        } else if (strcmp(format_buf, "text/html") == 0) {
          s->format = RGW_FORMAT_HTML;
        }
      }
    }
  }

  const string& mm = s->info.args.get("multipart-manifest");
  const bool multipart_delete = (mm.compare("delete") == 0);

  switch (s->format) {
    case RGW_FORMAT_PLAIN:
      {
        const bool use_kv_syntax = s->info.args.exists("bulk-delete") || multipart_delete;
        s->formatter = new RGWFormatter_Plain(use_kv_syntax);
        break;
      }
    case RGW_FORMAT_XML:
      {
        const bool lowercase_underscore = s->info.args.exists("bulk-delete") || multipart_delete;
        s->formatter = new XMLFormatter(false, lowercase_underscore);
        break;
      }
    case RGW_FORMAT_JSON:
      s->formatter = new JSONFormatter(false);
      break;
    case RGW_FORMAT_HTML:
      s->formatter = new HTMLFormatter(s->prot_flags & RGW_REST_WEBSITE);
      break;
    default:
      return -EINVAL;

  };
  //s->formatter->reset(); // All formatters should reset on create already

  return 0;
}

int RGWHandler_REST::validate_tenant_name(string const& t)
{
  struct tench {
    static bool is_good(char ch) {
      return isalnum(ch) || ch == '_';
    }
  };
  std::string::const_iterator it =
    std::find_if_not(t.begin(), t.end(), tench::is_good);
  return (it == t.end())? 0: -ERR_INVALID_TENANT_NAME;
}

// This function enforces Amazon's spec for bucket names.
// (The requirements, not the recommendations.)
int RGWHandler_REST::validate_bucket_name(const string& bucket)
{
  int len = bucket.size();
  if (len < 3) {
    if (len == 0) {
      // This request doesn't specify a bucket at all
      return 0;
    }
    // Name too short
    return -ERR_INVALID_BUCKET_NAME;
  }
  else if (len > 255) {
    // Name too long
    return -ERR_INVALID_BUCKET_NAME;
  }

  return 0;
}

// "The name for a key is a sequence of Unicode characters whose UTF-8 encoding
// is at most 1024 bytes long."
// However, we can still have control characters and other nasties in there.
// Just as long as they're utf-8 nasties.
int RGWHandler_REST::validate_object_name(const string& object)
{
  int len = object.size();
  if (len > 1024) {
    // Name too long
    return -ERR_INVALID_OBJECT_NAME;
  }

  if (check_utf8(object.c_str(), len)) {
    // Object names must be valid UTF-8.
    return -ERR_INVALID_OBJECT_NAME;
  }
  return 0;
}

int RGWHandler_REST::init_permissions(RGWOp* op)
{
  if (op->get_type() == RGW_OP_CREATE_BUCKET)
    return 0;

  return do_init_permissions();
}

int RGWHandler_REST::read_permissions(RGWOp* op_obj)
{
  bool only_bucket;

  switch (s->op) {
  case OP_HEAD:
  case OP_GET:
    only_bucket = false;
    break;
  case OP_PUT:
  case OP_POST:
  case OP_COPY:
    /* is it a 'multi-object delete' request? */
    if (s->info.request_params == "delete") {
      only_bucket = true;
      break;
    }
    if (is_obj_update_op()) {
      only_bucket = false;
      break;
    }
    /* is it a 'create bucket' request? */
    if (op_obj->get_type() == RGW_OP_CREATE_BUCKET)
      return 0;
    only_bucket = true;
    break;
  case OP_DELETE:
    only_bucket = true;
    break;
  case OP_OPTIONS:
    only_bucket = true;
    break;
  default:
    return -EINVAL;
  }

  return do_read_permissions(op_obj, only_bucket);
}
