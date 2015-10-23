// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <limits.h>

#include "common/Formatter.h"
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

#include "rgw_client_io.h"
#include "rgw_resolve.h"

#define dout_subsys ceph_subsys_rgw

void dump_bucket_from_state(struct req_state *s)
{
  int expose_bucket = g_conf->rgw_expose_bucket;
  if (expose_bucket) {
    if (!s->bucket_name_str.empty()) {
      string b;
      url_encode(s->bucket_name_str, b);
      s->cio->print("Bucket: %s\r\n", b.c_str());
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
    if (!s->bucket_name_str.empty()) {
      location += s->bucket_name_str;
      location += "/";
      if (!s->object.empty()) {
        location += s->object.name;
        s->cio->print("Location: %s\r\n", location.c_str());
      }
    }
  }
  else {
    s->cio->print("Location: \"%s\"\r\n", s->info.request_uri.c_str());
  }
}

void dump_owner(struct req_state *s, string& id, string& name, const char *section)
{
  if (!section)
    section = "Owner";
  s->formatter->open_object_section(section);
  s->formatter->dump_string("ID", id);
  s->formatter->dump_string("DisplayName", name);
  s->formatter->close_section();
}

void dump_access_control(struct req_state *s, const char *origin, const char *meth,
                         const char *hdr, const char *exp_hdr, uint32_t max_age) {
  if (origin && (origin[0] != '\0')) {
    s->cio->print("Access-Control-Allow-Origin: %s\r\n", origin);
    if (meth && (meth[0] != '\0'))
      s->cio->print("Access-Control-Allow-Methods: %s\r\n", meth);
    if (hdr && (hdr[0] != '\0'))
      s->cio->print("Access-Control-Allow-Headers: %s\r\n", hdr);
    if (exp_hdr && (exp_hdr[0] != '\0')) {
      s->cio->print("Access-Control-Expose-Headers: %s\r\n", exp_hdr);
    }
    if (max_age != CORS_MAX_AGE_INVALID) {
      s->cio->print("Access-Control-Max-Age: %d\r\n", max_age);
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

  dump_access_control(s, origin.c_str(), method.c_str(), header.c_str(), exp_header.c_str(), max_age);
}

int RGWGetObj_ObjStore::get_params()
{
  range_str = s->info.env->get("HTTP_RANGE");
  if_mod = s->info.env->get("HTTP_IF_MODIFIED_SINCE");
  if_unmod = s->info.env->get("HTTP_IF_UNMODIFIED_SINCE");
  if_match = s->info.env->get("HTTP_IF_MATCH");
  if_nomatch = s->info.env->get("HTTP_IF_NONE_MATCH");

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
    int r = s->cio->read(bp.c_str(), cl, &read_len);
    len = read_len;
    if (r < 0)
      return r;
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
       ret = -ENOMEM;
       return ret;
    }
    int read_len;
    int r = s->cio->read(data, cl, &read_len);
    len = read_len;
    if (r < 0)
      return r;
    data[len] = '\0';
  } else {
    len = 0;
  }

  return ret;
}


int RGWCompleteMultipart_ObjStore::get_params()
{
  upload_id = s->info.args.get("uploadId");

  if (upload_id.empty()) {
    ret = -ENOTSUP;
    return ret;
  }

#define COMPLETE_MULTIPART_MAX_LEN (1024 * 1024) /* api defines max 10,000 parts, this should be enough */
  ret = rgw_rest_read_all_input(s, &data, &len, COMPLETE_MULTIPART_MAX_LEN);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWListMultipart_ObjStore::get_params()
{
  upload_id = s->info.args.get("uploadId");

  if (upload_id.empty()) {
    ret = -ENOTSUP;
  }
  string marker_str = s->info.args.get("part-number-marker");

  if (!marker_str.empty()) {
    string err;
    marker = strict_strtol(marker_str.c_str(), 10, &err);
    if (!err.empty()) {
      ldout(s->cct, 20) << "bad marker: "  << marker << dendl;
      ret = -EINVAL;
      return ret;
    }
  }
  
  string str = s->info.args.get("max-parts");
  if (!str.empty())
    max_parts = atoi(str.c_str());

  return ret;
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
  bucket_name = s->bucket_name_str;

  if (bucket_name.empty()) {
    ret = -EINVAL;
    return ret;
  }

  // everything is probably fine, set the bucket
  bucket = s->bucket;

  size_t cl = 0;

  if (s->length)
    cl = atoll(s->length);
  if (cl) {
    data = (char *)malloc(cl + 1);
    if (!data) {
      ret = -ENOMEM;
      return ret;
    }
    int read_len;
    ret = s->cio->read(data, cl, &read_len);
    len = read_len;
    if (ret < 0)
      return ret;
    data[len] = '\0';
  } else {
    return -EINVAL;
  }

  return ret;
}


void RGWRESTOp::send_response()
{
  if (!flusher.did_start()) {
    set_req_state_err(s, http_ret);
    dump_errno(s);
    end_header(s, dump_access_control_f());
  }
  flusher.flush();
}

int RGWRESTOp::verify_permission()
{
  return check_caps(s->user.caps);
}


int RGWHandler_ObjStore::allocate_formatter(struct req_state *s, int default_type, bool configurable)
{
  s->format = default_type;
  if (configurable) {
    string format_str = s->info.args.get("format");
    if (format_str.compare("xml") == 0) {
      s->format = RGW_FORMAT_XML;
    } else if (format_str.compare("json") == 0) {
      s->format = RGW_FORMAT_JSON;
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
        }
      }
    }
  }

  switch (s->format) {
    case RGW_FORMAT_PLAIN:
      s->formatter = new RGWFormatter_Plain;
      break;
    case RGW_FORMAT_XML:
      s->formatter = new XMLFormatter(false);
      break;
    case RGW_FORMAT_JSON:
      s->formatter = new JSONFormatter(false);
      break;
    default:
      return -EINVAL;

  };
  s->formatter->reset();

  return 0;
}

// This function enforces Amazon's spec for bucket names.
// (The requirements, not the recommendations.)
int RGWHandler_ObjStore::validate_bucket_name(const string& bucket)
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
int RGWHandler_ObjStore::validate_object_name(const string& object)
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

int RGWHandler_ObjStore::read_permissions(RGWOp *op_obj)
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
