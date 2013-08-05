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

#include "rgw_client_io.h"
#include "rgw_resolve.h"

#define dout_subsys ceph_subsys_rgw


struct rgw_http_attr {
  const char *rgw_attr;
  const char *http_attr;
};

/*
 * mapping between rgw object attrs and output http fields
 */
static struct rgw_http_attr rgw_to_http_attr_list[] = {
  { RGW_ATTR_CONTENT_TYPE, "Content-Type"},
  { RGW_ATTR_CONTENT_LANG, "Content-Language"},
  { RGW_ATTR_EXPIRES, "Expires"},
  { RGW_ATTR_CACHE_CONTROL, "Cache-Control"},
  { RGW_ATTR_CONTENT_DISP, "Content-Disposition"},
  { RGW_ATTR_CONTENT_ENC, "Content-Encoding"},
  { NULL, NULL},
};


struct generic_attr {
  const char *http_header;
  const char *rgw_attr;
};

/*
 * mapping between http env fields and rgw object attrs
 */
struct generic_attr generic_attrs[] = {
  { "CONTENT_TYPE", RGW_ATTR_CONTENT_TYPE },
  { "HTTP_CONTENT_LANGUAGE", RGW_ATTR_CONTENT_LANG },
  { "HTTP_EXPIRES", RGW_ATTR_EXPIRES },
  { "HTTP_CACHE_CONTROL", RGW_ATTR_CACHE_CONTROL },
  { "HTTP_CONTENT_DISPOSITION", RGW_ATTR_CONTENT_DISP },
  { "HTTP_CONTENT_ENCODING", RGW_ATTR_CONTENT_ENC },
  { NULL, NULL },
};

map<string, string> rgw_to_http_attrs;
static map<string, string> generic_attrs_map;

/*
 * make attrs look_like_this
 */
string lowercase_http_attr(const string& orig)
{
  const char *s = orig.c_str();
  char buf[orig.size() + 1];
  buf[orig.size()] = '\0';

  for (size_t i = 0; i < orig.size(); ++i, ++s) {
    switch (*s) {
      case '-':
	buf[i] = '_';
	break;
      default:
	buf[i] = tolower(*s);
    }
  }
  return string(buf);
}

/*
 * make attrs LOOK_LIKE_THIS
 */
string uppercase_http_attr(const string& orig)
{
  const char *s = orig.c_str();
  char buf[orig.size() + 1];
  buf[orig.size()] = '\0';

  for (size_t i = 0; i < orig.size(); ++i, ++s) {
    switch (*s) {
      case '-':
	buf[i] = '_';
	break;
      default:
	buf[i] = toupper(*s);
    }
  }
  return string(buf);
}

/*
 * make attrs Look-Like-This
 */
string camelcase_dash_http_attr(const string& orig)
{
  const char *s = orig.c_str();
  char buf[orig.size() + 1];
  buf[orig.size()] = '\0';

  bool last_sep = true;

  for (size_t i = 0; i < orig.size(); ++i, ++s) {
    switch (*s) {
      case '_':
	buf[i] = '-';
	last_sep = true;
	break;
      default:
	if (last_sep)
	  buf[i] = toupper(*s);
	else
	  buf[i] = tolower(*s);
	last_sep = false;
    }
  }
  return string(buf);
}

void rgw_rest_init(CephContext *cct)
{
  for (struct rgw_http_attr *attr = rgw_to_http_attr_list; attr->rgw_attr; attr++) {
    rgw_to_http_attrs[attr->rgw_attr] = attr->http_attr;
  }

  for (struct generic_attr *gen_attr = generic_attrs; gen_attr->http_header; gen_attr++) {
    generic_attrs_map[gen_attr->http_header] = gen_attr->rgw_attr;
  }

  list<string> extended_http_attrs;
  get_str_list(cct->_conf->rgw_extended_http_attrs, extended_http_attrs);

  list<string>::iterator iter;
  for (iter = extended_http_attrs.begin(); iter != extended_http_attrs.end(); ++iter) {
    string rgw_attr = RGW_ATTR_PREFIX;
    rgw_attr.append(lowercase_http_attr(*iter));

    rgw_to_http_attrs[rgw_attr] = camelcase_dash_http_attr(*iter);

    string http_header = "HTTP_";
    http_header.append(uppercase_http_attr(*iter));

    generic_attrs_map[http_header] = rgw_attr;
  }
}

static void dump_status(struct req_state *s, const char *status)
{
  int r = s->cio->print("Status: %s\n", status);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void rgw_flush_formatter_and_reset(struct req_state *s, Formatter *formatter)
{
  std::ostringstream oss;
  formatter->flush(oss);
  std::string outs(oss.str());
  if (!outs.empty()) {
    s->cio->write(outs.c_str(), outs.size());
  }

  s->formatter->reset();
}

void rgw_flush_formatter(struct req_state *s, Formatter *formatter)
{
  std::ostringstream oss;
  formatter->flush(oss);
  std::string outs(oss.str());
  if (!outs.empty()) {
    s->cio->write(outs.c_str(), outs.size());
  }
}

void set_req_state_err(struct req_state *s, int err_no)
{
  const struct rgw_http_errors *r;

  if (err_no < 0)
    err_no = -err_no;
  s->err.ret = err_no;
  if (s->prot_flags & RGW_REST_SWIFT) {
    r = search_err(err_no, RGW_HTTP_SWIFT_ERRORS, ARRAY_LEN(RGW_HTTP_SWIFT_ERRORS));
    if (r) {
      s->err.http_ret = r->http_ret;
      s->err.s3_code = r->s3_code;
      return;
    }
  }
  r = search_err(err_no, RGW_HTTP_ERRORS, ARRAY_LEN(RGW_HTTP_ERRORS));
  if (r) {
    s->err.http_ret = r->http_ret;
    s->err.s3_code = r->s3_code;
    return;
  }
  dout(0) << "WARNING: set_req_state_err err_no=" << err_no << " resorting to 500" << dendl;

  s->err.http_ret = 500;
  s->err.s3_code = "UnknownError";
}

void dump_errno(struct req_state *s)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%d", s->err.http_ret);
  dump_status(s, buf);
}

void dump_errno(struct req_state *s, int err)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%d", err);
  dump_status(s, buf);
}

void dump_content_length(struct req_state *s, uint64_t len)
{
  char buf[21];
  snprintf(buf, sizeof(buf), "%"PRIu64, len);
  int r = s->cio->print("Content-Length: %s\n", buf);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
  r = s->cio->print("Accept-Ranges: %s\n", "bytes");
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void dump_etag(struct req_state *s, const char *etag)
{
  int r;
  if (s->prot_flags & RGW_REST_SWIFT)
    r = s->cio->print("etag: %s\n", etag);
  else
    r = s->cio->print("ETag: \"%s\"\n", etag);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void dump_pair(struct req_state *s, const char *key, const char *value)
{
  if ( (strlen(key) > 0) && (strlen(value) > 0))
    s->cio->print("%s: %s\n", key, value);
}

void dump_bucket_from_state(struct req_state *s)
{
  if (!s->bucket_name_str.empty())
    s->cio->print("Bucket: \"%s\"\n", s->bucket_name_str.c_str());
}

void dump_object_from_state(struct req_state *s)
{
  if (!s->object_str.empty())
    s->cio->print("Key: \"%s\"\n", s->object_str.c_str());
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
      if (!s->object_str.empty()) {
        location += s->object_str;
        s->cio->print("Location: %s\n", location.c_str());
      }
    }
  }
  else {
    s->cio->print("Location: \"%s\"\n", s->info.request_uri.c_str());
  }
}

void dump_redirect(struct req_state *s, const string& redirect)
{
  if (redirect.empty())
    return;

  s->cio->print("Location: %s\n", redirect.c_str());
}

static void dump_time_header(struct req_state *s, const char *name, time_t t)
{

  char timestr[TIME_BUF_SIZE];
  struct tm result;
  struct tm *tmp = gmtime_r(&t, &result);
  if (tmp == NULL)
    return;

  if (strftime(timestr, sizeof(timestr), "%a, %d %b %Y %H:%M:%S %Z", tmp) == 0)
    return;

  int r = s->cio->print("%s: %s\n", name, timestr);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void dump_last_modified(struct req_state *s, time_t t)
{
  dump_time_header(s, "Last-Modified", t);
}

void dump_epoch_header(struct req_state *s, const char *name, time_t t)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%lld", (long long)t);

  int r = s->cio->print("%s: %s\n", name, buf);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void dump_time(struct req_state *s, const char *name, time_t *t)
{
  char buf[TIME_BUF_SIZE];
  struct tm result;
  struct tm *tmp = gmtime_r(t, &result);
  if (tmp == NULL)
    return;

  if (strftime(buf, sizeof(buf), "%Y-%m-%dT%T.000Z", tmp) == 0)
    return;

  s->formatter->dump_string(name, buf);
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
    s->cio->print("Access-Control-Allow-Origin: %s\n", origin);
    if (meth && (meth[0] != '\0'))
      s->cio->print("Access-Control-Allow-Methods: %s\n", meth);
    if (hdr && (hdr[0] != '\0'))
      s->cio->print("Access-Control-Allow-Headers: %s\n", hdr);
    if (exp_hdr && (exp_hdr[0] != '\0')) {
      s->cio->print("Access-Control-Expose-Headers: %s\n", exp_hdr);
    }
    if (max_age != CORS_MAX_AGE_INVALID) {
      s->cio->print("Access-Control-Max-Age: %d\n", max_age);
    }
  }
}

void dump_start(struct req_state *s)
{
  if (!s->content_started) {
    if (s->format == RGW_FORMAT_XML)
      s->formatter->write_raw_data(XMLFormatter::XML_1_DTD);
    s->content_started = true;
  }
}

void end_header(struct req_state *s, const char *content_type)
{
  string ctype;

  if (!content_type || s->err.is_err()) {
    switch (s->format) {
    case RGW_FORMAT_XML:
      ctype = "application/xml";
      break;
    case RGW_FORMAT_JSON:
      ctype = "application/json";
      break;
    default:
      ctype = "text/plain";
      break;
    }
    if (s->prot_flags & RGW_REST_SWIFT)
      ctype.append("; charset=utf-8");
    content_type = ctype.c_str();
  }
  if (s->err.is_err()) {
    dump_start(s);
    s->formatter->open_object_section("Error");
    if (!s->err.s3_code.empty())
      s->formatter->dump_string("Code", s->err.s3_code);
    if (!s->err.message.empty())
      s->formatter->dump_string("Message", s->err.message);
    s->formatter->close_section();
    dump_content_length(s, s->formatter->get_len());
  }
  int r = s->cio->print("Content-type: %s\r\n\r\n", content_type);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
  s->cio->set_account(true);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void abort_early(struct req_state *s, int err_no)
{
  if (!s->formatter) {
    s->formatter = new JSONFormatter;
    s->format = RGW_FORMAT_JSON;
  }
  set_req_state_err(s, err_no);
  dump_errno(s);
  end_header(s);
  rgw_flush_formatter_and_reset(s, s->formatter);
  perfcounter->inc(l_rgw_failed_req);
}

void dump_continue(struct req_state *s)
{
  dump_status(s, "100");
  s->cio->flush();
}

void dump_range(struct req_state *s, uint64_t ofs, uint64_t end, uint64_t total)
{
  char range_buf[128];

  /* dumping range into temp buffer first, as libfcgi will fail to digest %lld */
  snprintf(range_buf, sizeof(range_buf), "%lld-%lld/%lld", (long long)ofs, (long long)end, (long long)total);
  int r = s->cio->print("Content-Range: bytes %s\n", range_buf);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
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

int RESTArgs::get_string(struct req_state *s, const string& name, const string& def_val, string *val, bool *existed)
{
  bool exists;
  *val = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *val = def_val;
    return 0;
  }

  return 0;
}

int RESTArgs::get_uint64(struct req_state *s, const string& name, uint64_t def_val, uint64_t *val, bool *existed)
{
  bool exists;
  string sval = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *val = def_val;
    return 0;
  }

  int r = stringtoull(sval, val);
  if (r < 0)
    return r;

  return 0;
}

int RESTArgs::get_int64(struct req_state *s, const string& name, int64_t def_val, int64_t *val, bool *existed)
{
  bool exists;
  string sval = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *val = def_val;
    return 0;
  }

  int r = stringtoll(sval, val);
  if (r < 0)
    return r;

  return 0;
}

int RESTArgs::get_uint32(struct req_state *s, const string& name, uint32_t def_val, uint32_t *val, bool *existed)
{
  bool exists;
  string sval = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *val = def_val;
    return 0;
  }

  int r = stringtoul(sval, val);
  if (r < 0)
    return r;

  return 0;
}

int RESTArgs::get_int32(struct req_state *s, const string& name, int32_t def_val, int32_t *val, bool *existed)
{
  bool exists;
  string sval = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *val = def_val;
    return 0;
  }

  int r = stringtol(sval, val);
  if (r < 0)
    return r;

  return 0;
}

int RESTArgs::get_time(struct req_state *s, const string& name, const utime_t& def_val, utime_t *val, bool *existed)
{
  bool exists;
  string sval = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *val = def_val;
    return 0;
  }

  uint64_t epoch, nsec;

  int r = utime_t::parse_date(sval, &epoch, &nsec);
  if (r < 0)
    return r;

  *val = utime_t(epoch, nsec);

  return 0;
}

int RESTArgs::get_epoch(struct req_state *s, const string& name, uint64_t def_val, uint64_t *epoch, bool *existed)
{
  bool exists;
  string date = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *epoch = def_val;
    return 0;
  }

  int r = utime_t::parse_date(date, epoch, NULL);
  if (r < 0)
    return r;

  return 0;
}

int RESTArgs::get_bool(struct req_state *s, const string& name, bool def_val, bool *val, bool *existed)
{
  bool exists;
  string sval = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *val = def_val;
    return 0;
  }

  const char *str = sval.c_str();

  if (sval.empty() ||
      strcasecmp(str, "true") == 0 ||
      sval.compare("1") == 0) {
    *val = true;
    return 0;
  }

  if (strcasecmp(str, "false") != 0 &&
      sval.compare("0") != 0) {
    *val = def_val;
    return -EINVAL;
  }

  *val = false;
  return 0;
}


void RGWRESTFlusher::do_start(int ret)
{
  set_req_state_err(s, ret); /* no going back from here */
  dump_errno(s);
  dump_start(s);
  end_header(s);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWRESTFlusher::do_flush()
{
  rgw_flush_formatter(s, s->formatter);
}

int RGWPutObj_ObjStore::verify_params()
{
  if (s->length) {
    off_t len = atoll(s->length);
    if (len > (off_t)RGW_MAX_PUT_SIZE) {
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
  if (s->length) {
    cl = atoll(s->length) - ofs;
    if (cl > RGW_MAX_CHUNK_SIZE)
      cl = RGW_MAX_CHUNK_SIZE;
  } else {
    cl = RGW_MAX_CHUNK_SIZE;
  }

  int len = 0;
  if (cl) {
    bufferptr bp(cl);

    int read_len; /* cio->read() expects int * */
    int r = s->cio->read(bp.c_str(), cl, &read_len);
    len = read_len;
    if (r < 0)
      return ret;
    bl.append(bp, 0, len);
  }

  if ((uint64_t)ofs + len > RGW_MAX_PUT_SIZE) {
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
  if (len > (off_t)RGW_MAX_PUT_SIZE) {
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

static int read_all_chunked_input(req_state *s, char **pdata, int *plen, int max_read)
{
#define READ_CHUNK 4096
#define MAX_READ_CHUNK (128 * 1024)
  int need_to_read = READ_CHUNK;
  int total = need_to_read;
  char *data = (char *)malloc(total + 1);
  if (!data)
    return -ENOMEM;

  int read_len = 0, len = 0;
  do {
    int r = s->cio->read(data + len, need_to_read, &read_len);
    if (r < 0) {
      free(data);
      return r;
    }

    len += read_len;

    if (read_len == need_to_read) {
      if (need_to_read < MAX_READ_CHUNK)
	need_to_read *= 2;

      if (total > max_read) {
        free(data);
        return -ERANGE;
      }
      total += need_to_read;

      void *p = realloc(data, total + 1);
      if (!p) {
        free(data);
        return -ENOMEM;
      }
      data = (char *)p;
    } else {
      break;
    }

  } while (true);
  data[len] = '\0';

  *pdata = data;
  *plen = len;

  return 0;
}

int rgw_rest_read_all_input(struct req_state *s, char **pdata, int *plen, int max_len)
{
  size_t cl = 0;
  int len = 0;
  char *data = NULL;

  if (s->length)
    cl = atoll(s->length);
  if (cl) {
    if (cl > (size_t)max_len) {
      return -ERANGE;
    }
    data = (char *)malloc(cl + 1);
    if (!data) {
       return -ENOMEM;
    }
    int ret = s->cio->read(data, cl, &len);
    if (ret < 0) {
      free(data);
      return ret;
    }
    data[len] = '\0';
  } else if (!s->length) {
    const char *encoding = s->info.env->get("HTTP_TRANSFER_ENCODING");
    if (!encoding || strcmp(encoding, "chunked") != 0)
      return -ERR_LENGTH_REQUIRED;

    int ret = read_all_chunked_input(s, &data, &len, max_len);
    if (ret < 0)
      return ret;
  }

  *plen = len;
  *pdata = data;

  return 0;
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
  string str = s->info.args.get("part-number-marker");
  if (!str.empty())
    marker = atoi(str.c_str());
  
  str = s->info.args.get("max-parts");
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
  bucket_name = s->bucket_name;

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
    end_header(s);
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

static http_op op_from_method(const char *method)
{
  if (!method)
    return OP_UNKNOWN;
  if (strcmp(method, "GET") == 0)
    return OP_GET;
  if (strcmp(method, "PUT") == 0)
    return OP_PUT;
  if (strcmp(method, "DELETE") == 0)
    return OP_DELETE;
  if (strcmp(method, "HEAD") == 0)
    return OP_HEAD;
  if (strcmp(method, "POST") == 0)
    return OP_POST;
  if (strcmp(method, "COPY") == 0)
    return OP_COPY;
  if (strcmp(method, "OPTIONS") == 0)
    return OP_OPTIONS;

  return OP_UNKNOWN;
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
    if ((s->op == OP_PUT) && s->object_str.size() == 0)
      return 0;
    only_bucket = true;
    break;
  case OP_DELETE:
    only_bucket = true;
    break;
  case OP_COPY: // op itself will read and verify the permissions
    return 0;
  case OP_OPTIONS:
    only_bucket = false;
    break;
  default:
    return -EINVAL;
  }

  return do_read_permissions(op_obj, only_bucket);
}

void RGWRESTMgr::register_resource(string resource, RGWRESTMgr *mgr)
{
  string r = "/";
  r.append(resource);

  /* do we have a resource manager registered for this entry point? */
  map<string, RGWRESTMgr *>::iterator iter = resource_mgrs.find(r);
  if (iter != resource_mgrs.end()) {
    delete iter->second;
  }
  resource_mgrs[r] = mgr;
  resources_by_size.insert(pair<size_t, string>(r.size(), r));

  /* now build default resource managers for the path (instead of nested entry points)
   * e.g., if the entry point is /auth/v1.0/ then we'd want to create a default
   * manager for /auth/
   */

  size_t pos = r.find('/', 1);

  while (pos != r.size() - 1 && pos != string::npos) {
    string s = r.substr(0, pos);

    iter = resource_mgrs.find(s);
    if (iter == resource_mgrs.end()) { /* only register it if one does not exist */
      resource_mgrs[s] = new RGWRESTMgr; /* a default do-nothing manager */
      resources_by_size.insert(pair<size_t, string>(s.size(), s));
    }

    pos = r.find('/', pos + 1);
  }
}

void RGWRESTMgr::register_default_mgr(RGWRESTMgr *mgr)
{
  delete default_mgr;
  default_mgr = mgr;
}

RGWRESTMgr *RGWRESTMgr::get_resource_mgr(struct req_state *s, const string& uri, string *out_uri)
{
  *out_uri = uri;

  if (resources_by_size.empty())
    return this;

  multimap<size_t, string>::reverse_iterator iter;

  for (iter = resources_by_size.rbegin(); iter != resources_by_size.rend(); ++iter) {
    string& resource = iter->second;
    if (uri.compare(0, iter->first, resource) == 0 &&
	(uri.size() == iter->first ||
	 uri[iter->first] == '/')) {
      string suffix = uri.substr(iter->first);
      return resource_mgrs[resource]->get_resource_mgr(s, suffix, out_uri);
    }
  }

  if (default_mgr)
    return default_mgr;

  return this;
}

RGWRESTMgr::~RGWRESTMgr()
{
  map<string, RGWRESTMgr *>::iterator iter;
  for (iter = resource_mgrs.begin(); iter != resource_mgrs.end(); ++iter) {
    delete iter->second;
  }
  delete default_mgr;
}

int RGWREST::preprocess(struct req_state *s, RGWClientIO *cio)
{
  req_info& info = s->info;

  s->cio = cio;
  if (g_conf->rgw_dns_name.length() && info.host) {
    string h(s->info.host);

    ldout(s->cct, 10) << "host=" << s->info.host << " rgw_dns_name=" << g_conf->rgw_dns_name << dendl;
    int pos = h.find(g_conf->rgw_dns_name);

    if (g_conf->rgw_resolve_cname && pos < 0) {
      string cname;
      bool found;
      int r = rgw_resolver->resolve_cname(h, cname, &found);
      if (r < 0) {
	dout(0) << "WARNING: rgw_resolver->resolve_cname() returned r=" << r << dendl;
      }
      if (found) {
        dout(0) << "resolved host cname " << h << " -> " << cname << dendl;
	h = cname;
        pos = h.find(g_conf->rgw_dns_name);
      }
    }

    if (pos > 0 && h[pos - 1] == '.') {
      string encoded_bucket = "/";
      encoded_bucket.append(h.substr(0, pos-1));
      if (s->info.request_uri[0] != '/')
	encoded_bucket.append("/'");
      encoded_bucket.append(s->info.request_uri);
      s->info.request_uri = encoded_bucket;
    }
  }

  url_decode(s->info.request_uri, s->decoded_uri);
  s->length = info.env->get("CONTENT_LENGTH");
  if (s->length) {
    if (*s->length == '\0')
      s->content_length = 0;
    else
      s->content_length = atoll(s->length);
  }

  map<string, string>::iterator giter;
  for (giter = generic_attrs_map.begin(); giter != generic_attrs_map.end(); ++giter) {
    const char *env = info.env->get(giter->first.c_str());
    if (env) {
      s->generic_attrs[giter->second] = env;
    }
  }

  s->http_auth = info.env->get("HTTP_AUTHORIZATION");

  if (g_conf->rgw_print_continue) {
    const char *expect = info.env->get("HTTP_EXPECT");
    s->expect_cont = (expect && !strcasecmp(expect, "100-continue"));
  }
  s->op = op_from_method(info.method);

  info.init_meta_info(&s->has_bad_meta);

  return 0;
}

RGWHandler *RGWREST::get_handler(RGWRados *store, struct req_state *s, RGWClientIO *cio,
				 RGWRESTMgr **pmgr, int *init_error)
{
  RGWHandler *handler;

  *init_error = preprocess(s, cio);
  if (*init_error < 0)
    return NULL;

  RGWRESTMgr *m = mgr.get_resource_mgr(s, s->decoded_uri, &s->relative_uri);
  if (!m) {
    *init_error = -ERR_METHOD_NOT_ALLOWED;
    return NULL;
  }

  if (pmgr)
    *pmgr = m;

  handler = m->get_handler(s);
  if (!handler) {
    *init_error = -ERR_METHOD_NOT_ALLOWED;
    return NULL;
  }
  *init_error = handler->init(store, s, cio);
  if (*init_error < 0)
    return NULL;

  return handler;
}

