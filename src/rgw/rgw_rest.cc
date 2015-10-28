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


struct rgw_http_attr {
  const char *rgw_attr;
  const char *http_attr;
};

/*
 * mapping between rgw object attrs and output http fields
 */
static const struct rgw_http_attr base_rgw_to_http_attrs[] = {
  { RGW_ATTR_CONTENT_LANG,      "Content-Language" },
  { RGW_ATTR_EXPIRES,           "Expires" },
  { RGW_ATTR_CACHE_CONTROL,     "Cache-Control" },
  { RGW_ATTR_CONTENT_DISP,      "Content-Disposition" },
  { RGW_ATTR_CONTENT_ENC,       "Content-Encoding" },
  { RGW_ATTR_USER_MANIFEST,     "X-Object-Manifest" },
  /* RGW_ATTR_AMZ_WEBSITE_REDIRECT_LOCATION header depends on access mode:
   * S3 endpoint: x-amz-website-redirect-location
   * S3Website endpoint: Location
   */
  { RGW_ATTR_AMZ_WEBSITE_REDIRECT_LOCATION, "x-amz-website-redirect-location" },
};


struct generic_attr {
  const char *http_header;
  const char *rgw_attr;
};

/*
 * mapping between http env fields and rgw object attrs
 */
static const struct generic_attr generic_attrs[] = {
  { "CONTENT_TYPE",             RGW_ATTR_CONTENT_TYPE },
  { "HTTP_CONTENT_LANGUAGE",    RGW_ATTR_CONTENT_LANG },
  { "HTTP_EXPIRES",             RGW_ATTR_EXPIRES },
  { "HTTP_CACHE_CONTROL",       RGW_ATTR_CACHE_CONTROL },
  { "HTTP_CONTENT_DISPOSITION", RGW_ATTR_CONTENT_DISP },
  { "HTTP_CONTENT_ENCODING",    RGW_ATTR_CONTENT_ENC },
};

map<string, string> rgw_to_http_attrs;
static map<string, string> generic_attrs_map;
map<int, const char *> http_status_names;

/*
 * make attrs look_like_this
 * converts dashes to underscores
 */
string lowercase_underscore_http_attr(const string& orig)
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
 * converts dashes to underscores
 */
string uppercase_underscore_http_attr(const string& orig)
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
 * make attrs look-like-this
 * converts underscores to dashes
 */
string lowercase_dash_http_attr(const string& orig)
{
  const char *s = orig.c_str();
  char buf[orig.size() + 1];
  buf[orig.size()] = '\0';

  for (size_t i = 0; i < orig.size(); ++i, ++s) {
    switch (*s) {
      case '_':
        buf[i] = '-';
        break;
      default:
        buf[i] = tolower(*s);
    }
  }
  return string(buf);
}

/*
 * make attrs Look-Like-This
 * converts underscores to dashes
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
      case '-':
        buf[i] = '-';
        last_sep = true;
        break;
      default:
        if (last_sep) {
          buf[i] = toupper(*s);
        } else {
          buf[i] = tolower(*s);
        }
        last_sep = false;
    }
  }
  return string(buf);
}

/* avoid duplicate hostnames in hostnames lists */
static set<string> hostnames_set;
static set<string> hostnames_s3website_set;

void rgw_rest_init(CephContext *cct, RGWRados *store, RGWZoneGroup& zone_group)
{
  store->init_host_id();

  for (const auto& rgw2http : base_rgw_to_http_attrs)  {
    rgw_to_http_attrs[rgw2http.rgw_attr] = rgw2http.http_attr;
  }

  for (const auto& http2rgw : generic_attrs) {
    generic_attrs_map[http2rgw.http_header] = http2rgw.rgw_attr;
  }

  list<string> extended_http_attrs;
  get_str_list(cct->_conf->rgw_extended_http_attrs, extended_http_attrs);

  list<string>::iterator iter;
  for (iter = extended_http_attrs.begin(); iter != extended_http_attrs.end(); ++iter) {
    string rgw_attr = RGW_ATTR_PREFIX;
    rgw_attr.append(lowercase_underscore_http_attr(*iter));

    rgw_to_http_attrs[rgw_attr] = camelcase_dash_http_attr(*iter);

    string http_header = "HTTP_";
    http_header.append(uppercase_underscore_http_attr(*iter));

    generic_attrs_map[http_header] = rgw_attr;
  }

  for (const struct rgw_http_status_code *h = http_codes; h->code; h++) {
    http_status_names[h->code] = h->name;
  }

  if (!cct->_conf->rgw_dns_name.empty()) {
    hostnames_set.insert(cct->_conf->rgw_dns_name);
  }
  hostnames_set.insert(zone_group.hostnames.begin(),  zone_group.hostnames.end());
  string s;
  ldout(cct, 20) << "RGW hostnames: " << std::accumulate(hostnames_set.begin(), hostnames_set.end(), s) << dendl;
  /* TODO: We should have a sanity check that no hostname matches the end of
   * any other hostname, otherwise we will get ambigious results from
   * rgw_find_host_in_domains.
   * Eg: 
   * Hostnames: [A, B.A]
   * Inputs: [Z.A, X.B.A]
   * Z.A clearly splits to subdomain=Z, domain=Z
   * X.B.A ambigously splits to both {X, B.A} and {X.B, A}
   */

  if (!cct->_conf->rgw_dns_s3website_name.empty()) {
    hostnames_s3website_set.insert(cct->_conf->rgw_dns_s3website_name);
  }
  hostnames_s3website_set.insert(zone_group.hostnames_s3website.begin(), zone_group.hostnames_s3website.end());
  s.clear();
  ldout(cct, 20) << "RGW S3website hostnames: " << std::accumulate(hostnames_s3website_set.begin(), hostnames_s3website_set.end(), s) << dendl;
  /* TODO: we should repeat the hostnames_set sanity check here
   * and ALSO decide about overlap, if any
   */
}

static bool str_ends_with(const string& s, const string& suffix, size_t *pos)
{
  size_t len = suffix.size();
  if (len > (size_t)s.size()) {
    return false;
  }

  ssize_t p = s.size() - len;
  if (pos) {
    *pos = p;
  }

  return s.compare(p, len, suffix) == 0;
}

static bool rgw_find_host_in_domains(const string& host, string *domain, string *subdomain, set<string> valid_hostnames_set)
{
  set<string>::iterator iter;
  /** TODO, Future optimization
   * store hostnames_set elements _reversed_, and look for a prefix match,
   * which is much faster than a suffix match.
   */
  for (iter = valid_hostnames_set.begin(); iter != valid_hostnames_set.end(); ++iter) {
    size_t pos;
    if (!str_ends_with(host, *iter, &pos))
      continue;

    if (pos == 0) {
      *domain = host;
      subdomain->clear();
    } else {
      if (host[pos - 1] != '.') {
	continue;
      }

      *domain = host.substr(pos);
      *subdomain = host.substr(0, pos - 1);
    }
    return true;
  }
  return false;
}

static void dump_status(struct req_state *s, int status,
			const char *status_name)
{
  s->formatter->set_status(status, status_name);
  int r = STREAM_IO(s)->send_status(status, status_name);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->send_status() returned err=" << r
		     << dendl;
  }
}

void rgw_flush_formatter_and_reset(struct req_state *s, Formatter *formatter)
{
  std::ostringstream oss;
  formatter->output_footer();
  formatter->flush(oss);
  std::string outs(oss.str());
  if (!outs.empty() && s->op != OP_HEAD) {
    STREAM_IO(s)->write(outs.c_str(), outs.size());
  }

  s->formatter->reset();
}

void rgw_flush_formatter(struct req_state *s, Formatter *formatter)
{
  std::ostringstream oss;
  formatter->flush(oss);
  std::string outs(oss.str());
  if (!outs.empty() && s->op != OP_HEAD) {
    STREAM_IO(s)->write(outs.c_str(), outs.size());
  }
}

void set_req_state_err(struct rgw_err& err,     /* out */
                       int err_no,              /* in  */
                       const int prot_flags)    /* in  */
{
  const struct rgw_http_errors *r;

  if (err_no < 0)
    err_no = -err_no;
  err.ret = -err_no;
  if (prot_flags & RGW_REST_SWIFT) {
    r = search_err(err_no, RGW_HTTP_SWIFT_ERRORS,
		   ARRAY_LEN(RGW_HTTP_SWIFT_ERRORS));
    if (r) {
      err.http_ret = r->http_ret;
      err.s3_code = r->s3_code;
      return;
    }
  }

  r = search_err(err_no, RGW_HTTP_ERRORS, ARRAY_LEN(RGW_HTTP_ERRORS));
  if (r) {
    err.http_ret = r->http_ret;
    err.s3_code = r->s3_code;
    return;
  }
  dout(0) << "WARNING: set_req_state_err err_no=" << err_no
	  << " resorting to 500" << dendl;

  err.http_ret = 500;
  err.s3_code = "UnknownError";
}

void set_req_state_err(struct req_state * const s, const int err_no)
{
  if (s) {
    set_req_state_err(s->err, err_no, s->prot_flags);
  }
}

void dump_errno(int http_ret, string& out) {
  stringstream ss;

  ss <<  http_ret << " " << http_status_names[http_ret];
  out = ss.str();
}

void dump_errno(const struct rgw_err &err, string& out) {
  dump_errno(err.http_ret, out);
}

void dump_errno(struct req_state *s)
{
  dump_status(s, s->err.http_ret, http_status_names[s->err.http_ret]);
}

void dump_errno(struct req_state *s, int http_ret)
{
  dump_status(s, http_ret, http_status_names[http_ret]);
}

void dump_string_header(struct req_state *s, const char *name, const char *val)
{
  int r = STREAM_IO(s)->print("%s: %s\r\n", name, val);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void dump_content_length(struct req_state *s, uint64_t len)
{
  int r = STREAM_IO(s)->send_content_length(len);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->send_content_length() returned err="
                     << r << dendl;
  }
  r = STREAM_IO(s)->print("Accept-Ranges: bytes\r\n");
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void dump_etag(struct req_state * const s, const char * const etag)
{
  if ('\0' == *etag) {
    return;
  }

  int r;
  if (s->prot_flags & RGW_REST_SWIFT) {
    r = STREAM_IO(s)->print("etag: %s\r\n", etag);
  } else {
    r = STREAM_IO(s)->print("ETag: \"%s\"\r\n", etag);
  }
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void dump_pair(struct req_state *s, const char *key, const char *value)
{
  if ( (strlen(key) > 0) && (strlen(value) > 0))
    STREAM_IO(s)->print("%s: %s\r\n", key, value);
}

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

void dump_redirect(struct req_state *s, const string& redirect)
{
  if (redirect.empty())
    return;

  STREAM_IO(s)->print("Location: %s\r\n", redirect.c_str());
}

void dump_time_header(struct req_state *s, const char *name, real_time t)
{
  utime_t ut(t);
  time_t secs = (time_t)ut.sec();

  char timestr[TIME_BUF_SIZE];
  struct tm result;
  struct tm *tmp = gmtime_r(&secs, &result);
  if (tmp == NULL)
    return;

  if (strftime(timestr, sizeof(timestr), "%a, %d %b %Y %H:%M:%S %Z", tmp) == 0)
    return;

  int r = STREAM_IO(s)->print("%s: %s\r\n", name, timestr);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void dump_last_modified(struct req_state *s, real_time t)
{
  dump_time_header(s, "Last-Modified", t);
}

void dump_epoch_header(struct req_state *s, const char *name, real_time t)
{
  utime_t ut(t);
  char sec_buf[32], nsec_buf[32];
  snprintf(sec_buf, sizeof(sec_buf), "%lld", (long long)ut.sec());
  snprintf(nsec_buf, sizeof(nsec_buf), "%09lld", (long long)ut.nsec());

  int r = STREAM_IO(s)->print("%s: %s.%s\r\n", name, sec_buf, nsec_buf);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void dump_time(struct req_state *s, const char *name, real_time *t)
{
  utime_t ut(*t);

  char buf[TIME_BUF_SIZE];
  struct tm result;
  time_t epoch = ut.sec();
  struct tm *tmp = gmtime_r(&epoch, &result);
  if (tmp == NULL)
    return;

  if (strftime(buf, sizeof(buf), "%Y-%m-%dT%T", tmp) == 0)
    return;

  char buf2[TIME_BUF_SIZE];
  snprintf(buf2, sizeof(buf2), "%s.%03dZ", buf, (int)(ut.usec() / 1000));

  s->formatter->dump_string(name, buf2);
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
    /* If the server specifies an origin host rather than "*",
     * then it must also include Origin in the Vary response header
     * to indicate to clients that server responses will differ
     * based on the value of the Origin request header.
     */
    if (strcmp(origin, "*") != 0)
      STREAM_IO(s)->print("Vary: Origin\r\n");

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

void dump_start(struct req_state *s)
{
  if (!s->content_started) {
    s->formatter->output_header();
    s->content_started = true;
  }
}

void dump_trans_id(req_state *s)
{
  if (s->prot_flags & RGW_REST_SWIFT) {
    STREAM_IO(s)->print("X-Trans-Id: %s\r\n", s->trans_id.c_str());
  }
  else if (s->trans_id.length()) {
    STREAM_IO(s)->print("x-amz-request-id: %s\r\n", s->trans_id.c_str());
  }
}

void end_header(struct req_state* s, RGWOp* op, const char *content_type,
		const int64_t proposed_content_length, bool force_content_type,
		bool force_no_error)
{
  string ctype;

  dump_trans_id(s);

  if ((!s->err.is_err()) &&
      (s->bucket_info.owner != s->user->user_id) &&
      (s->bucket_info.requester_pays)) {
    STREAM_IO(s)->print("x-amz-request-charged: requester\r\n");
  }

  if (op) {
    dump_access_control(s, op);
  }

  if (s->prot_flags & RGW_REST_SWIFT && !content_type) {
    force_content_type = true;
  }

  /* do not send content type if content length is zero
     and the content type was not set by the user */
  if (force_content_type ||
      (!content_type &&  s->formatter->get_len()  != 0) || s->err.is_err()){
    switch (s->format) {
    case RGW_FORMAT_XML:
      ctype = "application/xml";
      break;
    case RGW_FORMAT_JSON:
      ctype = "application/json";
      break;
    case RGW_FORMAT_HTML:
      ctype = "text/html";
      break;
    default:
      ctype = "text/plain";
      break;
    }
    if (s->prot_flags & RGW_REST_SWIFT)
      ctype.append("; charset=utf-8");
    content_type = ctype.c_str();
  }
  if (!force_no_error && s->err.is_err()) {
    dump_start(s);
    if (s->format != RGW_FORMAT_HTML) {
      s->formatter->open_object_section("Error");
    }
    if (!s->err.s3_code.empty())
      s->formatter->dump_string("Code", s->err.s3_code);
    if (!s->err.message.empty())
      s->formatter->dump_string("Message", s->err.message);
    if (!s->bucket_name.empty()) // TODO: connect to expose_bucket
      s->formatter->dump_string("BucketName", s->bucket_name);
    if (!s->trans_id.empty()) // TODO: connect to expose_bucket or another toggle
      s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->dump_string("HostId", s->host_id);
    if (s->format != RGW_FORMAT_HTML) {
      s->formatter->close_section();
    }
    s->formatter->output_footer();
    dump_content_length(s, s->formatter->get_len());
  } else {
    if (proposed_content_length != NO_CONTENT_LENGTH) {
      dump_content_length(s, proposed_content_length);
    }
  }

  int r;
  if (content_type) {
      r = STREAM_IO(s)->print("Content-Type: %s\r\n", content_type);
      if (r < 0) {
	ldout(s->cct, 0) << "ERROR: STREAM_IO(s)->print() returned err=" << r
			 << dendl;
      }
  }
  r = STREAM_IO(s)->complete_header();
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: STREAM_IO(s)->complete_header() returned err="
		     << r << dendl;
  }

  STREAM_IO(s)->set_account(true);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void abort_early(struct req_state *s, RGWOp *op, int err_no,
		 RGWHandler* handler)
{
  string error_content("");
  if (!s->formatter) {
    s->formatter = new JSONFormatter;
    s->format = RGW_FORMAT_JSON;
  }

  // op->error_handler is responsible for calling it's handler error_handler
  if (op != NULL) {
    int new_err_no;
    new_err_no = op->error_handler(err_no, &error_content);
    ldout(s->cct, 20) << "op->ERRORHANDLER: err_no=" << err_no
		      << " new_err_no=" << new_err_no << dendl;
    err_no = new_err_no;
  } else if (handler != NULL) {
    int new_err_no;
    new_err_no = handler->error_handler(err_no, &error_content);
    ldout(s->cct, 20) << "handler->ERRORHANDLER: err_no=" << err_no
		      << " new_err_no=" << new_err_no << dendl;
    err_no = new_err_no;
  }

  // If the error handler(s) above dealt with it completely, they should have
  // returned 0. If non-zero, we need to continue here.
  if (err_no) {
    // Watch out, we might have a custom error state already set!
    if (s->err.http_ret && s->err.http_ret != 200) {
      dump_errno(s);
    } else {
      set_req_state_err(s, err_no);
      dump_errno(s);
    }
    dump_bucket_from_state(s);
    if (err_no == -ERR_PERMANENT_REDIRECT || err_no == -ERR_WEBSITE_REDIRECT) {
      string dest_uri;
      if (!s->redirect.empty()) {
        dest_uri = s->redirect;
      } else if (!s->zonegroup_endpoint.empty()) {
        string dest_uri = s->zonegroup_endpoint;
        /*
         * reqest_uri is always start with slash, so we need to remove
         * the unnecessary slash at the end of dest_uri.
         */
        if (dest_uri[dest_uri.size() - 1] == '/') {
          dest_uri = dest_uri.substr(0, dest_uri.size() - 1);
        }
        dest_uri += s->info.request_uri;
        dest_uri += "?";
        dest_uri += s->info.request_params;
      }

      if (!dest_uri.empty()) {
        dump_redirect(s, dest_uri);
      }
    }

    if (!error_content.empty()) {
      /*
       * TODO we must add all error entries as headers here:
       * when having a working errordoc, then the s3 error fields are
       * rendered as HTTP headers, e.g.:
       *   x-amz-error-code: NoSuchKey
       *   x-amz-error-message: The specified key does not exist.
       *   x-amz-error-detail-Key: foo
       */
      end_header(s, op, NULL, error_content.size(), false, true);
      STREAM_IO(s)->write(error_content.c_str(), error_content.size());
    } else {
      end_header(s, op);
    }
    rgw_flush_formatter(s, s->formatter);
  }
  perfcounter->inc(l_rgw_failed_req);
}

void dump_continue(struct req_state *s)
{
  STREAM_IO(s)->send_100_continue();
}

void dump_range(struct req_state *s, uint64_t ofs, uint64_t end,
		uint64_t total)
{
  char range_buf[128];

  /* dumping range into temp buffer first, as libfcgi will fail to digest
   * %lld */

  if (!total) {
    snprintf(range_buf, sizeof(range_buf), "*/%lld", (long long)total);
  } else {
    snprintf(range_buf, sizeof(range_buf), "%lld-%lld/%lld", (long long)ofs,
		(long long)end, (long long)total);
  }
  int r = STREAM_IO(s)->print("Content-Range: bytes %s\r\n", range_buf);
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

  if (s->system_request) {
    mod_zone_id = s->info.env->get_int("HTTP_DEST_ZONE_SHORT_ID", 0);
    mod_pg_ver = s->info.env->get_int("HTTP_DEST_PG_VER", 0);
  }

  /* start gettorrent */
  bool is_torrent = s->info.args.exists(GET_TORRENT);
  bool torrent_flag = s->cct->_conf->rgw_torrent_flag;
  if (torrent_flag && is_torrent)
  {
    int ret = 0;
    ret = torrent.get_params();
    if (ret < 0)
    {
      return ret;
    }
  }
  /* end gettorrent */

  return 0;
}

int RESTArgs::get_string(struct req_state *s, const string& name,
			 const string& def_val, string *val, bool *existed)
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

int RESTArgs::get_uint64(struct req_state *s, const string& name,
			 uint64_t def_val, uint64_t *val, bool *existed)
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

int RESTArgs::get_int64(struct req_state *s, const string& name,
			int64_t def_val, int64_t *val, bool *existed)
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

int RESTArgs::get_uint32(struct req_state *s, const string& name,
			 uint32_t def_val, uint32_t *val, bool *existed)
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

int RESTArgs::get_int32(struct req_state *s, const string& name,
			int32_t def_val, int32_t *val, bool *existed)
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

int RESTArgs::get_time(struct req_state *s, const string& name,
		       const utime_t& def_val, utime_t *val, bool *existed)
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
  end_header(s, op);
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
    if (len > (off_t)(s->cct->_conf->rgw_max_put_size)) {
      return -ERR_TOO_LARGE;
    }
  }

  return 0;
}

int RGWPutObj_ObjStore::get_params()
{
  /* start gettorrent */
  if (s->cct->_conf->rgw_torrent_flag)
  {
    int ret = 0;
    ret = torrent.get_params();
    ldout(s->cct, 5) << "NOTICE:  open produce torrent file " << dendl;
    if (ret < 0)
    {
      return ret;
    }
    torrent.set_info_name((s->object).name);
  }
  /* end gettorrent */
  supplied_md5_b64 = s->info.env->get("HTTP_CONTENT_MD5");

  return 0;
}

int RGWPutObj_ObjStore::get_padding_last_aws4_chunk_encoded(bufferlist &bl, uint64_t chunk_size) {

  const int chunk_str_min_len = 1 + 17 + 64 + 2; /* len('0') = 1 */

  char *chunk_str = bl.c_str();
  int budget = bl.length();

  unsigned int chunk_data_size;
  unsigned int chunk_offset = 0;

  while (1) {

    /* check available metadata */
    if (budget < chunk_str_min_len) {
      return -ERR_SIGNATURE_NO_MATCH;
    }

    chunk_offset = 0;

    /* grab chunk size */
    while ((*(chunk_str+chunk_offset) != ';') && (chunk_offset < chunk_str_min_len))
      chunk_offset++;
    string str = string(chunk_str, chunk_offset);
    stringstream ss;
    ss << std::hex << str;
    ss >> chunk_data_size;

    /* next chunk */
    chunk_offset += 17 + 64 + 2 + chunk_data_size;

    /* last chunk? */
    budget -= chunk_offset;
    if (budget < 0) {
      budget *= -1;
      break;
    }

    chunk_str += chunk_offset;
  }

  return budget;
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

    /* read last aws4 chunk padding */
    if (s->aws4_auth_streaming_mode && len == (int)chunk_size) {
      int ret_auth = get_padding_last_aws4_chunk_encoded(bl, chunk_size);
      if (ret_auth < 0) {
        return ret_auth;
      }
      int len_padding = ret_auth;
      if (len_padding) {
        int read_len;
        bufferptr bp_extra(len_padding);
        int r = STREAM_IO(s)->read(bp_extra.c_str(), len_padding, &read_len,
                                   s->aws4_auth_needs_complete);
        if (r < 0) {
          return r;
        }
        if (read_len != len_padding) {
          return -ERR_SIGNATURE_NO_MATCH;
        }
        bl.append(bp_extra.c_str(), len_padding);
        bl.rebuild();
      }
    }

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

int RGWPutLC_ObjStore::get_params()
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
    int r = STREAM_IO(s)->read(data, cl, &read_len, s->aws4_auth_needs_complete);
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
    int r = STREAM_IO(s)->read(data + len, need_to_read, &read_len, s->aws4_auth_needs_complete);
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

int rgw_rest_read_all_input(struct req_state *s, char **pdata, int *plen,
			    int max_len)
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
    int ret = STREAM_IO(s)->read(data, cl, &len, s->aws4_auth_needs_complete);
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


void RGWRESTOp::send_response()
{
  if (!flusher.did_start()) {
    set_req_state_err(s, http_ret);
    dump_errno(s);
    end_header(s, this);
  }
  flusher.flush();
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
    if (s->info.args.exists("delete")) {
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

static int64_t parse_content_length(const char *content_length)
{
  int64_t len = -1;

  if (*content_length == '\0') {
    len = 0;
  } else {
    string err;
    len = strict_strtoll(content_length, 10, &err);
    if (!err.empty()) {
      len = -1;
    }
  }

  return len;
}

int RGWREST::preprocess(struct req_state *s, RGWClientIO* cio)
{
  req_info& info = s->info;

  /* save the request uri used to hash on the client side. request_uri may suffer
     modifications as part of the bucket encoding in the subdomain calling format.
     request_uri_aws4 will be used under aws4 auth */
  s->info.request_uri_aws4 = s->info.request_uri;

  s->cio = cio;

  // We need to know if this RGW instance is running the s3website API with a
  // higher priority than regular S3 API, or possibly in place of the regular
  // S3 API.
  // Map the listing of rgw_enable_apis in REVERSE order, so that items near
  // the front of the list have a higher number assigned (and -1 for items not in the list).
  list<string> apis;
  get_str_list(g_conf->rgw_enable_apis, apis);
  int api_priority_s3 = -1;
  int api_priority_s3website = -1;
  auto api_s3website_priority_rawpos = std::find(apis.begin(), apis.end(), "s3website");
  auto api_s3_priority_rawpos = std::find(apis.begin(), apis.end(), "s3");
  if (api_s3_priority_rawpos != apis.end()) {
    api_priority_s3 = apis.size() - std::distance(apis.begin(), api_s3_priority_rawpos);
  }
  if (api_s3website_priority_rawpos != apis.end()) {
    api_priority_s3website = apis.size() - std::distance(apis.begin(), api_s3website_priority_rawpos);
  }
  ldout(s->cct, 10) << "rgw api priority: s3=" << api_priority_s3 << " s3website=" << api_priority_s3website << dendl;
  bool s3website_enabled = api_priority_s3website >= 0;

  if (info.host.size()) {
    ldout(s->cct, 10) << "host=" << info.host << dendl;
    string domain;
    string subdomain;
    bool in_hosted_domain_s3website = false;
    bool in_hosted_domain = rgw_find_host_in_domains(info.host, &domain, &subdomain, hostnames_set);

    string s3website_domain;
    string s3website_subdomain;

    if (s3website_enabled) {
      in_hosted_domain_s3website = rgw_find_host_in_domains(info.host, &s3website_domain, &s3website_subdomain, hostnames_s3website_set);
      if (in_hosted_domain_s3website) {
	in_hosted_domain = true; // TODO: should hostnames be a strict superset of hostnames_s3website?
        domain = s3website_domain;
        subdomain = s3website_subdomain;
      }
    }

    ldout(s->cct, 20)
      << "subdomain=" << subdomain 
      << " domain=" << domain 
      << " in_hosted_domain=" << in_hosted_domain 
      << " in_hosted_domain_s3website=" << in_hosted_domain_s3website 
      << dendl;

    if (g_conf->rgw_resolve_cname
	&& !in_hosted_domain
	&& !in_hosted_domain_s3website) {
      string cname;
      bool found;
      int r = rgw_resolver->resolve_cname(info.host, cname, &found);
      if (r < 0) {
	ldout(s->cct, 0)
	  << "WARNING: rgw_resolver->resolve_cname() returned r=" << r
	  << dendl;
      }

      if (found) {
	ldout(s->cct, 5) << "resolved host cname " << info.host << " -> "
			 << cname << dendl;
	in_hosted_domain =
	  rgw_find_host_in_domains(cname, &domain, &subdomain, hostnames_set);

        if (s3website_enabled
	    && !in_hosted_domain_s3website) {
	  in_hosted_domain_s3website =
	    rgw_find_host_in_domains(cname, &s3website_domain,
				     &s3website_subdomain,
				     hostnames_s3website_set);
	  if (in_hosted_domain_s3website) {
	    in_hosted_domain = true; // TODO: should hostnames be a
				     // strict superset of hostnames_s3website?
	    domain = s3website_domain;
	    subdomain = s3website_subdomain;
	  }
        }

        ldout(s->cct, 20)
          << "subdomain=" << subdomain 
          << " domain=" << domain 
          << " in_hosted_domain=" << in_hosted_domain 
          << " in_hosted_domain_s3website=" << in_hosted_domain_s3website 
          << dendl;
      }
    }

    // Handle A/CNAME records that point to the RGW storage, but do match the
    // CNAME test above, per issue http://tracker.ceph.com/issues/15975
    // If BOTH domain & subdomain variables are empty, then none of the above
    // cases matched anything, and we should fall back to using the Host header
    // directly as the bucket name.
    // As additional checks:
    // - if the Host header is an IP, we're using path-style access without DNS
    // - Also check that the Host header is a valid bucket name before using it.
    if (subdomain.empty()
        && (domain.empty() || domain != info.host)
        && !looks_like_ip_address(info.host.c_str())
        && RGWHandler_REST::validate_bucket_name(info.host)) {
      subdomain.append(info.host);
      in_hosted_domain = 1;
    }

    if (s3website_enabled && api_priority_s3website > api_priority_s3) {
      in_hosted_domain_s3website = 1;
    }

    if (in_hosted_domain_s3website) {
      s->prot_flags |= RGW_REST_WEBSITE;
    }


    if (in_hosted_domain && !subdomain.empty()) {
      string encoded_bucket = "/";
      encoded_bucket.append(subdomain);
      if (s->info.request_uri[0] != '/')
        encoded_bucket.append("/");
      encoded_bucket.append(s->info.request_uri);
      s->info.request_uri = encoded_bucket;
    }

    if (!domain.empty()) {
      s->info.domain = domain;
    }

    ldout(s->cct, 20)
      << "final domain/bucket"
      << " subdomain=" << subdomain
      << " domain=" << domain
      << " in_hosted_domain=" << in_hosted_domain
      << " in_hosted_domain_s3website=" << in_hosted_domain_s3website
      << " s->info.domain=" << s->info.domain
      << " s->info.request_uri=" << s->info.request_uri
      << dendl;
  }

  if (s->info.domain.empty()) {
    s->info.domain = s->cct->_conf->rgw_dns_name;
  }

  url_decode(s->info.request_uri, s->decoded_uri);

  /* FastCGI specification, section 6.3
   * http://www.fastcgi.com/devkit/doc/fcgi-spec.html#S6.3
   * ===
   * The Authorizer application receives HTTP request information from the Web
   * server on the FCGI_PARAMS stream, in the same format as a Responder. The
   * Web server does not send CONTENT_LENGTH, PATH_INFO, PATH_TRANSLATED, and
   * SCRIPT_NAME headers.
   * ===
   * Ergo if we are in Authorizer role, we MUST look at HTTP_CONTENT_LENGTH
   * instead of CONTENT_LENGTH for the Content-Length.
   *
   * There is one slight wrinkle in this, and that's older versions of
   * nginx/lighttpd/apache setting BOTH headers. As a result, we have to check
   * both headers and can't always simply pick A or B.
   */
  const char* content_length = info.env->get("CONTENT_LENGTH");
  const char* http_content_length = info.env->get("HTTP_CONTENT_LENGTH");
  if (!http_content_length != !content_length) {
    /* Easy case: one or the other is missing */
    s->length = (content_length ? content_length : http_content_length);
  } else if (s->cct->_conf->rgw_content_length_compat &&
	     content_length && http_content_length) {
    /* Hard case: Both are set, we have to disambiguate */
    int64_t content_length_i, http_content_length_i;

    content_length_i = parse_content_length(content_length);
    http_content_length_i = parse_content_length(http_content_length);

    // Now check them:
    if (http_content_length_i < 0) {
      // HTTP_CONTENT_LENGTH is invalid, ignore it
    } else if (content_length_i < 0) {
      // CONTENT_LENGTH is invalid, and HTTP_CONTENT_LENGTH is valid
      // Swap entries
      content_length = http_content_length;
    } else {
      // both CONTENT_LENGTH and HTTP_CONTENT_LENGTH are valid
      // Let's pick the larger size
      if (content_length_i < http_content_length_i) {
	// prefer the larger value
	content_length = http_content_length;
      }
    }
    s->length = content_length;
    // End of: else if (s->cct->_conf->rgw_content_length_compat &&
    //   content_length &&
    // http_content_length)
  } else {
    /* no content length was defined */
    s->length = NULL;
  }

  if (s->length) {
    if (*s->length == '\0') {
      s->content_length = 0;
    } else {
      string err;
      s->content_length = strict_strtoll(s->length, 10, &err);
      if (!err.empty()) {
	ldout(s->cct, 10) << "bad content length, aborting" << dendl;
	return -EINVAL;
      }
    }
  }

  if (s->content_length < 0) {
    ldout(s->cct, 10) << "negative content length, aborting" << dendl;
    return -EINVAL;
  }

  map<string, string>::iterator giter;
  for (giter = generic_attrs_map.begin(); giter != generic_attrs_map.end();
       ++giter) {
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

RGWHandler_REST* RGWREST::get_handler(RGWRados *store, struct req_state *s,
				      RGWStreamIO *sio, RGWRESTMgr **pmgr,
				      int *init_error)
{
  RGWHandler_REST* handler;

  *init_error = preprocess(s, sio);
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
  *init_error = handler->init(store, s, sio);
  if (*init_error < 0) {
    m->put_handler(handler);
    return NULL;
  }

  return handler;
} /* get stream handler */
