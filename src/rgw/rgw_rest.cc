// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp


#include <errno.h>
#include <limits.h>

#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>
#include "ceph_ver.h"
#include "common/Formatter.h"
#include "common/HTMLFormatter.h"
#include "common/utf8.h"
#include "include/str_list.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_auth_s3.h"
#include "rgw_formats.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_swift.h"
#include "rgw_rest_s3.h"
#include "rgw_swift_auth.h"
#include "rgw_cors_s3.h"
#include "rgw_perf_counters.h"

#include "rgw_client_io.h"
#include "rgw_resolve.h"
#include "rgw_sal_rados.h"

#include "rgw_ratelimit.h"
#include <numeric>

#define dout_subsys ceph_subsys_rgw

using namespace std;

struct rgw_http_status_code {
  int code;
  const char *name;
};

const static struct rgw_http_status_code http_codes[] = {
  { 100, "Continue" },
  { 200, "OK" },
  { 201, "Created" },
  { 202, "Accepted" },
  { 204, "No Content" },
  { 205, "Reset Content" },
  { 206, "Partial Content" },
  { 207, "Multi Status" },
  { 208, "Already Reported" },
  { 300, "Multiple Choices" },
  { 301, "Moved Permanently" },
  { 302, "Found" },
  { 303, "See Other" },
  { 304, "Not Modified" },
  { 305, "User Proxy" },
  { 306, "Switch Proxy" },
  { 307, "Temporary Redirect" },
  { 308, "Permanent Redirect" },
  { 400, "Bad Request" },
  { 401, "Unauthorized" },
  { 402, "Payment Required" },
  { 403, "Forbidden" },
  { 404, "Not Found" },
  { 405, "Method Not Allowed" },
  { 406, "Not Acceptable" },
  { 407, "Proxy Authentication Required" },
  { 408, "Request Timeout" },
  { 409, "Conflict" },
  { 410, "Gone" },
  { 411, "Length Required" },
  { 412, "Precondition Failed" },
  { 413, "Request Entity Too Large" },
  { 414, "Request-URI Too Long" },
  { 415, "Unsupported Media Type" },
  { 416, "Requested Range Not Satisfiable" },
  { 417, "Expectation Failed" },
  { 422, "Unprocessable Entity" },
  { 498, "Rate Limited"},
  { 500, "Internal Server Error" },
  { 501, "Not Implemented" },
  { 503, "Slow Down"},
  { 0, NULL },
};

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
  { RGW_ATTR_X_ROBOTS_TAG ,     "X-Robots-Tag" },
  { RGW_ATTR_STORAGE_CLASS ,    "X-Amz-Storage-Class" },
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
  { "HTTP_X_ROBOTS_TAG",        RGW_ATTR_X_ROBOTS_TAG },
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

/* avoid duplicate hostnames in hostnames lists */
static set<string> hostnames_set;
static set<string> hostnames_s3website_set;

void rgw_rest_init(CephContext *cct, const rgw::sal::ZoneGroup& zone_group)
{
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

  std::list<std::string> rgw_dns_names;
  std::string rgw_dns_names_str = cct->_conf->rgw_dns_name;
  get_str_list(rgw_dns_names_str, ", ", rgw_dns_names);
  hostnames_set.insert(rgw_dns_names.begin(), rgw_dns_names.end());

  std::list<std::string> names;
  zone_group.get_hostnames(names);
  hostnames_set.insert(names.begin(), names.end());
  hostnames_set.erase(""); // filter out empty hostnames
  ldout(cct, 20) << "RGW hostnames: " << hostnames_set << dendl;
  /* TODO: We should have a sanity check that no hostname matches the end of
   * any other hostname, otherwise we will get ambiguous results from
   * rgw_find_host_in_domains.
   * Eg: 
   * Hostnames: [A, B.A]
   * Inputs: [Z.A, X.B.A]
   * Z.A clearly splits to subdomain=Z, domain=Z
   * X.B.A ambiguously splits to both {X, B.A} and {X.B, A}
   */

  zone_group.get_s3website_hostnames(names);
  hostnames_s3website_set.insert(cct->_conf->rgw_dns_s3website_name);
  hostnames_s3website_set.insert(names.begin(), names.end());
  hostnames_s3website_set.erase(""); // filter out empty hostnames
  ldout(cct, 20) << "RGW S3website hostnames: " << hostnames_s3website_set << dendl;
  /* TODO: we should repeat the hostnames_set sanity check here
   * and ALSO decide about overlap, if any
   */
}

static bool str_ends_with_nocase(const string& s, const string& suffix, size_t *pos)
{
  size_t len = suffix.size();
  if (len > (size_t)s.size()) {
    return false;
  }

  ssize_t p = s.size() - len;
  if (pos) {
    *pos = p;
  }

  return boost::algorithm::iends_with(s, suffix);
}

static bool rgw_find_host_in_domains(const string& host, string *domain, string *subdomain,
                                     const set<string>& valid_hostnames_set)
{
  set<string>::iterator iter;
  /** TODO, Future optimization
   * store hostnames_set elements _reversed_, and look for a prefix match,
   * which is much faster than a suffix match.
   */
  for (iter = valid_hostnames_set.begin(); iter != valid_hostnames_set.end(); ++iter) {
    size_t pos;
    if (!str_ends_with_nocase(host, *iter, &pos))
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

static void dump_status(req_state *s, int status,
			const char *status_name)
{
  if (s->formatter) {
    s->formatter->set_status(status, status_name);
  }
  try {
    RESTFUL_IO(s)->send_status(status, status_name);
  } catch (rgw::io::Exception& e) {
    ldpp_dout(s, 0) << "ERROR: s->cio->send_status() returned err="
                     << e.what() << dendl;
  }
}

void rgw_flush_formatter_and_reset(req_state *s, Formatter *formatter)
{
  std::ostringstream oss;
  formatter->output_footer();
  formatter->flush(oss);
  std::string outs(oss.str());
  if (!outs.empty() && s->op != OP_HEAD) {
    dump_body(s, outs);
  }

  s->formatter->reset();
}

void rgw_flush_formatter(req_state *s, Formatter *formatter)
{
  std::ostringstream oss;
  formatter->flush(oss);
  std::string outs(oss.str());
  if (!outs.empty() && s->op != OP_HEAD) {
    dump_body(s, outs);
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

void dump_errno(req_state *s)
{
  dump_status(s, s->err.http_ret, http_status_names[s->err.http_ret]);
}

void dump_errno(req_state *s, int http_ret)
{
  dump_status(s, http_ret, http_status_names[http_ret]);
}

void dump_header(req_state* const s,
                 const std::string_view& name,
                 const std::string_view& val)
{
  try {
    RESTFUL_IO(s)->send_header(name, val);
  } catch (rgw::io::Exception& e) {
    ldpp_dout(s, 0) << "ERROR: s->cio->send_header() returned err="
                     << e.what() << dendl;
  }
}

void dump_header(req_state* const s,
                 const std::string_view& name,
                 ceph::buffer::list& bl)
{
  return dump_header(s, name, rgw_sanitized_hdrval(bl));
}

void dump_header(req_state* const s,
                 const std::string_view& name,
                 const long long val)
{
  char buf[32];
  const auto len = snprintf(buf, sizeof(buf), "%lld", val);

  return dump_header(s, name, std::string_view(buf, len));
}

void dump_header(req_state* const s,
                 const std::string_view& name,
                 const utime_t& ut)
{
  char buf[32];
  const auto len = snprintf(buf, sizeof(buf), "%lld.%05d",
	                    static_cast<long long>(ut.sec()),
                            static_cast<int>(ut.usec() / 10));

  return dump_header(s, name, std::string_view(buf, len));
}

void dump_content_length(req_state* const s, const uint64_t len)
{
  try {
    RESTFUL_IO(s)->send_content_length(len);
  } catch (rgw::io::Exception& e) {
    ldpp_dout(s, 0) << "ERROR: s->cio->send_content_length() returned err="
                     << e.what() << dendl;
  }
  dump_header(s, "Accept-Ranges", "bytes");
}

static void dump_chunked_encoding(req_state* const s)
{
  try {
    RESTFUL_IO(s)->send_chunked_transfer_encoding();
  } catch (rgw::io::Exception& e) {
    ldpp_dout(s, 0) << "ERROR: RESTFUL_IO(s)->send_chunked_transfer_encoding()"
                     << " returned err=" << e.what() << dendl;
  }
}

void dump_etag(req_state* const s,
               const std::string_view& etag,
               const bool quoted)
{
  if (etag.empty()) {
    return;
  }

  if (s->prot_flags & RGW_REST_SWIFT && ! quoted) {
    return dump_header(s, "etag", etag);
  } else {
    return dump_header_quoted(s, "ETag", etag);
  }
}

void dump_bucket_from_state(req_state *s)
{
  if (g_conf()->rgw_expose_bucket && ! s->bucket_name.empty()) {
    if (! s->bucket_tenant.empty()) {
      dump_header(s, "Bucket",
                  url_encode(s->bucket_tenant + "/" + s->bucket_name));
    } else {
      dump_header(s, "Bucket", url_encode(s->bucket_name));
    }
  }
}

void dump_redirect(req_state * const s, const std::string& redirect)
{
  return dump_header_if_nonempty(s, "Location", redirect);
}

static size_t dump_time_header_impl(char (&timestr)[TIME_BUF_SIZE],
                                    const real_time t)
{
  const utime_t ut(t);
  time_t secs = static_cast<time_t>(ut.sec());

  struct tm result;
  const struct tm * const tmp = gmtime_r(&secs, &result);
  if (tmp == nullptr) {
    return 0;
  }

  return strftime(timestr, sizeof(timestr),
                  "%a, %d %b %Y %H:%M:%S %Z", tmp);
}

void dump_time_header(req_state *s, const char *name, real_time t)
{
  char timestr[TIME_BUF_SIZE];

  const size_t len = dump_time_header_impl(timestr, t);
  if (len == 0) {
    return;
  }

  return dump_header(s, name, std::string_view(timestr, len));
}

std::string dump_time_to_str(const real_time& t)
{
  char timestr[TIME_BUF_SIZE];
  dump_time_header_impl(timestr, t);

  return timestr;
}


void dump_last_modified(req_state *s, real_time t)
{
  dump_time_header(s, "Last-Modified", t);
}

void dump_epoch_header(req_state *s, const char *name, real_time t)
{
  utime_t ut(t);
  char buf[65];
  const auto len = snprintf(buf, sizeof(buf), "%lld.%09lld",
                            (long long)ut.sec(),
                            (long long)ut.nsec());

  return dump_header(s, name, std::string_view(buf, len));
}

void dump_time(req_state *s, const char *name, real_time t)
{
  char buf[TIME_BUF_SIZE];
  rgw_to_iso8601(t, buf, sizeof(buf));

  s->formatter->dump_string(name, buf);
}

void dump_owner(req_state *s, const rgw_user& id, const string& name,
		const char *section)
{
  if (!section)
    section = "Owner";
  s->formatter->open_object_section(section);
  s->formatter->dump_string("ID", id.to_str());
  s->formatter->dump_string("DisplayName", name);
  s->formatter->close_section();
}

void dump_access_control(req_state *s, const char *origin,
			 const char *meth,
			 const char *hdr, const char *exp_hdr,
			 uint32_t max_age) {
  if (origin && (origin[0] != '\0')) {
    dump_header(s, "Access-Control-Allow-Origin", origin);
    /* If the server specifies an origin host rather than "*",
     * then it must also include Origin in the Vary response header
     * to indicate to clients that server responses will differ
     * based on the value of the Origin request header.
     */
    if (strcmp(origin, "*") != 0) {
      dump_header(s, "Vary", "Origin");
    }

    if (meth && (meth[0] != '\0')) {
      dump_header(s, "Access-Control-Allow-Methods", meth);
    }
    if (hdr && (hdr[0] != '\0')) {
      dump_header(s, "Access-Control-Allow-Headers", hdr);
    }
    if (exp_hdr && (exp_hdr[0] != '\0')) {
      dump_header(s, "Access-Control-Expose-Headers", exp_hdr);
    }
    if (max_age != CORS_MAX_AGE_INVALID) {
      dump_header(s, "Access-Control-Max-Age", max_age);
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

void dump_start(req_state *s)
{
  if (!s->content_started) {
    s->formatter->output_header();
    s->content_started = true;
  }
}

void dump_trans_id(req_state *s)
{
  if (s->prot_flags & RGW_REST_SWIFT) {
    dump_header(s, "X-Trans-Id", s->trans_id);
    dump_header(s, "X-Openstack-Request-Id", s->trans_id);
  } else if (s->trans_id.length()) {
    dump_header(s, "x-amz-request-id", s->trans_id);
  }
}

void end_header(req_state* s, RGWOp* op, const char *content_type,
		const int64_t proposed_content_length, bool force_content_type,
		bool force_no_error)
{
  string ctype;

  dump_trans_id(s);

  if ((!s->is_err()) && s->bucket &&
      (s->bucket->get_info().owner != s->user->get_id()) &&
      (s->bucket->get_info().requester_pays)) {
    dump_header(s, "x-amz-request-charged", "requester");
  }

  if (op) {
    dump_access_control(s, op);
  }

  if (s->prot_flags & RGW_REST_SWIFT && !content_type) {
    force_content_type = true;
  }

  /* do not send content type if content length is zero
     and the content type was not set by the user */
  if (force_content_type || s->is_err() ||
      (!content_type && s->formatter && s->formatter->get_len() != 0)) {
    ctype = to_mime_type(s->format);
    if (s->prot_flags & RGW_REST_SWIFT)
      ctype.append("; charset=utf-8");
    content_type = ctype.c_str();
  }
  if (!force_no_error && s->is_err()) {
    dump_start(s);
    dump(s);
    s->formatter->output_footer();
    dump_content_length(s, s->formatter ? s->formatter->get_len() : 0);
  } else {
    if (proposed_content_length == CHUNKED_TRANSFER_ENCODING) {
      dump_chunked_encoding(s);
    } else if (proposed_content_length != NO_CONTENT_LENGTH) {
      dump_content_length(s, proposed_content_length);
    }
  }

  if (content_type) {
    dump_header(s, "Content-Type", content_type);
  }

  std::string srv = g_conf().get_val<std::string>("rgw_service_provider_name");
  if (!srv.empty()) {
    dump_header(s, "Server", srv);
  } else {
    dump_header(s, "Server", "Ceph Object Gateway (" CEPH_RELEASE_NAME ")");
  }

  try {
    RESTFUL_IO(s)->complete_header();
  } catch (rgw::io::Exception& e) {
    ldpp_dout(s, 0) << "ERROR: RESTFUL_IO(s)->complete_header() returned err="
		     << e.what() << dendl;
  }

  ACCOUNTING_IO(s)->set_account(true);
  if (s->formatter) {
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

static void build_redirect_url(req_state *s, const string& redirect_base, string *redirect_url)
{
  string& dest_uri = *redirect_url;
  
  dest_uri = redirect_base;
  /*
   * request_uri is always start with slash, so we need to remove
   * the unnecessary slash at the end of dest_uri.
   */
  if (dest_uri[dest_uri.size() - 1] == '/') {
    dest_uri = dest_uri.substr(0, dest_uri.size() - 1);
  }
  dest_uri += s->info.request_uri;
  dest_uri += "?";
  dest_uri += s->info.request_params;
}

void abort_early(req_state *s, RGWOp* op, int err_no,
		 RGWHandler* handler, optional_yield y)
{
  string error_content("");
  if (!s->formatter) {
    s->formatter = new JSONFormatter;
    s->format = RGWFormat::JSON;
  }

  // op->error_handler is responsible for calling it's handler error_handler
  if (op != NULL) {
    int new_err_no;
    new_err_no = op->error_handler(err_no, &error_content, y);
    ldpp_dout(s, 1) << "op->ERRORHANDLER: err_no=" << err_no
		      << " new_err_no=" << new_err_no << dendl;
    err_no = new_err_no;
  } else if (handler != NULL) {
    int new_err_no;
    new_err_no = handler->error_handler(err_no, &error_content, y);
    ldpp_dout(s, 1) << "handler->ERRORHANDLER: err_no=" << err_no
		      << " new_err_no=" << new_err_no << dendl;
    err_no = new_err_no;
  }

  // If the error handler(s) above dealt with it completely, they should have
  // returned 0. If non-zero, we need to continue here.
  if (err_no) {
    // Watch out, we might have a custom error state already set!
    if (!s->err.http_ret || s->err.http_ret == 200) {
      set_req_state_err(s, err_no);
    }

    if (s->err.http_ret == 404 && !s->redirect_zone_endpoint.empty()) {
      s->err.http_ret = 301;
      err_no = -ERR_PERMANENT_REDIRECT;
      build_redirect_url(s, s->redirect_zone_endpoint, &s->redirect);
    }

    dump_errno(s);
    dump_bucket_from_state(s);
    if (err_no == -ERR_PERMANENT_REDIRECT || err_no == -ERR_WEBSITE_REDIRECT) {
      string dest_uri;
      if (!s->redirect.empty()) {
        dest_uri = s->redirect;
      } else if (!s->zonegroup_endpoint.empty()) {
        build_redirect_url(s, s->zonegroup_endpoint, &dest_uri);
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
      RESTFUL_IO(s)->send_body(error_content.c_str(), error_content.size());
    } else {
      end_header(s, op);
    }
  }
  perfcounter->inc(l_rgw_failed_req);
}

void dump_continue(req_state * const s)
{
  try {
    RESTFUL_IO(s)->send_100_continue();
  } catch (rgw::io::Exception& e) {
    ldpp_dout(s, 0) << "ERROR: RESTFUL_IO(s)->send_100_continue() returned err="
		     << e.what() << dendl;
  }
}

void dump_range(req_state* const s,
                const uint64_t ofs,
                const uint64_t end,
		const uint64_t total)
{
  /* dumping range into temp buffer first, as libfcgi will fail to digest
   * %lld */
  char range_buf[128];
  size_t len;

  if (! total) {
    len = snprintf(range_buf, sizeof(range_buf), "bytes */%lld",
                   static_cast<long long>(total));
  } else {
    len = snprintf(range_buf, sizeof(range_buf), "bytes %lld-%lld/%lld",
                   static_cast<long long>(ofs),
                   static_cast<long long>(end),
                   static_cast<long long>(total));
  }

  return dump_header(s, "Content-Range", std::string_view(range_buf, len));
}


int dump_body(req_state* const s,
              const char* const buf,
              const size_t len)
{
  bool healthcheck = false;
  // we dont want to limit health checks
  if(s->op_type == RGW_OP_GET_HEALTH_CHECK)
    healthcheck = true;
  if(len > 0 && !healthcheck) {
    const char *method = s->info.method;
    s->ratelimit_data->decrease_bytes(method, s->ratelimit_user_name, len, &s->user_ratelimit);
    if(!rgw::sal::Bucket::empty(s->bucket.get()))
      s->ratelimit_data->decrease_bytes(method, s->ratelimit_bucket_marker, len, &s->bucket_ratelimit);
  }
  try {
    return RESTFUL_IO(s)->send_body(buf, len);
  } catch (rgw::io::Exception& e) {
    return -e.code().value();
  }
}

int dump_body(req_state* const s, /* const */ ceph::buffer::list& bl)
{
  return dump_body(s, bl.c_str(), bl.length());
}

int dump_body(req_state* const s, const std::string& str)
{
  return dump_body(s, str.c_str(), str.length());
}

int recv_body(req_state* const s,
              char* const buf,
              const size_t max)
{
  int len;
  try {
    len = RESTFUL_IO(s)->recv_body(buf, max);
  } catch (rgw::io::Exception& e) {
    return -e.code().value();
  }
  bool healthcheck = false;
  // we dont want to limit health checks
  if(s->op_type ==  RGW_OP_GET_HEALTH_CHECK)
    healthcheck = true;
  if(len > 0 && !healthcheck) {
    const char *method = s->info.method;
    s->ratelimit_data->decrease_bytes(method, s->ratelimit_user_name, len, &s->user_ratelimit);
    if(!rgw::sal::Bucket::empty(s->bucket.get()))
      s->ratelimit_data->decrease_bytes(method, s->ratelimit_bucket_marker, len, &s->bucket_ratelimit);
  }
  return len;

}

int RGWGetObj_ObjStore::get_params(optional_yield y)
{
  range_str = s->info.env->get("HTTP_RANGE");
  if_mod = s->info.env->get("HTTP_IF_MODIFIED_SINCE");
  if_unmod = s->info.env->get("HTTP_IF_UNMODIFIED_SINCE");
  if_match = s->info.env->get("HTTP_IF_MATCH");
  if_nomatch = s->info.env->get("HTTP_IF_NONE_MATCH");

  if (s->system_request) {
    mod_zone_id = s->info.env->get_int("HTTP_DEST_ZONE_SHORT_ID", 0);
    mod_pg_ver = s->info.env->get_int("HTTP_DEST_PG_VER", 0);
    rgwx_stat = s->info.args.exists(RGW_SYS_PARAM_PREFIX "stat");
    get_data &= (!rgwx_stat);
  }

  return 0;
}

int RESTArgs::get_string(req_state *s, const string& name,
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

int RESTArgs::get_uint64(req_state *s, const string& name,
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

int RESTArgs::get_int64(req_state *s, const string& name,
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

int RESTArgs::get_uint32(req_state *s, const string& name,
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

int RESTArgs::get_int32(req_state *s, const string& name,
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

int RESTArgs::get_time(req_state *s, const string& name,
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

int RESTArgs::get_epoch(req_state *s, const string& name, uint64_t def_val, uint64_t *epoch, bool *existed)
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

int RESTArgs::get_bool(req_state *s, const string& name, bool def_val, bool *val, bool *existed)
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

int RGWPutObj_ObjStore::get_params(optional_yield y)
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
  {
    ACCOUNTING_IO(s)->set_account(true);
    bufferptr bp(cl);

    const auto read_len  = recv_body(s, bp.c_str(), cl);
    if (read_len < 0) {
      return read_len;
    }

    len = read_len;
    bl.append(bp, 0, len);

    ACCOUNTING_IO(s)->set_account(false);
  }

  if ((uint64_t)ofs + len > s->cct->_conf->rgw_max_put_size) {
    return -ERR_TOO_LARGE;
  }

  return len;
}


/*
 * parses params in the format: 'first; param1=foo; param2=bar'
 */
void RGWPostObj_ObjStore::parse_boundary_params(const std::string& params_str,
                                                std::string& first,
                                                std::map<std::string,
                                                std::string>& params)
{
  size_t pos = params_str.find(';');
  if (std::string::npos == pos) {
    first = rgw_trim_whitespace(params_str);
    return;
  }

  first = rgw_trim_whitespace(params_str.substr(0, pos));
  pos++;

  while (pos < params_str.size()) {
    size_t end = params_str.find(';', pos);
    if (std::string::npos == end) {
      end = params_str.size();
    }

    std::string param = params_str.substr(pos, end - pos);
    size_t eqpos = param.find('=');

    if (std::string::npos != eqpos) {
      std::string param_name = rgw_trim_whitespace(param.substr(0, eqpos));
      std::string val = rgw_trim_quotes(param.substr(eqpos + 1));
      params[std::move(param_name)] = std::move(val);
    } else {
      params[rgw_trim_whitespace(param)] = "";
    }

    pos = end + 1;
  }
}

int RGWPostObj_ObjStore::parse_part_field(const std::string& line,
                                          std::string& field_name,  /* out */
                                          post_part_field& field)   /* out */
{
  size_t pos = line.find(':');
  if (pos == string::npos)
    return -EINVAL;

  field_name = line.substr(0, pos);
  if (pos >= line.size() - 1)
    return 0;

  parse_boundary_params(line.substr(pos + 1), field.val, field.params);

  return 0;
}

static bool is_crlf(const char *s)
{
  return (*s == '\r' && *(s + 1) == '\n');
}

/*
 * find the index of the boundary, if exists, or optionally the next end of line
 * also returns how many bytes to skip
 */
static int index_of(ceph::bufferlist& bl,
                    uint64_t max_len,
                    const std::string& str,
                    const bool check_crlf,
                    bool& reached_boundary,
                    int& skip)
{
  reached_boundary = false;
  skip = 0;

  if (str.size() < 2) // we assume boundary is at least 2 chars (makes it easier with crlf checks)
    return -EINVAL;

  if (bl.length() < str.size())
    return -1;

  const char *buf = bl.c_str();
  const char *s = str.c_str();

  if (max_len > bl.length())
    max_len = bl.length();

  for (uint64_t i = 0; i < max_len; i++, buf++) {
    if (check_crlf &&
	i >= 1 &&
	is_crlf(buf - 1)) {
      return i + 1; // skip the crlf
    }
    if ((i < max_len - str.size() + 1) &&
	(buf[0] == s[0] && buf[1] == s[1]) &&
	(strncmp(buf, s, str.size()) == 0)) {
      reached_boundary = true;
      skip = str.size();

      /* oh, great, now we need to swallow the preceding crlf
       * if exists
       */
      if ((i >= 2) &&
	  is_crlf(buf - 2)) {
	i -= 2;
	skip += 2;
      }
      return i;
    }
  }

  return -1;
}

int RGWPostObj_ObjStore::read_with_boundary(ceph::bufferlist& bl,
                                            uint64_t max,
                                            const bool check_crlf,
                                            bool& reached_boundary,
                                            bool& done)
{
  uint64_t cl = max + 2 + boundary.size();

  if (max > in_data.length()) {
    uint64_t need_to_read = cl - in_data.length();

    bufferptr bp(need_to_read);

    const auto read_len = recv_body(s, bp.c_str(), need_to_read);
    if (read_len < 0) {
      return read_len;
    }
    in_data.append(bp, 0, read_len);
  }

  done = false;
  int skip;
  const int index = index_of(in_data, cl, boundary, check_crlf,
                             reached_boundary, skip);
  if (index >= 0) {
    max = index;
  }

  if (max > in_data.length()) {
    max = in_data.length();
  }

  bl.substr_of(in_data, 0, max);

  ceph::bufferlist new_read_data;

  /*
   * now we need to skip boundary for next time, also skip any crlf, or
   * check to see if it's the last final boundary (marked with "--" at the end
   */
  if (reached_boundary) {
    int left = in_data.length() - max;
    if (left < skip + 2) {
      int need = skip + 2 - left;
      bufferptr boundary_bp(need);
      const int r = recv_body(s, boundary_bp.c_str(), need);
      if (r < 0) {
        return r;
      }
      in_data.append(boundary_bp);
    }
    max += skip; // skip boundary for next time
    if (in_data.length() >= max + 2) {
      const char *data = in_data.c_str();
      if (is_crlf(data + max)) {
	max += 2;
      } else {
	if (*(data + max) == '-' &&
	    *(data + max + 1) == '-') {
	  done = true;
	  max += 2;
	}
      }
    }
  }

  new_read_data.substr_of(in_data, max, in_data.length() - max);
  in_data = new_read_data;

  return 0;
}

int RGWPostObj_ObjStore::read_line(ceph::bufferlist& bl,
                                   const uint64_t max,
                                   bool& reached_boundary,
                                   bool& done)
{
  return read_with_boundary(bl, max, true, reached_boundary, done);
}

int RGWPostObj_ObjStore::read_data(ceph::bufferlist& bl,
                                   const uint64_t max,
                                   bool& reached_boundary,
                                   bool& done)
{
  return read_with_boundary(bl, max, false, reached_boundary, done);
}


int RGWPostObj_ObjStore::read_form_part_header(struct post_form_part* const part,
                                               bool& done)
{
  bufferlist bl;
  bool reached_boundary;
  uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
  int r = read_line(bl, chunk_size, reached_boundary, done);
  if (r < 0) {
    return r;
  }

  if (done) {
    return 0;
  }

  if (reached_boundary) { // skip the first boundary
    r = read_line(bl, chunk_size, reached_boundary, done);
    if (r < 0) {
      return r;
    } else if (done) {
      return 0;
    }
  }

  while (true) {
  /*
   * iterate through fields
   */
    std::string line = rgw_trim_whitespace(string(bl.c_str(), bl.length()));

    if (line.empty()) {
      break;
    }

    struct post_part_field field;

    string field_name;
    r = parse_part_field(line, field_name, field);
    if (r < 0) {
      return r;
    }

    part->fields[field_name] = field;

    if (stringcasecmp(field_name, "Content-Disposition") == 0) {
      part->name = field.params["name"];
    }

    if (reached_boundary) {
      break;
    }

    r = read_line(bl, chunk_size, reached_boundary, done);
    if (r < 0) {
      return r;
    }
  }

  return 0;
}

bool RGWPostObj_ObjStore::part_str(parts_collection_t& parts,
                                   const std::string& name,
                                   std::string* val)
{
  const auto iter = parts.find(name);
  if (std::end(parts) == iter) {
    return false;
  }

  ceph::bufferlist& data = iter->second.data;
  std::string str = string(data.c_str(), data.length());
  *val = rgw_trim_whitespace(str);
  return true;
}

std::string RGWPostObj_ObjStore::get_part_str(parts_collection_t& parts,
                                              const std::string& name,
                                              const std::string& def_val)
{
  std::string val;

  if (part_str(parts, name, &val)) {
    return val;
  } else {
    return rgw_trim_whitespace(def_val);
  }
}

bool RGWPostObj_ObjStore::part_bl(parts_collection_t& parts,
                                  const std::string& name,
                                  ceph::bufferlist* pbl)
{
  const auto iter = parts.find(name);
  if (std::end(parts) == iter) {
    return false;
  }

  *pbl = iter->second.data;
  return true;
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

  supplied_md5_b64 = s->info.env->get("HTTP_CONTENT_MD5");

  return 0;
}

int RGWPostObj_ObjStore::get_params(optional_yield y)
{
  if (s->expect_cont) {
    /* OK, here it really gets ugly. With POST, the params are embedded in the
     * request body, so we need to continue before being able to actually look
     * at them. This diverts from the usual request flow. */
    dump_continue(s);
    s->expect_cont = false;
  }

  std::string req_content_type_str = s->info.env->get("CONTENT_TYPE", "");
  std::string req_content_type;
  std::map<std::string, std::string> params;
  parse_boundary_params(req_content_type_str, req_content_type, params);

  if (req_content_type.compare("multipart/form-data") != 0) {
    err_msg = "Request Content-Type is not multipart/form-data";
    return -EINVAL;
  }

  if (s->cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
    ldpp_dout(s, 20) << "request content_type_str="
		      << req_content_type_str << dendl;
    ldpp_dout(s, 20) << "request content_type params:" << dendl;

    for (const auto& pair : params) {
      ldpp_dout(s, 20) << " " << pair.first << " -> " << pair.second
			<< dendl;
    }
  }

  const auto iter = params.find("boundary");
  if (std::end(params) == iter) {
    err_msg = "Missing multipart boundary specification";
    return -EINVAL;
  }

  /* Create the boundary. */
  boundary = "--";
  boundary.append(iter->second);

  return 0;
}


int RGWPutACLs_ObjStore::get_params(optional_yield y)
{
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  std::tie(op_ret, data) = read_all_input(s, max_size, false);
  ldpp_dout(s, 0) << "RGWPutACLs_ObjStore::get_params read data is: " << data.c_str() << dendl;
  return op_ret;
}

int RGWPutLC_ObjStore::get_params(optional_yield y)
{
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  std::tie(op_ret, data) = read_all_input(s, max_size, false);
  return op_ret;
}

int RGWPutBucketObjectLock_ObjStore::get_params(optional_yield y)
{
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  std::tie(op_ret, data) = read_all_input(s, max_size, false);
  return op_ret;
}

int RGWPutObjLegalHold_ObjStore::get_params(optional_yield y)
{
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  std::tie(op_ret, data) = read_all_input(s, max_size, false);
  return op_ret;
}


static std::tuple<int, bufferlist> read_all_chunked_input(req_state *s, const uint64_t max_read)
{
#define READ_CHUNK 4096
#define MAX_READ_CHUNK (128 * 1024)
  int need_to_read = READ_CHUNK;
  int total = need_to_read;
  bufferlist bl;

  int read_len = 0;
  do {
    bufferptr bp(need_to_read + 1);
    read_len = recv_body(s, bp.c_str(), need_to_read);
    if (read_len < 0) {
      return std::make_tuple(read_len, std::move(bl));
    }

    bp.c_str()[read_len] = '\0';
    bp.set_length(read_len);
    bl.append(bp);

    if (read_len == need_to_read) {
      if (need_to_read < MAX_READ_CHUNK)
	need_to_read *= 2;

      if ((unsigned)total > max_read) {
	return std::make_tuple(-ERANGE, std::move(bl));
      }
      total += need_to_read;
    } else {
      break;
    }
  } while (true);

  return std::make_tuple(0, std::move(bl));
}

std::tuple<int, bufferlist > rgw_rest_read_all_input(req_state *s,
                                        const uint64_t max_len,
                                        const bool allow_chunked)
{
  size_t cl = 0;
  int len = 0;
  bufferlist bl;

  if (s->length)
    cl = atoll(s->length);
  else if (!allow_chunked)
    return std::make_tuple(-ERR_LENGTH_REQUIRED, std::move(bl));

  if (cl) {
    if (cl > (size_t)max_len) {
      return std::make_tuple(-ERANGE, std::move(bl));
    }

    bufferptr bp(cl + 1);
  
    len = recv_body(s, bp.c_str(), cl);
    if (len < 0) {
      return std::make_tuple(len, std::move(bl));
    }

    bp.c_str()[len] = '\0';
    bp.set_length(len);
    bl.append(bp);

  } else if (allow_chunked && !s->length) {
    const char *encoding = s->info.env->get("HTTP_TRANSFER_ENCODING");
    if (!encoding || strcmp(encoding, "chunked") != 0)
      return std::make_tuple(-ERR_LENGTH_REQUIRED, std::move(bl));

    int ret = 0;
    std::tie(ret, bl) = read_all_chunked_input(s, max_len);
    if (ret < 0)
      return std::make_tuple(ret, std::move(bl));
  }

  return std::make_tuple(0, std::move(bl));
}

int RGWCompleteMultipart_ObjStore::get_params(optional_yield y)
{
  upload_id = s->info.args.get("uploadId");

  if (upload_id.empty()) {
    op_ret = -ENOTSUP;
    return op_ret;
  }

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  std::tie(op_ret, data) = read_all_input(s, max_size);
  if (op_ret < 0)
    return op_ret;

  return 0;
}

int RGWListMultipart_ObjStore::get_params(optional_yield y)
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
      ldpp_dout(s, 20) << "bad marker: "  << marker << dendl;
      op_ret = -EINVAL;
      return op_ret;
    }
  }
  
  string str = s->info.args.get("max-parts");
  op_ret = parse_value_and_bound(str, max_parts, 0,
			g_conf().get_val<uint64_t>("rgw_max_listing_results"),
			max_parts);

  return op_ret;
}

int RGWListBucketMultiparts_ObjStore::get_params(optional_yield y)
{
  delimiter = s->info.args.get("delimiter");
  prefix = s->info.args.get("prefix");
  string str = s->info.args.get("max-uploads");
  op_ret = parse_value_and_bound(str, max_uploads, 0,
			g_conf().get_val<uint64_t>("rgw_max_listing_results"),
			default_max);
  if (op_ret < 0) {
    return op_ret;
  }

  if (auto encoding_type = s->info.args.get_optional("encoding-type");
      encoding_type != boost::none) {
    if (strcasecmp(encoding_type->c_str(), "url") != 0) {
      op_ret = -EINVAL;
      s->err.message="Invalid Encoding Method specified in Request";
      return op_ret;
    }
    encode_url = true;
  }

  string key_marker = s->info.args.get("key-marker");
  string upload_id_marker = s->info.args.get("upload-id-marker");
  if (!key_marker.empty()) {
    std::unique_ptr<rgw::sal::MultipartUpload> upload;
    upload = s->bucket->get_multipart_upload(key_marker,
					 upload_id_marker);
    marker_meta = upload->get_meta();
    marker_key = upload->get_key();
    marker_upload_id = upload->get_upload_id();
  }

  return 0;
}

int RGWDeleteMultiObj_ObjStore::get_params(optional_yield y)
{

  if (s->bucket_name.empty()) {
    op_ret = -EINVAL;
    return op_ret;
  }

  // everything is probably fine, set the bucket
  bucket = s->bucket.get();

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  std::tie(op_ret, data) = read_all_input(s, max_size, false);
  return op_ret;
}


void RGWRESTOp::send_response()
{
  if (!flusher.did_start()) {
    set_req_state_err(s, get_ret());
    dump_errno(s);
    end_header(s, this);
  }
  flusher.flush();
}

int RGWRESTOp::verify_permission(optional_yield)
{
  return check_caps(s->user->get_info().caps);
}

RGWOp* RGWHandler_REST::get_op(void)
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
    op->init(driver, s, this);
  }
  return op;
} /* get_op */

void RGWHandler_REST::put_op(RGWOp* op)
{
  delete op;
} /* put_op */

int RGWHandler_REST::allocate_formatter(req_state *s,
					RGWFormat default_type,
					bool configurable)
{
  s->format = RGWFormat::BAD_FORMAT; // set to invalid value to allocation happens anyway
  auto type = default_type;
  if (configurable) {
    string format_str = s->info.args.get("format");
    if (format_str.compare("xml") == 0) {
      type = RGWFormat::XML;
    } else if (format_str.compare("json") == 0) {
      type = RGWFormat::JSON;
    } else if (format_str.compare("html") == 0) {
      type = RGWFormat::HTML;
    } else {
      const char *accept = s->info.env->get("HTTP_ACCEPT");
      if (accept) {
        // trim at first ;
        std::string_view format = accept;
        format = format.substr(0, format.find(';'));

        if (format == "text/xml" || format == "application/xml") {
          type = RGWFormat::XML;
        } else if (format == "application/json") {
          type = RGWFormat::JSON;
        } else if (format == "text/html") {
          type = RGWFormat::HTML;
        }
      }
    }
  }
  return RGWHandler_REST::reallocate_formatter(s, type);
}

int RGWHandler_REST::reallocate_formatter(req_state *s, const RGWFormat type)
{
  if (s->format == type) {
    // do nothing, just reset
    ceph_assert(s->formatter);
    s->formatter->reset();
    return 0;
  }

  delete s->formatter;
  s->formatter = nullptr;
  s->format = type;

  const string& mm = s->info.args.get("multipart-manifest");
  const bool multipart_delete = (mm.compare("delete") == 0);
  const bool swift_bulkupload = s->prot_flags & RGW_REST_SWIFT &&
                                s->info.args.exists("extract-archive");
  switch (s->format) {
    case RGWFormat::PLAIN:
      {
        const bool use_kv_syntax = s->info.args.exists("bulk-delete") ||
                                   multipart_delete || swift_bulkupload;
        s->formatter = new RGWFormatter_Plain(use_kv_syntax);
        break;
      }
    case RGWFormat::XML:
      {
        const bool lowercase_underscore = s->info.args.exists("bulk-delete") ||
                                          multipart_delete || swift_bulkupload;

        s->formatter = new XMLFormatter(false, lowercase_underscore);
        break;
      }
    case RGWFormat::JSON:
      s->formatter = new JSONFormatter(false);
      break;
    case RGWFormat::HTML:
      s->formatter = new HTMLFormatter(s->prot_flags & RGW_REST_WEBSITE);
      break;
    default:
      return -EINVAL;

  };
  //s->formatter->reset(); // All formatters should reset on create already

  return 0;
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
  else if (len > MAX_BUCKET_NAME_LEN) {
    // Name too long
    return -ERR_INVALID_BUCKET_NAME;
  }

  const char *s = bucket.c_str();
  for (int i = 0; i < len; ++i, ++s) {
    if (*(unsigned char *)s == 0xff)
      return -ERR_INVALID_BUCKET_NAME;
    if (*(unsigned char *)s == '/')
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
  if (len > MAX_OBJ_NAME_LEN) {
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

int RGWHandler_REST::init_permissions(RGWOp* op, optional_yield y)
{
  if (op->get_type() == RGW_OP_CREATE_BUCKET) {
    // We don't need user policies in case of STS token returned by AssumeRole, hence the check for user type
    if (! s->user->get_id().empty() && s->auth.identity->get_identity_type() != TYPE_ROLE) {
      try {
        if (auto ret = s->user->read_attrs(s, y); ! ret) {
          auto user_policies = get_iam_user_policy_from_attr(s->cct, s->user->get_attrs(), s->user->get_tenant());
          s->iam_user_policies.insert(s->iam_user_policies.end(),
                                      std::make_move_iterator(user_policies.begin()),
                                      std::make_move_iterator(user_policies.end()));

        }
      } catch (const std::exception& e) {
        ldpp_dout(op, -1) << "Error reading IAM User Policy: " << e.what() << dendl;
      }
    }
    rgw_build_iam_environment(driver, s);
    return 0;
  }

  return do_init_permissions(op, y);
}

int RGWHandler_REST::read_permissions(RGWOp* op_obj, optional_yield y)
{
  bool only_bucket = false;

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
    if (!s->info.args.exists("tagging")){
      only_bucket = true;
    }
    break;
  case OP_OPTIONS:
    only_bucket = true;
    break;
  default:
    return -EINVAL;
  }

  return do_read_permissions(op_obj, only_bucket, y);
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

RGWRESTMgr* RGWRESTMgr::get_resource_mgr(req_state* const s,
                                         const std::string& uri,
                                         std::string* const out_uri)
{
  *out_uri = uri;

  multimap<size_t, string>::reverse_iterator iter;

  for (iter = resources_by_size.rbegin(); iter != resources_by_size.rend(); ++iter) {
    string& resource = iter->second;
    if (uri.compare(0, iter->first, resource) == 0 &&
	(uri.size() == iter->first ||
	 uri[iter->first] == '/')) {
      std::string suffix = uri.substr(iter->first);
      return resource_mgrs[resource]->get_resource_mgr(s, suffix, out_uri);
    }
  }

  if (default_mgr) {
    return default_mgr->get_resource_mgr_as_default(s, uri, out_uri);
  }

  return this;
}

void RGWREST::register_x_headers(const string& s_headers)
{
  std::vector<std::string> hdrs = get_str_vec(s_headers);
  for (auto& hdr : hdrs) {
    boost::algorithm::to_upper(hdr); // XXX
    (void) x_headers.insert(hdr);
  }
}

RGWRESTMgr::~RGWRESTMgr()
{
  map<string, RGWRESTMgr *>::iterator iter;
  for (iter = resource_mgrs.begin(); iter != resource_mgrs.end(); ++iter) {
    delete iter->second;
  }
  delete default_mgr;
}

int64_t parse_content_length(const char *content_length)
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

int RGWREST::preprocess(req_state *s, rgw::io::BasicClient* cio)
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
  get_str_list(g_conf()->rgw_enable_apis, apis);
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
  ldpp_dout(s, 10) << "rgw api priority: s3=" << api_priority_s3 << " s3website=" << api_priority_s3website << dendl;
  bool s3website_enabled = api_priority_s3website >= 0;

  if (info.host.size()) {
    ssize_t pos;
    if (info.host.find('[') == 0) {
      pos = info.host.find(']');
      if (pos >=1) {
        info.host = info.host.substr(1, pos-1);
      }
    } else {
      pos = info.host.find(':');
      if (pos >= 0) {
        info.host = info.host.substr(0, pos);
      }
    }
    ldpp_dout(s, 10) << "host=" << info.host << dendl;
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

    ldpp_dout(s, 20)
      << "subdomain=" << subdomain 
      << " domain=" << domain 
      << " in_hosted_domain=" << in_hosted_domain 
      << " in_hosted_domain_s3website=" << in_hosted_domain_s3website 
      << dendl;

    if (g_conf()->rgw_resolve_cname
	&& !in_hosted_domain
	&& !in_hosted_domain_s3website) {
      string cname;
      bool found;
      int r = rgw_resolver->resolve_cname(info.host, cname, &found);
      if (r < 0) {
	ldpp_dout(s, 0)
	  << "WARNING: rgw_resolver->resolve_cname() returned r=" << r
	  << dendl;
      }

      if (found) {
	ldpp_dout(s, 5) << "resolved host cname " << info.host << " -> "
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

        ldpp_dout(s, 20)
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
    // - Don't enable virtual hosting if no hostnames are configured
    if (subdomain.empty()
        && (domain.empty() || domain != info.host)
        && !looks_like_ip_address(info.host.c_str())
        && RGWHandler_REST::validate_bucket_name(info.host) == 0
        && !(hostnames_set.empty() && hostnames_s3website_set.empty())) {
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

    ldpp_dout(s, 20)
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

  s->decoded_uri = url_decode(s->info.request_uri);
  /* Validate for being free of the '\0' buried in the middle of the string. */
  if (std::strlen(s->decoded_uri.c_str()) != s->decoded_uri.length()) {
    return -ERR_ZERO_IN_URL;
  }

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
	ldpp_dout(s, 10) << "bad content length, aborting" << dendl;
	return -EINVAL;
      }
    }
  }

  if (s->content_length < 0) {
    ldpp_dout(s, 10) << "negative content length, aborting" << dendl;
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

  if (g_conf()->rgw_print_continue) {
    const char *expect = info.env->get("HTTP_EXPECT");
    s->expect_cont = (expect && !strcasecmp(expect, "100-continue"));
  }
  s->op = op_from_method(info.method);

  return 0;
}

RGWHandler_REST* RGWREST::get_handler(
  rgw::sal::Driver*  const driver,
  req_state* const s,
  const rgw::auth::StrategyRegistry& auth_registry,
  const std::string& frontend_prefix,
  RGWRestfulIO* const rio,
  RGWRESTMgr** const pmgr,
  int* const init_error
) {
  *init_error = preprocess(s, rio);
  if (*init_error < 0) {
    return nullptr;
  }

  RGWRESTMgr *m = mgr.get_manager(s, frontend_prefix, s->decoded_uri,
                                  &s->relative_uri);
  if (! m) {
    *init_error = -ERR_METHOD_NOT_ALLOWED;
    return nullptr;
  }

  if (pmgr) {
    *pmgr = m;
  }

  RGWHandler_REST* handler = m->get_handler(driver, s, auth_registry, frontend_prefix);
  if (! handler) {
    *init_error = -ERR_METHOD_NOT_ALLOWED;
    return NULL;
  }

  ldpp_dout(s, 20) << __func__ << " handler=" << typeid(*handler).name() << dendl;
  
  *init_error = handler->init(driver, s, rio);
  if (*init_error < 0) {
    m->put_handler(handler);
    return nullptr;
  }

  s->info.init_meta_info(s, &s->has_bad_meta, s->prot_flags);

  return handler;
} /* get stream handler */
