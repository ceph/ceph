#include <errno.h>

#include "common/utf8.h"
#include "rgw_common.h"
#include "rgw_access.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_os.h"
#include "rgw_rest_s3.h"
#include "rgw_os_auth.h"

#include "rgw_formats.h"



static RGWHandler_REST_S3 rgwhandler_s3;
static RGWHandler_REST_OS rgwhandler_os;
static RGWHandler_OS_Auth rgwhandler_os_auth;

static RGWFormatter_Plain formatter_plain;
static RGWFormatter_XML formatter_xml;
static RGWFormatter_JSON formatter_json;


static void dump_status(struct req_state *s, const char *status)
{
  s->status = status;
  CGI_PRINTF(s,"Status: %s\n", status);
}

struct errno_http {
  int err;
  const char *http_str;
  const char *default_code;
};

const static struct errno_http hterrs[] = {
    { 0, "200", "" },
    { 201, "201", "Created" },
    { 204, "204", "NoContent" },
    { 206, "206", "" },
    { EINVAL, "400", "InvalidArgument" },
    { INVALID_BUCKET_NAME, "400", "InvalidBucketName" },
    { INVALID_OBJECT_NAME, "400", "InvalidObjectName" },
    { EACCES, "403", "AccessDenied" },
    { EPERM, "403", "AccessDenied" },
    { ENOENT, "404", "NoSuchKey" },
    { NO_SUCH_BUCKET, "404", "NoSuchBucket" },
    { ETIMEDOUT, "408", "RequestTimeout" },
    { EEXIST, "409", "BucketAlreadyExists" },
    { ENOTEMPTY, "409", "BucketNotEmpty" },
    { ERANGE, "416", "InvalidRange" },
    { 0, NULL, NULL }};

void dump_errno(struct req_state *s, int err, struct rgw_err *rgwerr)
{
  int orig_err = err;
  const char *err_str;
  const char *code = (rgwerr ? rgwerr->code : NULL);

  if (!rgwerr || !rgwerr->num) {  
    err_str = "500";

    if (err < 0)
      err = -err;

    int i=0;
    while (hterrs[i].http_str) {
      if (err == hterrs[i].err) {
        err_str = hterrs[i].http_str;
        if (!code)
          code = hterrs[i].default_code;
        break;
      }

      i++;
    }
  } else {
    err_str = rgwerr->num;
  }

  dump_status(s, err_str);
  if (orig_err < 0) {
    s->err_exist = true;
    s->err.code = code;
    s->err.message = (rgwerr ? rgwerr->message : NULL);
  }
}

void dump_content_length(struct req_state *s, size_t len)
{
  char buf[16];
  snprintf(buf, sizeof(buf), "%lu", (long unsigned int)len);
  CGI_PRINTF(s, "Content-Length: %s\n", buf);
  CGI_PRINTF(s, "Accept-Ranges: %s\n", "bytes");
}

void dump_etag(struct req_state *s, const char *etag)
{
  if (s->prot_flags & RGW_REST_OPENSTACK)
    CGI_PRINTF(s,"etag: %s\n", etag);
  else
    CGI_PRINTF(s,"ETag: \"%s\"\n", etag);
}

void dump_last_modified(struct req_state *s, time_t t)
{

  char timestr[TIME_BUF_SIZE];
  struct tm *tmp = gmtime(&t);
  if (tmp == NULL)
    return;

  if (strftime(timestr, sizeof(timestr), "%a, %d %b %Y %H:%M:%S %Z", tmp) == 0)
    return;

  CGI_PRINTF(s, "Last-Modified: %s\n", timestr);
}

static void dump_entry(struct req_state *s, const char *val)
{
  s->formatter->write_data("<?%s?>", val);
}


void dump_time(struct req_state *s, const char *name, time_t *t)
{
  char buf[TIME_BUF_SIZE];
  struct tm *tmp = localtime(t);
  if (tmp == NULL)
    return;

  if (strftime(buf, sizeof(buf), "%Y-%m-%dT%T.000Z", tmp) == 0)
    return;

  s->formatter->dump_value_str(name, buf); 
}

void dump_owner(struct req_state *s, string& id, string& name)
{
  s->formatter->open_obj_section("Owner");
  s->formatter->dump_value_str("ID", id.c_str());
  s->formatter->dump_value_str("DisplayName", name.c_str());
  s->formatter->close_section("Owner");
}

void dump_start(struct req_state *s)
{
  if (!s->content_started) {
    if (s->format == RGW_FORMAT_XML)
      dump_entry(s, "xml version=\"1.0\" encoding=\"UTF-8\"");
    s->content_started = true;
  }
}

void end_header(struct req_state *s, const char *content_type)
{
  if (!content_type) {
    switch (s->format) {
    case RGW_FORMAT_XML:
      content_type = "application/xml";
      break;
    case RGW_FORMAT_JSON:
      content_type = "application/json";
      break;
    default:
      content_type = "text/plain";
      break;
    }
  }
  CGI_PRINTF(s,"Content-type: %s\r\n\r\n", content_type);
  if (s->err_exist) {
    dump_start(s);
    struct rgw_err &err = s->err;
    s->formatter->open_obj_section("Error");
    if (err.code)
      s->formatter->dump_value_int("Code", "%s", err.code);
    if (err.message)
      s->formatter->dump_value_str("Message", err.message);
    s->formatter->close_section("Error");
  }
  s->header_ended = true;
}

void abort_early(struct req_state *s, int err)
{
  dump_errno(s, err);
  end_header(s);
  s->formatter->flush();
}

void dump_continue(struct req_state *s)
{
  dump_status(s, "100");
  FCGX_FFlush(s->fcgx->out);
}

void dump_range(struct req_state *s, off_t ofs, off_t end)
{
    CGI_PRINTF(s,"Content-Range: bytes %d-%d/%d\n", (int)ofs, (int)end, (int)end + 1);
}

int RGWGetObj_REST::get_params()
{
  range_str = FCGX_GetParam("HTTP_RANGE", s->fcgx->envp);
  if_mod = FCGX_GetParam("HTTP_IF_MODIFIED_SINCE", s->fcgx->envp);
  if_unmod = FCGX_GetParam("HTTP_IF_UNMODIFIED_SINCE", s->fcgx->envp);
  if_match = FCGX_GetParam("HTTP_IF_MATCH", s->fcgx->envp);
  if_nomatch = FCGX_GetParam("HTTP_IF_NONE_MATCH", s->fcgx->envp);

  return 0;
}


int RGWPutObj_REST::get_params()
{
  supplied_md5_b64 = FCGX_GetParam("HTTP_CONTENT_MD5", s->fcgx->envp);

  return 0;
}

int RGWPutObj_REST::get_data()
{
  size_t cl;
  if (s->length) {
    cl = atoll(s->length) - ofs;
    if (cl > RGW_MAX_CHUNK_SIZE)
      cl = RGW_MAX_CHUNK_SIZE;
  } else {
    cl = RGW_MAX_CHUNK_SIZE;
  }

  len = 0;
  if (cl) {
    data = (char *)malloc(cl);
    if (!data)
       return -ENOMEM;

    len = FCGX_GetStr(data, cl, s->fcgx->in);
  }

  if (!ofs)
    supplied_md5_b64 = FCGX_GetParam("HTTP_CONTENT_MD5", s->fcgx->envp);

  return 0;
}

int RGWCopyObj_REST::get_params()
{
  if_mod = FCGX_GetParam("HTTP_X_AMZ_COPY_IF_MODIFIED_SINCE", s->fcgx->envp);
  if_unmod = FCGX_GetParam("HTTP_X_AMZ_COPY_IF_UNMODIFIED_SINCE", s->fcgx->envp);
  if_match = FCGX_GetParam("HTTP_X_AMZ_COPY_IF_MATCH", s->fcgx->envp);
  if_nomatch = FCGX_GetParam("HTTP_X_AMZ_COPY_IF_NONE_MATCH", s->fcgx->envp);

  return 0;
}

int RGWPutACLs_REST::get_params()
{
  size_t cl = atoll(s->length);
  if (cl) {
    data = (char *)malloc(cl + 1);
    if (!data) {
       ret = -ENOMEM;
       return ret;
    }
    len = FCGX_GetStr(data, cl, s->fcgx->in);
    data[len] = '\0';
  } else {
    len = 0;
  }

  return ret;
}

static void next_tok(string& str, string& tok, char delim)
{
  if (str.size() == 0) {
    tok = "";
    return;
  }
  tok = str;
  int pos = str.find(delim);
  if (pos > 0) {
    tok = str.substr(0, pos);
    str = str.substr(pos + 1);
  } else {
    str = "";
  }
}

void init_entities_from_header(struct req_state *s)
{
  const char *gateway_dns_name;
  string req;
  string first;

  gateway_dns_name = FCGX_GetParam("RGW_DNS_NAME", s->fcgx->envp);
  if (!gateway_dns_name)
    gateway_dns_name = "s3.";

  RGW_LOG(20) << "gateway_dns_name = " << gateway_dns_name << endl;

  s->bucket = NULL;
  s->bucket_str = "";
  s->object = NULL;
  s->object_str = "";

  s->status = NULL;
  s->err_exist = false;
  s->header_ended = false;
  s->bytes_sent = 0;

  /* this is the default, might change in a few lines */
  s->format = RGW_FORMAT_XML;
  s->formatter = &formatter_xml;

  int pos;
  if (s->host) {
    string h(s->host);

    RGW_LOG(10) << "host=" << s->host << endl;
    pos = h.find(gateway_dns_name);

    if (pos > 0 && h[pos - 1] == '.') {
      string encoded_bucket = h.substr(0, pos-1);
      url_decode(encoded_bucket, s->bucket_str);
      s->bucket = s->bucket_str.c_str();
      s->host_bucket = s->bucket;
    } else {
      s->host_bucket = NULL;
    }
  } else s->host_bucket = NULL;

  const char *req_name = s->path_name;
  const char *p;

  if (*req_name == '?') {
    p = req_name;
  } else {
    p = s->query;
  }

  s->args.set(p);
  s->args.parse();

  if (*req_name != '/')
    goto done;

  req_name++;

  if (!*req_name)
    goto done;

  req = req_name;

  pos = req.find('/');
  if (pos >= 0) {
    const char *openstack_url_prefix = FCGX_GetParam("RGW_OPENSTACK_URL_PREFIX", s->fcgx->envp);
    bool cut_url = (openstack_url_prefix != NULL);
    if (!openstack_url_prefix)
      openstack_url_prefix = "v1";
    first = req.substr(0, pos);
    if (first.compare(openstack_url_prefix) == 0) {
      s->prot_flags |= RGW_REST_OPENSTACK;
      if (cut_url) {
        next_tok(req, first, '/');
      }
    }
  } else {
    first = req;
  }

  if (s->prot_flags & RGW_REST_OPENSTACK) {
    s->format = 0;
    s->formatter = &formatter_plain;
    string format_str = s->args.get("format");
    if (format_str.compare("xml") == 0) {
      s->format = RGW_FORMAT_XML;
      s->formatter = &formatter_xml;
    } else if (format_str.compare("json") == 0) {
      s->format = RGW_FORMAT_JSON;
      s->formatter = &formatter_json;
    }
  }

  RGW_LOG(0) << "s->formatter=" << (void *)s->formatter << std::endl;

  if (s->prot_flags & RGW_REST_OPENSTACK) {
    string ver;
    string auth_key;

    RGW_LOG(10) << "before2" << std::endl;
    next_tok(req, ver, '/');
    RGW_LOG(10) << "ver=" << ver << std::endl;
    next_tok(req, auth_key, '/');
    RGW_LOG(10) << "auth_key=" << auth_key << std::endl;
    s->os_auth_token = FCGX_GetParam("HTTP_X_AUTH_TOKEN", s->fcgx->envp);
    next_tok(req, first, '/');

    RGW_LOG(10) << "ver=" << ver << " auth_key=" << auth_key << " first=" << first << " req=" << req << std::endl;
    if (first.size() == 0)
      goto done;

    url_decode(first, s->bucket_str);
    s->bucket = s->bucket_str.c_str();
   
    if (req.size()) {
      url_decode(req, s->object_str);
      s->object = s->object_str.c_str();
    }

    goto done;
  }

  if (!s->bucket) {
    url_decode(first, s->bucket_str);
    s->bucket = s->bucket_str.c_str();
  } else {
    url_decode(req, s->object_str);
    s->object = s->object_str.c_str();
    goto done;
  }

  if (strcmp(s->bucket, "auth") == 0)
    s->prot_flags |= RGW_REST_OPENSTACK_AUTH;

  if (pos >= 0) {
    string encoded_obj_str = req.substr(pos+1);
    url_decode(encoded_obj_str, s->object_str);

    if (s->object_str.size() > 0) {
      s->object = s->object_str.c_str();
    }
  }
done:
  s->formatter->init(s);
}

static void line_unfold(const char *line, string& sdest)
{
  char dest[strlen(line) + 1];
  const char *p = line;
  char *d = dest;

  while (isspace(*p))
    ++p;

  bool last_space = false;

  while (*p) {
    switch (*p) {
    case '\n':
    case '\r':
      *d = ' ';
      if (!last_space)
        ++d;
      last_space = true;
      break;
    default:
      *d = *p;
      ++d;
      last_space = false;
      break;
    }
    ++p;
  }
  *d = 0;
  sdest = dest;
}

static void init_auth_info(struct req_state *s)
{
  const char *p;

  s->x_amz_map.clear();

  for (int i=0; (p = s->fcgx->envp[i]); ++i) {
#define HTTP_X_AMZ "HTTP_X_AMZ"
    if (strncmp(p, HTTP_X_AMZ, sizeof(HTTP_X_AMZ) - 1) == 0) {
      RGW_LOG(10) << "amz>> " << p << endl;
      const char *amz = p+5; /* skip the HTTP_ part */
      const char *eq = strchr(amz, '=');
      if (!eq) /* shouldn't happen! */
        continue;
      int len = eq - amz;
      char amz_low[len + 1];
      int j;
      for (j=0; j<len; j++) {
        amz_low[j] = tolower(amz[j]);
        if (amz_low[j] == '_')
          amz_low[j] = '-';
      }
      amz_low[j] = 0;
      string val;
      line_unfold(eq + 1, val);

      map<string, string>::iterator iter;
      iter = s->x_amz_map.find(amz_low);
      if (iter != s->x_amz_map.end()) {
        string old = iter->second;
        int pos = old.find_last_not_of(" \t"); /* get rid of any whitespaces after the value */
        old = old.substr(0, pos + 1);
        old.append(",");
        old.append(val);
        s->x_amz_map[amz_low] = old;
      } else {
        s->x_amz_map[amz_low] = val;
      }
    }
  }
  map<string, string>::iterator iter;
  for (iter = s->x_amz_map.begin(); iter != s->x_amz_map.end(); ++iter) {
    RGW_LOG(10) << "x>> " << iter->first << ":" << iter->second << endl;
  }
}

static bool looks_like_ip_address(const char *bucket)
{
  int num_periods = 0;
  bool expect_period = false;
  for (const char *b = bucket; *b; ++b) {
    if (*b == '.') {
      if (!expect_period)
	return false;
      ++num_periods;
      if (num_periods > 3)
	return false;
      expect_period = false;
    }
    else if (isdigit(*b)) {
      expect_period = true;
    }
    else {
      return false;
    }
  }
  return (num_periods == 3);
}

// This function enforces Amazon's spec for bucket names.
// (The requirements, not the recommendations.)
static int validate_bucket_name(const char *bucket)
{
  int len = strlen(bucket);
  if (len < 3) {
    if (len == 0) {
      // This request doesn't specify a bucket at all
      return 0;
    }
    // Name too short
    return INVALID_BUCKET_NAME;
  }
  else if (len > 255) {
    // Name too long
    return INVALID_BUCKET_NAME;
  }

  if (!(isalpha(bucket[0]) || isdigit(bucket[0]))) {
    // bucket names must start with a number or letter
    return INVALID_BUCKET_NAME;
  }

  for (const char *s = bucket; *s; ++s) {
    char c = *s;
    if (isdigit(c) || (c == '.'))
      continue;
    if (isalpha(c))
      continue;
    if ((c == '-') || (c == '_'))
      continue;
    // Invalid character
    return INVALID_BUCKET_NAME;
  }

  if (looks_like_ip_address(bucket))
    return INVALID_BUCKET_NAME;
  return 0;
}

// "The name for a key is a sequence of Unicode characters whose UTF-8 encoding
// is at most 1024 bytes long."
// However, we can still have control characters and other nasties in there.
// Just as long as they're utf-8 nasties.
static int validate_object_name(const char *object)
{
  int len = strlen(object);
  if (len > 1024) {
    // Name too long
    return INVALID_OBJECT_NAME;
  }

  if (check_utf8(object, len)) {
    // Object names must be valid UTF-8.
    return INVALID_OBJECT_NAME;
  }
  return 0;
}

int RGWHandler_REST::init_rest(struct req_state *s, struct fcgx_state *fcgx)
{
  int ret = 0;
  RGWHandler::init_state(s, fcgx);

  s->path_name = FCGX_GetParam("SCRIPT_NAME", s->fcgx->envp);
  s->path_name_url = FCGX_GetParam("REQUEST_URI", s->fcgx->envp);
  int pos = s->path_name_url.find('?');
  if (pos >= 0)
    s->path_name_url = s->path_name_url.substr(0, pos);
  s->method = FCGX_GetParam("REQUEST_METHOD", s->fcgx->envp);
  s->host = FCGX_GetParam("HTTP_HOST", s->fcgx->envp);
  s->query = FCGX_GetParam("QUERY_STRING", s->fcgx->envp);
  s->length = FCGX_GetParam("CONTENT_LENGTH", s->fcgx->envp);
  s->content_type = FCGX_GetParam("CONTENT_TYPE", s->fcgx->envp);
  s->prot_flags = 0;

  if (!s->method)
    s->op = OP_UNKNOWN;
  else if (strcmp(s->method, "GET") == 0)
    s->op = OP_GET;
  else if (strcmp(s->method, "PUT") == 0)
    s->op = OP_PUT;
  else if (strcmp(s->method, "DELETE") == 0)
    s->op = OP_DELETE;
  else if (strcmp(s->method, "HEAD") == 0)
    s->op = OP_HEAD;
  else
    s->op = OP_UNKNOWN;

  init_entities_from_header(s);
  ret = validate_bucket_name(s->bucket_str.c_str());
  if (ret)
    return ret;
  ret = validate_object_name(s->object_str.c_str());
  if (ret)
    return ret;
  RGW_LOG(10) << "s->object=" << (s->object ? s->object : "<NULL>") << " s->bucket=" << (s->bucket ? s->bucket : "<NULL>") << endl;

  init_auth_info(s);

  const char *cacl = FCGX_GetParam("HTTP_X_AMZ_ACL", s->fcgx->envp);
  if (cacl)
    s->canned_acl = cacl;

  s->copy_source = FCGX_GetParam("HTTP_X_AMZ_COPY_SOURCE", s->fcgx->envp);
  s->http_auth = FCGX_GetParam("HTTP_AUTHORIZATION", s->fcgx->envp);

  const char *cgi_env_continue = FCGX_GetParam("RGW_PRINT_CONTINUE", s->fcgx->envp);
  if (rgw_str_to_bool(cgi_env_continue, 0)) {
    const char *expect = FCGX_GetParam("HTTP_EXPECT", s->fcgx->envp);
    s->expect_cont = (expect && !strcasecmp(expect, "100-continue"));
  }
  return ret;
}

int RGWHandler_REST::read_permissions()
{
  bool only_bucket;

  switch (s->op) {
  case OP_HEAD:
  case OP_GET:
    only_bucket = false;
    break;
  case OP_PUT:
    /* is it a 'create bucket' request? */
    if (s->object_str.size() == 0)
      return 0;
    if (is_acl_op(s)) {
      only_bucket = false;
      break;
    }
  case OP_DELETE:
    only_bucket = true;
    break;
  default:
    return -EINVAL;
  }

  return do_read_permissions(only_bucket);
}

RGWOp *RGWHandler_REST::get_op()
{
  RGWOp *op;
  switch (s->op) {
   case OP_GET:
     op = get_retrieve_op(s, true);
     break;
   case OP_PUT:
     op = get_create_op(s);
     break;
   case OP_DELETE:
     op = get_delete_op(s);
     break;
   case OP_HEAD:
     op = get_retrieve_op(s, false);
     break;
   default:
     return NULL;
  }

  if (op) {
    op->init(s);
  }
  return op;
}


RGWHandler *RGWHandler_REST::init_handler(struct req_state *s, struct fcgx_state *fcgx,
					  int *init_error)
{
  RGWHandler *handler;

  *init_error = init_rest(s, fcgx);

  if (s->prot_flags & RGW_REST_OPENSTACK)
    handler = &rgwhandler_os;
  else if (s->prot_flags & RGW_REST_OPENSTACK_AUTH)
    handler = &rgwhandler_os_auth;
  else
    handler = &rgwhandler_s3;

  handler->set_state(s);

  return handler;
}

