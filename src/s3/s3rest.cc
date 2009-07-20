#include <errno.h>

#include "s3access.h"
#include "s3op.h"
#include "s3rest.h"

#define CGI_PRINTF(stream, format, ...) do { \
   fprintf(stderr, format, __VA_ARGS__); \
   FCGX_FPrintF(stream, format, __VA_ARGS__); \
} while (0)

static void dump_status(struct req_state *s, const char *status)
{
  CGI_PRINTF(s->fcgx->out,"Status: %s\n", status);
}

struct errno_http {
  int err;
  const char *http_str;
  const char *default_code;
};

static struct errno_http hterrs[] = {
    { 0, "200", "" },
    { EINVAL, "400", "InvalidArgument" },
    { EACCES, "403", "AccessDenied" },
    { EPERM, "403", "AccessDenied" },
    { ENOENT, "404", "NoSuchKey" },
    { ETIMEDOUT, "408", "RequestTimeout" },
    { EEXIST, "409", "BucketAlreadyExists" },
    { ENOTEMPTY, "409", "BucketNotEmpty" },
    { ERANGE, "416", "InvalidRange" },
    { 0, NULL }};

void dump_errno(struct req_state *s, int err, struct s3_err *s3err)
{
  const char *err_str;
  const char *code = (s3err ? s3err->code : NULL);

  if (!s3err || !s3err->num) {  
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
    err_str = s3err->num;
  }

  dump_status(s, err_str);
  if (err) {
    s->err_exist = true;
    s->err.code = code;
    s->err.message = (s3err ? s3err->message : NULL);
  }
}

void open_section(struct req_state *s, const char *name)
{
  //CGI_PRINTF(s->fcgx->out, "%*s<%s>\n", s->indent, "", name);
  CGI_PRINTF(s->fcgx->out, "<%s>", name);
  ++s->indent;
}

void close_section(struct req_state *s, const char *name)
{
  --s->indent;
  //CGI_PRINTF(s->fcgx->out, "%*s</%s>\n", s->indent, "", name);
  CGI_PRINTF(s->fcgx->out, "</%s>", name);
}

static void dump_content_length(struct req_state *s, int len)
{
  CGI_PRINTF(s->fcgx->out, "Content-Length: %d\n", len);
}

static void dump_etag(struct req_state *s, const char *etag)
{
  CGI_PRINTF(s->fcgx->out,"ETag: \"%s\"\n", etag);
}

void dump_value(struct req_state *s, const char *name, const char *fmt, ...)
{
#define LARGE_SIZE 8192
  char buf[LARGE_SIZE];
  va_list ap;

  va_start(ap, fmt);
  int n = vsnprintf(buf, LARGE_SIZE, fmt, ap);
  va_end(ap);
  if (n >= LARGE_SIZE)
    return;
  // CGI_PRINTF(s->fcgx->out, "%*s<%s>%s</%s>\n", s->indent, "", name, buf, name);
  CGI_PRINTF(s->fcgx->out, "<%s>%s</%s>", name, buf, name);
}

static void dump_entry(struct req_state *s, const char *val)
{
  // CGI_PRINTF(s->fcgx->out, "%*s<?%s?>\n", s->indent, "", val);
  CGI_PRINTF(s->fcgx->out, "<?%s?>", val);
}


void dump_time(struct req_state *s, const char *name, time_t *t)
{
#define TIME_BUF_SIZE 128
  char buf[TIME_BUF_SIZE];
  struct tm *tmp = localtime(t);
  if (tmp == NULL)
    return;

  if (strftime(buf, sizeof(buf), "%Y-%m-%dT%T.000Z", tmp) == 0)
    return;

  dump_value(s, name, buf); 
}

void dump_owner(struct req_state *s, string& id, string& name)
{
  open_section(s, "Owner");
  dump_value(s, "ID", id.c_str());
  dump_value(s, "DisplayName", name.c_str());
  close_section(s, "Owner");
}

void dump_start_xml(struct req_state *s)
{
  if (!s->content_started) {
    dump_entry(s, "xml version=\"1.0\" encoding=\"UTF-8\"");
    s->content_started = true;
  }
}

void end_header(struct req_state *s, const char *content_type)
{
  if (!content_type)
    content_type = "text/plain";
  CGI_PRINTF(s->fcgx->out,"Content-type: %s\r\n\r\n", content_type);
  if (s->err_exist) {
    dump_start_xml(s);
    struct s3_err &err = s->err;
    open_section(s, "Error");
    if (err.code)
      dump_value(s, "Code", err.code);
    if (err.message)
      dump_value(s, "Message", err.message);
    close_section(s, "Error");
  }
}

void list_all_buckets_start(struct req_state *s)
{
  open_section(s, "ListAllMyBucketsResult xmlns=\"http://doc.s3.amazonaws.com/2006-03-01\"");
}

void list_all_buckets_end(struct req_state *s)
{
  close_section(s, "ListAllMyBucketsResult");
}

void dump_bucket(struct req_state *s, S3ObjEnt& obj)
{
  open_section(s, "Bucket");
  dump_value(s, "Name", obj.name.c_str());
  dump_time(s, "CreationDate", &obj.mtime);
  close_section(s, "Bucket");
}

void abort_early(struct req_state *s, int err)
{
  dump_errno(s, err);
  end_header(s);
}

int S3GetObj_REST::get_params()
{
  range_str = FCGX_GetParam("HTTP_RANGE", s->fcgx->envp);
  if_mod = FCGX_GetParam("HTTP_IF_MODIFIED_SINCE", s->fcgx->envp);
  if_unmod = FCGX_GetParam("HTTP_IF_UNMODIFIED_SINCE", s->fcgx->envp);
  if_match = FCGX_GetParam("HTTP_IF_MATCH", s->fcgx->envp);
  if_nomatch = FCGX_GetParam("HTTP_IF_NONE_MATCH", s->fcgx->envp);

  return 0;
}

int S3GetObj_REST::send_response()
{
  const char *content_type = NULL;

  if (!get_data && !ret) {
    dump_content_length(s, len);
  }
  if (!ret) {
    map<nstring, bufferlist>::iterator iter = attrs.find(S3_ATTR_ETAG);
    if (iter != attrs.end()) {
      bufferlist& bl = iter->second;
      if (bl.length()) {
        char *etag = bl.c_str();
        dump_etag(s, etag);
      }
    }
    for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
       const char *name = iter->first.c_str();
       if (strncmp(name, S3_ATTR_META_PREFIX, sizeof(S3_ATTR_META_PREFIX)-1) == 0) {
         name += sizeof(S3_ATTR_PREFIX) - 1;
         CGI_PRINTF(s->fcgx->out,"%s: %s\r\n", name, iter->second.c_str());
       } else if (!content_type && strcmp(name, S3_ATTR_CONTENT_TYPE) == 0) {
         content_type = iter->second.c_str();
       }
    }
  }
  dump_errno(s, ret, &err);
  end_header(s, content_type);
  if (get_data && !ret) {
    FCGX_PutStr(data, len, s->fcgx->out); 
  }

  return 0;
}

void S3ListBuckets_REST::send_response()
{
  dump_errno(s, ret);
  end_header(s, "application/xml");
  dump_start_xml(s);

  list_all_buckets_start(s);
  dump_owner(s, s->user.user_id, s->user.display_name);

  map<string, S3ObjEnt>& m = buckets.get_buckets();
  map<string, S3ObjEnt>::iterator iter;

  open_section(s, "Buckets");
  for (iter = m.begin(); iter != m.end(); ++iter) {
    S3ObjEnt obj = iter->second;
    dump_bucket(s, obj);
  }
  close_section(s, "Buckets");
  list_all_buckets_end(s);
}

void S3ListBucket_REST::send_response()
{
  dump_errno(s, (ret < 0 ? ret : 0));

  end_header(s, "application/xml");
  dump_start_xml(s);
  if (ret < 0)
    return;

  open_section(s, "ListBucketResult");
  dump_value(s, "Name", s->bucket);
  if (!prefix.empty())
    dump_value(s, "Prefix", prefix.c_str());
  if (!marker.empty())
    dump_value(s, "Marker", marker.c_str());
  if (!max_keys.empty()) {
    dump_value(s, "MaxKeys", max_keys.c_str());
  }
  if (!delimiter.empty())
    dump_value(s, "Delimiter", delimiter.c_str());

  if (ret >= 0) {
    vector<S3ObjEnt>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      open_section(s, "Contents");
      dump_value(s, "Key", iter->name.c_str());
      dump_time(s, "LastModified", &iter->mtime);
      dump_value(s, "ETag", "&quot;%s&quot;", iter->etag);
      dump_value(s, "Size", "%lld", iter->size);
      dump_value(s, "StorageClass", "STANDARD");
      dump_owner(s, s->user.user_id, s->user.display_name);
      close_section(s, "Contents");
    }
    if (common_prefixes.size() > 0) {
      open_section(s, "CommonPrefixes");
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin(); pref_iter != common_prefixes.end(); ++pref_iter) {
        dump_value(s, "Prefix", pref_iter->first.c_str());
      }
      close_section(s, "CommonPrefixes");
    }
  }
  close_section(s, "ListBucketResult");
}

void S3CreateBucket_REST::send_response()
{
  dump_errno(s, ret);
  end_header(s);
}

void S3DeleteBucket_REST::send_response()
{
  dump_errno(s, ret);
  end_header(s);
}


int S3PutObj_REST::get_params()
{
  size_t cl = atoll(s->length);
  if (cl) {
    data = (char *)malloc(cl);
    if (!data)
       return -ENOMEM;

    len = FCGX_GetStr(data, cl, s->fcgx->in);
  }

  supplied_md5_b64 = FCGX_GetParam("HTTP_CONTENT_MD5", s->fcgx->envp);

  return 0;
}

void S3PutObj_REST::send_response()
{
  dump_errno(s, ret, &err);
  end_header(s);
}

void S3DeleteObj_REST::send_response()
{
  dump_errno(s, ret);
  end_header(s);
}

int S3CopyObj_REST::get_params()
{
  if_mod = FCGX_GetParam("HTTP_X_AMZ_COPY_IF_MODIFIED_SINCE", s->fcgx->envp);
  if_unmod = FCGX_GetParam("HTTP_X_AMZ_COPY_IF_UNMODIFIED_SINCE", s->fcgx->envp);
  if_match = FCGX_GetParam("HTTP_X_AMZ_COPY_IF_MATCH", s->fcgx->envp);
  if_nomatch = FCGX_GetParam("HTTP_X_AMZ_COPY_IF_NONE_MATCH", s->fcgx->envp);

  return 0;
}

void S3CopyObj_REST::send_response()
{
  dump_errno(s, ret, &err);

  end_header(s);
  if (ret == 0) {
    open_section(s, "CopyObjectResult");
    dump_time(s, "LastModified", &mtime);
    map<nstring, bufferlist>::iterator iter = attrs.find(S3_ATTR_ETAG);
    if (iter != attrs.end()) {
      bufferlist& bl = iter->second;
      if (bl.length()) {
        char *etag = bl.c_str();
        dump_value(s, "ETag", etag);
      }
    }
    close_section(s, "CopyObjectResult");
  }
}

void S3GetACLs_REST::send_response()
{
  end_header(s, "application/xml");
  dump_start_xml(s);
  FCGX_PutStr(acls.c_str(), acls.size(), s->fcgx->out); 
}

int S3PutACLs_REST::get_params()
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

void S3PutACLs_REST::send_response()
{
  dump_errno(s, ret);
  end_header(s, "application/xml");
  dump_start_xml(s);
}

void init_entities_from_header(struct req_state *s)
{
  s->bucket = NULL;
  s->bucket_str = "";
  s->object = NULL;
  s->object_str = "";

  string h(s->host);

  cerr << "host=" << s->host << std::endl;
  int pos = h.find("s3.");

  if (pos > 0) {
    s->bucket_str = h.substr(0, pos-1);
    s->bucket = s->bucket_str.c_str();
    s->host_bucket = s->bucket;
  } else {
    s->host_bucket = NULL;
  }

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
    return;

  req_name++;

  if (!*req_name)
    return;

  string req(req_name);
  string first;

  pos = req.find('/');
  if (pos >= 0) {
    first = req.substr(0, pos);
  } else {
    first = req;
  }

  if (!s->bucket) {
    s->bucket_str = first;
    s->bucket = s->bucket_str.c_str();
  } else {
    s->object_str = req;
    s->object = s->object_str.c_str();
    return;
  }

  if (pos >= 0) {
    s->object_str = req.substr(pos+1);

    if (s->object_str.size() > 0) {
      s->object = s->object_str.c_str();
    }
  }
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
      cerr << "amz>> " << p << std::endl;
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
    cerr << "x>> " << iter->first << ":" << iter->second << std::endl;
  }
}

void S3Handler_REST::provider_init_state()
{
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
  cerr << "s->object=" << (s->object ? s->object : "<NULL>") << " s->bucket=" << (s->bucket ? s->bucket : "<NULL>") << std::endl;

  init_auth_info(s);

  const char *cacl = FCGX_GetParam("HTTP_X_AMZ_ACL", s->fcgx->envp);
  if (cacl)
    s->canned_acl = cacl;

  s->copy_source = FCGX_GetParam("HTTP_X_AMZ_COPY_SOURCE", s->fcgx->envp);
  s->http_auth = FCGX_GetParam("HTTP_AUTHORIZATION", s->fcgx->envp);
}
