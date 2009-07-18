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
  dump_errno(s, -EPERM);
  end_header(s);
}

int S3GetObj_REST::get_params()
{
  range_str = FCGX_GetParam("HTTP_RANGE", s->fcgx->envp);
  if_mod = FCGX_GetParam("HTTP_IF_MODIFIED_SINCE", s->fcgx->envp);
  if_unmod = FCGX_GetParam("HTTP_IF_UNMODIFIED_SINCE", s->fcgx->envp);
  if_match = FCGX_GetParam("HTTP_IF_MATCH", s->fcgx->envp);
  if_nomatch = FCGX_GetParam("HTTP_IF_NONE_MATCH", s->fcgx->envp);

  int r = init();

  return r;
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


