#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>

#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <openssl/md5.h>

#include "fcgiapp.h"

#include "s3access.h"
#include "s3acl.h"
#include "user.h"

#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>

#include "include/types.h"
#include "include/base64.h"
#include "common/BackTrace.h"

using namespace std;



#define CGI_PRINTF(stream, ...) do { \
   fprintf(stderr, "OUT> " __VA_ARGS__); \
   FCGX_FPrintF(stream, __VA_ARGS__); \
} while (0)

class NameVal
{
   string str;
   string name;
   string val;
 public:
    NameVal(string nv) : str(nv) {}

    int parse();

    string& get_name() { return name; }
    string& get_val() { return val; }
};

int NameVal::parse()
{
  int delim_pos = str.find('=');
  int ret = 0;

  if (delim_pos < 0) {
    name = str;
    val = "";
    ret = 1;
  } else {
    name = str.substr(0, delim_pos);
    val = str.substr(delim_pos + 1);
  }

  cout << "parsed: name=" << name << " val=" << val << std::endl;
  return ret; 
}

class XMLArgs
{
  string str, empty_str;
  map<string, string> val_map;
  string sub_resource;
 public:
   XMLArgs() {}
   XMLArgs(string s) : str(s) {}
   void set(string s) { val_map.clear(); str = s; }
   int parse();
   string& get(string& name);
   string& get(const char *name);
   bool exists(const char *name) {
     map<string, string>::iterator iter = val_map.find(name);
     return (iter != val_map.end());
   }
   string& get_sub_resource() { return sub_resource; }
};

int XMLArgs::parse()
{
  int pos = 0, fpos;
  bool end = false;
  if (str[pos] == '?') pos++;

  while (!end) {
    fpos = str.find('&', pos);
    if (fpos  < pos) {
       end = true;
       fpos = str.size(); 
    }
    NameVal nv(str.substr(pos, fpos - pos));
    int ret = nv.parse();
    if (ret >= 0) {
      val_map[nv.get_name()] = nv.get_val();

      if (ret > 0) { /* this is a sub-resource */
        sub_resource = nv.get_name();
      }
    }

    pos = fpos + 1;  
  }

  return 0;
}

string& XMLArgs::get(string& name)
{
  map<string, string>::iterator iter;
  iter = val_map.find(name);
  if (iter == val_map.end())
    return empty_str;
  return iter->second;
}

string& XMLArgs::get(const char *name)
{
  string s(name);
  return get(s);
}

struct req_state {
   FCGX_ParamArray envp;
   FCGX_Stream *in;
   FCGX_Stream *out;
   bool content_started;
   int indent;
   const char *path_name;
   const char *host;
   const char *method;
   const char *query;
   const char *length;
   bool err_exist;
   struct s3_err err;

   XMLArgs args;

   const char *bucket;
   const char *object;

   const char *host_bucket;

   string bucket_str;
   string object_str;

   map<string, string> x_amz_map;

   S3UserInfo user; 
};

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
    s->object_str = first;
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

  for (int i=0; (p = s->envp[i]); ++i) {
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

static void get_canon_amz_hdr(struct req_state *s, string& dest)
{
  dest = "";
  map<string, string>::iterator iter;
  for (iter = s->x_amz_map.begin(); iter != s->x_amz_map.end(); ++iter) {
    dest.append(iter->first);
    dest.append(":");
    dest.append(iter->second);
    dest.append("\n");
  }
}

static void get_canon_resource(struct req_state *s, string& dest)
{
  if (s->host_bucket) {
    dest = "/";
    dest.append(s->host_bucket);
  }

  dest.append(s->path_name);

  string& sub = s->args.get_sub_resource();
  if (sub.size() > 0) {
    dest.append("?");
    dest.append(sub);
  }
}

static void get_auth_header(struct req_state *s, string& dest)
{
  dest = "";
  if (s->method)
    dest = s->method;
  dest.append("\n");
  
  const char *md5 = FCGX_GetParam("HTTP_CONTENT_MD5", s->envp);
  if (md5)
    dest.append(md5);
  dest.append("\n");

  const char *type = FCGX_GetParam("CONTENT_TYPE", s->envp);
  if (type)
    dest.append(type);
  dest.append("\n");

  const char *date = FCGX_GetParam("HTTP_DATE", s->envp);
  if (date)
    dest.append(date);
  dest.append("\n");

  string canon_amz_hdr;
  get_canon_amz_hdr(s, canon_amz_hdr);
  dest.append(canon_amz_hdr);

  string canon_resource;
  get_canon_resource(s, canon_resource);
  dest.append(canon_resource);
}

static void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  int i;
  str[0] = '\0';
  for (i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

static void calc_hmac_sha1(const char *key, int key_len,
                           const char *msg, int msg_len,
                           char *dest, int *len) /* dest should be large enough to hold result */
{
  char hex_str[128];
  unsigned char *result = HMAC(EVP_sha1(), key, key_len, (const unsigned char *)msg,
                               msg_len, (unsigned char *)dest, (unsigned int *)len);

  buf_to_hex(result, *len, hex_str);

  cerr << "hmac=" << hex_str << std::endl;
}

static void init_state(struct req_state *s, FCGX_ParamArray envp, FCGX_Stream *in, FCGX_Stream *out)
{
  char *p;
  for (int i=0; (p = envp[i]); ++i) {
    cerr << p << std::endl;
  }
  s->envp = envp;
  s->in = in;
  s->out = out;
  s->content_started = false;
  s->indent = 0;
  s->path_name = FCGX_GetParam("SCRIPT_NAME", envp);
  s->method = FCGX_GetParam("REQUEST_METHOD", envp);
  s->host = FCGX_GetParam("HTTP_HOST", envp);
  s->query = FCGX_GetParam("QUERY_STRING", envp);
  s->length = FCGX_GetParam("CONTENT_LENGTH", envp);
  s->err_exist = false;
  memset(&s->err, 0, sizeof(s->err));

  init_entities_from_header(s);
  cerr << "s->object=" << (s->object ? s->object : "<NULL>") << " s->bucket=" << (s->bucket ? s->bucket : "<NULL>") << std::endl;

  init_auth_info(s);
}

static void dump_status(struct req_state *s, const char *status)
{
  CGI_PRINTF(s->out,"Status: %s\n", status);
}

static void dump_content_length(struct req_state *s, int len)
{
  CGI_PRINTF(s->out,"Content-Length: %d\n", len);
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

static void dump_errno(struct req_state *s, int err, struct s3_err *s3err = NULL)
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

static void open_section(struct req_state *s, const char *name)
{
  CGI_PRINTF(s->out, "%*s<%s>\n", s->indent, "", name);
  ++s->indent;
}

static void close_section(struct req_state *s, const char *name)
{
  --s->indent;
  CGI_PRINTF(s->out, "%*s</%s>\n", s->indent, "", name);
}

static void dump_value(struct req_state *s, const char *name, const char *fmt, ...)
{
#define LARGE_SIZE 8192
  char buf[LARGE_SIZE];
  va_list ap;

  va_start(ap, fmt);
  int n = vsnprintf(buf, LARGE_SIZE, fmt, ap);
  va_end(ap);
  if (n >= LARGE_SIZE)
    return;
  CGI_PRINTF(s->out, "%*s<%s>%s</%s>\n", s->indent, "", name, buf, name);
}

static void dump_entry(struct req_state *s, const char *val)
{
  CGI_PRINTF(s->out, "%*s<?%s?>\n", s->indent, "", val);
}


static void dump_time(struct req_state *s, const char *name, time_t *t)
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

static void dump_owner(struct req_state *s, const char *id, int id_size, const char *name)
{
  char id_str[id_size*2 + 1];

  buf_to_hex((const unsigned char *)id, id_size, id_str);

  open_section(s, "Owner");
  dump_value(s, "ID", id_str);
  dump_value(s, "DisplayName", name);
  close_section(s, "Owner");
}

static void dump_start_xml(struct req_state *s)
{
  if (!s->content_started) {
    dump_entry(s, "xml version=\"1.0\" encoding=\"UTF-8\"");
    s->content_started = true;
  }
}

static void end_header(struct req_state *s, const char *content_type = NULL)
{
  if (!content_type)
    content_type = "text/plain";
  CGI_PRINTF(s->out,"Content-type: %s\r\n\r\n", content_type);
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

static void list_all_buckets_start(struct req_state *s)
{
  open_section(s, "ListAllMyBucketsResult");
}

static void list_all_buckets_end(struct req_state *s)
{
  close_section(s, "ListAllMyBucketsResult");
}

static void dump_bucket(struct req_state *s, S3ObjEnt& obj)
{
  open_section(s, "Bucket");
  dump_value(s, "Name", obj.name.c_str());
  dump_time(s, "CreationDate", &obj.mtime);
  close_section(s, "Bucket");
}

static void do_list_buckets(struct req_state *s)
{
  string id = "0123456789ABCDEF";
  S3AccessHandle handle;
  int r;
  S3ObjEnt obj;

  r = s3store->list_buckets_init(id, &handle);
  dump_errno(s, (r < 0 ? r : 0));
  end_header(s);
  dump_start_xml(s);

  list_all_buckets_start(s);
  dump_owner(s, (const char *)id.c_str(), id.size(), "foobi");

  open_section(s, "Buckets");
  while (r >= 0) {
    r = s3store->list_buckets_next(id, obj, &handle);
    if (r < 0)
      continue;
    dump_bucket(s, obj);
  }
  close_section(s, "Buckets");
  list_all_buckets_end(s);
}


static int parse_range(const char *range, off_t ofs, off_t end)
{
  int r = -ERANGE;
  string s(range);
  int pos = s.find("bytes=");
  string ofs_str;
  string end_str;

  if (pos < 0)
    goto done;

  s = s.substr(pos + 6); /* size of("bytes=")  */
  pos = s.find('-');
  if (pos < 0)
    goto done;

  ofs_str = s.substr(0, pos);
  end_str = s.substr(pos + 1);
  ofs = atoll(ofs_str.c_str());
  end = atoll(end_str.c_str());

  if (end < ofs)
    goto done;

  r = 0;
done:
  return r;
}

int parse_time(const char *time_str, time_t *time)
{
  struct tm tm;
  memset(&tm, 0, sizeof(struct tm));
  if (!strptime(time_str, "%FT%T%z", &tm))
    return -EINVAL;

  *time = mktime(&tm);

  return 0;
}

static bool verify_signature(struct req_state *s)
{
  const char *http_auth = FCGX_GetParam("HTTP_AUTHORIZATION", s->envp);
  if (strncmp(http_auth, "AWS ", 4))
    return false;
  string auth_str(http_auth + 4);
  int pos = auth_str.find(':');
  if (pos < 0)
    return false;

  string auth_id = auth_str.substr(0, pos);
  string auth_sign = auth_str.substr(pos + 1);

  /* first get the user info */
  if (s3_get_user_info(auth_id, s->user) < 0) {
    cerr << "error reading user info, uid=" << auth_id << " can't authenticate" << std::endl;
    dump_errno(s, -EPERM);
    end_header(s);
    dump_start_xml(s);
    return false;
  }

  /* now verify signature */
   
  string auth_hdr;
  get_auth_header(s, auth_hdr);
  cerr << "auth_hdr:" << std::endl << auth_hdr << std::endl;

  const char *key = s->user.secret_key.c_str();
  int key_len = strlen(key);

  char hmac_sha1[EVP_MAX_MD_SIZE];
  int len;
  calc_hmac_sha1(key, key_len, auth_hdr.c_str(), auth_hdr.size(), hmac_sha1, &len);

  char b64[64]; /* 64 is really enough */
  int ret = encode_base64(hmac_sha1, len, b64, sizeof(b64));
  if (ret < 0) {
    cerr << "encode_base64 failed" << std::endl;
    return false;
  }

  cerr << "b64=" << b64 << std::endl;
  cerr << "auth_sign=" << b64 << std::endl;
  cerr << "compare=" << auth_sign.compare(b64) << std::endl;
  return (auth_sign.compare(b64) == 0);
}

static void get_object(struct req_state *s, string& bucket, string& obj, bool get_data)
{
  struct s3_err err;
  const char *range_str = FCGX_GetParam("HTTP_RANGE", s->envp);
  const char *if_mod = FCGX_GetParam("HTTP_IF_MODIFIED_SINCE", s->envp);
  const char *if_unmod = FCGX_GetParam("HTTP_IF_UNMODIFIED_SINCE", s->envp);
  const char *if_match = FCGX_GetParam("HTTP_IF_MATCH", s->envp);
  const char *if_nomatch = FCGX_GetParam("HTTP_IF_NONE_MATCH", s->envp);
  time_t mod_time;
  time_t unmod_time;
  time_t *mod_ptr = NULL;
  time_t *unmod_ptr = NULL;
  int r = -EINVAL;
  off_t ofs = 0, end = -1, len = 0;
  char *data;

  if (range_str) {
    r = parse_range(range_str, ofs, end);
    if (r < 0)
      goto done;
  }
  if (if_mod) {
    if (parse_time(if_mod, &mod_time) < 0)
      goto done;
    mod_ptr = &mod_time;
  }

  if (if_unmod) {
    if (parse_time(if_unmod, &unmod_time) < 0)
      goto done;
    unmod_ptr = &unmod_time;
  }

  r = 0;
  len = s3store->get_obj(bucket, obj, &data, ofs, end, mod_ptr, unmod_ptr, if_match, if_nomatch, get_data, &err);
  if (len < 0)
    r = len;

done:
  if (!get_data && !r) {
    dump_content_length(s, len);
  }
  dump_errno(s, r, &err);
  end_header(s);
  if (get_data && !r) {
    FCGX_PutStr(data, len, s->out); 
  }
}

static void get_acls(struct req_state *s)
{
  S3AccessControlPolicy policy;
  bufferlist bl;
  int ret = s3store->get_attr(s->bucket_str, s->object_str,
                       "user.s3acl", bl);

  if (ret < 0) {
    /* should we do that, or should we just abort? */
    // policy.create_default(s->user.user_id, s->user.display_name);
  } else {
    bufferlist::iterator iter = bl.begin();
    policy.decode(iter);
  }

  stringstream ss;
  policy.to_xml(ss);
  string str = ss.str(); 
  end_header(s, "application/xml");
  dump_start_xml(s);
  FCGX_PutStr(str.c_str(), str.size(), s->out); 
}

static void set_acls(struct req_state *s)
{
  S3AccessControlPolicy def;
  def.create_default(s->user.user_id, s->user.display_name);

  stringstream ss;
  def.to_xml(ss);
  string str = ss.str(); 
  end_header(s, "application/xml");
  dump_start_xml(s);
  FCGX_PutStr(str.c_str(), str.size(), s->out); 
}

static void do_retrieve_objects(struct req_state *s, bool get_data)
{
  string prefix, marker, max_keys, delimiter;
  int max;
  string id = "0123456789ABCDEF";

  if (s->args.exists("acl")) {
    get_acls(s);
    return;
  }

  if (s->object) {
    get_object(s, s->bucket_str, s->object_str, get_data);
    return;
  } else if (!s->bucket) {
    return;
  }

  prefix = s->args.get("prefix");
  marker = s->args.get("marker");
  max_keys = s->args.get("max-keys");
 if (!max_keys.empty()) {
    max = atoi(max_keys.c_str());
  } else {
    max = -1;
  }
  delimiter = s->args.get("delimiter");

  vector<S3ObjEnt> objs;
  int r = s3store->list_objects(id, s->bucket_str, max, prefix, marker, objs);
  dump_errno(s, (r < 0 ? r : 0));

  end_header(s);
  dump_start_xml(s);
  if (r < 0)
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

  if (r >= 0) {
    vector<S3ObjEnt>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      open_section(s, "Contents");
      dump_value(s, "Key", iter->name.c_str());
      dump_time(s, "LastModified", &iter->mtime);
      dump_value(s, "ETag", "&quot;828ef3fdfa96f00ad9f27c383fc9ac7f&quot;");
      dump_value(s, "Size", "%lld", iter->size);
      dump_value(s, "StorageClass", "STANDARD");
      dump_owner(s, (const char *)&id, sizeof(id), "foobi");
      close_section(s, "Contents");
    }
  }
  close_section(s, "ListBucketResult");
  //if (args.get("name"))
}

static void do_create_bucket(struct req_state *s)
{
  string id = "0123456789ABCDEF";

  int r = s3store->create_bucket(id, s->bucket_str);

  dump_errno(s, r);
  end_header(s);
}

static void do_create_object(struct req_state *s)
{
  int r = -EINVAL;
  char *data = NULL;
  struct s3_err err;
  if (!s->object) {
    goto done;
  } else {
    string id = "0123456789ABCDEF";

    size_t cl = atoll(s->length);
    size_t actual = 0;
    if (cl) {
      data = (char *)malloc(cl);
      if (!data) {
         r = -ENOMEM;
         goto done;
      }
      actual = FCGX_GetStr(data, cl, s->in);
    }

    char *supplied_md5 = FCGX_GetParam("HTTP_CONTENT_MD5", s->envp);
    char calc_md5[MD5_DIGEST_LENGTH * 2];
    MD5_CTX c;
    unsigned char m[MD5_DIGEST_LENGTH];

    if (supplied_md5 && strlen(supplied_md5) != MD5_DIGEST_LENGTH*2) {
      err.code = "InvalidDigest";
      r = -EINVAL;
      goto done;
    }

    MD5_Init(&c);
    MD5_Update(&c, data, (unsigned long)actual);
    MD5_Final(m, &c);

    buf_to_hex(m, MD5_DIGEST_LENGTH, calc_md5);

    if (supplied_md5 && strcmp(calc_md5, supplied_md5)) {
       err.code = "BadDigest";
       r = -EINVAL;
       goto done;
    }
    S3AccessControlPolicy def;
    def.create_default(s->user.user_id, s->user.display_name);
    bufferlist aclbl;
    def.encode(aclbl);

    string md5_str(calc_md5);
    vector<pair<string, bufferlist> > attrs;
    bufferlist bl;
    bl.append(md5_str.c_str(), md5_str.size());
    attrs.push_back(pair<string, bufferlist>("user.etag", bl));
    attrs.push_back(pair<string, bufferlist>("user.s3acl", aclbl));
    r = s3store->put_obj(id, s->bucket_str, s->object_str, data, actual, attrs);
  }
done:
  free(data);
  dump_errno(s, r, &err);

  end_header(s);
}

static void do_delete_bucket(struct req_state *s)
{
  int r = -EINVAL;
  string id = "0123456789ABCDEF";

  if (s->bucket) {
    r = s3store->delete_bucket(id, s->bucket_str);
  }

  dump_errno(s, r);
  end_header(s);
}

static void do_delete_object(struct req_state *s)
{
  int r = -EINVAL;
  if (s->object) {
    string id = "0123456789ABCDEF";

    r = s3store->delete_obj(id, s->bucket_str, s->object_str);
  }

  dump_errno(s, r);
  end_header(s);
}

static void do_retrieve(struct req_state *s, bool get_data)
{
  if (s->bucket)
    do_retrieve_objects(s, get_data);
  else
    do_list_buckets(s);
}

static void do_create(struct req_state *s)
{
  if (s->object)
    do_create_object(s);
  else if (s->bucket)
    do_create_bucket(s);
  else
    return;
}

static void do_delete(struct req_state *s)
{
  if (s->object)
    do_delete_object(s);
  else if (s->bucket)
    do_delete_bucket(s);
  else
    return;
}

static sighandler_t sighandler;

static void godown(int signum)
{
  BackTrace bt(0);
  bt.print(cerr);

  signal(SIGSEGV, sighandler);
}


int main(void)
{
  FCGX_Stream *in, *out, *err;
  FCGX_ParamArray envp;
  struct req_state s;

  if (!S3Access::init_storage_provider("fs")) {
    cerr << "couldn't init storage provider" << std::endl;
  }

  sighandler = signal(SIGSEGV, godown);

  while (FCGX_Accept(&in, &out, &err, &envp) >= 0) 
  {
    init_state(&s, envp, in, out);

    bool ret = verify_signature(&s);
    if (!ret) {
      cerr << "signature DOESN'T match" << std::endl;
      continue;
    }

    if (!s.method)
      continue;

    if (strcmp(s.method, "GET") == 0)
      do_retrieve(&s, true);
    else if (strcmp(s.method, "PUT") == 0)
      do_create(&s);
    else if (strcmp(s.method, "DELETE") == 0)
      do_delete(&s);
    else if (strcmp(s.method, "HEAD") == 0)
      do_retrieve(&s, false);
  }
  return 0;
}

