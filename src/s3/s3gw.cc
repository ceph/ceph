#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>

#include <openssl/md5.h>

#include "fcgiapp.h"

#include "s3access.h"
#include "s3acl.h"

#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>

#include "include/types.h"
#include "common/BackTrace.h"

using namespace std;


#define CGI_PRINTF(stream, ...) do { \
   fprintf(stderr, "OUT> " __VA_ARGS__); \
   FCGX_FPrintF(stream, __VA_ARGS__); \
} while (0)

static FILE *dbg;

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

  if (delim_pos < 0) {
    name = str;
    val = "";
  } else {
    name = str.substr(0, delim_pos);
    val = str.substr(delim_pos + 1);
  }

  cout << "parsed: name=" << name << " val=" << val << std::endl;
  return 0; 
}

class XMLArgs
{
  string str, empty_str;
  map<string, string> val_map;
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
    if (nv.parse() >= 0) {
      val_map[nv.get_name()] = nv.get_val();
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

   string bucket_str;
   string object_str;
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

  if (pos < 0)
    return;

  s->object_str = req.substr(pos+1);

  if (s->object_str.size() > 0) {
    s->object = s->object_str.c_str();
  }
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
}

static void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  int i;
  str[0] = '\0';
  for (i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
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

  r = list_buckets_init(id, &handle);
  dump_errno(s, (r < 0 ? r : 0));
  end_header(s);
  dump_start_xml(s);

  list_all_buckets_start(s);
  dump_owner(s, (const char *)id.c_str(), id.size(), "foobi");

  open_section(s, "Buckets");
  while (r >= 0) {
    r = list_buckets_next(id, obj, &handle);
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
  len = get_obj(bucket, obj, &data, ofs, end, mod_ptr, unmod_ptr, if_match, if_nomatch, get_data, &err);
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
  S3AccessControlPolicy def;
  string id="thisistheid!";
  string name="foobar";
  def.create_default(id, name);

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
  int r = list_objects(id, s->bucket_str, max, prefix, marker, objs);
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

  int r = create_bucket(id, s->bucket_str);

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

    string md5_str(calc_md5);
    r = put_obj(id, s->bucket_str, s->object_str, data, actual, md5_str);
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
    r = delete_bucket(id, s->bucket_str);
  }

  dump_errno(s, r);
  end_header(s);
}

static void do_delete_object(struct req_state *s)
{
  int r = -EINVAL;
  if (s->object) {
    string id = "0123456789ABCDEF";

    r = delete_obj(id, s->bucket_str, s->object_str);
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

  sighandler = signal(SIGSEGV, godown);

  dbg = fopen("/tmp/fcgi.out", "a");

  while (FCGX_Accept(&in, &out, &err, &envp) >= 0) 
  {
    init_state(&s, envp, in, out);
    static int i=0;

    fprintf(dbg, "%d %s\n", i++, s.method);

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

