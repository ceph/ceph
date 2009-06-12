#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "fcgiapp.h"

#include "s3access.h"

#include <map>
#include <string>
#include <iostream>

using namespace std;


struct req_state {
   FCGX_Stream *in;
   FCGX_Stream *out;
   int indent;
   const char *path_name;
   const char *host;
   const char *method;
   const char *query;
};


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

  if (delim_pos < 0)
    return -1;

  name = str.substr(0, delim_pos);
  val = str.substr(delim_pos + 1);

  cout << "parsed: name=" << name << " val=" << val << std::endl;
  return 0; 
}

class XMLArgs
{
  string str, empty_str;
  map<string, string> val_map;
 public:
   XMLArgs(string s) : str(s) {}
   int parse();
   string& get(string& name);
   string& get(const char *name);
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

static void init_state(struct req_state *s, FCGX_ParamArray envp, FCGX_Stream *in, FCGX_Stream *out)
{
  s->in = in;
  s->out = out;
  s->indent = 0;
  s->path_name = FCGX_GetParam("SCRIPT_NAME", envp);
  s->method = FCGX_GetParam("REQUEST_METHOD", envp);
  s->host = FCGX_GetParam("HTTP_HOST", envp);
  s->query = FCGX_GetParam("QUERY_STRING", envp);
  
}

static void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  int i;
  str[0] = '\0';
  for (i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

static void open_section(struct req_state *s, const char *name)
{
  FCGX_FPrintF(s->out, "%*s<%s>\n", s->indent, "", name);
  ++s->indent;
}

static void close_section(struct req_state *s, const char *name)
{
  --s->indent;
  FCGX_FPrintF(s->out, "%*s</%s>\n", s->indent, "", name);
}

static void dump_value(struct req_state *s, const char *name, const char *val)
{
  FCGX_FPrintF(s->out, "%*s<%s>%s</%s>\n", s->indent, "", name, val, name);
}

static void dump_entry(struct req_state *s, const char *val)
{
  FCGX_FPrintF(s->out, "%*s<?%s?>\n", s->indent, "", val);
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

static void dump_start(struct req_state *s)
{
  dump_entry(s, "xml version=\"1.0\" encoding=\"UTF-8\"");
}

static void list_all_buckets_start(struct req_state *s)
{
  open_section(s, "ListAllMyBucketsResult");
}

static void list_all_buckets_end(struct req_state *s)
{
  close_section(s, "ListAllMyBucketsResult");
}


static void dump_bucket(struct req_state *s, const char *name, const char *date)
{
  open_section(s, "Bucket");
  dump_value(s, "Name", name);
  dump_value(s, "CreationDate", date);
  close_section(s, "Bucket");
}

void do_list_buckets(struct req_state *s)
{
  string id = "0123456789ABCDEF";
  S3AccessHandle handle;
  int r;
#define BUF_SIZE 256
  char buf[BUF_SIZE];
  list_all_buckets_start(s);
  dump_owner(s, (const char *)id.c_str(), id.size(), "foobi");
  r = list_buckets_init(id, &handle);
  open_section(s, "Buckets");
  while (r >= 0) {
    r = list_buckets_next(id, buf, BUF_SIZE, &handle);
    if (r < 0)
      continue;
    dump_bucket(s, buf, "123123");
  }
  close_section(s, "Buckets");
  list_all_buckets_end(s);
}


void do_list_objects(struct req_state *s)
{
  int pos;
  string bucket, host_str;
  string prefix, marker, max_keys, delimiter;
  int max;
  const char *p;
  string id = "0123456789ABCDEF";

  if (s->path_name[0] == '/') {
    string tmp = s->path_name;
    bucket = tmp.substr(1);
    p = s->query;
  } else if (s->path_name [0] == '?') {
    if (!s->host)
      return;
    host_str = s->host;
    pos = host_str.find('.');
    if (pos >= 0) {
      bucket = host_str.substr(pos);
    } else {
      bucket = host_str;
    }
    p = s->path_name;
  } else {
    return;
  }

  XMLArgs args(p);
  args.parse();

  prefix = args.get("prefix");
  marker = args.get("marker");
  max_keys = args.get("max-keys");
  delimiter = args.get("delimiter");

  open_section(s, "ListBucketResult");
  dump_value(s, "Name", bucket.c_str()); 
  if (!prefix.empty())
    dump_value(s, "Prefix", prefix.c_str());
  if (!marker.empty())
    dump_value(s, "Marker", marker.c_str());
  if (!max_keys.empty()) {
    dump_value(s, "MaxKeys", max_keys.c_str());
    max = atoi(max_keys.c_str());
  } else {
    max = -1;
  }
  if (!delimiter.empty())
    dump_value(s, "Delimiter", delimiter.c_str());

  vector<string> objs;
  int r = list_objects(id, bucket, max, prefix, marker, objs);
  if (r >= 0) {
    vector<string>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      open_section(s, "Contents");
      dump_value(s, "Key", iter->c_str());
      dump_value(s, "LastModified", "2006-01-01T12:00:00.000Z");
      dump_value(s, "ETag", "&quot;828ef3fdfa96f00ad9f27c383fc9ac7f&quot;");
      dump_value(s, "Size", "5");
      dump_value(s, "StorageClass", "STANDARD");
      dump_owner(s, (const char *)&id, sizeof(id), "foobi");
      close_section(s, "Contents");
    }
  }
  close_section(s, "ListBucketResult");
  //if (args.get("name"))
}

void do_retrieve(struct req_state *s)
{
  dump_start(s);
  if (strcmp(s->path_name, "/") == 0)
    do_list_buckets(s);
  else
    do_list_objects(s);
}

int main(void)
{
  FCGX_Stream *in, *out, *err;
  FCGX_ParamArray envp;
  struct req_state s;

  while (FCGX_Accept(&in, &out, &err, &envp) >= 0) 
  {
    FCGX_FPrintF(out,"Content-type: text/plain\r\n\r\n");      

    init_state(&s, envp, in, out);

    if (!s.method)
      continue;

    if (strcmp(s.method, "GET") == 0)
      do_retrieve(&s);
  }
  return 0;
}

