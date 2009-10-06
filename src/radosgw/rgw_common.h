#ifndef __RGWCOMMON_H
#define __RGWCOMMON_H

#include "fcgiapp.h"

#include <openssl/md5.h>
#include <string>
#include <map>
#include "include/types.h"

using namespace std;

#define SERVER_NAME "RGWFS"

#define RGW_ATTR_PREFIX  "user.rgw."

#define RGW_ATTR_ACL		RGW_ATTR_PREFIX "acl"
#define RGW_ATTR_ETAG    	RGW_ATTR_PREFIX "etag"
#define RGW_ATTR_BUCKETS		RGW_ATTR_PREFIX "buckets"
#define RGW_ATTR_META_PREFIX	RGW_ATTR_PREFIX "x-amz-meta-"
#define RGW_ATTR_CONTENT_TYPE	RGW_ATTR_PREFIX "content_type"

#define USER_INFO_VER 1

typedef void *RGWAccessHandle;

struct rgw_err {
  const char *num;
  const char *code;
  const char *message;

  rgw_err() : num(NULL), code(NULL), message(NULL) {}
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

class XMLArgs
{
  string str, empty_str;
  map<string, string> val_map;
  string sub_resource;
 public:
   XMLArgs() {}
   XMLArgs(string s) : str(s) {}
   void set(string s) { val_map.clear(); sub_resource.clear(); str = s; }
   int parse();
   string& get(string& name);
   string& get(const char *name);
   bool exists(const char *name) {
     map<string, string>::iterator iter = val_map.find(name);
     return (iter != val_map.end());
   }
   string& get_sub_resource() { return sub_resource; }
};

enum http_op {
  OP_GET,
  OP_PUT,
  OP_DELETE,
  OP_HEAD,
  OP_UNKNOWN,
};

struct fcgx_state {
   FCGX_ParamArray envp;
   FCGX_Stream *in;
   FCGX_Stream *out;
   FCGX_Stream *err;
};

class RGWAccessControlPolicy;


struct RGWUserInfo
{
  string user_id;
  string secret_key;
  string display_name;
  string user_email;

  void encode(bufferlist& bl) const {
     __u32 ver = USER_INFO_VER;
     ::encode(ver, bl);
     ::encode(user_id, bl);
     ::encode(secret_key, bl);
     ::encode(display_name, bl);
     ::encode(user_email, bl);
  }
  void decode(bufferlist::iterator& bl) {
     __u32 ver;
    ::decode(ver, bl);
    ::decode(user_id, bl);
    ::decode(secret_key, bl);
    ::decode(display_name, bl);
    ::decode(user_email, bl);
  }

  void clear() {
    user_id.clear();
    secret_key.clear();
    display_name.clear();
    user_email.clear();
  }
};
WRITE_CLASS_ENCODER(RGWUserInfo)


struct req_state {
   struct fcgx_state *fcgx;
   http_op op;
   bool content_started;
   int indent;
   const char *path_name;
   string path_name_url;
   const char *host;
   const char *method;
   const char *query;
   const char *length;
   const char *content_type;
   bool err_exist;
   struct rgw_err err;

   XMLArgs args;

   const char *bucket;
   const char *object;

   const char *host_bucket;

   string bucket_str;
   string object_str;

   map<string, string> x_amz_map;

   vector<pair<string, string> > x_amz_meta;

   RGWUserInfo user; 
   RGWAccessControlPolicy *acl;

   string canned_acl;
   const char *copy_source;
   const char *http_auth;

   req_state() : acl(NULL) {}
};

struct RGWObjEnt {
  std::string name;
  size_t size;
  time_t mtime;
  char etag[MD5_DIGEST_LENGTH * 2 + 1];

  void encode(bufferlist& bl) const {
    __u64 s = size;
    __u32 mt = mtime;
    ::encode(name, bl);
    ::encode(s, bl);
    ::encode(mt, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u32 mt;
    __u64 s;
    ::decode(name, bl);
    ::decode(s, bl);
    ::decode(mt, bl);
    size = s;
    mtime = mt;
  }
};
WRITE_CLASS_ENCODER(RGWObjEnt)

static inline void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  int i;
  str[0] = '\0';
  for (i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

extern int parse_time(const char *time_str, time_t *time);
extern bool verify_permission(RGWAccessControlPolicy *policy, string& uid, int perm);
extern bool verify_permission(struct req_state *s, int perm);
extern bool url_decode(string& src_str, string& dest_str);


#endif
