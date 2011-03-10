// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_RGW_COMMON_H
#define CEPH_RGW_COMMON_H

#include "fcgiapp.h"

#include <string.h>
#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <cryptopp/md5.h>
#include <string>
#include <map>
#include "include/types.h"
#include "include/utime.h"

using namespace std;

#define RGW_ATTR_PREFIX  "user.rgw."

#define RGW_ATTR_ACL		RGW_ATTR_PREFIX "acl"
#define RGW_ATTR_ETAG    	RGW_ATTR_PREFIX "etag"
#define RGW_ATTR_BUCKETS	RGW_ATTR_PREFIX "buckets"
#define RGW_ATTR_META_PREFIX	RGW_ATTR_PREFIX "x-amz-meta-"
#define RGW_ATTR_CONTENT_TYPE	RGW_ATTR_PREFIX "content_type"

#define RGW_BUCKETS_OBJ_PREFIX ".buckets"

#define USER_INFO_VER 3

#define RGW_MAX_CHUNK_SIZE	(4*1024*1024)

#define RGW_LOG_BEGIN "RADOS S3 Gateway:"
#define RGW_LOG(x) if ((x) <= rgw_log_level) cout << RGW_LOG_BEGIN << " "

#define RGW_FORMAT_XML          1
#define RGW_FORMAT_JSON         2

#define CGI_PRINTF(state, format, ...) do { \
   int __ret = FCGX_FPrintF(state->fcgx->out, format, __VA_ARGS__); \
   if (state->header_ended) \
     state->bytes_sent = __ret; \
   printf(">" format, __VA_ARGS__); \
} while (0)


typedef void *RGWAccessHandle;

/** Store error returns for output at a different point in the program */
struct rgw_err {
  const char *num;
  const char *code;
  const char *message;

  rgw_err() : num(NULL), code(NULL), message(NULL) {}
};

/* Helper class used for XMLArgs parsing */
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

/** Stores the XML arguments associated with the HTTP request in req_state*/
class XMLArgs
{
  string str, empty_str;
  map<string, string> val_map;
  string sub_resource;
 public:
   XMLArgs() {}
   XMLArgs(string s) : str(s) {}
   /** Set the arguments; as received */
   void set(string s) { val_map.clear(); sub_resource.clear(); str = s; }
   /** parse the received arguments */
   int parse();
   /** Get the value for a specific argument parameter */
   string& get(string& name);
   string& get(const char *name);
   /** see if a parameter is contained in this XMLArgs */
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
  uint64_t auid;
  string user_id;
  string secret_key;
  string display_name;
  string user_email;
  string openstack_name;

  RGWUserInfo() : auid(0) {}

  void encode(bufferlist& bl) const {
     __u32 ver = USER_INFO_VER;
     ::encode(ver, bl);
     ::encode(auid, bl);
     ::encode(user_id, bl);
     ::encode(secret_key, bl);
     ::encode(display_name, bl);
     ::encode(user_email, bl);
     ::encode(openstack_name, bl);
  }
  void decode(bufferlist::iterator& bl) {
     __u32 ver;
    ::decode(ver, bl);
     if (ver >= 2) ::decode(auid, bl);
     else auid = CEPH_AUTH_UID_DEFAULT;
    ::decode(user_id, bl);
    ::decode(secret_key, bl);
    ::decode(display_name, bl);
    ::decode(user_email, bl);
    if (ver >= 3) ::decode(openstack_name, bl);
  }

  void clear() {
    user_id.clear();
    secret_key.clear();
    display_name.clear();
    user_email.clear();
    auid = CEPH_AUTH_UID_DEFAULT;
  }
};
WRITE_CLASS_ENCODER(RGWUserInfo)

struct req_state;

class RGWFormatter {
protected:
  struct req_state *s;

  virtual void formatter_init() = 0;
public:
  RGWFormatter() {}
  void init(struct req_state *_s) {
    s = _s;
    formatter_init();
  }

  virtual void open_array_section(const char *name) = 0;
  virtual void open_obj_section(const char *name) = 0;
  virtual void close_section(const char *name) = 0;
  virtual void dump_value_int(const char *name, const char *fmt, ...) = 0;
  virtual void dump_value_str(const char *name, const char *fmt, ...) = 0;
};

/** Store all the state necessary to complete and respond to an HTTP request*/
struct req_state {
   struct fcgx_state *fcgx;
   http_op op;
   bool content_started;
   int format;
   RGWFormatter *formatter;
   const char *path_name;
   string path_name_url;
   const char *host;
   const char *method;
   const char *query;
   const char *length;
   const char *content_type;
   bool err_exist;
   struct rgw_err err;
   const char *status;
   bool expect_cont;
   bool header_ended;
   uint64_t bytes_sent; // bytes sent as a response, excluding header
   bool should_log;

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

   int prot_flags;

   char *os_auth_token;
   char *os_user;
   char *os_groups;

   utime_t time;

   req_state() : acl(NULL), os_auth_token(NULL), os_user(NULL), os_groups(NULL) {}
};

/** Store basic data on an object */
struct RGWObjEnt {
  std::string name;
  size_t size;
  time_t mtime;
  char etag[CryptoPP::Weak::MD5::DIGESTSIZE * 2 + 1];

  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    uint64_t s = size;
    __u32 mt = mtime;
    ::encode(name, bl);
    ::encode(s, bl);
    ::encode(mt, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    __u32 mt;
    uint64_t s;
    ::decode(name, bl);
    ::decode(s, bl);
    ::decode(mt, bl);
    size = s;
    mtime = mt;
  }
};
WRITE_CLASS_ENCODER(RGWObjEnt)

/** Store basic data on an object */
struct RGWBucketEnt {
  std::string name;
  size_t size;
  time_t mtime;
  char etag[CryptoPP::Weak::MD5::DIGESTSIZE * 2 + 1];
  uint64_t count;

  void encode(bufferlist& bl) const {
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    uint64_t s = size;
    __u32 mt = mtime;
    ::encode(name, bl);
    ::encode(s, bl);
    ::encode(mt, bl);
    ::encode(count, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    __u32 mt;
    uint64_t s;
    ::decode(name, bl);
    ::decode(s, bl);
    ::decode(mt, bl);
    size = s;
    mtime = mt;
    if (struct_v >= 2)
      ::decode(count, bl);
  }
  void clear() {
    name="";
    size = 0;
    mtime = 0;
    memset(etag, 0, sizeof(etag));
    count = 0;
  }
};
WRITE_CLASS_ENCODER(RGWBucketEnt)

static inline void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  int i;
  str[0] = '\0';
  for (i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

static inline int rgw_str_to_bool(const char *s, int def_val)
{
  if (!s)
    return def_val;

  return (strcasecmp(s, "on") == 0 ||
          strcasecmp(s, "yes") == 0 ||
          strcasecmp(s, "1"));
}

/** */
extern int parse_time(const char *time_str, time_t *time);
/** Check if a user has a permission on that ACL */
extern bool verify_permission(RGWAccessControlPolicy *policy, string& uid, int perm);
/** Check if the req_state's user has the necessary permissions
 * to do the requested action */
extern bool verify_permission(struct req_state *s, int perm);
/** Convert an input URL into a sane object name
 * by converting %-escaped strings into characters, etc*/
extern bool url_decode(string& src_str, string& dest_str);

/* loglevel of the gateway */
extern int rgw_log_level;

#endif
