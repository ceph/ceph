// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef RGW_LIB_H
#define RGW_LIB_H

#include "include/unordered_map.h"
#include "rgw_common.h"
#include "rgw_client_io.h"
#include "rgw_rest.h"
#include "rgw_request.h"
#include "rgw_process.h"


class RGWLibFrontendConfig;
class RGWLibFrontend;
class OpsLogSocket;

class RGWLib {
  RGWFrontendConfig* fec;
  RGWLibFrontend* fe;
  OpsLogSocket* olog;
  RGWREST rest; // XXX needed for RGWProcessEnv
  RGWProcessEnv env;
  RGWRados* store;
  ceph::unordered_map<string, uint64_t> allocated_objects_handles;
  ceph::unordered_map<uint64_t, string> handles_map;
  atomic64_t last_allocated_handle;
public:
  RGWLib() {}
  ~RGWLib() {}

  RGWRados* get_store() { return store; }

  int init();
  int init(vector<const char *>& args);
  int stop();

  /* generate dynamic handle currently unique per librgw object
   */
  uint64_t get_handle(const string& url);

  int check_handle(uint64_t handle);

  int get_uri(const uint64_t handle, string &uri);

#if 0 /* XXXX delete if possible */
  /* User interface */
  int get_userinfo_by_uid(const string& uid, RGWUserInfo& info);
  int get_user_acl();
  int set_user_permissions();
  int set_user_quota();
  int get_user_quota();

  /* buckets */
  int get_user_buckets_list();
  int get_bucket_objects_list();
  int create_bucket();
  int delete_bucket();
  int get_bucket_attributes();
  int set_bucket_attributes();

  /* objects */
  int create_object ();
  int delete_object();
  int write();
  int read();
  int get_object_attributes();
  int set_object_attributes();
#endif /* 0 */
};

/* request interface */

struct RGWLibRequestEnv {
  /* XXXX do we need ANY of this??? Matt */
  int port;
  uint64_t content_length;
  string content_type;
  string request_method;
  string uri;
  string query_string;
  string date_str;

  map<string, string> headers;

  RGWLibRequestEnv() : port(0), content_length(0) {}

  void set_date(utime_t& tm);
  int sign(RGWAccessKey& access_key);
};

class RGWLibIO : public RGWClientIO
{
  uint64_t left_to_read;
  RGWLibRequestEnv* re;
  RGWUserInfo user_info;
public:
  RGWLibIO(RGWLibRequestEnv *_re): re(_re) {}
  RGWLibIO(RGWLibRequestEnv *_re, const RGWUserInfo &_user_info)
    : re(_re), user_info(_user_info) {}

  void init_env(CephContext *cct);

  const RGWUserInfo& get_user() {
    return user_info;
  }

  int set_uid(RGWRados* store, const rgw_user& uid);

  int write_data(const char *buf, int len);
  int read_data(char *buf, int len);
  int send_status(int status, const char *status_name);
  int send_100_continue();
  int complete_header();
  int send_content_length(uint64_t len);

  int complete_request() { /* XXX */
    return 0;
  };

}; /* RGWLibIO */

class RGWLibRequest : public RGWRequest {
public:
  CephContext* cct;

  RGWLibRequest(CephContext* _cct)
    :  RGWRequest(0), cct(_cct)
    {}

  virtual bool only_bucket() = 0;

  virtual RGWHandler* get_handler() /* = 0; */ { return nullptr; }

  int read_permissions(RGWOp *op);

}; /* RGWLibRequest */

#endif /* RGW_LIB_H */
