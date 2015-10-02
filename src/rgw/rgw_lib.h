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
#include "rgw_rest_s3.h" // RGW_Auth_S3


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

  /* generate dynamic handle currently unique per librgw object */
  uint64_t get_handle(const string& url);

  /* look for a matching handle (by number) */
  int check_handle(uint64_t handle);

  /* return the saved uri corresponding to handle */
  int get_uri(const uint64_t handle, string &uri);
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

/* XXX */
class RGWRESTMgr_Lib : public RGWRESTMgr {
public:
  RGWRESTMgr_Lib() {}
  virtual ~RGWRESTMgr_Lib() {}
}; /* RGWRESTMgr_Lib */

/* XXX */
class RGWHandler_Lib : public RGWHandler {
  friend class RGWRESTMgr_Lib;
public:

  virtual int authorize() {
    return RGW_Auth_S3::authorize(store, s);
  }

  RGWHandler_Lib() {}
  virtual ~RGWHandler_Lib() {}
  static int init_from_header(struct req_state *s);
}; /* RGWHandler_Lib */

class RGWLibRequest : public RGWRequest,
		      public RGWHandler_Lib {
public:
  CephContext* cct;

  /* unambiguiously return req_state */
  inline struct req_state* get_state() { return this->RGWRequest::s; }

  RGWLibRequest(CephContext* _cct)
    :  RGWRequest(0), cct(_cct)
    {}

  /* descendant equivalent of *REST*::init_from_header(...):
   * prepare request for execute()--should mean, fixup URI-alikes
   * and any other expected stat vars in local req_state, for
   * now */
  virtual int header_init() = 0;

  /* descendant initializer responsible to call RGWOp::init()--which
   * descendants are required to inherit */
  virtual int op_init() = 0;

  int init(const RGWEnv& rgw_env, RGWObjectCtx* rados_ctx,
	  RGWLibIO* io, struct req_state* _s) {

    RGWRequest::init_state(_s);
    RGWHandler::init(rados_ctx->store, _s, io);

    log_init();

    get_state()->obj_ctx = rados_ctx;
    get_state()->req_id = store->unique_id(id);
    get_state()->trans_id = store->unique_trans_id(id);

    log_format(_s, "initializing for trans_id = %s",
	      get_state()->trans_id.c_str());

    return header_init();
  }

  virtual bool only_bucket() = 0;

  virtual int read_permissions(RGWOp *op);

  virtual int postauth_init() { return 0; }

}; /* RGWLibRequest */

#endif /* RGW_LIB_H */
