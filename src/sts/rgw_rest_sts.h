// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_S3_H
#define CEPH_RGW_REST_S3_H
#define TIME_BUF_SIZE 128

#include "rgw_op.h"
#include "rgw_http_errors.h"
// #include "rgw_acl_s3.h"
// #include "rgw_policy_s3.h"

#define RGW_AUTH_GRACE_MINS 15

void rgw_get_errno_sts(rgw_http_errors *e, int err_no);

// if you change tsErrorType or tsErrorCode,
//	please change error_types[] in
//	rgw_rest_sts.cc too!
enum tsErrorType {
  Unknown = 0, Sender, Receiver,
};

struct sts_err : public rgw_err {
  tsErrorType type;
  uuid_d request_id;
  sts_err();
  virtual ~sts_err() { };
  virtual void dump(Formatter *f) const;
  virtual bool set_rgw_err(int);
};

extern string error_types[];

class sts_credentials {
public:
  string session_token;		// base64
  string secret_access_key;	// base64
  utime_t expiration;		// yyyy-mm-ddThh:mm:ss.fffZ
  string access_key_id;		// string
};

class sts_usertype {
public:
  string arn;				// string: arn:aws:...
  string assumed_role_id;		// string: [A-Z0-9]*:Name
};

class assume_role_result {
public:
  sts_credentials credentials;
  sts_usertype assumed_role_user;
  int packed_policy_size;
};

class assume_role_response: public assume_role_result {
public:
  uuid_d request_id;
  assume_role_response(uuid_d &id) {
    request_id = id;
  }
  void dump(Formatter *f);
};

#if 0
struct post_part_field {
  string val;
  map<string, string> params;
};

struct post_form_part {
  string name;
  string content_type;
  map<string, struct post_part_field, ltstr_nocase> fields;
  bufferlist data;
};

class RGWPostObj_ObjStore_STS : public RGWPostObj_ObjStore {
  string boundary;
  string filename;
  bufferlist in_data;
  map<string, post_form_part, const ltstr_nocase> parts;  
  RGWPolicyEnv env;
  RGWPolicy post_policy;
  string err_msg;

  int read_with_boundary(bufferlist& bl, uint64_t max, bool check_eol,
                         bool *reached_boundary,
			 bool *done);

  int read_line(bufferlist& bl, uint64_t max,
                bool *reached_boundary, bool *done);

  int read_data(bufferlist& bl, uint64_t max, bool *reached_boundary, bool *done);

  int read_form_part_header(struct post_form_part *part,
                            bool *done);
  bool part_str(const string& name, string *val);
  bool part_bl(const string& name, bufferlist *pbl);

  int get_policy();
  void rebuild_key(string& key);
public:
  RGWPostObj_ObjStore_STS() {}
  ~RGWPostObj_ObjStore_STS() {}

  int get_params();
  int complete_get_params();
  void send_response();
  int get_data(bufferlist& bl);
};
#endif

#if 0
class RGW_Auth_S3_Keystone_ValidateToken : public RGWHTTPClient {
private:
  bufferlist rx_buffer;
  bufferlist tx_buffer;
  bufferlist::iterator tx_buffer_it;
  list<string> roles_list;

public:
  KeystoneToken response;

private:
  void set_tx_buffer(const string& d) {
    tx_buffer.clear();
    tx_buffer.append(d);
    tx_buffer_it = tx_buffer.begin();
    set_send_length(tx_buffer.length());
  }

public:
  RGW_Auth_S3_Keystone_ValidateToken(CephContext *_cct)
      : RGWHTTPClient(_cct) {
    get_str_list(cct->_conf->rgw_keystone_accepted_roles, roles_list);
  }

  int receive_header(void *ptr, size_t len) {
    return 0;
  }
  int receive_data(void *ptr, size_t len) {
    rx_buffer.append((char *)ptr, len);
    return 0;
  }

  int send_data(void *ptr, size_t len) {
    if (!tx_buffer_it.get_remaining())
      return 0; // nothing left to send

    int l = MIN(tx_buffer_it.get_remaining(), len);
    memcpy(ptr, tx_buffer_it.get_current_ptr().c_str(), l);
    try {
      tx_buffer_it.advance(l);
    } catch (buffer::end_of_buffer &e) {
      assert(0);
    }

    return l;
  }

  int validate_s3token(const string& auth_id, const string& auth_token, const string& auth_sign);

};
#endif

class RGWHandler_STS: public RGWHandler {
  friend class RGWRESTMgr_STS;
public:
  static int init_from_header(struct req_state *s, int default_formatter, bool configurable_format);
  
  int init(RGWRados *store, struct req_state *state, RGWClientIO *cio);
  int authorize();

protected:
  RGWOp *get_obj_op(bool get_data);

  RGWOp *op_get();
  RGWOp *op_head();
  RGWOp *op_put();
  RGWOp *op_delete();
  RGWOp *op_post();
  RGWOp *op_options();
public:
  RGWHandler_STS() {}
  ~RGWHandler_STS() {}

  int read_permissions(RGWOp *op) {
    return 0;
  }
};

class RGWRESTMgr_STS : public RGWRESTMgr {
public:
  RGWRESTMgr_STS() {}
  virtual ~RGWRESTMgr_STS() {}

  RGWHandler *get_handler(struct req_state *s);
};


#endif
