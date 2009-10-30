#ifndef __RGW_OP_H
#define __RGW_OP_H

#include <string>

#include "rgw_access.h"
#include "rgw_user.h"

using namespace std;

struct req_state;

extern void get_request_metadata(struct req_state *s, map<nstring, bufferlist>& attrs);
extern int read_acls(RGWAccessControlPolicy *policy, string& bucket, string& object);
extern int read_acls(struct req_state *s, bool only_bucket = false);


class RGWOp {
protected:
  struct req_state *s;
public:
  RGWOp() {}
  ~RGWOp() {}

  virtual void init(struct req_state *s) { this->s = s; }
  virtual void execute() = 0;
};

class RGWGetObj : public RGWOp {
protected:
  const char *range_str;
  const char *if_mod;
  const char *if_unmod;
  const char *if_match;
  const char *if_nomatch;
  off_t ofs;
  off_t len;
  off_t end;
  time_t mod_time;
  time_t unmod_time;
  time_t *mod_ptr;
  time_t *unmod_ptr;
  map<nstring, bufferlist> attrs;
  char *data;
  int ret;
  struct rgw_err err;
  bool get_data;

  int init_common();
public:
  RGWGetObj() {}
  ~RGWGetObj() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ofs = 0;
    len = 0;
    end = -1;
    mod_ptr = NULL;
    unmod_ptr = NULL;
  }
  void set_get_data(bool get_data) {
    this->get_data = get_data;
  }
  void execute();

  virtual int get_params() = 0;
  virtual int send_response() = 0;
};

class RGWListBuckets : public RGWOp {
protected:
  int ret;
  RGWUserBuckets buckets;

public:
  virtual void init(struct req_state *s) {
    RGWOp::init(s);
  }
  RGWListBuckets() {}
  ~RGWListBuckets() {}

  void execute();

  virtual void send_response() = 0;
};

class RGWListBucket : public RGWOp {
protected:
  string prefix;
  string marker; 
  string max_keys;
  string delimiter;
  int max;
  int ret;
  vector<RGWObjEnt> objs;
  map<string, bool> common_prefixes;

public:
  RGWListBucket() {}
  ~RGWListBucket() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    prefix.clear();
    marker.clear();
    max_keys.clear();
    delimiter.clear();
    max = 0;
    ret = 0;
    objs.clear();
    common_prefixes.clear();
  }
  void execute();

  virtual void send_response() = 0;
};

class RGWCreateBucket : public RGWOp {
protected:
  int ret;

public:
  RGWCreateBucket() {}
  ~RGWCreateBucket() {}

  void execute();
  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
  }
  virtual void send_response() = 0;
};

class RGWDeleteBucket : public RGWOp {
protected:
  int ret;

public:
  RGWDeleteBucket() {}
  ~RGWDeleteBucket() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
  }
  void execute();

  virtual void send_response() = 0;
};

class RGWPutObj : public RGWOp {
protected:
  int ret;
  size_t len;
  char *data;
  struct rgw_err err;
  char *supplied_md5_b64;

public:
  RGWPutObj() {}
  ~RGWPutObj() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
    len = 0;
    data = NULL;
    supplied_md5_b64 = NULL;
  }
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
};

class RGWDeleteObj : public RGWOp {
protected:
  int ret;

public:
  RGWDeleteObj() {}
  ~RGWDeleteObj() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
  }
  void execute();

  virtual void send_response() = 0;
};

class RGWCopyObj : public RGWOp {
protected:
  const char *if_mod;
  const char *if_unmod;
  const char *if_match;
  const char *if_nomatch;
  off_t ofs;
  off_t len;
  off_t end;
  time_t *mod_ptr;
  time_t *unmod_ptr;
  int ret;
  map<nstring, bufferlist> attrs;
  struct rgw_err err;
  string src_bucket;
  string src_object;
  time_t mtime;

  int init_common();
public:
  RGWCopyObj() {}
  ~RGWCopyObj() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    if_mod = NULL;
    if_unmod = NULL;
    if_match = NULL;
    if_nomatch = NULL;
    ofs = 0;
    len = 0;
    end = -1;
    mod_ptr = NULL;
    unmod_ptr = NULL;
    ret = 0;
    attrs.clear();
    memset(&err, 0, sizeof(err));
    src_bucket.clear();
    src_object.clear();
    mtime = 0;
  }
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
};

class RGWGetACLs : public RGWOp {
protected:
  int ret;
  string acls;

public:
  RGWGetACLs() {}
  ~RGWGetACLs() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
    acls.clear();
  }
  void execute();

  virtual void send_response() = 0;
};

class RGWPutACLs : public RGWOp {
protected:
  int ret;
  size_t len;
  char *data;

public:
  RGWPutACLs() {}
  ~RGWPutACLs() {}

  virtual void init(struct req_state *s) {
    RGWOp::init(s);
    ret = 0;
    len = 0;
    data = NULL;
  }
  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
};

class RGWHandler {
protected:
  struct req_state *s;

  virtual void provider_init_state() = 0;
  int do_read_permissions(bool only_bucket);
public:
  RGWHandler() {}
  virtual ~RGWHandler() {}
  void init_state(struct req_state *s, struct fcgx_state *fcgx);
  RGWOp *get_op();
  virtual int read_permissions() = 0;
};

#endif
