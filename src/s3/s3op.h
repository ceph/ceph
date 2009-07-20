#ifndef __S3OP_H
#define __S3OP_H

#include <string>

#include "s3access.h"
#include "s3user.h"

using namespace std;

struct req_state;

extern void get_request_metadata(struct req_state *s, map<nstring, bufferlist>& attrs);
extern int read_acls(S3AccessControlPolicy *policy, string& bucket, string& object);
extern int read_acls(struct req_state *s, bool only_bucket = false);


class S3Op {
protected:
  struct req_state *s;
public:
  S3Op(struct req_state *s) { this->s = s; }
  ~S3Op() {}
  virtual void execute() = 0;
};

class S3GetObj : public S3Op {
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
  struct s3_err err;
  bool get_data;

  int init_common();
public:
  S3GetObj(struct req_state *s, bool _get_data) : S3Op(s), ofs(0), len(0),
                                                  end(-1), mod_ptr(NULL), unmod_ptr(NULL),
                                                  get_data(_get_data) {}
  ~S3GetObj() {}

  void execute();

  virtual int get_params() = 0;
  virtual int send_response() = 0;
};

class S3ListBuckets : public S3Op {
protected:
  int ret;
  S3UserBuckets buckets;

public:
  S3ListBuckets(struct req_state *s) : S3Op(s) {}
  ~S3ListBuckets() {}

  void execute();

  virtual void send_response() = 0;
};

class S3ListBucket : public S3Op {
protected:
  string prefix;
  string marker; 
  string max_keys;
  string delimiter;
  int max;
  int ret;
  vector<S3ObjEnt> objs;
  map<string, bool> common_prefixes;

public:
  S3ListBucket(struct req_state *s) : S3Op(s) {}
  ~S3ListBucket() {}

  void execute();

  virtual void send_response() = 0;
};

class S3CreateBucket : public S3Op {
protected:
  int ret;

public:
  S3CreateBucket(struct req_state *s) : S3Op(s) {}
  ~S3CreateBucket() {}

  void execute();

  virtual void send_response() = 0;
};

class S3DeleteBucket : public S3Op {
protected:
  int ret;

public:
  S3DeleteBucket(struct req_state *s) : S3Op(s) {}
  ~S3DeleteBucket() {}

  void execute();

  virtual void send_response() = 0;
};

class S3PutObj : public S3Op {
protected:
  int ret;
  size_t len;
  char *data;
  struct s3_err err;
  char *supplied_md5_b64;

public:
  S3PutObj(struct req_state *s) : S3Op(s), ret(0), len(0), data(NULL), supplied_md5_b64(NULL) {}
  ~S3PutObj() {}

  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
};

class S3DeleteObj : public S3Op {
protected:
  int ret;

public:
  S3DeleteObj(struct req_state *s) : S3Op(s) {}
  ~S3DeleteObj() {}

  void execute();

  virtual void send_response() = 0;
};

class S3CopyObj : public S3Op {
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
  struct s3_err err;
  string src_bucket;
  string src_object;
  time_t mtime;

  int init_common();
public:
  S3CopyObj(struct req_state *s) : S3Op(s), ofs(0), len(0),
                                                  end(-1), mod_ptr(NULL), unmod_ptr(NULL) {}
  ~S3CopyObj() {}

  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
};

class S3GetACLs : public S3Op {
protected:
  int ret;
  string acls;

public:
  S3GetACLs(struct req_state *s) : S3Op(s) {}
  ~S3GetACLs() {}

  void execute();

  virtual void send_response() = 0;
};

class S3PutACLs : public S3Op {
protected:
  int ret;
  size_t len;
  char *data;

public:
  S3PutACLs(struct req_state *s) : S3Op(s), ret(0), len(0), data(NULL) {}
  ~S3PutACLs() {}

  void execute();

  virtual int get_params() = 0;
  virtual void send_response() = 0;
};

class S3Handler {
protected:
  struct req_state *s;

  virtual void provider_init_state() = 0;
public:
  S3Handler() {}
  virtual ~S3Handler() {}
  void init_state(struct req_state *s, struct fcgx_state *fcgx);
};

#endif
