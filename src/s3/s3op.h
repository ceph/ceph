#ifndef __S3OP_H
#define __S3OP_H

#include <string>

#include "s3access.h"

using namespace std;

struct req_state;

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

  int init();
public:
  S3GetObj(struct req_state *s, bool _get_data) : S3Op(s), ofs(0), len(0),
                                                  end(-1), mod_ptr(NULL), unmod_ptr(NULL),
                                                  get_data(_get_data) {}
  ~S3GetObj() {}

  virtual int get_params() = 0;
  virtual int send_response() = 0;

  virtual void execute();
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

  virtual void execute();
  virtual void send_response() = 0;
};

#endif
