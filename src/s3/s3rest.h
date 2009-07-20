#ifndef __S3REST_H
#define __S3REST_H

#include "s3op.h"

class S3GetObj_REST : public S3GetObj
{
public:
  S3GetObj_REST(struct req_state *s, bool get_data) : S3GetObj(s, get_data) {}
  ~S3GetObj_REST() {}
  int get_params();
  int send_response();
};

class S3ListBuckets_REST : public S3ListBuckets {
public:
  S3ListBuckets_REST(struct req_state *s) : S3ListBuckets(s) {}
  ~S3ListBuckets_REST() {}

  void send_response();
};

class S3ListBucket_REST : public S3ListBucket {
public:
  S3ListBucket_REST(struct req_state *s) : S3ListBucket(s) {}
  ~S3ListBucket_REST() {}

  void send_response();
};

class S3CreateBucket_REST : public S3CreateBucket {
public:
  S3CreateBucket_REST(struct req_state *s) : S3CreateBucket(s) {}
  ~S3CreateBucket_REST() {}

  void send_response();
};

class S3DeleteBucket_REST : public S3DeleteBucket {
public:
  S3DeleteBucket_REST(struct req_state *s) : S3DeleteBucket(s) {}
  ~S3DeleteBucket_REST() {}

  void send_response();
};

class S3PutObj_REST : public S3PutObj
{
public:
  S3PutObj_REST(struct req_state *s) : S3PutObj(s) {}
  ~S3PutObj_REST() {}

  int get_params();
  void send_response();
};

class S3DeleteObj_REST : public S3DeleteObj {
public:
  S3DeleteObj_REST(struct req_state *s) : S3DeleteObj(s) {}
  ~S3DeleteObj_REST() {}

  void send_response();
};

class S3CopyObj_REST : public S3CopyObj {
public:
  S3CopyObj_REST(struct req_state *s) : S3CopyObj(s) {}
  ~S3CopyObj_REST() {}

  int get_params();
  void send_response();
};

class S3GetACLs_REST : public S3GetACLs {
public:
  S3GetACLs_REST(struct req_state *s) : S3GetACLs(s) {}
  ~S3GetACLs_REST() {}

  void send_response();
};

class S3PutACLs_REST : public S3PutACLs {
public:
  S3PutACLs_REST(struct req_state *s) : S3PutACLs(s) {}
  ~S3PutACLs_REST() {}

  int get_params();
  void send_response();
};


class S3Handler_REST : public S3Handler {
protected:
  void provider_init_state();
public:
  S3Handler_REST() : S3Handler() {}
  ~S3Handler_REST() {}
};

extern void dump_errno(struct req_state *s, int err, struct s3_err *s3err = NULL);
extern void end_header(struct req_state *s, const char *content_type = NULL);
extern void dump_start_xml(struct req_state *s);
extern void list_all_buckets_start(struct req_state *s);
extern void dump_owner(struct req_state *s, string& id, string& name);
extern void open_section(struct req_state *s, const char *name);
extern void close_section(struct req_state *s, const char *name);
extern void dump_bucket(struct req_state *s, S3ObjEnt& obj);
extern void abort_early(struct req_state *s, int err);
extern void list_all_buckets_end(struct req_state *s);
extern void dump_value(struct req_state *s, const char *name, const char *fmt, ...);
extern void dump_time(struct req_state *s, const char *name, time_t *t);

#endif
