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

class S3ListBucket_REST : public S3ListBucket {
public:
  S3ListBucket_REST(struct req_state *s) : S3ListBucket(s) {}
  ~S3ListBucket_REST() {}

  void send_response();
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
