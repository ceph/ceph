#ifndef CEPH_RGW_LOG_H
#define CEPH_RGW_LOG_H

#include "rgw_common.h"
#include "include/utime.h"

#define LOG_ENTRY_VER 3

#define RGW_SHOULD_LOG_DEFAULT 1

#define RGW_LOG_BUCKET_NAME ".log"

struct rgw_log_entry {
  string owner;
  string bucket;
  utime_t time;
  string remote_addr;
  string user;
  string obj;
  string op;
  string uri;
  string http_status;
  string error_code;
  uint64_t bytes_sent;
  uint64_t bytes_received;
  uint64_t obj_size;
  utime_t total_time;
  string user_agent;
  string referrer;
  uint64_t pool_id;

  void encode(bufferlist &bl) const {
    uint8_t ver;
    ver = LOG_ENTRY_VER;
    ::encode(ver, bl);
    ::encode(owner, bl);
    ::encode(bucket, bl);
    ::encode(time, bl);
    ::encode(remote_addr, bl);
    ::encode(user, bl);
    ::encode(obj, bl);
    ::encode(op, bl);
    ::encode(uri, bl);
    ::encode(http_status, bl);
    ::encode(error_code, bl);
    ::encode(bytes_sent, bl);
    ::encode(obj_size, bl);
    ::encode(total_time, bl);
    ::encode(user_agent, bl);
    ::encode(referrer, bl);
    ::encode(bytes_received, bl);
    ::encode(pool_id, bl);
  }
  void decode(bufferlist::iterator &p) {
    uint8_t ver;
    ::decode(ver, p);
    ::decode(owner, p);
    ::decode(bucket, p);
    ::decode(time, p);
    ::decode(remote_addr, p);
    ::decode(user, p);
    ::decode(obj, p);
    ::decode(op, p);
    ::decode(uri, p);
    ::decode(http_status, p);
    ::decode(error_code, p);
    ::decode(bytes_sent, p);
    ::decode(obj_size, p);
    ::decode(total_time, p);
    ::decode(user_agent, p);
    ::decode(referrer, p);
    if (ver >= 2)
      ::decode(bytes_received, p);
    else
      bytes_received = 0;

    if (ver >= 3)
      ::decode(pool_id, p);
    else
      pool_id = -1;
  }
};
WRITE_CLASS_ENCODER(rgw_log_entry)


int rgw_log_op(struct req_state *s);

#endif

