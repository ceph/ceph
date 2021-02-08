// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_LOG_H
#define CEPH_RGW_LOG_H

#include <boost/container/flat_map.hpp>
#include "rgw_common.h"
#include "common/OutputDataSocket.h"

class RGWRados;

struct rgw_log_entry {

  using headers_map = boost::container::flat_map<std::string, std::string>;
  using Clock = req_state::Clock;

  rgw_user object_owner;
  rgw_user bucket_owner;
  string bucket;
  Clock::time_point time;
  string remote_addr;
  string user;
  rgw_obj_key obj;
  string op;
  string uri;
  string http_status;
  string error_code;
  uint64_t bytes_sent = 0;
  uint64_t bytes_received = 0;
  uint64_t obj_size = 0;
  Clock::duration total_time{};
  string user_agent;
  string referrer;
  string bucket_id;
  headers_map x_headers;
  string trans_id;
  std::vector<string> token_claims;

  void encode(bufferlist &bl) const {
    ENCODE_START(11, 5, bl);
    encode(object_owner.id, bl);
    encode(bucket_owner.id, bl);
    encode(bucket, bl);
    encode(time, bl);
    encode(remote_addr, bl);
    encode(user, bl);
    encode(obj.name, bl);
    encode(op, bl);
    encode(uri, bl);
    encode(http_status, bl);
    encode(error_code, bl);
    encode(bytes_sent, bl);
    encode(obj_size, bl);
    encode(total_time, bl);
    encode(user_agent, bl);
    encode(referrer, bl);
    encode(bytes_received, bl);
    encode(bucket_id, bl);
    encode(obj, bl);
    encode(object_owner, bl);
    encode(bucket_owner, bl);
    encode(x_headers, bl);
    encode(trans_id, bl);
    encode(token_claims, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &p) {
    DECODE_START_LEGACY_COMPAT_LEN(11, 5, 5, p);
    decode(object_owner.id, p);
    if (struct_v > 3)
      decode(bucket_owner.id, p);
    decode(bucket, p);
    decode(time, p);
    decode(remote_addr, p);
    decode(user, p);
    decode(obj.name, p);
    decode(op, p);
    decode(uri, p);
    decode(http_status, p);
    decode(error_code, p);
    decode(bytes_sent, p);
    decode(obj_size, p);
    decode(total_time, p);
    decode(user_agent, p);
    decode(referrer, p);
    if (struct_v >= 2)
      decode(bytes_received, p);
    else
      bytes_received = 0;

    if (struct_v >= 3) {
      if (struct_v <= 5) {
        uint64_t id;
        decode(id, p);
        char buf[32];
        snprintf(buf, sizeof(buf), "%" PRIu64, id);
        bucket_id = buf;
      } else {
        decode(bucket_id, p);
      }
    } else {
      bucket_id = "";
    }
    if (struct_v >= 7) {
      decode(obj, p);
    }
    if (struct_v >= 8) {
      decode(object_owner, p);
      decode(bucket_owner, p);
    }
    if (struct_v >= 9) {
      decode(x_headers, p);
    }
    if (struct_v >= 10) {
      decode(trans_id, p);
    }
    if (struct_v >= 11) {
      decode(token_claims, p);
    }
    DECODE_FINISH(p);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(list<rgw_log_entry*>& o);
};
WRITE_CLASS_ENCODER(rgw_log_entry)

class OpsLogSocket : public OutputDataSocket {
  ceph::Formatter *formatter;
  ceph::mutex lock = ceph::make_mutex("OpsLogSocket");

  void formatter_to_bl(bufferlist& bl);

protected:
  void init_connection(bufferlist& bl) override;

public:
  OpsLogSocket(CephContext *cct, uint64_t _backlog);
  ~OpsLogSocket() override;

  void log(struct rgw_log_entry& entry);
};

class RGWREST;

int rgw_log_op(RGWRados *store, RGWREST* const rest, struct req_state *s,
	       const string& op_name, OpsLogSocket *olog);
void rgw_log_usage_init(CephContext *cct, RGWRados *store);
void rgw_log_usage_finalize();
void rgw_format_ops_log_entry(struct rgw_log_entry& entry,
			      ceph::Formatter *formatter);

#endif /* CEPH_RGW_LOG_H */

