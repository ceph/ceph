// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <boost/container/flat_map.hpp>
#include "rgw_common.h"
#include "common/OutputDataSocket.h"
#include <vector>
#include <fstream>
#include "rgw_sal_fwd.h"

class RGWOp;

struct delete_multi_obj_entry {
  std::string key;
  std::string version_id;
  std::string error_message;
  std::string marker_version_id;
  uint32_t http_status = 0;
  bool error = false;
  bool delete_marker = false;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(key, bl);
    encode(version_id, bl);
    encode(error_message, bl);
    encode(marker_version_id, bl);
    encode(http_status, bl);
    encode(error, bl);
    encode(delete_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &p) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, p);
    decode(key, p);
    decode(version_id, p);
    decode(error_message, p);
    decode(marker_version_id, p);
    decode(http_status, p);
    decode(error, p);
    decode(delete_marker, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(delete_multi_obj_entry)

struct delete_multi_obj_op_meta {
  uint32_t num_ok = 0;
  uint32_t num_err = 0;
  std::vector<delete_multi_obj_entry> objects;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(num_ok, bl);
    encode(num_err, bl);
    encode(objects, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &p) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, p);
    decode(num_ok, p);
    decode(num_err, p);
    decode(objects, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(delete_multi_obj_op_meta)

struct rgw_log_entry {

  using headers_map = boost::container::flat_map<std::string, std::string>;
  using Clock = req_state::Clock;

  rgw_user object_owner;
  rgw_user bucket_owner;
  std::string bucket;
  Clock::time_point time;
  std::string remote_addr;
  std::string user;
  rgw_obj_key obj;
  std::string op;
  std::string uri;
  std::string http_status;
  std::string error_code;
  uint64_t bytes_sent = 0;
  uint64_t bytes_received = 0;
  uint64_t obj_size = 0;
  Clock::duration total_time{};
  std::string user_agent;
  std::string referrer;
  std::string bucket_id;
  headers_map x_headers;
  std::string trans_id;
  std::vector<std::string> token_claims;
  uint32_t identity_type = TYPE_NONE;
  std::string access_key_id;
  std::string subuser;
  bool temp_url {false};
  delete_multi_obj_op_meta delete_multi_obj_meta;

  void encode(bufferlist &bl) const {
    ENCODE_START(14, 5, bl);
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
    encode(identity_type,bl);
    encode(access_key_id, bl);
    encode(subuser, bl);
    encode(temp_url, bl);
    encode(delete_multi_obj_meta, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &p) {
    DECODE_START_LEGACY_COMPAT_LEN(14, 5, 5, p);
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
    if (struct_v >= 12) {
      decode(identity_type, p);
    }
    if (struct_v >= 13) {
      decode(access_key_id, p);
      decode(subuser, p);
      decode(temp_url, p);
    }
    if (struct_v >= 14) {
      decode(delete_multi_obj_meta, p);
    }
    DECODE_FINISH(p);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_log_entry*>& o);
};
WRITE_CLASS_ENCODER(rgw_log_entry)

class OpsLogSink {
public:
  virtual int log(req_state* s, struct rgw_log_entry& entry) = 0;
  virtual ~OpsLogSink() = default;
};

class OpsLogManifold: public OpsLogSink {
  std::vector<OpsLogSink*> sinks;
public:
  ~OpsLogManifold() override;
  void add_sink(OpsLogSink* sink);
  int log(req_state* s, struct rgw_log_entry& entry) override;
};

class JsonOpsLogSink : public OpsLogSink {
  ceph::Formatter *formatter;
  ceph::mutex lock = ceph::make_mutex("JsonOpsLogSink");

  void formatter_to_bl(bufferlist& bl);
protected:
  virtual int log_json(req_state* s, bufferlist& bl) = 0;
public:
  JsonOpsLogSink();
  ~JsonOpsLogSink() override;
  int log(req_state* s, struct rgw_log_entry& entry) override;
};

class OpsLogFile : public JsonOpsLogSink, public Thread, public DoutPrefixProvider {
  CephContext* cct;
  ceph::mutex mutex = ceph::make_mutex("OpsLogFile");
  std::vector<bufferlist> log_buffer;
  std::vector<bufferlist> flush_buffer;
  ceph::condition_variable cond;
  std::ofstream file;
  bool stopped;
  uint64_t data_size;
  uint64_t max_data_size;
  std::string path;
  std::atomic_bool need_reopen;

  void flush();
protected:
  int log_json(req_state* s, bufferlist& bl) override;
  void *entry() override;
public:
  OpsLogFile(CephContext* cct, std::string& path, uint64_t max_data_size);
  ~OpsLogFile() override;
  CephContext *get_cct() const override { return cct; }
  unsigned get_subsys() const override;
  std::ostream& gen_prefix(std::ostream& out) const override { return out << "rgw OpsLogFile: "; }
  void reopen();
  void start();
  void stop();
};

class OpsLogSocket : public OutputDataSocket, public JsonOpsLogSink {
protected:
  int log_json(req_state* s, bufferlist& bl) override;
  void init_connection(bufferlist& bl) override;

public:
  OpsLogSocket(CephContext *cct, uint64_t _backlog);
};

class OpsLogRados : public OpsLogSink {
  // main()'s driver pointer as a reference, possibly modified by RGWRealmReloader
  rgw::sal::Driver* const& driver;

public:
  OpsLogRados(rgw::sal::Driver* const& driver);
  int log(req_state* s, struct rgw_log_entry& entry) override;
};

class RGWREST;

int rgw_log_op(RGWREST* const rest, struct req_state* s,
	             const RGWOp* op, OpsLogSink* olog);
void rgw_log_usage_init(CephContext* cct, rgw::sal::Driver* driver);
void rgw_log_usage_finalize();
void rgw_format_ops_log_entry(struct rgw_log_entry& entry,
			      ceph::Formatter *formatter);
