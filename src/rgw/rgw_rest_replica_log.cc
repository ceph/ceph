// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include "common/ceph_json.h"
#include "common/strtol.h"
#include "rgw_rest.h"
#include "rgw_op.h"
#include "rgw_rest_s3.h"
#include "rgw_replica_log.h"
#include "rgw_metadata.h"
#include "rgw_bucket.h"
#include "rgw_rest_replica_log.h"
#include "rgw_client_io.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_rgw
#define REPLICA_INPUT_MAX_LEN (512*1024)

static int parse_to_utime(string& in, utime_t& out) {
  struct tm tm;
  
  if (!parse_iso8601(in.c_str(), &tm)) 
    return -EINVAL;

  time_t tt = mktime(&tm);
  out = utime_t(tt, 0);
  return 0;
}

static int parse_input_list(const char *data, int data_len, 
                            const char *el_name, list<pair<string, utime_t> >& out) {
  JSONParser parser;

  if (!parser.parse(data, data_len)) {
    return -EINVAL;
  }
  if (!parser.is_array()) {
    dout(5) << "Should have been an array" << dendl;
    return -EINVAL;
  }

  vector<string> l;

  l = parser.get_array_elements();
  for (vector<string>::iterator it = l.begin();
       it != l.end(); it++) {
    JSONParser el_parser;

    if (!el_parser.parse((*it).c_str(), (*it).length())) {
      dout(5) << "Error parsing an array element" << dendl;
      return -EINVAL;
    }

    string name, time;

    JSONDecoder::decode_json(el_name, name, (JSONObj *)&el_parser);
    JSONDecoder::decode_json("time", time, (JSONObj *)&el_parser);

    utime_t ut;
    if (parse_to_utime(time, ut) < 0) {
      return -EINVAL;
    }
    out.push_back(make_pair(name, ut));
  }

  return 0;
}

static int get_input_list(req_state *s, const char *element_name, list<pair<string, utime_t> >& out) {
  int rv, data_len;
  char *data;

  if ((rv = rgw_rest_read_all_input(s, &data, &data_len, REPLICA_INPUT_MAX_LEN)) < 0) {
    dout(5) << "Error - reading input data - " << rv << dendl;
    return rv;
  }
  
  if ((rv = parse_input_list(data, data_len, element_name, out)) < 0) {
    dout(5) << "Error parsing input list - " << rv << dendl;
    return rv;
  }

  free(data);
  return 0;
}

static void item_encode_json(const char *name, 
                 const char *el_name,
                 cls_replica_log_item_marker& val,
                 Formatter *f) {
  f->open_object_section(name);
  f->dump_string(el_name, val.item_name);
  encode_json("time", val.item_timestamp, f);
  f->close_section();
}

static void progress_encode_json(const char *name, 
                 const char *sub_array_name,
                 const char *sub_array_el_name,
                 cls_replica_log_progress_marker &val, 
                 Formatter *f) {
  f->open_object_section(name);
  f->dump_string("daemon_id", val.entity_id);
  f->dump_string("marker", val.position_marker);
  encode_json("time", val.position_time, f);

  f->open_array_section(sub_array_name);
  for (list<cls_replica_log_item_marker>::iterator it = val.items.begin();
       it != val.items.end(); it++) {
    cls_replica_log_item_marker& entry = (*it);

    item_encode_json(sub_array_name, sub_array_el_name, entry, f);
  }
  f->close_section();
  f->close_section();
}

void RGWOp_OBJLog_SetBounds::execute() {
  string id_str = s->info.args.get("id"),
         marker = s->info.args.get("marker"),
         time = s->info.args.get("time"),
         daemon_id = s->info.args.get("daemon_id");

  if (id_str.empty() ||
      marker.empty() ||
      time.empty() ||
      daemon_id.empty()) {
    dout(5) << "Error - invalid parameter list" << dendl;
    http_ret = -EINVAL;
    return;
  }
  
  int shard;
  string err;
  utime_t ut;

  shard = (int)strict_strtol(id_str.c_str(), 10, &err);
  if (!err.empty()) {
    dout(5) << "Error parsing id parameter - " << id_str << ", err " << err << dendl;
    http_ret = -EINVAL;
    return;
  }
  
  if (parse_to_utime(time, ut) < 0) {
    http_ret = -EINVAL;
    return;
  }

  string pool;
  RGWReplicaObjectLogger rl(store, pool, prefix);
  bufferlist bl;
  list<pair<string, utime_t> > entries;

  if ((http_ret = get_input_list(s, "bucket", entries)) < 0) {
    return;
  }

  http_ret = rl.update_bound(shard, daemon_id, marker, ut, &entries);
}

void RGWOp_OBJLog_GetBounds::execute() {
  string id = s->info.args.get("id");

  if (id.empty()) {
    dout(5) << " Error - invalid parameter list" << dendl;
    http_ret = -EINVAL;
    return;
  }

  int shard;
  string err;

  shard = (int)strict_strtol(id.c_str(), 10, &err);
  if (!err.empty()) {
    dout(5) << "Error parsing id parameter - " << id << ", err " << err << dendl;
    http_ret = -EINVAL;
    return;
  }
 
  string pool;
  RGWReplicaObjectLogger rl(store, pool, prefix);
  http_ret = rl.get_bounds(shard, lowest_bound, oldest_time, entries);
}

void RGWOp_OBJLog_GetBounds::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  s->formatter->open_object_section("container");
  s->formatter->open_array_section("items");
  for (list<cls_replica_log_progress_marker>::iterator it = entries.begin();
       it != entries.end(); it++) {
    cls_replica_log_progress_marker entry = (*it);
    progress_encode_json("entry", "buckets", "bucket", entry, s->formatter);
    flusher.flush();
  }
  s->formatter->close_section();
  s->formatter->dump_string("lowest_bound", lowest_bound);
  encode_json("oldest_time", oldest_time, s->formatter);
  s->formatter->close_section();
  flusher.flush();
}

void RGWOp_OBJLog_DeleteBounds::execute() {
  string id = s->info.args.get("id"),
         daemon_id = s->info.args.get("daemon_id");

  if (id.empty() ||
      daemon_id.empty()) {
    dout(5) << "Error - invalid parameter list" << dendl;
    http_ret = -EINVAL;
    return;
  }
  
  int shard;
  string err;

  shard = (int)strict_strtol(id.c_str(), 10, &err);
  if (!err.empty()) {
    dout(5) << "Error parsing id parameter - " << id << ", err " << err << dendl;
    http_ret = -EINVAL;
  }
  
  string pool;
  RGWReplicaObjectLogger rl(store, pool, prefix);
  http_ret = rl.delete_bound(shard, daemon_id);
}

static int bucket_name_to_bucket(RGWRados *store, string& bucket_str, rgw_bucket& bucket) {
  RGWBucketInfo bucket_info;
  RGWObjVersionTracker objv_tracker;
  time_t mtime;
  
  int r = store->get_bucket_info(NULL, bucket_str, bucket_info, &objv_tracker, &mtime);
  if (r < 0) {
    dout(5) << "could not get bucket info for bucket=" << bucket_str << dendl;
    return -EINVAL;
  }

  bucket = bucket_info.bucket;
  return 0;
}

void RGWOp_BILog_SetBounds::execute() {
  string bucket_str = s->info.args.get("bucket"),
         marker = s->info.args.get("marker"),
         time = s->info.args.get("time"),
         daemon_id = s->info.args.get("daemon_id");

  if (bucket_str.empty() ||
      marker.empty() ||
      time.empty() ||
      daemon_id.empty()) {
    dout(5) << "Error - invalid parameter list" << dendl;
    http_ret = -EINVAL;
    return;
  }
  
  utime_t ut;
  
  if (parse_to_utime(time, ut) < 0) {
    http_ret = -EINVAL;
    return;
  }

  rgw_bucket bucket;
  if ((http_ret = bucket_name_to_bucket(store, bucket_str, bucket)) < 0) 
    return;

  RGWReplicaBucketLogger rl(store);
  bufferlist bl;
  list<pair<string, utime_t> > entries;

  if ((http_ret = get_input_list(s, "object", entries)) < 0) {
    return;
  }

  http_ret = rl.update_bound(bucket, daemon_id, marker, ut, &entries);
}

void RGWOp_BILog_GetBounds::execute() {
  string bucket_str = s->info.args.get("bucket");

  if (bucket_str.empty()) {
    dout(5) << " Error - invalid parameter list" << dendl;
    http_ret = -EINVAL;
    return;
  }

  rgw_bucket bucket;
  if ((http_ret = bucket_name_to_bucket(store, bucket_str, bucket)) < 0) 
    return;

  RGWReplicaBucketLogger rl(store);
  http_ret = rl.get_bounds(bucket, lowest_bound, oldest_time, entries);
}

void RGWOp_BILog_GetBounds::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  s->formatter->open_object_section("container");
  s->formatter->open_array_section("entries");
  for (list<cls_replica_log_progress_marker>::iterator it = entries.begin();
       it != entries.end(); it++) {
    cls_replica_log_progress_marker entry = (*it);
    progress_encode_json("entry", "objects", "object", entry, s->formatter);
    flusher.flush();
  }
  s->formatter->close_section();
  s->formatter->dump_string("lowest_bound", lowest_bound);
  encode_json("oldest_time", oldest_time, s->formatter);
  s->formatter->close_section();
  flusher.flush();
}

void RGWOp_BILog_DeleteBounds::execute() {
  string bucket_str = s->info.args.get("bucket"),
         daemon_id = s->info.args.get("daemon_id");

  if (bucket_str.empty() ||
      daemon_id.empty()) {
    dout(5) << "Error - invalid parameter list" << dendl;
    http_ret = -EINVAL;
    return;
  }
  
  rgw_bucket bucket;
  if ((http_ret = bucket_name_to_bucket(store, bucket_str, bucket)) < 0) 
    return;
  
  RGWReplicaBucketLogger rl(store);
  http_ret = rl.delete_bound(bucket, daemon_id);
}

RGWOp *RGWHandler_ReplicaLog::op_get() {
  bool exists;
  string type = s->info.args.get("type", &exists);

  if (!exists) {
    return NULL;
  }

  if (type.compare("metadata") == 0) {
    return new RGWOp_OBJLog_GetBounds(META_REPLICA_LOG_OBJ_PREFIX, "mdlog");
  } else if (type.compare("bucket-index") == 0) {
    return new RGWOp_BILog_GetBounds;
  } else if (type.compare("data") == 0) {
    return new RGWOp_OBJLog_GetBounds(DATA_REPLICA_LOG_OBJ_PREFIX, "datalog");
  }
  return NULL;
}

RGWOp *RGWHandler_ReplicaLog::op_delete() {
  bool exists;
  string type = s->info.args.get("type", &exists);

  if (!exists) {
    return NULL;
  }

  if (type.compare("metadata") == 0)
    return new RGWOp_OBJLog_DeleteBounds(META_REPLICA_LOG_OBJ_PREFIX, "mdlog");
  else if (type.compare("bucket-index") == 0) 
    return new RGWOp_BILog_DeleteBounds;
  else if (type.compare("data") == 0)
    return new RGWOp_OBJLog_DeleteBounds(DATA_REPLICA_LOG_OBJ_PREFIX, "datalog");
  
  return NULL;
}

RGWOp *RGWHandler_ReplicaLog::op_post() {
  bool exists;
  string type = s->info.args.get("type", &exists);

  if (!exists) {
    return NULL;
  }

  if (type.compare("metadata") == 0) {
    return new RGWOp_OBJLog_SetBounds(META_REPLICA_LOG_OBJ_PREFIX, "mdlog");
  } else if (type.compare("bucket-index") == 0) {
    return new RGWOp_BILog_SetBounds;
  } else if (type.compare("data") == 0) {
    return new RGWOp_OBJLog_SetBounds(DATA_REPLICA_LOG_OBJ_PREFIX, "datalog");
  }
  return NULL;
}

