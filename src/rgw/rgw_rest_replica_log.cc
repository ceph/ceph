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
#include "include/assert.h"

#define dout_subsys ceph_subsys_rgw
#define REPLICA_INPUT_MAX_LEN (512*1024)

static int parse_to_utime(string& in, utime_t& out) {
  uint64_t sec = 0;
  uint64_t nsec = 0;
  int ret = utime_t::parse_date(in.c_str(), &sec, &nsec);
  if (ret < 0)
    return ret;

  out = utime_t(sec, nsec);
  return 0;
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
  list<RGWReplicaItemMarker> markers;

  if ((http_ret = rgw_rest_get_json_input(store->ctx(), s, markers, REPLICA_INPUT_MAX_LEN, NULL)) < 0) {
    dout(5) << "Error - retrieving input data - " << http_ret << dendl;
    return;
  }

  http_ret = rl.update_bound(shard, daemon_id, marker, ut, &markers);
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
  http_ret = rl.get_bounds(shard, bounds);
}

void RGWOp_OBJLog_GetBounds::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  encode_json("bounds", bounds, s->formatter);
  flusher.flush();
}

void RGWOp_OBJLog_DeleteBounds::execute() {
  string id = s->info.args.get("id"),
         daemon_id = s->info.args.get("daemon_id");
  bool purge_all;

  s->info.args.get_bool("purge-all", &purge_all, false);

  if (id.empty() ||
      (!purge_all && daemon_id.empty())) {
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
  http_ret = rl.delete_bound(shard, daemon_id, purge_all);
}

static int bucket_instance_to_bucket(RGWRados *store, const string& bucket_instance, rgw_bucket& bucket) {
  RGWBucketInfo bucket_info;
  real_time mtime;
  
  RGWObjectCtx obj_ctx(store);
  int r = store->get_bucket_instance_info(obj_ctx, bucket_instance, bucket_info, &mtime, NULL);
  if (r < 0) {
    dout(5) << "could not get bucket instance info for bucket=" << bucket_instance << ": " << cpp_strerror(r) << dendl;
    if (r == -ENOENT)
      return r;
    return -EINVAL;
  }

  bucket = bucket_info.bucket;
  return 0;
}

void RGWOp_BILog_SetBounds::execute() {
  string bucket_instance = s->info.args.get("bucket-instance"),
         marker = s->info.args.get("marker"),
         time = s->info.args.get("time"),
         daemon_id = s->info.args.get("daemon_id");

  if (marker.empty() ||
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

  int shard_id;
  http_ret = rgw_bucket_parse_bucket_instance(bucket_instance, &bucket_instance, &shard_id);
  if (http_ret < 0) {
    dout(5) << "failed to parse bucket instance" << dendl;
    return;
  }

  rgw_bucket bucket;

  if ((http_ret = bucket_instance_to_bucket(store, bucket_instance, bucket)) < 0) {
    return;
  }
  
  RGWReplicaBucketLogger rl(store);
  bufferlist bl;
  list<RGWReplicaItemMarker> markers;

  if ((http_ret = rgw_rest_get_json_input(store->ctx(), s, markers, REPLICA_INPUT_MAX_LEN, NULL)) < 0) {
    dout(5) << "Error - retrieving input data - " << http_ret << dendl;
    return;
  }

  http_ret = rl.update_bound(bucket, shard_id, daemon_id, marker, ut, &markers);
}

void RGWOp_BILog_GetBounds::execute() {
  string bucket_instance = s->info.args.get("bucket-instance");
  rgw_bucket bucket;

  int shard_id;

  http_ret = rgw_bucket_parse_bucket_instance(bucket_instance, &bucket_instance, &shard_id);
  if (http_ret < 0) {
    dout(5) << "failed to parse bucket instance" << dendl;
    return;
  }

  if ((http_ret = bucket_instance_to_bucket(store, bucket_instance, bucket)) < 0) 
    return;

  RGWReplicaBucketLogger rl(store);
  http_ret = rl.get_bounds(bucket, shard_id, bounds);
}

void RGWOp_BILog_GetBounds::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  encode_json("bounds", bounds, s->formatter);
  flusher.flush();
}

void RGWOp_BILog_DeleteBounds::execute() {
  string bucket_instance = s->info.args.get("bucket-instance");
  string daemon_id = s->info.args.get("daemon_id");
  bool purge_all;

  s->info.args.get_bool("purge-all", &purge_all, false);

  if (daemon_id.empty() && !purge_all) {
    dout(5) << "Error - invalid parameter list" << dendl;
    http_ret = -EINVAL;
    return;
  }
  
  int shard_id;
  http_ret = rgw_bucket_parse_bucket_instance(bucket_instance, &bucket_instance, &shard_id);
  if (http_ret < 0) {
    dout(5) << "failed to parse bucket instance" << dendl;
    return;
  }

  rgw_bucket bucket;

  if ((http_ret = bucket_instance_to_bucket(store, bucket_instance, bucket)) < 0) {
    return;
  }

  RGWReplicaBucketLogger rl(store);
  http_ret = rl.delete_bound(bucket, shard_id, daemon_id, purge_all);
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

