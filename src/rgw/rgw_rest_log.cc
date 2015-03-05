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
#include "rgw_rest_log.h"
#include "rgw_client_io.h"
#include "common/errno.h"

#define LOG_CLASS_LIST_MAX_ENTRIES (1000)
#define dout_subsys ceph_subsys_rgw

static int parse_date_str(string& in, utime_t& out) {
  uint64_t epoch = 0;
  uint64_t nsec = 0;

  if (!in.empty()) {
    if (utime_t::parse_date(in, &epoch, &nsec) < 0) {
      dout(5) << "Error parsing date " << in << dendl;
      return -EINVAL;
    }
  }
  out = utime_t(epoch, nsec);
  return 0;
}

void RGWOp_MDLog_List::execute() {
  string   shard = s->info.args.get("id");
  string   max_entries_str = s->info.args.get("max-entries");
  string   st = s->info.args.get("start-time"),
           et = s->info.args.get("end-time"),
           marker = s->info.args.get("marker"),
           err;
  utime_t  ut_st, 
           ut_et;
  void    *handle;
  unsigned shard_id, max_entries = LOG_CLASS_LIST_MAX_ENTRIES;

  shard_id = (unsigned)strict_strtol(shard.c_str(), 10, &err);
  if (!err.empty()) {
    dout(5) << "Error parsing shard_id " << shard << dendl;
    http_ret = -EINVAL;
    return;
  }

  if (parse_date_str(st, ut_st) < 0) {
    http_ret = -EINVAL;
    return;
  }

  if (parse_date_str(et, ut_et) < 0) {
    http_ret = -EINVAL;
    return;
  }

  if (!max_entries_str.empty()) {
    max_entries = (unsigned)strict_strtol(max_entries_str.c_str(), 10, &err);
    if (!err.empty()) {
      dout(5) << "Error parsing max-entries " << max_entries_str << dendl;
      http_ret = -EINVAL;
      return;
    }
  } 
  
  RGWMetadataLog *meta_log = store->meta_mgr->get_log();

  meta_log->init_list_entries(shard_id, ut_st, ut_et, marker, &handle);

  do {
    http_ret = meta_log->list_entries(handle, max_entries, entries,
				      &last_marker, &truncated);
    if (http_ret < 0) 
      break;

    if (!max_entries_str.empty()) 
      max_entries -= entries.size();
  } while (truncated && (max_entries > 0));

  meta_log->complete_list_entries(handle);
}

void RGWOp_MDLog_List::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  s->formatter->open_object_section("log_entries");
  s->formatter->dump_string("marker", last_marker);
  s->formatter->dump_bool("truncated", truncated);
  {
    s->formatter->open_array_section("entries");
    for (list<cls_log_entry>::iterator iter = entries.begin();
	 iter != entries.end(); ++iter) {
      cls_log_entry& entry = *iter;
      store->meta_mgr->dump_log_entry(entry, s->formatter);
      flusher.flush();
    }
    s->formatter->close_section();
  }
  s->formatter->close_section();
  flusher.flush();
}

void RGWOp_MDLog_Info::execute() {
  num_objects = s->cct->_conf->rgw_md_log_max_shards;
  http_ret = 0;
}

void RGWOp_MDLog_Info::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  s->formatter->open_object_section("num_objects");
  s->formatter->dump_unsigned("num_objects", num_objects);
  s->formatter->close_section();
  flusher.flush();
}

void RGWOp_MDLog_ShardInfo::execute() {
  string shard = s->info.args.get("id");
  string err;

  unsigned shard_id = (unsigned)strict_strtol(shard.c_str(), 10, &err);
  if (!err.empty()) {
    dout(5) << "Error parsing shard_id " << shard << dendl;
    http_ret = -EINVAL;
    return;
  }

  RGWMetadataLog *meta_log = store->meta_mgr->get_log();

  http_ret = meta_log->get_info(shard_id, &info);
}

void RGWOp_MDLog_ShardInfo::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  encode_json("info", info, s->formatter);
  flusher.flush();
}

void RGWOp_MDLog_Delete::execute() {
  string   st = s->info.args.get("start-time"),
           et = s->info.args.get("end-time"),
           start_marker = s->info.args.get("start-marker"),
           end_marker = s->info.args.get("end-marker"),
           shard = s->info.args.get("id"),
           err;
  utime_t  ut_st, 
           ut_et;
  unsigned shard_id;

  http_ret = 0;

  shard_id = (unsigned)strict_strtol(shard.c_str(), 10, &err);
  if (!err.empty()) {
    dout(5) << "Error parsing shard_id " << shard << dendl;
    http_ret = -EINVAL;
    return;
  }
  if (et.empty() && end_marker.empty()) { /* bounding end */
    http_ret = -EINVAL;
    return;
  }

  if (parse_date_str(st, ut_st) < 0) {
    http_ret = -EINVAL;
    return;
  }

  if (parse_date_str(et, ut_et) < 0) {
    http_ret = -EINVAL;
    return;
  }
  RGWMetadataLog *meta_log = store->meta_mgr->get_log();

  http_ret = meta_log->trim(shard_id, ut_st, ut_et, start_marker, end_marker);
}

void RGWOp_MDLog_Lock::execute() {
  string shard_id_str, duration_str, locker_id, zone_id;
  unsigned shard_id;

  http_ret = 0;

  shard_id_str = s->info.args.get("id");
  duration_str = s->info.args.get("length");
  locker_id    = s->info.args.get("locker-id");
  zone_id      = s->info.args.get("zone-id");

  if (shard_id_str.empty() ||
      (duration_str.empty()) ||
      locker_id.empty() ||
      zone_id.empty()) {
    dout(5) << "Error invalid parameter list" << dendl;
    http_ret = -EINVAL;
    return;
  }

  string err;
  shard_id = (unsigned)strict_strtol(shard_id_str.c_str(), 10, &err);
  if (!err.empty()) {
    dout(5) << "Error parsing shard_id param " << shard_id_str << dendl;
    http_ret = -EINVAL;
    return;
  }

  RGWMetadataLog *meta_log = store->meta_mgr->get_log();
  unsigned dur;
  dur = (unsigned)strict_strtol(duration_str.c_str(), 10, &err);
  if (!err.empty() || dur <= 0) {
    dout(5) << "invalid length param " << duration_str << dendl;
    http_ret = -EINVAL;
    return;
  }
  utime_t time(dur, 0);
  http_ret = meta_log->lock_exclusive(shard_id, time, zone_id, locker_id);
  if (http_ret == -EBUSY)
    http_ret = -ERR_LOCKED;
}

void RGWOp_MDLog_Unlock::execute() {
  string shard_id_str, locker_id, zone_id;
  unsigned shard_id;

  http_ret = 0;

  shard_id_str = s->info.args.get("id");
  locker_id    = s->info.args.get("locker-id");
  zone_id      = s->info.args.get("zone-id");

  if (shard_id_str.empty() ||
      locker_id.empty() ||
      zone_id.empty()) {
    dout(5) << "Error invalid parameter list" << dendl;
    http_ret = -EINVAL;
    return;
  }

  string err;
  shard_id = (unsigned)strict_strtol(shard_id_str.c_str(), 10, &err);
  if (!err.empty()) {
    dout(5) << "Error parsing shard_id param " << shard_id_str << dendl;
    http_ret = -EINVAL;
    return;
  }

  RGWMetadataLog *meta_log = store->meta_mgr->get_log();
  http_ret = meta_log->unlock(shard_id, zone_id, locker_id);
}

void RGWOp_BILog_List::execute() {
  string bucket_name = s->info.args.get("bucket"),
         marker = s->info.args.get("marker"),
         max_entries_str = s->info.args.get("max-entries"),
         bucket_instance = s->info.args.get("bucket-instance");
  RGWBucketInfo bucket_info;
  unsigned max_entries;

  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);

  if (bucket_name.empty() && bucket_instance.empty()) {
    dout(5) << "ERROR: neither bucket nor bucket instance specified" << dendl;
    http_ret = -EINVAL;
    return;
  }

  int shard_id;
  http_ret = rgw_bucket_parse_bucket_instance(bucket_instance, &bucket_instance, &shard_id);
  if (http_ret < 0) {
    return;
  }

  if (!bucket_instance.empty()) {
    http_ret = store->get_bucket_instance_info(obj_ctx, bucket_instance, bucket_info, NULL, NULL);
    if (http_ret < 0) {
      dout(5) << "could not get bucket instance info for bucket instance id=" << bucket_instance << dendl;
      return;
    }
  } else { /* !bucket_name.empty() */
    http_ret = store->get_bucket_info(obj_ctx, bucket_name, bucket_info, NULL, NULL);
    if (http_ret < 0) {
      dout(5) << "could not get bucket info for bucket=" << bucket_name << dendl;
      return;
    }
  }

  bool truncated;
  unsigned count = 0;
  string err;

  max_entries = (unsigned)strict_strtol(max_entries_str.c_str(), 10, &err);
  if (!err.empty())
    max_entries = LOG_CLASS_LIST_MAX_ENTRIES;

  send_response();
  do {
    list<rgw_bi_log_entry> entries;
    int ret = store->list_bi_log_entries(bucket_info.bucket, shard_id,
                                          marker, max_entries - count, 
                                          entries, &truncated);
    if (ret < 0) {
      dout(5) << "ERROR: list_bi_log_entries()" << dendl;
      return;
    }

    count += entries.size();

    send_response(entries, marker);
  } while (truncated && count < max_entries);

  send_response_end();
}

void RGWOp_BILog_List::send_response() {
  if (sent_header)
    return;

  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  sent_header = true;

  if (http_ret < 0)
    return;

  s->formatter->open_array_section("entries");
}

void RGWOp_BILog_List::send_response(list<rgw_bi_log_entry>& entries, string& marker)
{
  for (list<rgw_bi_log_entry>::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
    rgw_bi_log_entry& entry = *iter;
    encode_json("entry", entry, s->formatter);

    marker = entry.id;
    flusher.flush();
  }
}

void RGWOp_BILog_List::send_response_end() {
  s->formatter->close_section();
  flusher.flush();
}
      
void RGWOp_BILog_Info::execute() {
  string bucket_name = s->info.args.get("bucket"),
         bucket_instance = s->info.args.get("bucket-instance");
  RGWBucketInfo bucket_info;

  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);

  if (bucket_name.empty() && bucket_instance.empty()) {
    dout(5) << "ERROR: neither bucket nor bucket instance specified" << dendl;
    http_ret = -EINVAL;
    return;
  }

  if (!bucket_instance.empty()) {
    http_ret = store->get_bucket_instance_info(obj_ctx, bucket_instance, bucket_info, NULL, NULL);
    if (http_ret < 0) {
      dout(5) << "could not get bucket instance info for bucket instance id=" << bucket_instance << dendl;
      return;
    }
  } else { /* !bucket_name.empty() */
    http_ret = store->get_bucket_info(obj_ctx, bucket_name, bucket_info, NULL, NULL);
    if (http_ret < 0) {
      dout(5) << "could not get bucket info for bucket=" << bucket_name << dendl;
      return;
    }
  }
  map<RGWObjCategory, RGWStorageStats> stats;
  int ret =  store->get_bucket_stats(bucket_info.bucket, &bucket_ver, &master_ver, stats, &max_marker);
  if (ret < 0 && ret != -ENOENT) {
    http_ret = ret;
    return;
  }
}

void RGWOp_BILog_Info::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  s->formatter->open_object_section("info");
  encode_json("bucket_ver", bucket_ver, s->formatter);
  encode_json("master_ver", master_ver, s->formatter);
  encode_json("max_marker", max_marker, s->formatter);
  s->formatter->close_section();

  flusher.flush();
}

void RGWOp_BILog_Delete::execute() {
  string bucket_name = s->info.args.get("bucket"),
         start_marker = s->info.args.get("start-marker"),
         end_marker = s->info.args.get("end-marker"),
         bucket_instance = s->info.args.get("bucket-instance");

  RGWBucketInfo bucket_info;

  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);

  http_ret = 0;
  if ((bucket_name.empty() && bucket_instance.empty()) ||
      end_marker.empty()) {
    dout(5) << "ERROR: one of bucket and bucket instance, and also end-marker is mandatory" << dendl;
    http_ret = -EINVAL;
    return;
  }

  int shard_id;
  http_ret = rgw_bucket_parse_bucket_instance(bucket_instance, &bucket_instance, &shard_id);
  if (http_ret < 0) {
    return;
  }

  if (!bucket_instance.empty()) {
    http_ret = store->get_bucket_instance_info(obj_ctx, bucket_instance, bucket_info, NULL, NULL);
    if (http_ret < 0) {
      dout(5) << "could not get bucket instance info for bucket instance id=" << bucket_instance << dendl;
      return;
    }
  } else { /* !bucket_name.empty() */
    http_ret = store->get_bucket_info(obj_ctx, bucket_name, bucket_info, NULL, NULL);
    if (http_ret < 0) {
      dout(5) << "could not get bucket info for bucket=" << bucket_name << dendl;
      return;
    }
  }
  http_ret = store->trim_bi_log_entries(bucket_info.bucket, shard_id, start_marker, end_marker);
  if (http_ret < 0) {
    dout(5) << "ERROR: trim_bi_log_entries() " << dendl;
  }
  return;
}

void RGWOp_DATALog_List::execute() {
  string   shard = s->info.args.get("id");

  string   st = s->info.args.get("start-time"),
           et = s->info.args.get("end-time"),
           max_entries_str = s->info.args.get("max-entries"),
           marker = s->info.args.get("marker"),
           err;
  utime_t  ut_st, 
           ut_et;
  unsigned shard_id, max_entries = LOG_CLASS_LIST_MAX_ENTRIES;

  shard_id = (unsigned)strict_strtol(shard.c_str(), 10, &err);
  if (!err.empty()) {
    dout(5) << "Error parsing shard_id " << shard << dendl;
    http_ret = -EINVAL;
    return;
  }

  if (parse_date_str(st, ut_st) < 0) {
    http_ret = -EINVAL;
    return;
  }

  if (parse_date_str(et, ut_et) < 0) {
    http_ret = -EINVAL;
    return;
  }

  if (!max_entries_str.empty()) {
    max_entries = (unsigned)strict_strtol(max_entries_str.c_str(), 10, &err);
    if (!err.empty()) {
      dout(5) << "Error parsing max-entries " << max_entries_str << dendl;
      http_ret = -EINVAL;
      return;
    }
  } 
  
  do {
    // Note that last_marker is updated to be the marker of the last
    // entry listed
    http_ret = store->data_log->list_entries(shard_id, ut_st, ut_et, 
					     max_entries, entries, marker,
					     &last_marker, &truncated);
    if (http_ret < 0) 
      break;

    if (!max_entries_str.empty()) 
      max_entries -= entries.size();
  } while (truncated && (max_entries > 0));
}

void RGWOp_DATALog_List::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  s->formatter->open_object_section("log_entries");
  s->formatter->dump_string("marker", last_marker);
  s->formatter->dump_bool("truncated", truncated);
  {
    s->formatter->open_array_section("entries");
    for (list<rgw_data_change>::iterator iter = entries.begin();
	 iter != entries.end(); ++iter) {
      rgw_data_change& entry = *iter;
      encode_json("entry", entry, s->formatter);
      flusher.flush();
    }
    s->formatter->close_section();
  }
  s->formatter->close_section();
  flusher.flush();
}


void RGWOp_DATALog_Info::execute() {
  num_objects = s->cct->_conf->rgw_data_log_num_shards;
  http_ret = 0;
}

void RGWOp_DATALog_Info::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  s->formatter->open_object_section("num_objects");
  s->formatter->dump_unsigned("num_objects", num_objects);
  s->formatter->close_section();
  flusher.flush();
}

void RGWOp_DATALog_ShardInfo::execute() {
  string shard = s->info.args.get("id");
  string err;

  unsigned shard_id = (unsigned)strict_strtol(shard.c_str(), 10, &err);
  if (!err.empty()) {
    dout(5) << "Error parsing shard_id " << shard << dendl;
    http_ret = -EINVAL;
    return;
  }

  http_ret = store->data_log->get_info(shard_id, &info);
}

void RGWOp_DATALog_ShardInfo::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  encode_json("info", info, s->formatter);
  flusher.flush();
}

void RGWOp_DATALog_Lock::execute() {
  string shard_id_str, duration_str, locker_id, zone_id;
  unsigned shard_id;

  http_ret = 0;

  shard_id_str = s->info.args.get("id");
  duration_str = s->info.args.get("length");
  locker_id    = s->info.args.get("locker-id");
  zone_id      = s->info.args.get("zone-id");

  if (shard_id_str.empty() ||
      (duration_str.empty()) ||
      locker_id.empty() ||
      zone_id.empty()) {
    dout(5) << "Error invalid parameter list" << dendl;
    http_ret = -EINVAL;
    return;
  }

  string err;
  shard_id = (unsigned)strict_strtol(shard_id_str.c_str(), 10, &err);
  if (!err.empty()) {
    dout(5) << "Error parsing shard_id param " << shard_id_str << dendl;
    http_ret = -EINVAL;
    return;
  }

  unsigned dur;
  dur = (unsigned)strict_strtol(duration_str.c_str(), 10, &err);
  if (!err.empty() || dur <= 0) {
    dout(5) << "invalid length param " << duration_str << dendl;
    http_ret = -EINVAL;
    return;
  }
  utime_t time(dur, 0);
  http_ret = store->data_log->lock_exclusive(shard_id, time, zone_id, locker_id);
  if (http_ret == -EBUSY)
    http_ret = -ERR_LOCKED;
}

void RGWOp_DATALog_Unlock::execute() {
  string shard_id_str, locker_id, zone_id;
  unsigned shard_id;

  http_ret = 0;

  shard_id_str = s->info.args.get("id");
  locker_id    = s->info.args.get("locker-id");
  zone_id      = s->info.args.get("zone-id");

  if (shard_id_str.empty() ||
      locker_id.empty() ||
      zone_id.empty()) {
    dout(5) << "Error invalid parameter list" << dendl;
    http_ret = -EINVAL;
    return;
  }

  string err;
  shard_id = (unsigned)strict_strtol(shard_id_str.c_str(), 10, &err);
  if (!err.empty()) {
    dout(5) << "Error parsing shard_id param " << shard_id_str << dendl;
    http_ret = -EINVAL;
    return;
  }

  http_ret = store->data_log->unlock(shard_id, zone_id, locker_id);
}

void RGWOp_DATALog_Delete::execute() {
  string   st = s->info.args.get("start-time"),
           et = s->info.args.get("end-time"),
           start_marker = s->info.args.get("start-marker"),
           end_marker = s->info.args.get("end-marker"),
           shard = s->info.args.get("id"),
           err;
  utime_t  ut_st, 
           ut_et;
  unsigned shard_id;

  http_ret = 0;

  shard_id = (unsigned)strict_strtol(shard.c_str(), 10, &err);
  if (!err.empty()) {
    dout(5) << "Error parsing shard_id " << shard << dendl;
    http_ret = -EINVAL;
    return;
  }
  if (et.empty() && end_marker.empty()) { /* bounding end */
    http_ret = -EINVAL;
    return;
  }

  if (parse_date_str(st, ut_st) < 0) {
    http_ret = -EINVAL;
    return;
  }

  if (parse_date_str(et, ut_et) < 0) {
    http_ret = -EINVAL;
    return;
  }

  http_ret = store->data_log->trim_entries(shard_id, ut_st, ut_et, start_marker, end_marker);
}

RGWOp *RGWHandler_Log::op_get() {
  bool exists;
  string type = s->info.args.get("type", &exists);

  if (!exists) {
    return NULL;
  }

  if (type.compare("metadata") == 0) {
    if (s->info.args.exists("id")) {
      if (s->info.args.exists("info")) {
        return new RGWOp_MDLog_ShardInfo;
      } else {
        return new RGWOp_MDLog_List;
      }
    } else {
      return new RGWOp_MDLog_Info;
    }
  } else if (type.compare("bucket-index") == 0) {
    if (s->info.args.exists("info")) {
      return new RGWOp_BILog_Info;
    } else {
      return new RGWOp_BILog_List;
    }
  } else if (type.compare("data") == 0) {
    if (s->info.args.exists("id")) {
      if (s->info.args.exists("info")) {
        return new RGWOp_DATALog_ShardInfo;
      } else {
        return new RGWOp_DATALog_List;
      }
    } else {
      return new RGWOp_DATALog_Info;
    }
  }
  return NULL;
}

RGWOp *RGWHandler_Log::op_delete() {
  bool exists;
  string type = s->info.args.get("type", &exists);

  if (!exists) {
    return NULL;
  }

  if (type.compare("metadata") == 0)
    return new RGWOp_MDLog_Delete;
  else if (type.compare("bucket-index") == 0) 
    return new RGWOp_BILog_Delete;
  else if (type.compare("data") == 0)
    return new RGWOp_DATALog_Delete;
  return NULL;
}

RGWOp *RGWHandler_Log::op_post() {
  bool exists;
  string type = s->info.args.get("type", &exists);

  if (!exists) {
    return NULL;
  }

  if (type.compare("metadata") == 0) {
    if (s->info.args.exists("lock"))
      return new RGWOp_MDLog_Lock;
    else if (s->info.args.exists("unlock"))
      return new RGWOp_MDLog_Unlock;
  } else if (type.compare("data") == 0) {
    if (s->info.args.exists("lock"))
      return new RGWOp_DATALog_Lock;
    else if (s->info.args.exists("unlock"))
      return new RGWOp_DATALog_Unlock;
  }
  return NULL;
}

