// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <iostream>
#include <sstream>
#include <string>

using namespace std;

#include "auth/Crypto.h"

#include "common/armor.h"
#include "common/ceph_json.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/Formatter.h"
#include "common/errno.h"

#include "global/global_init.h"

#include "include/utime.h"
#include "include/str_list.h"

#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_log.h"
#include "rgw_formats.h"
#include "rgw_usage.h"
#include "rgw_replica_log.h"
#include "rgw_object_expirer_core.h"

#include "cls/lock/cls_lock_client.h"

#define dout_subsys ceph_subsys_rgw

static string objexp_lock_name = "gc_process";

int RGWObjectExpirer::init_bucket_info(const string& tenant_name,
                                       const string& bucket_name,
                                       const string& bucket_id,
                                       RGWBucketInfo& bucket_info)
{
  RGWObjectCtx obj_ctx(store);

  /*
   * XXX Here's where it gets tricky. We went to all the trouble of
   * punching the tenant through the objexp_hint_entry, but now we
   * find that our instances do not actually have tenants. They are
   * unique thanks to IDs. So the tenant string is not needed...
   */
  const string bucket_instance_id = bucket_name + ":" + bucket_id;
  int ret = store->get_bucket_instance_info(obj_ctx, bucket_instance_id,
          bucket_info, NULL, NULL);
  return ret;
}

int RGWObjectExpirer::garbage_single_object(objexp_hint_entry& hint)
{
  RGWBucketInfo bucket_info;

  int ret = init_bucket_info(hint.tenant, hint.bucket_name,
          hint.bucket_id, bucket_info);
  if (-ENOENT == ret) {
    ldout(store->ctx(), 15) << "NOTICE: cannot find bucket = " \
        << hint.bucket_name << ". The object must be already removed" << dendl;
    return -ERR_PRECONDITION_FAILED;
  } else if (ret < 0) {
    ldout(store->ctx(),  1) << "ERROR: could not init bucket = " \
        << hint.bucket_name << "due to ret = " << ret << dendl;
    return ret;
  }

  RGWObjectCtx rctx(store);

  rgw_obj_key key = hint.obj_key;
  if (key.instance.empty()) {
    key.instance = "null";
  }

  rgw_obj obj(bucket_info.bucket, key);
  store->set_atomic(&rctx, obj);
  ret = store->delete_obj(rctx, bucket_info, obj,
          bucket_info.versioning_status(), 0, hint.exp_time);

  return ret;
}

void RGWObjectExpirer::garbage_chunk(list<cls_timeindex_entry>& entries,      /* in  */
                                  bool& need_trim)                         /* out */
{
  need_trim = false;

  for (list<cls_timeindex_entry>::iterator iter = entries.begin();
       iter != entries.end();
       ++iter)
  {
    objexp_hint_entry hint;
    ldout(store->ctx(), 15) << "got removal hint for: " << iter->key_ts.sec() \
        << " - " << iter->key_ext << dendl;

    int ret = store->objexp_hint_parse(*iter, hint);
    if (ret < 0) {
      ldout(store->ctx(), 1) << "cannot parse removal hint for " << hint.obj_key << dendl;
      continue;
    }

    /* PRECOND_FAILED simply means that our hint is not valid.
     * We can silently ignore that and move forward. */
    ret = garbage_single_object(hint);
    if (ret == -ERR_PRECONDITION_FAILED) {
      ldout(store->ctx(), 15) << "not actual hint for object: " << hint.obj_key << dendl;
    } else if (ret < 0) {
      ldout(store->ctx(), 1) << "cannot remove expired object: " << hint.obj_key << dendl;
    }

    need_trim = true;
  }

  return;
}

void RGWObjectExpirer::trim_chunk(const string& shard,
                               const utime_t& from,
                               const utime_t& to)
{
  ldout(store->ctx(), 20) << "trying to trim removal hints to  " << to << dendl;

  real_time rt_from = from.to_real_time();
  real_time rt_to = to.to_real_time();

  int ret = store->objexp_hint_trim(shard, rt_from, rt_to);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR during trim: " << ret << dendl;
  }

  return;
}

void RGWObjectExpirer::process_single_shard(const string& shard,
                                         const utime_t& last_run,
                                         const utime_t& round_start)
{
  string marker;
  string out_marker;
  bool truncated = false;

  CephContext *cct = store->ctx();
  int num_entries = cct->_conf->rgw_objexp_chunk_size;

  int max_secs = cct->_conf->rgw_objexp_gc_interval;
  utime_t end = ceph_clock_now(cct);
  end += max_secs;

  rados::cls::lock::Lock l(objexp_lock_name);

  utime_t time(max_secs, 0);
  l.set_duration(time);

  int ret = l.lock_exclusive(&store->objexp_pool_ctx, shard);
  if (ret == -EBUSY) { /* already locked by another processor */
    dout(5) << __func__ << "(): failed to acquire lock on " << shard << dendl;
    return;
  }
  do {
    real_time rt_last = last_run.to_real_time();
    real_time rt_start = round_start.to_real_time();

    list<cls_timeindex_entry> entries;
    ret = store->objexp_hint_list(shard, rt_last, rt_start,
                                      num_entries, marker, entries,
                                      &out_marker, &truncated);
    if (ret < 0) {
      ldout(cct, 10) << "cannot get removal hints from shard: " << shard << dendl;
      continue;
    }

    bool need_trim;
    garbage_chunk(entries, need_trim);

    if (need_trim) {
      trim_chunk(shard, last_run, round_start);
    }

    utime_t now = ceph_clock_now(g_ceph_context);
    if (now >= end) {
      break;
    }

    marker = out_marker;
  } while (truncated);

  l.unlock(&store->objexp_pool_ctx, shard);
  return;
}

void RGWObjectExpirer::inspect_all_shards(const utime_t& last_run, const utime_t& round_start)
{
  utime_t shard_marker;

  CephContext *cct = store->ctx();
  int num_shards = cct->_conf->rgw_objexp_hints_num_shards;

  for (int i = 0; i < num_shards; i++) {
    string shard;
    store->objexp_get_shard(i, shard);

    ldout(store->ctx(), 20) << "proceeding shard = " << shard << dendl;

    process_single_shard(shard, last_run, round_start);
  }

  return;
}

bool RGWObjectExpirer::going_down()
{
  return (down_flag.read() != 0);
}

void RGWObjectExpirer::start_processor()
{
  worker = new OEWorker(store->ctx(), this);
  worker->create("rgw_obj_expirer");
}

void RGWObjectExpirer::stop_processor()
{
  down_flag.set(1);
  if (worker) {
    worker->stop();
    worker->join();
  }
  delete worker;
  worker = NULL;
}

void *RGWObjectExpirer::OEWorker::entry() {
  utime_t last_run;
  do {
    utime_t start = ceph_clock_now(cct);
    ldout(cct, 2) << "object expiration: start" << dendl;
    oe->inspect_all_shards(last_run, start);
    ldout(cct, 2) << "object expiration: stop" << dendl;

    last_run = start;

    if (oe->going_down())
      break;

    utime_t end = ceph_clock_now(cct);
    end -= start;
    int secs = cct->_conf->rgw_objexp_gc_interval;

    if (secs <= end.sec())
      continue; // next round

    secs -= end.sec();

    lock.Lock();
    cond.WaitInterval(cct, lock, utime_t(secs, 0));
    lock.Unlock();
  } while (!oe->going_down());

  return NULL;
}

void RGWObjectExpirer::OEWorker::stop()
{
  Mutex::Locker l(lock);
  cond.Signal();
}

