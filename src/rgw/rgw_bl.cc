// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <string.h>
#include <iostream>
#include <map>

#include <common/errno.h>
#include "auth/Crypto.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/lock/cls_lock_client.h"
#include "rgw_common.h"
#include "rgw_bucket.h"
#include "rgw_bl.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace librados;

const char* BL_STATUS[] = {
  "UNINITIAL",
  "PROCESSING",
  "FAILED",
  "COMPLETE"
};

void *RGWBL::BLWorker::entry() {
  do {
    utime_t start = ceph_clock_now();
    if (should_work(start)) {
      dout(5) << "bucket logging deliver: start" << dendl;
      int r = bl->process();
      if (r < 0) {
        dout(0) << "ERROR: bucket logging process() err=" << r << dendl;
      }
      dout(5) << "bucket logging deliver: stop" << dendl;
    }
    if (bl->going_down())
      break;

    utime_t end = ceph_clock_now();
    int secs = schedule_next_start_time(end);
    time_t next_time = end + secs;
    char buf[30];
    char *nt = ctime_r(&next_time, buf);
    dout(5) << "schedule bucket logging deliver next start time: "
            << nt <<dendl;

    lock.Lock();
    cond.WaitInterval(lock, utime_t(secs, 0));
    lock.Unlock();
  } while (!bl->going_down());

  return nullptr;
}

void RGWBL::initialize(CephContext *_cct, RGWRados *_store) {
  cct = _cct;
  store = _store;
  max_objs = cct->_conf->rgw_bl_max_objs;
  if (max_objs > BL_HASH_PRIME)
    max_objs = BL_HASH_PRIME;

  obj_names = new string[max_objs];

  for (int i = 0; i < max_objs; i++) {
    obj_names[i] = bl_oid_prefix;
    char buf[32];
    snprintf(buf, 32, ".%d", i);
    obj_names[i].append(buf); // bl.X
  }

#define BL_COOKIE_LEN 16
  char cookie_buf[BL_COOKIE_LEN + 1];
  gen_rand_alphanumeric(cct, cookie_buf, sizeof(cookie_buf) - 1);
  cookie = cookie_buf;
}

bool RGWBL::if_already_run_today(time_t& start_date)
{
  struct tm bdt;
  time_t begin_of_day;
  utime_t now = ceph_clock_now();
  localtime_r(&start_date, &bdt);

  bdt.tm_hour = 0;
  bdt.tm_min = 0;
  bdt.tm_sec = 0;
  begin_of_day = mktime(&bdt);
  if (now - begin_of_day < 24*60*60)
    return true;
  else
    return false;
}

void RGWBL::finalize()
{
  delete[] obj_names;
}

int RGWBL::bucket_bl_prepare(int index)
{
  map<string, int > entries;

  string marker;

#define MAX_BL_LIST_ENTRIES 100
  do {
    int ret = cls_rgw_bl_list(store->bl_pool_ctx,
                              obj_names[index], marker,
                              MAX_BL_LIST_ENTRIES, entries);
    if (ret < 0)
      return ret;
    for (auto iter = entries.begin(); iter != entries.end(); ++iter) {
      pair<string, int> entry(iter->first, bl_uninitial);
      ret = cls_rgw_bl_set_entry(store->bl_pool_ctx, obj_names[index], entry);
      if (ret < 0) {
        dout(0) << "RGWBL::bucket_bl_prepare() failed to set entry "
                << obj_names[index] << dendl;
        break;
      }
      marker = iter->first;
    }
  } while (!entries.empty());

  return 0;
}

static vector<string> &split_shard_id(const string &s, char delim,
				      vector<string> &elems) {
  stringstream ss(s);
  string item;
  while (getline(ss, item, delim)) {
    elems.push_back(item);
  }
  return elems;
}

static vector<string> split_shard_id(const string &s, char delim) {
  vector<std::string> elems;
  split_shard_id(s, delim, elems);
  return elems;
}

int RGWBL::bucket_bl_process(string& shard_id)
{
  RGWBucketLoggingStatus status(cct);
  RGWBucketInfo bucket_info;
  map<string, bufferlist> bucket_attrs;
  RGWObjectCtx obj_ctx(store);

  vector<std::string> result;
  result = split_shard_id(shard_id, ':');
  string bucket_tenant = result[0];
  string bucket_name = result[1];
  string bucket_id = result[2];

  int ret = store->get_bucket_info(obj_ctx, bucket_tenant, bucket_name,
                                   bucket_info, NULL, &bucket_attrs);
  if (ret < 0) {
    ldout(cct, 0) << "RGWBL:get_bucket_info failed, bucket_name="
                  << bucket_name << dendl;
    return ret;
  }

  ret = bucket_info.bucket.bucket_id.compare(bucket_id) ;
  if (ret != 0) {
    ldout(cct, 0) << "RGWBL:old bucket id found, bucket_name=" << bucket_name
                  << "should be deleted." << dendl;
    return -ENOENT;
  }

  map<string, bufferlist>::iterator aiter = bucket_attrs.find(RGW_ATTR_BL);
  if (aiter == bucket_attrs.end())
    return 0;

  bufferlist::iterator iter(&aiter->second);
  try {
    status.decode(iter);
  } catch (const buffer::error& e) {
    ldout(cct, 0) << __func__ <<  "decode bucket logging status failed" << dendl;
    return -1;
  }

  return ret;
}

int RGWBL::bucket_bl_post(int index, int max_lock_sec,
                          pair<string, int>& entry, int& result)
{
  utime_t lock_duration(cct->_conf->rgw_bl_lock_max_time, 0);

  rados::cls::lock::Lock l(bl_index_lock_name);
  l.set_cookie(cookie);
  l.set_duration(lock_duration);

  do {
    int ret = l.lock_exclusive(&store->bl_pool_ctx, obj_names[index]);
    if (ret == -EBUSY) { /* already locked by another bl processor */
      dout(0) << "RGWBL::bucket_bl_post() failed to acquire lock on, sleep 5, try again. "
              << "obj " << obj_names[index] << dendl;
      sleep(5);
      continue;
    }
    if (ret < 0)
      return 0;
    dout(20) << "RGWBL::bucket_bl_post() get lock" << obj_names[index] << dendl;
    if (result == -ENOENT) {
      ret = cls_rgw_bl_rm_entry(store->bl_pool_ctx, obj_names[index],  entry);
      if (ret < 0) {
        dout(0) << "RGWBL::bucket_bl_post() failed to remove entry "
                << obj_names[index] << dendl;
      }
      goto clean;
    } else if (result < 0) {
      entry.second = bl_failed;
    } else {
      entry.second = bl_complete;
    }

    ret = cls_rgw_bl_set_entry(store->bl_pool_ctx, obj_names[index],  entry);
    if (ret < 0) {
      dout(0) << "RGWBL::process() failed to set entry "
              << obj_names[index] << dendl;
    }
 clean:
    l.unlock(&store->bl_pool_ctx, obj_names[index]);
    dout(20) << "RGWBL::bucket_bl_post() unlock" << obj_names[index] << dendl;
    return 0;
  } while (true);
}

int RGWBL::list_bl_progress(const string& marker, uint32_t max_entries,
                            map<string, int> *progress_map)
{
  int index = 0;
  progress_map->clear();
  for(; index < max_objs; index++) {
    map<string, int > entries;
    int ret = cls_rgw_bl_list(store->bl_pool_ctx, obj_names[index],
                              marker, max_entries, entries);
    if (ret < 0) {
      dout(0) << __func__ << " can't list on bl object=" << obj_names[index]
              << " ret=" << ret << dendl;
    }
    map<string, int>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      progress_map->insert(*iter);
    }
  }
  return 0;
}

int RGWBL::process()
{
  int max_secs = cct->_conf->rgw_bl_lock_max_time;

  unsigned start;
  int ret = get_random_bytes((char *)&start, sizeof(start));
  if (ret < 0)
    return ret;

  for (int i = 0; i < max_objs; i++) {
    int index = (i + start) % max_objs;
    ret = process(index, max_secs);
    if (ret < 0)
      return ret;
  }

  return 0;
}

int RGWBL::process(int index, int max_lock_secs)
{
  rados::cls::lock::Lock l(bl_index_lock_name);
  do {
    utime_t now = ceph_clock_now();
    pair<string, int> entry; // string = bucket_name:bucket_id ,int = BL_BUCKET_STATUS
    if (max_lock_secs <= 0)
      return -EAGAIN;

    utime_t time(max_lock_secs, 0);
    l.set_duration(time);

    int ret = l.lock_exclusive(&store->bl_pool_ctx, obj_names[index]);
    if (ret == -EBUSY) { /* already locked by another bl processor */
      dout(0) << "RGWBL::process() failed to acquire lock on,"
              << " sleep 5, try again"
              << "obj " << obj_names[index] << dendl;
      sleep(5);
      continue;
    }
    if (ret < 0)
      return 0;

    string marker;
    cls_rgw_bl_obj_head head;
    ret = cls_rgw_bl_get_head(store->bl_pool_ctx, obj_names[index], head);
    if (ret < 0) {
      dout(0) << "RGWBL::process() failed to get obj head "
              << obj_names[index] << ret << dendl;
      goto exit;
    }

    if(!if_already_run_today(head.start_date)) {
      head.start_date = now;
      head.marker.clear();
      ret = bucket_bl_prepare(index);
      if (ret < 0) {
        dout(0) << "RGWBL::process() failed to update bl object "
                << obj_names[index] << ret << dendl;
        goto exit;
      }
    }

    ret = cls_rgw_bl_get_next_entry(store->bl_pool_ctx, obj_names[index],
                                    head.marker, entry);
    if (ret < 0) {
      dout(0) << "RGWBL::process() failed to get obj entry "
              <<  obj_names[index] << dendl;
      goto exit;
    }

    if (entry.first.empty())
      goto exit;

    entry.second = bl_processing;
    ret = cls_rgw_bl_set_entry(store->bl_pool_ctx, obj_names[index],  entry);
    if (ret < 0) {
      dout(0) << "RGWBL::process() failed to set obj entry "
              << obj_names[index] << entry.first << entry.second << dendl;
      goto exit;
    }

    head.marker = entry.first;
    ret = cls_rgw_bl_put_head(store->bl_pool_ctx, obj_names[index],  head);
    if (ret < 0) {
      dout(0) << "RGWBL::process() failed to put head "
              << obj_names[index] << dendl;
      goto exit;
    }

    l.unlock(&store->bl_pool_ctx, obj_names[index]);
    ret = bucket_bl_process(entry.first);
    ret = bucket_bl_post(index, max_lock_secs, entry, ret);
    return 0;

 exit:
    l.unlock(&store->bl_pool_ctx, obj_names[index]);
    return 0;

  }while(1);

}

void RGWBL::start_processor()
{
  worker = new BLWorker(cct, this);
  worker->create("bl");
}

void RGWBL::stop_processor()
{
  down_flag.set(1);
  if (worker) {
    worker->stop();
    worker->join();
  }
  delete worker;
  worker = nullptr;
}

void RGWBL::BLWorker::stop()
{
  Mutex::Locker l(lock);
  cond.Signal();
}

bool RGWBL::going_down()
{
  return (down_flag.read() != 0);
}

bool RGWBL::BLWorker::should_work(utime_t& now)
{
  int start_hour;
  int start_minute;
  int end_hour;
  int end_minute;
  string worktime = cct->_conf->rgw_bl_work_time;
  sscanf(worktime.c_str(),"%d:%d-%d:%d",
         &start_hour, &start_minute, &end_hour, &end_minute);
  struct tm bdt;
  time_t tt = now.sec();
  localtime_r(&tt, &bdt);

  if ((bdt.tm_hour*60 + bdt.tm_min >= start_hour*60 + start_minute) &&
      (bdt.tm_hour*60 + bdt.tm_min <= end_hour*60 + end_minute)) {
    return true;
  } else {
    return false;
  }

}

int RGWBL::BLWorker::schedule_next_start_time(utime_t& now)
{
  int start_hour;
  int start_minute;
  int end_hour;
  int end_minute;
  string worktime = cct->_conf->rgw_bl_work_time;
  sscanf(worktime.c_str(),"%d:%d-%d:%d",&start_hour, &start_minute, &end_hour, &end_minute);
  struct tm bdt;
  time_t tt = now.sec();
  time_t nt;
  localtime_r(&tt, &bdt);
  bdt.tm_hour = start_hour;
  bdt.tm_min = start_minute;
  bdt.tm_sec = 0;
  nt = mktime(&bdt);

  return (nt+24*60*60 - tt);
}
