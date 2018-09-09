// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>
#include <iostream>
#include <map>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>

#include "common/Formatter.h"
#include <common/errno.h>
#include "include/random.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/lock/cls_lock_client.h"
#include "rgw_common.h"
#include "rgw_bucket.h"
#include "rgw_sts.h"
#include "rgw_user.h"
#include "rgw_rest_user.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace librados;

void *RGWSTS::STSWorker::entry() {
  do {
    utime_t start = ceph_clock_now();
    if (should_work(start)) {
      ldout(cct, 2) << "sts: start" << dendl;
      int r = sts->process();
      if (r < 0) {
        ldout(cct, 0) << "ERROR: do sts process() returned error r=" << r << dendl;
      }
      ldout(cct, 2) << "sts: stop" << dendl;
    }
    if (sts->going_down())
      break;

    utime_t end = ceph_clock_now();
    int secs = schedule_next_start_time(start, end);
    utime_t next;
    next.set_from_double(end + secs);

    ldout(cct, 5) << "schedule sts next start time: " << rgw_to_asctime(next) << dendl;

    lock.Lock();
    cond.WaitInterval(lock, utime_t(secs, 0));
    lock.Unlock();
  } while (!sts->going_down());

  return NULL;
}

void RGWSTS::initialize(CephContext *_cct, RGWRados *_store) {
  cct = _cct;
  store = _store;
  max_objs = 32;  //cct->_conf->rgw_sts_max_objs;
  if (max_objs > HASH_PRIME)
    max_objs = HASH_PRIME;

  obj_names = new string[max_objs];

  for (int i = 0; i < max_objs; i++) {
    obj_names[i] = sts_oid_prefix;
    char buf[32];
    snprintf(buf, 32, ".%d", i);
    obj_names[i].append(buf);
  }

#define COOKIE_LEN 16
  char cookie_buf[COOKIE_LEN + 1];
  gen_rand_alphanumeric(cct, cookie_buf, sizeof(cookie_buf) - 1);
  cookie = cookie_buf;
}

void RGWSTS::finalize()
{
  delete[] obj_names;
}

bool RGWSTS::if_already_run_today(time_t& start_date)
{
  struct tm bdt;
  time_t begin_of_day;
  utime_t now = ceph_clock_now();
  localtime_r(&start_date, &bdt);

  if (cct->_conf->rgw_sts_debug_interval > 0) {
    if (now - start_date < cct->_conf->rgw_lc_debug_interval)
      return true;
    else
      return false;
  }

  bdt.tm_hour = 0;
  bdt.tm_min = 0;
  bdt.tm_sec = 0;
  begin_of_day = mktime(&bdt);
  if (now - begin_of_day < 24*60*60)
    return true;
  else
    return false;
}

int RGWSTS::user_sts_process(const string& access_key, int64_t expire)
{
  if (expire <= ceph_clock_now().sec()) {
    string uid_str;
    string subuser;
    string key_type_str;
    rgw_user uid(uid_str);

    RGWUserAdminOpState op_state;
    op_state.set_user_id(uid);
    op_state.set_subuser(subuser);
    op_state.set_access_key(access_key);
    op_state.set_key_type(KEY_TYPE_S3);

    RGWRESTFlusher flusher;
    int http_ret = RGWUserAdminOp_Key::remove(store, op_state, flusher);
    if ( http_ret < 0) {
      ldout(cct, 0) << "RGWSTS remove key failed, http_ret: " << http_ret  << dendl; 
      return 0;
    }
    return -ENOENT;
  }
  return 0;
}

int RGWSTS::user_sts_post(int index, int max_lock_sec, pair<string, int >& entry, int& result)
{
  utime_t lock_duration(cct->_conf->rgw_sts_lock_max_time, 0);

  rados::cls::lock::Lock l(sts_index_lock_name);
  l.set_cookie(cookie);
  l.set_duration(lock_duration);

  do {
    int ret = l.lock_exclusive(&store->sts_pool_ctx, obj_names[index]);
    if (ret == -EBUSY) { /* already locked by another sts processor */
      ldout(cct, 0) << "RGWSTS::user_sts_post() failed to acquire lock on "
          << obj_names[index] << ", sleep 5, try again" << dendl;
      sleep(5);
      continue;
    }
    if (ret < 0)
      return 0;
    ldout(cct, 20) << "RGWSTS::user_sts_post() lock " << obj_names[index] << dendl;
    if (result ==  -ENOENT) {
      ret = cls_rgw_sts_rm_entry(store->sts_pool_ctx, obj_names[index],  entry);
      if (ret < 0) {
        ldout(cct, 0) << "RGWSTS::user_sts_post() failed to remove entry "
            << obj_names[index] << dendl;
      }
      goto clean;
    } 

clean:
    l.unlock(&store->sts_pool_ctx, obj_names[index]);
    ldout(cct, 20) << "RGWSTS::user_sts_post() unlock " << obj_names[index] << dendl;
    return 0;
  } while (true);
}

int RGWSTS::list_sts_progress(const string& marker, uint32_t max_entries, map<string, int> *progress_map)
{
  int index = 0;
  progress_map->clear();
  for(; index <max_objs; index++) {
    map<string, int > entries;
    int ret = cls_rgw_sts_list(store->sts_pool_ctx, obj_names[index], marker, max_entries, entries);
    if (ret < 0) {
      if (ret == -ENOENT) {
        ldout(cct, 10) << __func__ << "() ignoring unfound sts object="
                             << obj_names[index] << dendl;
        continue;
      } else {
        return ret;
      }
    }
    map<string, int>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      progress_map->insert(*iter);
    }
  }
  return 0;
}

int RGWSTS::process()
{
  int max_secs = cct->_conf->rgw_lc_lock_max_time;

  const int start = ceph::util::generate_random_number(0, max_objs - 1);

  for (int i = 0; i < max_objs; i++) {
    int index = (i + start) % max_objs;
    int ret = process(index, max_secs);
    if (ret < 0)
      return ret;
  }

  return 0;
}

int RGWSTS::process(int index, int max_lock_secs)
{
  rados::cls::lock::Lock l(sts_index_lock_name);
  do {
    utime_t now = ceph_clock_now();
    pair<string, int> entry;
    if (max_lock_secs <= 0)
      return -EAGAIN;

    utime_t time(max_lock_secs, 0);
    l.set_duration(time);

    int ret = l.lock_exclusive(&store->sts_pool_ctx, obj_names[index]);
    if (ret == -EBUSY) { /* already locked by another sts processor */
      ldout(cct, 0) << "RGWSTS::process() failed to acquire lock on "
          << obj_names[index] << ", sleep 5, try again" << dendl;
      sleep(5);
      continue;
    }
    if (ret < 0)
      return 0;

    cls_rgw_sts_obj_head head;
    ret = cls_rgw_sts_get_head(store->sts_pool_ctx, obj_names[index], head);
    if (ret < 0) {
      ldout(cct, 0) << "RGWSTS::process() failed to get obj head "
          << obj_names[index] << ", ret=" << ret << dendl;
      goto exit;
    }

    if(!if_already_run_today(head.start_date)) {
      head.start_date = now;
      head.marker.clear();
      if (ret < 0) {
      ldout(cct, 0) << "RGWSTS::process() failed to update sts object "
          << obj_names[index] << ", ret=" << ret << dendl;
      goto exit;
      }
    }

    ret = cls_rgw_sts_get_next_entry(store->sts_pool_ctx, obj_names[index], head.marker, entry);
    if (ret < 0) {
      ldout(cct, 0) << "RGWSTS::process() failed to get obj entry "
          << obj_names[index] << dendl;
      goto exit;
    }

    if (entry.first.empty())
      goto exit;

    head.marker = entry.first;
    ret = cls_rgw_sts_put_head(store->sts_pool_ctx, obj_names[index],  head);
    if (ret < 0) {
      ldout(cct, 0) << "RGWSTS::process() failed to put head " << obj_names[index] << dendl;
      goto exit;
    }
    l.unlock(&store->sts_pool_ctx, obj_names[index]);
    ret = user_sts_process(entry.first, entry.second);
    user_sts_post(index, max_lock_secs, entry, ret);
  }while(1);

exit:
    l.unlock(&store->sts_pool_ctx, obj_names[index]);
    return 0;
}

void RGWSTS::start_processor()
{
  worker = new STSWorker(cct, this);
  worker->create("sts_thr");
}

void RGWSTS::stop_processor()
{
  down_flag = true;
  if (worker) {
    worker->stop();
    worker->join();
  }
  delete worker;
  worker = NULL;
}

void RGWSTS::STSWorker::stop()
{
  Mutex::Locker l(lock);
  cond.Signal();
}

bool RGWSTS::going_down()
{
  return down_flag;
}

bool RGWSTS::STSWorker::should_work(utime_t& now)
{
  int start_hour;
  int start_minute;
  int end_hour;
  int end_minute;
  string worktime = cct->_conf->rgw_sts_work_time;
  sscanf(worktime.c_str(),"%d:%d-%d:%d",&start_hour, &start_minute, &end_hour, &end_minute);
  struct tm bdt;
  time_t tt = now.sec();
  localtime_r(&tt, &bdt);

  if (cct->_conf->rgw_sts_debug_interval > 0) {
	  /* We're debugging, so say we can run */
	  return true;
  } else if ((bdt.tm_hour*60 + bdt.tm_min >= start_hour*60 + start_minute) &&
		     (bdt.tm_hour*60 + bdt.tm_min <= end_hour*60 + end_minute)) {
	  return true;
  } else {
	  return false;
  }

}

int RGWSTS::STSWorker::schedule_next_start_time(utime_t &start, utime_t& now)
{
  int secs;

  if (cct->_conf->rgw_sts_debug_interval > 0) {
	secs = start + cct->_conf->rgw_sts_debug_interval - now;
	if (secs < 0)
	  secs = 0;
	return (secs);
  }

  int start_hour;
  int start_minute;
  int end_hour;
  int end_minute;
  string worktime = cct->_conf->rgw_sts_work_time;
  sscanf(worktime.c_str(),"%d:%d-%d:%d",&start_hour, &start_minute, &end_hour, &end_minute);
  struct tm bdt;
  time_t tt = now.sec();
  time_t nt;
  localtime_r(&tt, &bdt);
  bdt.tm_hour = start_hour;
  bdt.tm_min = start_minute;
  bdt.tm_sec = 0;
  nt = mktime(&bdt);
  secs = nt - tt;

  return secs>0 ? secs : secs+24*60*60;
}

