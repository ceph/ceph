#include <string.h>
#include <iostream>
#include <map>

#include "include/types.h"

#include "common/Formatter.h"
#include <common/errno.h>
#include "auth/Crypto.h"
#include "include/rados/librados.hpp"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/lock/cls_lock_client.h"
#include <common/dout.h>
#include "rgw_common.h"
#include "rgw_bucket.h"
#include "rgw_lc.h"
#include "rgw_lc_s3.h"



#define dout_subsys ceph_subsys_rgw

const char* LC_STATUS[] = {
      "UNINITIAL",
      "PROCESSING",
      "FAILED",
      "COMPLETE"
};

using namespace std;
using namespace librados;
void RGWLifecycleConfiguration::add_rule(LCRule *rule)
{
  string id;
  rule->get_id(id); // not that this will return false for groups, but that's ok, we won't search groups
  rule_map.insert(pair<string, LCRule>(id, *rule));
  _add_rule(rule);
}

void RGWLifecycleConfiguration::_add_rule(LCRule *rule)
{
  prefix_map[rule->get_prefix()] = rule->get_expiration().get_days();
}

void *RGWLC::LCWorker::entry() {
  do {
    utime_t start = ceph_clock_now(cct);
    if (should_work(start)) {
      dout(5) << "life cycle: start" << dendl;
      int r = lc->process();
      if (r < 0) {
        dout(0) << "ERROR: do life cycle process() returned error r=" << r << dendl;
      }
      dout(5) << "life cycle: stop" << dendl;
    }
    if (lc->going_down())
      break;

    utime_t end = ceph_clock_now(cct);
    int secs = schedule_next_start_time(start, end);
    time_t next_time = end + secs;
    char buf[30];
    char *nt = ctime_r(&next_time, buf);
    dout(5) << "schedule life cycle next start time: " << nt <<dendl;

    lock.Lock();
    cond.WaitInterval(cct, lock, utime_t(secs, 0));
    lock.Unlock();
  } while (!lc->going_down());

  return NULL;
}

void RGWLC::initialize(CephContext *_cct, RGWRados *_store) {
  cct = _cct;
  store = _store;
  max_objs = cct->_conf->rgw_lc_max_objs;
  if (max_objs > HASH_PRIME)
    max_objs = HASH_PRIME;

  obj_names = new string[max_objs];

  for (int i = 0; i < max_objs; i++) {
    obj_names[i] = lc_oid_prefix;
    char buf[32];
    snprintf(buf, 32, ".%d", i);
    obj_names[i].append(buf);
  }

#define COOKIE_LEN 16
  char cookie_buf[COOKIE_LEN + 1];
  gen_rand_alphanumeric(cct, cookie_buf, sizeof(cookie_buf) - 1);
  cookie = cookie_buf;
}

void RGWLC::finalize()
{
  delete[] obj_names;
}

bool RGWLC::if_already_run_today(time_t& start_date)
{
  struct tm bdt;
  time_t begin_of_day;
  utime_t now = ceph_clock_now(cct);
  localtime_r(&start_date, &bdt);

  if (cct->_conf->rgw_lc_debug_interval > 0) {
	  /* We're debugging, so say we can run */
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

static std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
      elems.push_back(item);
  }
  return elems;
}

static std::vector<std::string> split(const std::string &s, char delim) {
  std::vector<std::string> elems;
  split(s, delim, elems);
  return elems;
}

int RGWLC::bucket_lc_prepare(int index)
{
  map<string, int > entries;

  string marker;

#define MAX_LC_LIST_ENTRIES 100
  do {
    int ret = cls_rgw_lc_list(store->lc_pool_ctx, obj_names[index], marker, MAX_LC_LIST_ENTRIES, entries);
    if (ret < 0)
      return ret;
    map<string, int>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      pair<string, int > entry(iter->first, lc_uninitial);
      ret = cls_rgw_lc_set_entry(store->lc_pool_ctx, obj_names[index],  entry);
      if (ret < 0) {
        dout(0) << "RGWLC::bucket_lc_prepare() failed to set entry " << obj_names[index] << dendl;
        break;
      }
      marker = iter->first;
    }
  } while (!entries.empty());

  return 0;
}

bool RGWLC::obj_has_expired(double timediff, int days)
{
	double cmp;

	if (cct->_conf->rgw_lc_debug_interval <= 0) {
		/* Normal case, run properly */
		cmp = days*24*60*60;
	} else {
		/* We're in debug mode; Treat each rgw_lc_debug_interval seconds as a day */
		cmp = days*cct->_conf->rgw_lc_debug_interval;
	}

	return (timediff >= cmp);
}

int RGWLC::bucket_lc_process(string& shard_id)
{
  RGWLifecycleConfiguration  config(cct);
  RGWBucketInfo bucket_info;
  map<string, bufferlist> bucket_attrs;
  string next_marker, no_ns, list_versions;
  bool is_truncated;
  bool default_config = false;
  int default_days = 0;
  vector<RGWObjEnt> objs;
  RGWObjectCtx obj_ctx(store);
  vector<std::string> result;
  result = split(shard_id, ':');
  string bucket_tenant = result[0];
  string bucket_name = result[1];
  string bucket_id = result[2];
  int ret = store->get_bucket_info(obj_ctx, bucket_tenant, bucket_name, bucket_info, NULL, &bucket_attrs);
  if (ret < 0) {
    ldout(cct, 0) << "LC:get_bucket_info failed" << bucket_name <<dendl;
    return ret;
  }

  ret = bucket_info.bucket.bucket_id.compare(bucket_id) ;
  if (ret !=0) {
    ldout(cct, 0) << "LC:old bucket id find, should be delete" << bucket_name <<dendl;
    return -ENOENT;
  }

  RGWRados::Bucket target(store, bucket_info);
  RGWRados::Bucket::List list_op(&target);

  map<string, bufferlist>::iterator aiter = bucket_attrs.find(RGW_ATTR_LC);
  if (aiter == bucket_attrs.end())
    return 0;

  bufferlist::iterator iter(&aiter->second);
  try {
      config.decode(iter);
    } catch (const buffer::error& e) {
      ldout(cct, 0) << __func__ <<  "decode life cycle config failed" << dendl;
      return -1;
    }

  map<string, int>& prefix_map = config.get_prefix_map();
  for(map<string, int>::iterator prefix_iter = prefix_map.begin(); prefix_iter != prefix_map.end();  prefix_iter++) {
    if (prefix_iter->first.empty()) {
      default_config = true;
      default_days = prefix_iter->second;
      break;
    }
  }

  if (default_config) {
    do {

      objs.clear();
      list_op.params.marker = list_op.get_next_marker();
      ret = list_op.list_objects(1000, &objs, NULL, &is_truncated);
      if (ret < 0) {
        if (ret == -ENOENT)
          return 0;
        ldout(cct, 0) << "ERROR: store->list_objects():" <<dendl;
        return ret;
      }

      vector<RGWObjEnt>::iterator obj_iter;
      int pos = 0;
      utime_t now = ceph_clock_now(cct);
      for (obj_iter = objs.begin(); obj_iter != objs.end(); obj_iter++) {
        bool prefix_match = false;
        int match_days = 0;
        map<string, int>& prefix_map = config.get_prefix_map();

        for(map<string, int>::iterator prefix_iter = prefix_map.begin(); prefix_iter != prefix_map.end();  prefix_iter++) {
          if (prefix_iter->first.empty()) {
            continue;
          }
          pos = (*obj_iter).key.name.find(prefix_iter->first, 0);
          if (pos != 0) {
            continue;
          }
          prefix_match = true;
          match_days = prefix_iter->second;
          break;
        }
        int days = 0;
        if (prefix_match) {
          days = match_days;
        } else if (default_config) {
          days = default_days;
        } else {
          continue;
        }
        if (obj_has_expired(now - ceph::real_clock::to_time_t((*obj_iter).mtime), days)) {
          RGWObjectCtx rctx(store);
          rgw_obj obj(bucket_info.bucket, (*obj_iter).key.name);
          RGWObjState *state;
          int ret = store->get_obj_state(&rctx, obj, &state, false);
          if (ret < 0) {
            return ret;
          }
          if (state->mtime != (*obj_iter).mtime) //Check mtime again to avoid delete a recently update object as much as possible
            continue;
          ret = rgw_remove_object(store, bucket_info, bucket_info.bucket, (*obj_iter).key);
          if (ret < 0) {
            ldout(cct, 0) << "ERROR: rgw_remove_object " << dendl;
          } else {
            ldout(cct, 10) << "DELETED:" << bucket_name << ":" << (*obj_iter).key.name <<dendl;
          }
        }
      }
    } while (is_truncated);
  } else {
    for(map<string, int>::iterator prefix_iter = prefix_map.begin(); prefix_iter != prefix_map.end();  prefix_iter++) {
      if (prefix_iter->first.empty()) {
        continue;
      }
      list_op.params.prefix = prefix_iter->first;

      do {

        objs.clear();
        list_op.params.marker = list_op.get_next_marker();
        ret = list_op.list_objects(1000, &objs, NULL, &is_truncated);

        if (ret < 0) {
          if (ret == (-ENOENT))
            return 0;
          ldout(cct, 0) << "ERROR: store->list_objects():" <<dendl;
          return ret;
        }

        vector<RGWObjEnt>::iterator obj_iter;
        int days = prefix_iter->second;
        utime_t now = ceph_clock_now(cct);

        for (obj_iter = objs.begin(); obj_iter != objs.end(); obj_iter++) {
          if (obj_has_expired(now - ceph::real_clock::to_time_t((*obj_iter).mtime), days)) {
            RGWObjectCtx rctx(store);
            rgw_obj obj(bucket_info.bucket, (*obj_iter).key.name);
            RGWObjState *state;
            int ret = store->get_obj_state(&rctx, obj, &state, false);
            if (ret < 0) {
              return ret;
            }
            if (state->mtime != (*obj_iter).mtime)//Check mtime again to avoid delete a recently update object as much as possible
              continue;
            ret = rgw_remove_object(store, bucket_info, bucket_info.bucket, (*obj_iter).key);
            if (ret < 0) {
              ldout(cct, 0) << "ERROR: rgw_remove_object " << dendl;
            } else {
              ldout(cct, 10) << "DELETED:" << bucket_name << ":" << (*obj_iter).key.name << dendl;
            }
          }
        }
      } while (is_truncated);
    }
  }

  return ret;
}

int RGWLC::bucket_lc_post(int index, int max_lock_sec, cls_rgw_lc_obj_head& head,
                                                              pair<string, int >& entry, int& result)
{
  rados::cls::lock::Lock l(lc_index_lock_name);
  l.set_cookie(cookie);
  do {
    int ret = l.lock_exclusive(&store->lc_pool_ctx, obj_names[index]);
    if (ret == -EBUSY) { /* already locked by another lc processor */
      dout(0) << "RGWLC::bucket_lc_post() failed to acquire lock on, sleep 5, try again" << obj_names[index] << dendl;
      sleep(5);
      continue;
    }
    if (ret < 0)
      return 0;
    dout(20) << "RGWLC::bucket_lc_post()  get lock" << obj_names[index] << dendl;
    if (result ==  -ENOENT) {
      ret = cls_rgw_lc_rm_entry(store->lc_pool_ctx, obj_names[index],  entry);
      if (ret < 0) {
        dout(0) << "RGWLC::bucket_lc_post() failed to remove entry " << obj_names[index] << dendl;
        goto clean;
      }
    } else if (result < 0) {
      entry.second = lc_failed;
    } else {
      entry.second = lc_complete;
    }

    ret = cls_rgw_lc_set_entry(store->lc_pool_ctx, obj_names[index],  entry);
    if (ret < 0) {
      dout(0) << "RGWLC::process() failed to set entry " << obj_names[index] << dendl;
    }
clean:
    l.unlock(&store->lc_pool_ctx, obj_names[index]);
    dout(20) << "RGWLC::bucket_lc_post()  unlock" << obj_names[index] << dendl;
    return 0;
  } while (true);
}

int RGWLC::list_lc_progress(const string& marker, uint32_t max_entries, map<string, int> *progress_map)
{
  int index = 0;
  progress_map->clear();
  for(; index <max_objs; index++) {
    map<string, int > entries;
    int ret = cls_rgw_lc_list(store->lc_pool_ctx, obj_names[index], marker, max_entries, entries);
    if (ret < 0)
      return ret;
    map<string, int>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      progress_map->insert(*iter);
    }
  }
  return 0;
}

int RGWLC::process()
{
  int max_secs = cct->_conf->rgw_lc_lock_max_time;

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

int RGWLC::process(int index, int max_lock_secs)
{
  rados::cls::lock::Lock l(lc_index_lock_name);
  do {
    utime_t now = ceph_clock_now(g_ceph_context);
    pair<string, int > entry;//string = bucket_name:bucket_id ,int = LC_BUCKET_STATUS
    if (max_lock_secs <= 0)
      return -EAGAIN;

    utime_t time(max_lock_secs, 0);
    l.set_duration(time);

    int ret = l.lock_exclusive(&store->lc_pool_ctx, obj_names[index]);
    if (ret == -EBUSY) { /* already locked by another lc processor */
      dout(0) << "RGWLC::process() failed to acquire lock on, sleep 5, try again" << obj_names[index] << dendl;
      sleep(5);
      continue;
    }
    if (ret < 0)
      return 0;

    string marker;
    cls_rgw_lc_obj_head head;
    ret = cls_rgw_lc_get_head(store->lc_pool_ctx, obj_names[index], head);
    if (ret < 0) {
      dout(0) << "RGWLC::process() failed to get obj head " << obj_names[index] << ret << dendl;
      goto exit;
    }

    if(!if_already_run_today(head.start_date)) {
      head.start_date = now;
      head.marker.clear();
      ret = bucket_lc_prepare(index);
      if (ret < 0) {
      dout(0) << "RGWLC::process() failed to update lc object " << obj_names[index] << ret << dendl;
      goto exit;
      }
    }

    ret = cls_rgw_lc_get_next_entry(store->lc_pool_ctx, obj_names[index], head.marker, entry);
    if (ret < 0) {
      dout(0) << "RGWLC::process() failed to get obj entry " << obj_names[index] << dendl;
      goto exit;
    }

    if (entry.first.empty())
      goto exit;

    entry.second = lc_processing;
    ret = cls_rgw_lc_set_entry(store->lc_pool_ctx, obj_names[index],  entry);
    if (ret < 0) {
      dout(0) << "RGWLC::process() failed to set obj entry " << obj_names[index] << entry.first << entry.second << dendl;
      goto exit;
    }

    head.marker = entry.first;
    ret = cls_rgw_lc_put_head(store->lc_pool_ctx, obj_names[index],  head);
    if (ret < 0) {
      dout(0) << "RGWLC::process() failed to put head " << obj_names[index] << dendl;
      goto exit;
    }
    l.unlock(&store->lc_pool_ctx, obj_names[index]);
    ret = bucket_lc_process(entry.first);
    ret = bucket_lc_post(index, max_lock_secs, head, entry, ret);
    return 0;
exit:
    l.unlock(&store->lc_pool_ctx, obj_names[index]);
    return 0;

  }while(1);

}

void RGWLC::start_processor()
{
  worker = new LCWorker(cct, this);
  worker->create("lifecycle_thr");
}

void RGWLC::stop_processor()
{
  down_flag.set(1);
  if (worker) {
    worker->stop();
    worker->join();
  }
  delete worker;
  worker = NULL;
}

void RGWLC::LCWorker::stop()
{
  Mutex::Locker l(lock);
  cond.Signal();
}

bool RGWLC::going_down()
{
  return (down_flag.read() != 0);
}

bool RGWLC::LCWorker::should_work(utime_t& now)
{
  int start_hour;
  int start_minute;
  int end_hour;
  int end_minute;
  string worktime = cct->_conf->rgw_lifecycle_work_time;
  sscanf(worktime.c_str(),"%d:%d-%d:%d",&start_hour, &start_minute, &end_hour, &end_minute);
  struct tm bdt;
  time_t tt = now.sec();
  localtime_r(&tt, &bdt);

  if (cct->_conf->rgw_lc_debug_interval > 0) {
	  /* We're debugging, so say we can run */
	  return true;
  } else if ((bdt.tm_hour*60 + bdt.tm_min >= start_hour*60 + start_minute) ||
		     (bdt.tm_hour*60 + bdt.tm_min <= end_hour*60 + end_minute)) {
	  return true;
  } else {
	  return false;
  }

}

int RGWLC::LCWorker::schedule_next_start_time(utime_t &start, utime_t& now)
{
  if (cct->_conf->rgw_lc_debug_interval > 0) {
	int secs = start + cct->_conf->rgw_lc_debug_interval - now;
	if (secs < 0)
	  secs = 0;
	return (secs);
  }

  int start_hour;
  int start_minute;
  int end_hour;
  int end_minute;
  string worktime = cct->_conf->rgw_lifecycle_work_time;
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

