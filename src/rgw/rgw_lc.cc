// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>
#include <iostream>
#include <map>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>

#include "common/Formatter.h"
#include <common/errno.h>
#include "auth/Crypto.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/lock/cls_lock_client.h"
#include "rgw_common.h"
#include "rgw_bucket.h"
#include "rgw_lc.h"
#include "rgw_string.h"

// this seems safe to use, at least for now--arguably, we should
// prefer header-only fmt, in general
#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include "fmt/format.h"

#include "include/assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

const char* LC_STATUS[] = {
      "UNINITIAL",
      "PROCESSING",
      "FAILED",
      "COMPLETE"
};

using namespace std;
using namespace librados;

bool LCRule::valid()
{
  if (id.length() > MAX_ID_LEN) {
    return false;
  }
  else if(expiration.empty() && noncur_expiration.empty() && mp_expiration.empty() && !dm_expiration) {
    return false;
  }
  else if (!expiration.valid() || !noncur_expiration.valid() || !mp_expiration.valid()) {
    return false;
  }
  return true;
}

void RGWLifecycleConfiguration::add_rule(LCRule *rule)
{
  string id;
  rule->get_id(id); // not that this will return false for groups, but that's ok, we won't search groups
  rule_map.insert(pair<string, LCRule>(id, *rule));
}

bool RGWLifecycleConfiguration::_add_rule(LCRule *rule)
{
  lc_op op(rule->get_id());
  op.status = rule->is_enabled();
  if (rule->get_expiration().has_days()) {
    op.expiration = rule->get_expiration().get_days();
  }
  if (rule->get_expiration().has_date()) {
    op.expiration_date = ceph::from_iso_8601(rule->get_expiration().get_date());
  }
  if (rule->get_noncur_expiration().has_days()) {
    op.noncur_expiration = rule->get_noncur_expiration().get_days();
  }
  if (rule->get_mp_expiration().has_days()) {
    op.mp_expiration = rule->get_mp_expiration().get_days();
  }
  op.dm_expiration = rule->get_dm_expiration();

  std::string prefix;
  if (rule->get_filter().has_prefix()){
    prefix = rule->get_filter().get_prefix();
  } else {
    prefix = rule->get_prefix();
  }

  if (rule->get_filter().has_tags()){
    op.obj_tags = rule->get_filter().get_tags();
  }

  prefix_map.emplace(std::move(prefix), std::move(op));
  return true;
}

int RGWLifecycleConfiguration::check_and_add_rule(LCRule *rule)
{
  if (!rule->valid()) {
    return -EINVAL;
  }
  string id;
  rule->get_id(id);
  if (rule_map.find(id) != rule_map.end()) {  //id shouldn't be the same 
    return -EINVAL;
  }
  rule_map.insert(pair<string, LCRule>(id, *rule));

  if (!_add_rule(rule)) {
    return -ERR_INVALID_REQUEST;
  }
  return 0;
}

bool RGWLifecycleConfiguration::has_same_action(const lc_op& first, const lc_op& second) {
  if ((first.expiration > 0 || first.expiration_date != boost::none) && 
    (second.expiration > 0 || second.expiration_date != boost::none)) {
    return true;
  } else if (first.noncur_expiration > 0 && second.noncur_expiration > 0) {
    return true;
  } else if (first.mp_expiration > 0 && second.mp_expiration > 0) {
    return true;
  } else {
    return false;
  }
}

/* Formerly, this method checked for duplicate rules using an invalid
 * method (prefix uniqueness). */
bool RGWLifecycleConfiguration::valid() 
{
  return true;
}

void *RGWLC::LCWorker::entry() {
  do {
    utime_t start = ceph_clock_now();
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

    utime_t end = ceph_clock_now();
    int secs = schedule_next_start_time(start, end);
    utime_t next;
    next.set_from_double(end + secs);

    dout(5) << "schedule life cycle next start time: " << rgw_to_asctime(next) <<dendl;

    lock.Lock();
    cond.WaitInterval(lock, utime_t(secs, 0));
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
  utime_t now = ceph_clock_now();
  localtime_r(&start_date, &bdt);

  if (cct->_conf->rgw_lc_debug_interval > 0) {
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

bool RGWLC::obj_has_expired(ceph::real_time mtime, int days)
{
  double timediff, cmp;
  utime_t base_time;
  if (cct->_conf->rgw_lc_debug_interval <= 0) {
    /* Normal case, run properly */
    cmp = days*24*60*60;
    base_time = ceph_clock_now().round_to_day();
  } else {
    /* We're in debug mode; Treat each rgw_lc_debug_interval seconds as a day */
    cmp = days*cct->_conf->rgw_lc_debug_interval;
    base_time = ceph_clock_now();
  }
  timediff = base_time - ceph::real_clock::to_time_t(mtime);

  return (timediff >= cmp);
}

int RGWLC::remove_expired_obj(RGWBucketInfo& bucket_info, rgw_obj_key obj_key, const string& owner, const string& owner_display_name, bool remove_indeed)
{
  if (remove_indeed) {
    return rgw_remove_object(store, bucket_info, bucket_info.bucket, obj_key);
  } else {
    obj_key.instance.clear();
    RGWObjectCtx rctx(store);
    rgw_obj obj(bucket_info.bucket, obj_key);
    ACLOwner obj_owner;
    obj_owner.set_id(rgw_user {owner});
    obj_owner.set_name(owner_display_name);

    RGWRados::Object del_target(store, bucket_info, rctx, obj);
    RGWRados::Object::Delete del_op(&del_target);

    del_op.params.bucket_owner = bucket_info.owner;
    del_op.params.versioning_status = bucket_info.versioning_status();
    del_op.params.obj_owner = obj_owner;

    return del_op.delete_obj();
  }
}

int RGWLC::handle_multipart_expiration(RGWRados::Bucket *target,
				       const multimap<string, lc_op>& prefix_map)
{
  MultipartMetaFilter mp_filter;
  vector<rgw_bucket_dir_entry> objs;
  RGWMPObj mp_obj;
  bool is_truncated;
  int ret;
  RGWBucketInfo& bucket_info = target->get_bucket_info();
  RGWRados::Bucket::List list_op(target);
  list_op.params.list_versions = false;
  list_op.params.ns = RGW_OBJ_NS_MULTIPART;
  list_op.params.filter = &mp_filter;
  for (auto prefix_iter = prefix_map.begin(); prefix_iter != prefix_map.end(); ++prefix_iter) {
    if (!prefix_iter->second.status || prefix_iter->second.mp_expiration <= 0) {
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

      for (auto obj_iter = objs.begin(); obj_iter != objs.end(); ++obj_iter) {
        if (obj_has_expired(obj_iter->meta.mtime, prefix_iter->second.mp_expiration)) {
          rgw_obj_key key(obj_iter->key);
          if (!mp_obj.from_meta(key.name)) {
            continue;
          }
          RGWObjectCtx rctx(store);
          ret = abort_multipart_upload(store, cct, &rctx, bucket_info, mp_obj);
          if (ret < 0 && ret != -ERR_NO_SUCH_UPLOAD) {
            ldout(cct, 0) << "ERROR: abort_multipart_upload failed, ret=" << ret <<dendl;
            return ret;
          }
          if (going_down())
            return 0;
        }
      }
    } while(is_truncated);
  }
  return 0;
}

static int read_obj_tags(RGWRados *store, RGWBucketInfo& bucket_info, rgw_obj& obj, RGWObjectCtx& ctx, bufferlist& tags_bl)
{
  RGWRados::Object op_target(store, bucket_info, ctx, obj);
  RGWRados::Object::Read read_op(&op_target);

  return read_op.get_attr(RGW_ATTR_TAGS, tags_bl);
}

static inline bool has_all_tags(const lc_op& rule_action,
				const RGWObjTags& object_tags)
{
  for (const auto& tag : object_tags.get_tags()) {

    if (! rule_action.obj_tags)
      return false;

    const auto& rule_tags = rule_action.obj_tags->get_tags();
    const auto& iter = rule_tags.find(tag.first);

    if ((iter == rule_tags.end()) ||
	(iter->second != tag.second))
      return false;
  }
  /* all tags matched */
  return true;
}

int RGWLC::bucket_lc_process(string& shard_id)
{
  RGWLifecycleConfiguration  config(cct);
  RGWBucketInfo bucket_info;
  map<string, bufferlist> bucket_attrs;
  string next_marker, no_ns, list_versions;
  bool is_truncated;
  vector<rgw_bucket_dir_entry> objs;
  RGWObjectCtx obj_ctx(store);
  vector<std::string> result;
  boost::split(result, shard_id, boost::is_any_of(":"));
  string bucket_tenant = result[0];
  string bucket_name = result[1];
  string bucket_marker = result[2];
  int ret = store->get_bucket_info(obj_ctx, bucket_tenant, bucket_name, bucket_info, NULL, &bucket_attrs);
  if (ret < 0) {
    ldout(cct, 0) << "LC:get_bucket_info failed" << bucket_name <<dendl;
    return ret;
  }

  if (bucket_info.bucket.marker != bucket_marker) {
    ldout(cct, 1) << "LC: deleting stale entry found for bucket=" << bucket_tenant
		   << ":" << bucket_name << " cur_marker=" << bucket_info.bucket.marker
		   << " orig_marker=" << bucket_marker << dendl;
    return -ENOENT;
  }

  RGWRados::Bucket target(store, bucket_info);

  map<string, bufferlist>::iterator aiter = bucket_attrs.find(RGW_ATTR_LC);
  if (aiter == bucket_attrs.end())
    return 0;

  bufferlist::iterator iter(&aiter->second);
  try {
      config.decode(iter);
    } catch (const buffer::error& e) {
      ldout(cct, 0) << __func__ <<  "() decode life cycle config failed" << dendl;
      return -1;
    }

  multimap<string, lc_op>& prefix_map = config.get_prefix_map();

  ldout(cct, 10) << __func__ <<  "() scanning prefix_map size="
		 << prefix_map.size()
		 << dendl;

  for(auto prefix_iter = prefix_map.begin(); prefix_iter != prefix_map.end(); ++prefix_iter) {
    ldout(cct, 16) << __func__
		   << "() prefix iter: " << prefix_iter->first
		   << " rule-id: " << prefix_iter->second.id
		   << dendl;
  }

  if (! bucket_info.versioned()) {
    for(auto prefix_iter = prefix_map.begin();
	prefix_iter != prefix_map.end(); ++prefix_iter) {

      RGWRados::Bucket::List list_op(&target);
      list_op.params.list_versions = false;

      ldout(cct, 16) << __func__
		     << "() prefix iter: " << prefix_iter->first
		     << " rule-id: " << prefix_iter->second.id
		     << dendl;

      if (!prefix_iter->second.status || 
        (prefix_iter->second.expiration <=0 && prefix_iter->second.expiration_date == boost::none)) {
        continue;
      }
      if (prefix_iter->second.expiration_date != boost::none && 
        ceph_clock_now() < ceph::real_clock::to_time_t(*prefix_iter->second.expiration_date)) {
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
        
        bool is_expired;
        for (auto obj_iter = objs.begin(); obj_iter != objs.end(); ++obj_iter) {
          rgw_obj_key key(obj_iter->key);
          RGWObjState *state;
          rgw_obj obj(bucket_info.bucket, key);
          RGWObjectCtx rctx(store);
          if (prefix_iter->second.obj_tags != boost::none) {
            bufferlist tags_bl;
            int ret = read_obj_tags(store, bucket_info, obj, rctx, tags_bl);
            if (ret < 0) {
              if (ret != -ENODATA)
                ldout(cct, 5) << "ERROR: read_obj_tags returned r=" << ret << dendl;
              continue;
            }
            RGWObjTags dest_obj_tags;
            try {
              auto iter = tags_bl.begin();
              dest_obj_tags.decode(iter);
            } catch (buffer::error& err) {
               ldout(cct,0) << "ERROR: caught buffer::error, couldn't decode TagSet" << dendl;
              return -EIO;
            }

	    if (! has_all_tags(prefix_iter->second, dest_obj_tags)) {
              ldout(cct, 16) << __func__ << "() skipping obj " << key << " as tags do not match" << dendl;
              continue;
            }
          }

          if (!key.ns.empty()) {
            continue;
          }
          if (prefix_iter->second.expiration_date != boost::none) {
            //we have checked it before
            is_expired = true;
          } else {
            is_expired = obj_has_expired(obj_iter->meta.mtime, prefix_iter->second.expiration);
          }
          if (is_expired) {
            int ret = store->get_obj_state(&rctx, bucket_info, obj, &state, false);
            if (ret < 0) {
              return ret;
            }
            if (state->mtime != obj_iter->meta.mtime) {
              //Check mtime again to avoid delete a recently update object as much as possible
              ldout(cct, 20) << __func__ << "() skipping removal: state->mtime " << state->mtime << " obj->mtime " << obj_iter->meta.mtime << dendl;
              continue;
            }
            ret = remove_expired_obj(bucket_info, obj_iter->key, obj_iter->meta.owner, obj_iter->meta.owner_display_name, true);
            if (ret < 0) {
              ldout(cct, 0) << "ERROR: remove_expired_obj " << dendl;
            } else {
              ldout(cct, 10) << "DELETED case 1:" << bucket_name << ":" << key << dendl;
            }

            if (going_down())
              return 0;
          }
        }
      } while (is_truncated);
    }
  } else {
    /* bucket versioning is enabled or suspended */
    RGWRados::Bucket::List list_op(&target);
    list_op.params.list_versions = true;

    rgw_obj_key pre_marker;
    for(auto prefix_iter = prefix_map.begin();
	prefix_iter != prefix_map.end(); ++prefix_iter) {
      if (!prefix_iter->second.status || (prefix_iter->second.expiration <= 0 
        && prefix_iter->second.expiration_date == boost::none
        && prefix_iter->second.noncur_expiration <= 0 && !prefix_iter->second.dm_expiration)) {
        continue;
      }
      if (prefix_iter != prefix_map.begin() && 
          (prefix_iter->first.compare(0, prev(prefix_iter)->first.length(), prev(prefix_iter)->first) == 0)) {
	list_op.get_next_marker() = pre_marker;
      } else {
	pre_marker = list_op.get_next_marker();
      }
      list_op.params.prefix = prefix_iter->first;
      rgw_bucket_dir_entry pre_obj;
      do {
        if (!objs.empty()) {
          pre_obj = objs.back();
        }
        objs.clear();
        list_op.params.marker = list_op.get_next_marker();
        ret = list_op.list_objects(1000, &objs, NULL, &is_truncated);

        if (ret < 0) {
          if (ret == (-ENOENT))
            return 0;
          ldout(cct, 0) << "ERROR: store->list_objects():" <<dendl;
          return ret;
        }

        ceph::real_time mtime;
        bool remove_indeed = true;
        int expiration;
        bool skip_expiration;
        bool is_expired;
        for (auto obj_iter = objs.begin(); obj_iter != objs.end(); ++obj_iter) {
          skip_expiration = false;
          is_expired = false;
          if (obj_iter->is_current()) {
            if (prefix_iter->second.expiration <= 0 && prefix_iter->second.expiration_date == boost::none
              && !prefix_iter->second.dm_expiration) {
              continue;
            }
            if (obj_iter->is_delete_marker()) {
              if ((obj_iter + 1)==objs.end()) {
                if (is_truncated) {
                  //deal with it in next round because we can't judge whether this marker is the only version
                  list_op.get_next_marker() = obj_iter->key;
                  break;
                }
              } else if (obj_iter->key.name.compare((obj_iter + 1)->key.name) == 0) {   //*obj_iter is delete marker and isn't the only version, do nothing.
                continue;
              }
              skip_expiration = prefix_iter->second.dm_expiration;
              remove_indeed = true;   //we should remove the delete marker if it's the only version
            } else {
              remove_indeed = false;
            }
            mtime = obj_iter->meta.mtime;
            expiration = prefix_iter->second.expiration;
            if (!skip_expiration && expiration <= 0 && prefix_iter->second.expiration_date == boost::none) {
              continue;
            } else if (!skip_expiration) {

	      rgw_obj_key key(obj_iter->key);
	      rgw_obj obj(bucket_info.bucket, key);
	      RGWObjectCtx rctx(store);
	      if (prefix_iter->second.obj_tags != boost::none) {
		bufferlist tags_bl;
		int ret = read_obj_tags(store, bucket_info, obj, rctx, tags_bl);
		if (ret < 0) {
		  if (ret != -ENODATA)
		    ldout(cct, 5) << "ERROR: read_obj_tags returned r=" << ret << dendl;
		  continue;
		}
		RGWObjTags dest_obj_tags;
		try {
		  auto iter = tags_bl.begin();
		  dest_obj_tags.decode(iter);
		} catch (buffer::error& err) {
		  ldout(cct,0) << "ERROR: caught buffer::error, couldn't decode TagSet" << dendl;
		  return -EIO;
		}

		if (! has_all_tags(prefix_iter->second, dest_obj_tags)) {
		  ldout(cct, 16) << __func__ << "() skipping obj " << key << " as tags do not match" << dendl;
		  continue;
		}
	      }

              if (expiration > 0) {
                is_expired = obj_has_expired(mtime, expiration);
              } else {
                is_expired = ceph_clock_now() >= ceph::real_clock::to_time_t(*prefix_iter->second.expiration_date);
              }
            }
          } else {
            if (prefix_iter->second.noncur_expiration <=0) {
              continue;
            }
            remove_indeed = true;
            mtime = (obj_iter == objs.begin())?pre_obj.meta.mtime:(obj_iter - 1)->meta.mtime;
            expiration = prefix_iter->second.noncur_expiration;
            is_expired = obj_has_expired(mtime, expiration);
          }
          if (skip_expiration || is_expired) {
            if (obj_iter->is_visible()) {
              RGWObjectCtx rctx(store);
              rgw_obj obj(bucket_info.bucket, obj_iter->key);
              RGWObjState *state;
              int ret = store->get_obj_state(&rctx, bucket_info, obj, &state, false);
              if (ret < 0) {
                return ret;
              }
            }
            ret = remove_expired_obj(bucket_info, obj_iter->key, obj_iter->meta.owner, obj_iter->meta.owner_display_name, remove_indeed);
            if (ret < 0) {
              ldout(cct, 0) << "ERROR: remove_expired_obj " << dendl;
            } else {
              ldout(cct, 10) << "DELETED case 2:" << bucket_name << ":" << obj_iter->key << dendl;
            }

            if (going_down())
              return 0;
          }
        }
      } while (is_truncated);
    }
  }

  ret = handle_multipart_expiration(&target, prefix_map);

  return ret;
}

int RGWLC::bucket_lc_post(int index, int max_lock_sec, pair<string, int >& entry, int& result)
{
  utime_t lock_duration(cct->_conf->rgw_lc_lock_max_time, 0);

  rados::cls::lock::Lock l(lc_index_lock_name);
  l.set_cookie(cookie);
  l.set_duration(lock_duration);

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
      }
      goto clean;
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
    if (ret < 0) {
      if (ret == -ENOENT) {
        dout(10) << __func__ << "() ignoring unfound lc object="
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
    utime_t now = ceph_clock_now();
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
    bucket_lc_post(index, max_lock_secs, entry, ret);
  }while(1);

exit:
    l.unlock(&store->lc_pool_ctx, obj_names[index]);
    return 0;
}

void RGWLC::start_processor()
{
  worker = new LCWorker(cct, this);
  worker->create("lifecycle_thr");
}

void RGWLC::stop_processor()
{
  down_flag = true;
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
  return down_flag;
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
  } else if ((bdt.tm_hour*60 + bdt.tm_min >= start_hour*60 + start_minute) &&
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

std::string rgwlc_s3_expiration_header(
  CephContext* cct,
  const rgw_obj_key& obj_key,
  const RGWObjTags& obj_tagset,
  const ceph::real_time& mtime,
  /* const */ std::map<std::string, buffer::list>& bucket_attrs)
{
  RGWLifecycleConfiguration config(cct); // TODO: save in bucket info
  std::string hdr{""};

  map<string, bufferlist>::iterator aiter = bucket_attrs.find(RGW_ATTR_LC);
  if (aiter == bucket_attrs.end())
    return hdr;

  bufferlist::iterator iter{&aiter->second};
  try {
      config.decode(iter);
  } catch (const buffer::error& e) {
    ldout(cct, 0) << __func__
		  <<  "() decode life cycle config failed"
		  << dendl;
      return hdr;
  } /* catch */

  /* dump tags at debug level 16 */
  RGWObjTags::tag_map_t obj_tag_map = obj_tagset.get_tags();
  if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 16)) {
    for (const auto& elt : obj_tag_map) {
      ldout(cct, 16) << __func__
		     <<  "() key=" << elt.first << " val=" << elt.second
		     << dendl;
    }
  }

  boost::optional<ceph::real_time> expiration_date;
  boost::optional<std::string> rule_id;

  const auto& rule_map = config.get_rule_map();
  for (const auto& ri : rule_map) {
    const auto& rule = ri.second;
    auto& id = rule.get_id();
    auto& prefix = rule.get_prefix();
    auto& filter = rule.get_filter();
    auto& expiration = rule.get_expiration();
    auto& noncur_expiration = rule.get_noncur_expiration();

    ldout(cct, 10) << "rule: " << ri.first
		   << " prefix: " << prefix
		   << " expiration: "
		   << " date: " << expiration.get_date()
		   << " days: " << expiration.get_days()
		   << " noncur_expiration: "
		   << " date: " << noncur_expiration.get_date()
		   << " days: " << noncur_expiration.get_days()
		   << dendl;

    /* skip if rule !enabled
     * if rule has prefix, skip iff object !match prefix
     * if rule has tags, skip iff object !match tags
     * note if object is current or non-current, compare accordingly
     * if rule has days, construct date expression and save iff older
     * than last saved
     * if rule has date, convert date expression and save iff older
     * than last saved
     * if the date accum has a value, format it into hdr
     */

    if (! rule.is_enabled())
      continue;

    if(! prefix.empty()) {
      if (! boost::starts_with(obj_key.name, prefix))
	continue;
    }

    if (filter.has_tags()) {
      bool tag_match = true;
      const RGWObjTags& rule_tagset = filter.get_tags();
      for (auto& tag : rule_tagset.get_tags()) {
	if (obj_tag_map.find(tag.first) == obj_tag_map.end()) {
	  tag_match = false;
	  break;
	}
      }
      if (! tag_match)
	continue;
    }

    // compute a uniform expiration date
    boost::optional<ceph::real_time> rule_expiration_date;
    const LCExpiration& rule_expiration =
      (obj_key.instance.empty()) ? expiration : noncur_expiration;

    if (rule_expiration.has_date()) {
      rule_expiration_date =
	boost::optional<ceph::real_time>(
	  ceph::from_iso_8601(rule.get_expiration().get_date()));
      rule_id = boost::optional<std::string>(id);
    } else {
      if (rule_expiration.has_days()) {
	rule_expiration_date =
	  boost::optional<ceph::real_time>(
	    mtime + make_timespan(rule_expiration.get_days()*24*60*60));
	rule_id = boost::optional<std::string>(id);
      }
    }

    // update earliest expiration
    if (rule_expiration_date) {
      if ((! expiration_date) ||
	  ((expiration_date &&
	    (*expiration_date < *rule_expiration_date)))) {
      expiration_date =
	boost::optional<ceph::real_time>(rule_expiration_date);
      }
    }
  }

  // cond format header
  if (expiration_date && rule_id) {
    // Fri, 23 Dec 2012 00:00:00 GMT
    char exp_buf[100];
    time_t exp = ceph::real_clock::to_time_t(*expiration_date);
    if (std::strftime(exp_buf, sizeof(exp_buf),
		      "%a, %d %b %y %T %z", std::gmtime(&exp))) {
      hdr = fmt::format("expiry-date=\"{0}\", rule-id=\"{1}\"", exp_buf,
			*rule_id);
    } else {
      ldout(cct, 0) << __func__ <<
	"() strftime of life cycle expiration header failed"
		    << dendl;
    }
  }

  return hdr;

} /* rgwlc_s3_expiration_header */

void RGWLifecycleConfiguration::generate_test_instances(list<RGWLifecycleConfiguration*>& o)
{
  o.push_back(new RGWLifecycleConfiguration);
}

void get_lc_oid(CephContext *cct, const string& shard_id, string *oid)
{
  int max_objs = (cct->_conf->rgw_lc_max_objs > HASH_PRIME ? HASH_PRIME : cct->_conf->rgw_lc_max_objs);
  int index = ceph_str_hash_linux(shard_id.c_str(), shard_id.size()) % HASH_PRIME % max_objs;
  *oid = lc_oid_prefix;
  char buf[32];
  snprintf(buf, 32, ".%d", index);
  oid->append(buf);
  return;
}



static std::string get_lc_shard_name(const rgw_bucket& bucket){
  return string_join_reserve(':', bucket.tenant, bucket.name, bucket.marker);
}

template<typename F>
static int guard_lc_modify(RGWRados* store, const rgw_bucket& bucket, const string& cookie, const F& f) {
  CephContext *cct = store->ctx();

  string shard_id = get_lc_shard_name(bucket);

  string oid; 
  get_lc_oid(cct, shard_id, &oid);

  pair<string, int> entry(shard_id, lc_uninitial);
  int max_lock_secs = cct->_conf->rgw_lc_lock_max_time;

  rados::cls::lock::Lock l(lc_index_lock_name); 
  utime_t time(max_lock_secs, 0);
  l.set_duration(time);
  l.set_cookie(cookie);

  librados::IoCtx *ctx = store->get_lc_pool_ctx();
  int ret;

  do {
    ret = l.lock_exclusive(ctx, oid);
    if (ret == -EBUSY) {
      ldout(cct, 0) << "RGWLC::RGWPutLC() failed to acquire lock on "
          << oid << ", sleep 5, try again" << dendl;
      sleep(5); // XXX: return retryable error
      continue;
    }
    if (ret < 0) {
      ldout(cct, 0) << "RGWLC::RGWPutLC() failed to acquire lock on "
          << oid << ", ret=" << ret << dendl;
      break;
    }
    ret = f(ctx, oid, entry);
    if (ret < 0) {
      ldout(cct, 0) << "RGWLC::RGWPutLC() failed to set entry on "
          << oid << ", ret=" << ret << dendl;
    }
    break;
  } while(true);
  l.unlock(ctx, oid);
  return ret;
}

int RGWLC::set_bucket_config(RGWBucketInfo& bucket_info,
                         const map<string, bufferlist>& bucket_attrs,
                         RGWLifecycleConfiguration *config)
{
  map<string, bufferlist> attrs = bucket_attrs;
  config->encode(attrs[RGW_ATTR_LC]);
  int ret = rgw_bucket_set_attrs(store, bucket_info, attrs, &bucket_info.objv_tracker);
  if (ret < 0)
    return ret;

  rgw_bucket& bucket = bucket_info.bucket;


  ret = guard_lc_modify(store, bucket, cookie, [&](librados::IoCtx *ctx, const string& oid,
                                                   const pair<string, int>& entry) {
    return cls_rgw_lc_set_entry(*ctx, oid, entry);
  });

  return ret;
}

int RGWLC::remove_bucket_config(RGWBucketInfo& bucket_info,
                                const map<string, bufferlist>& bucket_attrs)
{
  map<string, bufferlist> attrs = bucket_attrs;
  attrs.erase(RGW_ATTR_LC);
  int ret = rgw_bucket_set_attrs(store, bucket_info, attrs,
				&bucket_info.objv_tracker);

  rgw_bucket& bucket = bucket_info.bucket;

  if (ret < 0) {
    ldout(cct, 0) << "RGWLC::RGWDeleteLC() failed to set attrs on bucket="
        << bucket.name << " returned err=" << ret << dendl;
    return ret;
  }


  ret = guard_lc_modify(store, bucket, cookie, [&](librados::IoCtx *ctx, const string& oid,
                                                   const pair<string, int>& entry) {
    return cls_rgw_lc_rm_entry(*ctx, oid, entry);
  });

  return ret;
}

namespace rgw { namespace lc {

int fix_lc_shard_entry(RGWRados* store, const RGWBucketInfo& bucket_info,
		       const map<std::string,bufferlist>& battrs)
{
  auto aiter = battrs.find(RGW_ATTR_LC);
  if (aiter == battrs.end()) {
    return 0;    // No entry, nothing to fix
  }

  auto shard_name = get_lc_shard_name(bucket_info.bucket);
  std::string lc_oid;
  get_lc_oid(store->ctx(), shard_name, &lc_oid);

  rgw_lc_entry_t entry;
  // There are multiple cases we need to encounter here
  // 1. entry exists and is already set to marker, happens in plain buckets & newly resharded buckets
  // 2. entry doesn't exist, which usually happens when reshard has happened prior to update and next LC process has already dropped the update
  // 3. entry exists matching the current bucket id which was after a reshard (needs to be updated to the marker)
  // We are not dropping the old marker here as that would be caught by the next LC process update
  auto lc_pool_ctx = store->get_lc_pool_ctx();
  int ret = cls_rgw_lc_get_entry(*lc_pool_ctx,
				 lc_oid, shard_name, entry);
  if (ret == 0) {
    ldout(store->ctx(), 5) << "Entry already exists, nothing to do" << dendl;
    return ret; // entry is already existing correctly set to marker
  }
  ldout(store->ctx(), 5) << "cls_rgw_lc_get_entry errored ret code=" << ret << dendl;
  if (ret == -ENOENT) {
    ldout(store->ctx(), 1) << "No entry for bucket=" << bucket_info.bucket.name
			   << " creating " << dendl;
    // TODO: we have too many ppl making cookies like this!
    char cookie_buf[COOKIE_LEN + 1];
    gen_rand_alphanumeric(store->ctx(), cookie_buf, sizeof(cookie_buf) - 1);
    std::string cookie = cookie_buf;

    ret = guard_lc_modify(store, bucket_info.bucket, cookie,
			  [&lc_pool_ctx, &lc_oid](librados::IoCtx *ctx, const string& oid,
					    const pair<string, int>& entry) {
			    return cls_rgw_lc_set_entry(*lc_pool_ctx,
							lc_oid, entry);
			  });

  }

  return ret;
}

}} // namespace rgw::lc

