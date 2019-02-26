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
#include "rgw_lc.h"

#include "services/svc_sys_obj.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

const char* LC_STATUS[] = {
      "UNINITIAL",
      "PROCESSING",
      "FAILED",
      "COMPLETE"
};

using namespace librados;

bool LCRule::valid() const
{
  if (id.length() > MAX_ID_LEN) {
    return false;
  }
  else if(expiration.empty() && noncur_expiration.empty() && mp_expiration.empty() && !dm_expiration &&
          transitions.empty() && noncur_transitions.empty()) {
    return false;
  }
  else if (!expiration.valid() || !noncur_expiration.valid() || !mp_expiration.valid()) {
    return false;
  }
  if (!transitions.empty()) {
    bool using_days = expiration.has_days();
    bool using_date = expiration.has_date();
    for (const auto& elem : transitions) {
      if (!elem.second.valid()) {
        return false;
      }
      using_days = using_days || elem.second.has_days();
      using_date = using_date || elem.second.has_date();
      if (using_days && using_date) {
        return false;
      }
    }
  }
  for (const auto& elem : noncur_transitions) {
    if (!elem.second.valid()) {
      return false;
    }
  }

  return true;
}

void LCRule::init_simple_days_rule(std::string_view _id, std::string_view _prefix, int num_days)
{
  id = _id;
  prefix = _prefix;
  char buf[32];
  snprintf(buf, sizeof(buf), "%d", num_days);
  expiration.set_days(buf);
  set_enabled(true);
}

void RGWLifecycleConfiguration::add_rule(const LCRule& rule)
{
  auto& id = rule.get_id(); // note that this will return false for groups, but that's ok, we won't search groups
  rule_map.insert(pair<string, LCRule>(id, rule));
}

bool RGWLifecycleConfiguration::_add_rule(const LCRule& rule)
{
  lc_op op;
  op.status = rule.is_enabled();
  if (rule.get_expiration().has_days()) {
    op.expiration = rule.get_expiration().get_days();
  }
  if (rule.get_expiration().has_date()) {
    op.expiration_date = ceph::from_iso_8601(rule.get_expiration().get_date());
  }
  if (rule.get_noncur_expiration().has_days()) {
    op.noncur_expiration = rule.get_noncur_expiration().get_days();
  }
  if (rule.get_mp_expiration().has_days()) {
    op.mp_expiration = rule.get_mp_expiration().get_days();
  }
  op.dm_expiration = rule.get_dm_expiration();
  for (const auto &elem : rule.get_transitions()) {
    transition_action action;
    if (elem.second.has_days()) {
      action.days = elem.second.get_days();
    } else {
      action.date = ceph::from_iso_8601(elem.second.get_date());
    }
    action.storage_class = rgw_placement_rule::get_canonical_storage_class(elem.first);
    op.transitions.emplace(elem.first, std::move(action));
  }
  for (const auto &elem : rule.get_noncur_transitions()) {
    transition_action action;
    action.days = elem.second.get_days();
    action.date = ceph::from_iso_8601(elem.second.get_date());
    action.storage_class = elem.first;
    op.noncur_transitions.emplace(elem.first, std::move(action));
  }
  std::string prefix;
  if (rule.get_filter().has_prefix()){
    prefix = rule.get_filter().get_prefix();
  } else {
    prefix = rule.get_prefix();
  }

  if (rule.get_filter().has_tags()){
    op.obj_tags = rule.get_filter().get_tags();
  }
  auto ret = prefix_map.emplace(std::move(prefix), std::move(op));
  return ret.second;
}

int RGWLifecycleConfiguration::check_and_add_rule(const LCRule& rule)
{
  if (!rule.valid()) {
    return -EINVAL;
  }
  auto& id = rule.get_id();
  if (rule_map.find(id) != rule_map.end()) {  //id shouldn't be the same 
    return -EINVAL;
  }
  rule_map.insert(pair<string, LCRule>(id, rule));

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
  } else if (!first.transitions.empty() && !second.transitions.empty()) {
    for (auto &elem : first.transitions) {
      if (second.transitions.find(elem.first) != second.transitions.end()) {
        return true;
      }
    }
  } else if (!first.noncur_transitions.empty() && !second.noncur_transitions.empty()) {
    for (auto &elem : first.noncur_transitions) {
      if (second.noncur_transitions.find(elem.first) != second.noncur_transitions.end()) {
        return true;
      }
    }
  }
  return false;
}

//Rules are conflicted: if one rule's prefix starts with other rule's prefix, and these two rules
//define same action. 
bool RGWLifecycleConfiguration::valid() 
{
  if (prefix_map.size() < 2) {
    return true;
  }
  auto cur_iter = prefix_map.begin();
  while (cur_iter != prefix_map.end()) {
    auto next_iter = cur_iter;
    ++next_iter;
    while (next_iter != prefix_map.end()) {
      string c_pre = cur_iter->first;
      string n_pre = next_iter->first;
      if (n_pre.compare(0, c_pre.length(), c_pre) == 0) {
        if (has_same_action(cur_iter->second, next_iter->second)) {
          return false;
        } else {
          ++next_iter;
        }
      } else {
        break;
      }
    }
    ++cur_iter;
  }
  return true;
}

void *RGWLC::LCWorker::entry() {
  do {
    utime_t start = ceph_clock_now();
    if (should_work(start)) {
      ldpp_dout(dpp, 2) << "life cycle: start" << dendl;
      int r = lc->process();
      if (r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: do life cycle process() returned error r=" << r << dendl;
      }
      ldpp_dout(dpp, 2) << "life cycle: stop" << dendl;
    }
    if (lc->going_down())
      break;

    utime_t end = ceph_clock_now();
    int secs = schedule_next_start_time(start, end);
    utime_t next;
    next.set_from_double(end + secs);

    ldpp_dout(dpp, 5) << "schedule life cycle next start time: " << rgw_to_asctime(next) << dendl;

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
        ldpp_dout(this, 0) << "RGWLC::bucket_lc_prepare() failed to set entry on "
            << obj_names[index] << dendl;
        return ret;
      }
    }

    if (!entries.empty()) {
      marker = std::move(entries.rbegin()->first);
    }
  } while (!entries.empty());

  return 0;
}

static bool obj_has_expired(CephContext *cct, ceph::real_time mtime, int days, ceph::real_time *expire_time = nullptr)
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

  if (expire_time) {
    *expire_time = mtime + make_timespan(cmp);
  }
  ldout(cct, 20) << __func__ << "(): mtime=" << mtime << " days=" << days << " base_time=" << base_time << " timediff=" << timediff << " cmp=" << cmp << dendl;

  return (timediff >= cmp);
}

int RGWLC::handle_multipart_expiration(RGWRados::Bucket *target, const map<string, lc_op>& prefix_map)
{
  MultipartMetaFilter mp_filter;
  vector<rgw_bucket_dir_entry> objs;
  RGWMPObj mp_obj;
  bool is_truncated;
  int ret;
  RGWBucketInfo& bucket_info = target->get_bucket_info();
  RGWRados::Bucket::List list_op(target);
  auto delay_ms = cct->_conf.get_val<int64_t>("rgw_lc_thread_delay");
  list_op.params.list_versions = false;
  /* lifecycle processing does not depend on total order, so can
   * take advantage of unorderd listing optimizations--such as
   * operating on one shard at a time */
  list_op.params.allow_unordered = true;
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
          ldpp_dout(this, 0) << "ERROR: store->list_objects():" <<dendl;
          return ret;
      }

      for (auto obj_iter = objs.begin(); obj_iter != objs.end(); ++obj_iter) {
        if (obj_has_expired(cct, obj_iter->meta.mtime, prefix_iter->second.mp_expiration)) {
          rgw_obj_key key(obj_iter->key);
          if (!mp_obj.from_meta(key.name)) {
            continue;
          }
          RGWObjectCtx rctx(store);
          ret = abort_multipart_upload(store, cct, &rctx, bucket_info, mp_obj);
          if (ret < 0 && ret != -ERR_NO_SUCH_UPLOAD) {
            ldpp_dout(this, 0) << "ERROR: abort_multipart_upload failed, ret=" << ret << ", meta:" << obj_iter->key << dendl;
          } else if (ret == -ERR_NO_SUCH_UPLOAD) {
            ldpp_dout(this, 5) << "ERROR: abort_multipart_upload failed, ret=" << ret << ", meta:" << obj_iter->key << dendl;
          }
          if (going_down())
            return 0;
        }
      } /* for objs */
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
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

static bool is_valid_op(const lc_op& op)
{
      return (op.status &&
              (op.expiration > 0 
               || op.expiration_date != boost::none
               || op.noncur_expiration > 0
               || op.dm_expiration
               || !op.transitions.empty()
               || !op.noncur_transitions.empty()));
}

class LCObjsLister {
  RGWRados *store;
  RGWBucketInfo& bucket_info;
  RGWRados::Bucket target;
  RGWRados::Bucket::List list_op;
  bool is_truncated{false};
  rgw_obj_key next_marker;
  string prefix;
  vector<rgw_bucket_dir_entry> objs;
  vector<rgw_bucket_dir_entry>::iterator obj_iter;
  rgw_bucket_dir_entry pre_obj;
  int64_t delay_ms;

public:
  LCObjsLister(RGWRados *_store, RGWBucketInfo& _bucket_info) :
      store(_store), bucket_info(_bucket_info),
      target(store, bucket_info), list_op(&target) {
    list_op.params.list_versions = bucket_info.versioned();
    list_op.params.allow_unordered = true;
    delay_ms = store->ctx()->_conf.get_val<int64_t>("rgw_lc_thread_delay");
  }

  void set_prefix(const string& p) {
    prefix = p;
    list_op.params.prefix = prefix;
  }

  int init() {
    return fetch();
  }

  int fetch() {
    int ret = list_op.list_objects(1000, &objs, NULL, &is_truncated);
    if (ret < 0) {
      return ret;
    }

    obj_iter = objs.begin();

    return 0;
  }

  void delay() {
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
  }

  bool get_obj(rgw_bucket_dir_entry *obj) {
    if (obj_iter == objs.end()) {
      delay();
      return false;
    }
    if (is_truncated && (obj_iter + 1)==objs.end()) {
      list_op.params.marker = obj_iter->key;

      int ret = fetch();
      if (ret < 0) {
        ldout(store->ctx(), 0) << "ERROR: list_op returned ret=" << ret << dendl;
        return ret;
      } else {
        obj_iter = objs.begin();
      }
      delay();
    }
    *obj = *obj_iter;
    return true;
  }

  rgw_bucket_dir_entry get_prev_obj() {
    return pre_obj;
  }

  void next() {
    pre_obj = *obj_iter;
    ++obj_iter;
  }

  bool next_has_same_name()
  {
    if ((obj_iter + 1) == objs.end()) {
      /* this should have been called after get_obj() was called, so this should
       * only happen if is_truncated is false */
      return false;
    }
    return (obj_iter->key.name.compare((obj_iter + 1)->key.name) == 0);
  }
};


struct op_env {
  lc_op& op;
  RGWRados *store;
  RGWLC *lc;
  RGWBucketInfo& bucket_info;
  LCObjsLister& ol;

  op_env(lc_op& _op, RGWRados *_store, RGWLC *_lc, RGWBucketInfo& _bucket_info,
         LCObjsLister& _ol) : op(_op), store(_store), lc(_lc), bucket_info(_bucket_info), ol(_ol) {}
};

class LCRuleOp;

struct lc_op_ctx {
  CephContext *cct;
  op_env& env;
  rgw_bucket_dir_entry& o;

  RGWRados *store;
  RGWBucketInfo& bucket_info;
  lc_op& op;
  LCObjsLister& ol;

  rgw_obj obj;
  RGWObjectCtx rctx;

  lc_op_ctx(op_env& _env, rgw_bucket_dir_entry& _o) : cct(_env.store->ctx()), env(_env), o(_o),
                 store(env.store), bucket_info(env.bucket_info), op(env.op), ol(env.ol),
                 obj(env.bucket_info.bucket, o.key), rctx(env.store) {}
};

static int remove_expired_obj(lc_op_ctx& oc, bool remove_indeed)
{
  auto& store = oc.store;
  auto& bucket_info = oc.bucket_info;
  auto& o = oc.o;
  auto obj_key = o.key;
  auto& meta = o.meta;

  if (!remove_indeed) {
    obj_key.instance.clear();
  } else if (obj_key.instance.empty()) {
    obj_key.instance = "null";
  }

  rgw_obj obj(bucket_info.bucket, obj_key);
  ACLOwner obj_owner;
  obj_owner.set_id(rgw_user {meta.owner});
  obj_owner.set_name(meta.owner_display_name);

  RGWRados::Object del_target(store, bucket_info, oc.rctx, obj);
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.bucket_owner = bucket_info.owner;
  del_op.params.versioning_status = bucket_info.versioning_status();
  del_op.params.obj_owner = obj_owner;
  del_op.params.unmod_since = meta.mtime;

  return del_op.delete_obj();
}

class LCOpAction {
public:
  virtual ~LCOpAction() {}

  virtual bool check(lc_op_ctx& oc, ceph::real_time *exp_time) {
    return false;
  };

  /* called after check(). Check should tell us whether this action
   * is applicable. If there are multiple actions, we'll end up executing
   * the latest applicable action
   * For example:
   *   one action after 10 days, another after 20, third after 40.
   *   After 10 days, the latest applicable action would be the first one,
   *   after 20 days it will be the second one. After 21 days it will still be the
   *   second one. So check() should return true for the second action at that point,
   *   but should_process() if the action has already been applied. In object removal
   *   it doesn't matter, but in object transition it does.
   */
  virtual bool should_process() {
    return true;
  }

  virtual int process(lc_op_ctx& oc) {
    return 0;
  }
};

class LCOpFilter {
public:
virtual ~LCOpFilter() {}
  virtual bool check(lc_op_ctx& oc) {
    return false;
  }
};

class LCOpRule {
  friend class LCOpAction;

  op_env& env;

  std::vector<unique_ptr<LCOpFilter> > filters;
  std::vector<unique_ptr<LCOpAction> > actions;

public:
  LCOpRule(op_env& _env) : env(_env) {}

  void build();
  int process(rgw_bucket_dir_entry& o);
};

static int check_tags(lc_op_ctx& oc, bool *skip)
{
  auto& op = oc.op;

  if (op.obj_tags != boost::none) {
    *skip = true;

    bufferlist tags_bl;
    int ret = read_obj_tags(oc.store, oc.bucket_info, oc.obj, oc.rctx, tags_bl);
    if (ret < 0) {
      if (ret != -ENODATA) {
        ldout(oc.cct, 5) << "ERROR: read_obj_tags returned r=" << ret << dendl;
      }
      return 0;
    }
    RGWObjTags dest_obj_tags;
    try {
      auto iter = tags_bl.cbegin();
      dest_obj_tags.decode(iter);
    } catch (buffer::error& err) {
      ldout(oc.cct,0) << "ERROR: caught buffer::error, couldn't decode TagSet" << dendl;
      return -EIO;
    }

    if (!includes(dest_obj_tags.get_tags().begin(),
                  dest_obj_tags.get_tags().end(),
                  op.obj_tags->get_tags().begin(),
                  op.obj_tags->get_tags().end())){
      ldout(oc.cct, 20) << __func__ << "() skipping obj " << oc.obj << " as tags do not match" << dendl;
      return 0;
    }
  }
  *skip = false;
  return 0;
}

class LCOpFilter_Tags : public LCOpFilter {
public:
  bool check(lc_op_ctx& oc) override {
    auto& o = oc.o;

    if (o.is_delete_marker()) {
      return true;
    }

    bool skip;

    int ret = check_tags(oc, &skip);
    if (ret < 0) {
      if (ret == -ENOENT) {
        return false;
      }
      ldout(oc.cct, 0) << "ERROR: check_tags on obj=" << oc.obj << " returned ret=" << ret << dendl;
      return false;
    }

    return !skip;
  };
};

class LCOpAction_CurrentExpiration : public LCOpAction {
public:
  bool check(lc_op_ctx& oc, ceph::real_time *exp_time) override {
    auto& o = oc.o;
    if (!o.is_current()) {
      ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": not current, skipping" << dendl;
      return false;
    }

    auto& mtime = o.meta.mtime;
    bool is_expired;
    auto& op = oc.op;
    if (op.expiration <= 0) {
      if (op.expiration_date == boost::none) {
        ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": no expiration set in rule, skipping" << dendl;
        return false;
      }
      is_expired = ceph_clock_now() >= ceph::real_clock::to_time_t(*op.expiration_date);
      *exp_time = *op.expiration_date;
    } else {
      is_expired = obj_has_expired(oc.cct, mtime, op.expiration, exp_time);
    }

    ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": is_expired=" << (int)is_expired << dendl;
    return is_expired;
  }

  int process(lc_op_ctx& oc) {
    auto& o = oc.o;
    int r = remove_expired_obj(oc, !oc.bucket_info.versioned());
    if (r < 0) {
      ldout(oc.cct, 0) << "ERROR: remove_expired_obj " << dendl;
      return r;
    }
    ldout(oc.cct, 2) << "DELETED:" << oc.bucket_info.bucket << ":" << o.key << dendl;
    return 0;
  }
};

class LCOpAction_NonCurrentExpiration : public LCOpAction {
public:
  bool check(lc_op_ctx& oc, ceph::real_time *exp_time) override {
    auto& o = oc.o;
    if (o.is_current()) {
      ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": current version, skipping" << dendl;
      return false;
    }

    auto mtime = oc.ol.get_prev_obj().meta.mtime;
    int expiration = oc.op.noncur_expiration;
    bool is_expired = obj_has_expired(oc.cct, mtime, expiration, exp_time);

    ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": is_expired=" << is_expired << dendl;
    return is_expired;
  }

  int process(lc_op_ctx& oc) {
    auto& o = oc.o;
    int r = remove_expired_obj(oc, true);
    if (r < 0) {
      ldout(oc.cct, 0) << "ERROR: remove_expired_obj " << dendl;
      return r;
    }
    ldout(oc.cct, 2) << "DELETED:" << oc.bucket_info.bucket << ":" << o.key << " (non-current expiration)" << dendl;
    return 0;
  }
};

class LCOpAction_DMExpiration : public LCOpAction {
public:
  bool check(lc_op_ctx& oc, ceph::real_time *exp_time) override {
    auto& o = oc.o;
    if (!o.is_delete_marker()) {
      ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": not a delete marker, skipping" << dendl;
      return false;
    }

    if (oc.ol.next_has_same_name()) {
      ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": next is same object, skipping" << dendl;
      return false;
    }

    *exp_time = real_clock::now();

    return true;
  }

  int process(lc_op_ctx& oc) {
    auto& o = oc.o;
    int r = remove_expired_obj(oc, true);
    if (r < 0) {
      ldout(oc.cct, 0) << "ERROR: remove_expired_obj " << dendl;
      return r;
    }
    ldout(oc.cct, 2) << "DELETED:" << oc.bucket_info.bucket << ":" << o.key << " (delete marker expiration)" << dendl;
    return 0;
  }
};

class LCOpAction_Transition : public LCOpAction {
  const transition_action& transition;
  bool need_to_process{false};

protected:
  virtual bool check_current_state(bool is_current) = 0;
  virtual ceph::real_time get_effective_mtime(lc_op_ctx& oc) = 0;
public:
  LCOpAction_Transition(const transition_action& _transition) : transition(_transition) {}

  bool check(lc_op_ctx& oc, ceph::real_time *exp_time) override {
    auto& o = oc.o;

    if (o.is_delete_marker()) {
      return false;
    }

    if (!check_current_state(o.is_current())) {
      return false;
    }

    auto mtime = get_effective_mtime(oc);
    bool is_expired;
    if (transition.days <= 0) {
      if (transition.date == boost::none) {
        ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": no transition day/date set in rule, skipping" << dendl;
        return false;
      }
      is_expired = ceph_clock_now() >= ceph::real_clock::to_time_t(*transition.date);
      *exp_time = *transition.date;
    } else {
      is_expired = obj_has_expired(oc.cct, mtime, transition.days, exp_time);
    }

    ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": is_expired=" << is_expired << dendl;

    need_to_process = (rgw_placement_rule::get_canonical_storage_class(o.meta.storage_class) != transition.storage_class);

    return is_expired;
  }

  bool should_process() override {
    return need_to_process;
  }

  int process(lc_op_ctx& oc) {
    auto& o = oc.o;

    rgw_placement_rule target_placement;
    target_placement.inherit_from(oc.bucket_info.placement_rule);
    target_placement.storage_class = transition.storage_class;

    int r = oc.store->transition_obj(oc.rctx, oc.bucket_info, oc.obj,
                                     target_placement, o.meta.mtime, o.versioned_epoch);
    if (r < 0) {
      ldout(oc.cct, 0) << "ERROR: failed to transition obj (r=" << r << ")" << dendl;
      return r;
    }
    ldout(oc.cct, 2) << "TRANSITIONED:" << oc.bucket_info.bucket << ":" << o.key << " -> " << transition.storage_class << dendl;
    return 0;
  }
};

class LCOpAction_CurrentTransition : public LCOpAction_Transition {
protected:
  bool check_current_state(bool is_current) override {
    return is_current;
  }

  ceph::real_time get_effective_mtime(lc_op_ctx& oc) override {
    return oc.o.meta.mtime;
  }
public:
  LCOpAction_CurrentTransition(const transition_action& _transition) : LCOpAction_Transition(_transition) {}
};

class LCOpAction_NonCurrentTransition : public LCOpAction_Transition {
protected:
  bool check_current_state(bool is_current) override {
    return !is_current;
  }

  ceph::real_time get_effective_mtime(lc_op_ctx& oc) override {
    return oc.ol.get_prev_obj().meta.mtime;
  }
public:
  LCOpAction_NonCurrentTransition(const transition_action& _transition) : LCOpAction_Transition(_transition) {}
};

void LCOpRule::build()
{
  filters.emplace_back(new LCOpFilter_Tags);

  auto& op = env.op;

  if (op.expiration > 0 ||
      op.expiration_date != boost::none) {
    actions.emplace_back(new LCOpAction_CurrentExpiration);
  }

  if (op.dm_expiration) {
    actions.emplace_back(new LCOpAction_DMExpiration);
  }

  if (op.noncur_expiration > 0) {
    actions.emplace_back(new LCOpAction_NonCurrentExpiration);
  }

  for (auto& iter : op.transitions) {
    actions.emplace_back(new LCOpAction_CurrentTransition(iter.second));
  }

  for (auto& iter : op.noncur_transitions) {
    actions.emplace_back(new LCOpAction_NonCurrentTransition(iter.second));
  }
}

int LCOpRule::process(rgw_bucket_dir_entry& o)
{
  lc_op_ctx ctx(env, o);

  unique_ptr<LCOpAction> *selected = nullptr;
  real_time exp;

  for (auto& a : actions) {
    real_time action_exp;

    if (a->check(ctx, &action_exp)) {
      if (action_exp > exp) {
        exp = action_exp;
        selected = &a;
      }
    }
  }

  if (selected &&
      (*selected)->should_process()) {

    /*
     * Calling filter checks after action checks because
     * all action checks (as they are implemented now) do
     * not access the objects themselves, but return result
     * from info from bucket index listing. The current tags filter
     * check does access the objects, so we avoid unnecessary rados calls
     * having filters check later in the process.
     */

    bool cont = false;
    for (auto& f : filters) {
      if (f->check(ctx)) {
        cont = true;
        break;
      }
    }

    if (!cont) {
      ldout(env.store->ctx(), 20) << __func__ << "(): key=" << o.key << ": no rule match, skipping" << dendl;
      return 0;
    }

    int r = (*selected)->process(ctx);
    if (r < 0) {
      ldout(ctx.cct, 0) << "ERROR: remove_expired_obj " << dendl;
      return r;
    }
    ldout(ctx.cct, 20) << "processed:" << env.bucket_info.bucket << ":" << o.key << dendl;
  }

  return 0;

}


int RGWLC::bucket_lc_process(string& shard_id)
{
  RGWLifecycleConfiguration  config(cct);
  RGWBucketInfo bucket_info;
  map<string, bufferlist> bucket_attrs;
  string no_ns, list_versions;
  vector<rgw_bucket_dir_entry> objs;
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  vector<std::string> result;
  boost::split(result, shard_id, boost::is_any_of(":"));
  string bucket_tenant = result[0];
  string bucket_name = result[1];
  string bucket_id = result[2];
  int ret = store->get_bucket_info(obj_ctx, bucket_tenant, bucket_name, bucket_info, NULL, &bucket_attrs);
  if (ret < 0) {
    ldpp_dout(this, 0) << "LC:get_bucket_info for " << bucket_name << " failed" << dendl;
    return ret;
  }

  ret = bucket_info.bucket.bucket_id.compare(bucket_id) ;
  if (ret != 0) {
    ldpp_dout(this, 0) << "LC:old bucket id found. " << bucket_name << " should be deleted" << dendl;
    return -ENOENT;
  }

  RGWRados::Bucket target(store, bucket_info);
  LCObjsLister ol(store, bucket_info);

  map<string, bufferlist>::iterator aiter = bucket_attrs.find(RGW_ATTR_LC);
  if (aiter == bucket_attrs.end())
    return 0;

  bufferlist::const_iterator iter{&aiter->second};
  try {
      config.decode(iter);
    } catch (const buffer::error& e) {
      ldpp_dout(this, 0) << __func__ <<  "() decode life cycle config failed" << dendl;
      return -1;
    }

  map<string, lc_op>& prefix_map = config.get_prefix_map();
  rgw_obj_key pre_marker;
  rgw_obj_key next_marker;
  for(auto prefix_iter = prefix_map.begin(); prefix_iter != prefix_map.end(); ++prefix_iter) {
    auto& op = prefix_iter->second;
    if (!is_valid_op(op)) {
      continue;
    }
    ldpp_dout(this, 20) << __func__ << "(): prefix=" << prefix_iter->first << dendl;
    if (prefix_iter != prefix_map.begin() && 
        (prefix_iter->first.compare(0, prev(prefix_iter)->first.length(), prev(prefix_iter)->first) == 0)) {
      next_marker = pre_marker;
    } else {
      pre_marker = next_marker;
    }
    ol.set_prefix(prefix_iter->first);

    ret = ol.init();

    if (ret < 0) {
      if (ret == (-ENOENT))
        return 0;
      ldpp_dout(this, 0) << "ERROR: store->list_objects():" <<dendl;
      return ret;
    }

    op_env oenv(op, store, this, bucket_info, ol);

    LCOpRule orule(oenv);

    orule.build();

    ceph::real_time mtime;
    rgw_bucket_dir_entry o;
    for (; ol.get_obj(&o); ol.next()) {
      ldpp_dout(this, 20) << __func__ << "(): key=" << o.key << dendl;
      int ret = orule.process(o);
      if (ret < 0) {
        ldpp_dout(this, 20) << "ERROR: orule.process() returned ret=" << ret << dendl;
      }

      if (going_down()) {
        return 0;
      }
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
    if (ret == -EBUSY || ret == -EEXIST) { /* already locked by another lc processor */
      ldpp_dout(this, 0) << "RGWLC::bucket_lc_post() failed to acquire lock on "
          << obj_names[index] << ", sleep 5, try again" << dendl;
      sleep(5);
      continue;
    }
    if (ret < 0)
      return 0;
    ldpp_dout(this, 20) << "RGWLC::bucket_lc_post() lock " << obj_names[index] << dendl;
    if (result ==  -ENOENT) {
      ret = cls_rgw_lc_rm_entry(store->lc_pool_ctx, obj_names[index],  entry);
      if (ret < 0) {
        ldpp_dout(this, 0) << "RGWLC::bucket_lc_post() failed to remove entry "
            << obj_names[index] << dendl;
      }
      goto clean;
    } else if (result < 0) {
      entry.second = lc_failed;
    } else {
      entry.second = lc_complete;
    }

    ret = cls_rgw_lc_set_entry(store->lc_pool_ctx, obj_names[index],  entry);
    if (ret < 0) {
      ldpp_dout(this, 0) << "RGWLC::process() failed to set entry on "
          << obj_names[index] << dendl;
    }
clean:
    l.unlock(&store->lc_pool_ctx, obj_names[index]);
    ldpp_dout(this, 20) << "RGWLC::bucket_lc_post() unlock " << obj_names[index] << dendl;
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
        ldpp_dout(this, 10) << __func__ << "() ignoring unfound lc object="
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

  const int start = ceph::util::generate_random_number(0, max_objs - 1);

  for (int i = 0; i < max_objs; i++) {
    int index = (i + start) % max_objs;
    int ret = process(index, max_secs);
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
    if (ret == -EBUSY || ret == -EEXIST) { /* already locked by another lc processor */
      ldpp_dout(this, 0) << "RGWLC::process() failed to acquire lock on "
          << obj_names[index] << ", sleep 5, try again" << dendl;
      sleep(5);
      continue;
    }
    if (ret < 0)
      return 0;

    cls_rgw_lc_obj_head head;
    ret = cls_rgw_lc_get_head(store->lc_pool_ctx, obj_names[index], head);
    if (ret < 0) {
      ldpp_dout(this, 0) << "RGWLC::process() failed to get obj head "
          << obj_names[index] << ", ret=" << ret << dendl;
      goto exit;
    }

    if(!if_already_run_today(head.start_date)) {
      head.start_date = now;
      head.marker.clear();
      ret = bucket_lc_prepare(index);
      if (ret < 0) {
      ldpp_dout(this, 0) << "RGWLC::process() failed to update lc object "
          << obj_names[index] << ", ret=" << ret << dendl;
      goto exit;
      }
    }

    ret = cls_rgw_lc_get_next_entry(store->lc_pool_ctx, obj_names[index], head.marker, entry);
    if (ret < 0) {
      ldpp_dout(this, 0) << "RGWLC::process() failed to get obj entry "
          << obj_names[index] << dendl;
      goto exit;
    }

    if (entry.first.empty())
      goto exit;

    entry.second = lc_processing;
    ret = cls_rgw_lc_set_entry(store->lc_pool_ctx, obj_names[index],  entry);
    if (ret < 0) {
      ldpp_dout(this, 0) << "RGWLC::process() failed to set obj entry " << obj_names[index]
          << " (" << entry.first << "," << entry.second << ")" << dendl;
      goto exit;
    }

    head.marker = entry.first;
    ret = cls_rgw_lc_put_head(store->lc_pool_ctx, obj_names[index],  head);
    if (ret < 0) {
      ldpp_dout(this, 0) << "RGWLC::process() failed to put head " << obj_names[index] << dendl;
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
  worker = new LCWorker(this, cct, this);
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


unsigned RGWLC::get_subsys() const
{
  return dout_subsys;
}

std::ostream& RGWLC::gen_prefix(std::ostream& out) const
{
  return out << "lifecycle: ";
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
  int secs;

  if (cct->_conf->rgw_lc_debug_interval > 0) {
	secs = start + cct->_conf->rgw_lc_debug_interval - now;
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
  secs = nt - tt;

  return secs>0 ? secs : secs+24*60*60;
}

void RGWLifecycleConfiguration::generate_test_instances(list<RGWLifecycleConfiguration*>& o)
{
  o.push_back(new RGWLifecycleConfiguration);
}

static void get_lc_oid(CephContext *cct, const string& shard_id, string *oid)
{
  int max_objs = (cct->_conf->rgw_lc_max_objs > HASH_PRIME ? HASH_PRIME : cct->_conf->rgw_lc_max_objs);
  int index = ceph_str_hash_linux(shard_id.c_str(), shard_id.size()) % HASH_PRIME % max_objs;
  *oid = lc_oid_prefix;
  char buf[32];
  snprintf(buf, 32, ".%d", index);
  oid->append(buf);
  return;
}

template<typename F>
static int guard_lc_modify(RGWRados* store, const rgw_bucket& bucket, const string& cookie, const F& f) {
  CephContext *cct = store->ctx();

  string shard_id = bucket.tenant + ':' + bucket.name + ':' + bucket.bucket_id;  

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
    if (ret == -EBUSY || ret == -EEXIST) {
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

