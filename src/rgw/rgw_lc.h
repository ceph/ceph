// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_LC_H
#define CEPH_RGW_LC_H

#include <map>
#include <string>
#include <iostream>

#include "common/debug.h"

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/iso_8601.h"
#include "common/Thread.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_multi.h"
#include "cls/rgw/cls_rgw_types.h"
#include "rgw_tag.h"

#include <atomic>

#define HASH_PRIME 7877
#define MAX_ID_LEN 255
static string lc_oid_prefix = "lc";
static string lc_index_lock_name = "lc_process";

extern const char* LC_STATUS[];

typedef enum {
  lc_uninitial = 0,
  lc_processing,
  lc_failed,
  lc_complete,
} LC_BUCKET_STATUS;

class LCExpiration
{
protected:
  string days;
  //At present only current object has expiration date
  string date;
public:
  LCExpiration() {}
  LCExpiration(const string& _days, const string& _date) : days(_days), date(_date) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 2, bl);
    encode(days, bl);
    encode(date, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
    decode(days, bl);
    if (struct_v >= 3) {
      decode(date, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
//  static void generate_test_instances(list<ACLOwner*>& o);
  void set_days(const string& _days) { days = _days; }
  string get_days_str() const {
    return days;
  }
  int get_days() const {return atoi(days.c_str()); }
  bool has_days() const {
    return !days.empty();
  }
  void set_date(const string& _date) { date = _date; }
  string get_date() const {
    return date;
  }
  bool has_date() const {
    return !date.empty();
  }
  bool empty() const {
    return days.empty() && date.empty();
  }
  bool valid() const {
    if (!days.empty() && !date.empty()) {
      return false;
    } else if (!days.empty() && get_days() <= 0) {
      return false;
    }
    //We've checked date in xml parsing
    return true;
  }
};
WRITE_CLASS_ENCODER(LCExpiration)

class LCTransition
{
protected:
  string days;
  string date;
  string storage_class;

public:
  int get_days() const {
    return atoi(days.c_str());
  }

  string get_date() const {
    return date;
  }

  string get_storage_class() const {
    return storage_class;
  }

  bool has_days() const {
    return !days.empty();
  }

  bool has_date() const {
    return !date.empty();
  }

  bool empty() const {
    return days.empty() && date.empty();
  }

  bool valid() const {
    if (!days.empty() && !date.empty()) {
      return false;
    } else if (!days.empty() && get_days() <=0) {
      return false;
    }
    //We've checked date in xml parsing
    return true;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(days, bl);
    encode(date, bl);
    encode(storage_class, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(days, bl);
    decode(date, bl);
    decode(storage_class, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(LCTransition)

class LCFilter
{
 protected:
  std::string prefix;
  RGWObjTags obj_tags;

 public:

  const std::string& get_prefix() const {
    return prefix;
  }

  const RGWObjTags& get_tags() const {
    return obj_tags;
  }

  bool empty() const {
    return !(has_prefix() || has_tags());
  }

  // Determine if we need AND tag when creating xml
  bool has_multi_condition() const {
    if (obj_tags.count() > 1)
      return true;
    else if (has_prefix() && has_tags())
      return true;

    return false;
  }

  bool has_prefix() const {
    return !prefix.empty();
  }

  bool has_tags() const {
    return !obj_tags.empty();
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(prefix, bl);
    encode(obj_tags, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(prefix, bl);
    if (struct_v >= 2) {
      decode(obj_tags, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(LCFilter)



class LCRule
{
protected:
  string id;
  string prefix;
  string status;
  LCExpiration expiration;
  LCExpiration noncur_expiration;
  LCExpiration mp_expiration;
  LCFilter filter;
  map<string, LCTransition> transitions;
  map<string, LCTransition> noncur_transitions;
  bool dm_expiration = false;

public:

  LCRule(){};
  ~LCRule(){};

  const string& get_id() const {
      return id;
  }

  const string& get_status() const {
      return status;
  }

  bool is_enabled() const {
    return status == "Enabled";
  }

  void set_enabled(bool flag) {
    status = (flag ? "Enabled" : "Disabled");
  }

  const string& get_prefix() const {
      return prefix;
  }

  const LCFilter& get_filter() const {
    return filter;
  }

  const LCExpiration& get_expiration() const {
    return expiration;
  }

  const LCExpiration& get_noncur_expiration() const {
    return noncur_expiration;
  }

  const LCExpiration& get_mp_expiration() const {
    return mp_expiration;
  }

  bool get_dm_expiration() const {
    return dm_expiration;
  }

  const map<string, LCTransition>& get_transitions() const {
    return transitions;
  }

  const map<string, LCTransition>& get_noncur_transitions() const {
    return noncur_transitions;
  }

  void set_id(const string& _id) {
    id = _id;
  }

  void set_prefix(const string& _prefix) {
    prefix = _prefix;
  }

  void set_status(const string& _status) {
    status = _status;
  }

  void set_expiration(const LCExpiration& _expiration) {
    expiration = _expiration;
  }

  void set_noncur_expiration(const LCExpiration& _noncur_expiration) {
    noncur_expiration = _noncur_expiration;
  }

  void set_mp_expiration(const LCExpiration& _mp_expiration) {
    mp_expiration = _mp_expiration;
  }

  void set_dm_expiration(bool _dm_expiration) {
    dm_expiration = _dm_expiration;
  }

  bool add_transition(const LCTransition& _transition) {
    auto ret = transitions.emplace(_transition.get_storage_class(), _transition);
    return ret.second;
  }

  bool add_noncur_transition(const LCTransition& _noncur_transition) {
    auto ret = noncur_transitions.emplace(_noncur_transition.get_storage_class(), _noncur_transition);
    return ret.second;
  }

  bool valid() const;
  
  void encode(bufferlist& bl) const {
     ENCODE_START(6, 1, bl);
     encode(id, bl);
     encode(prefix, bl);
     encode(status, bl);
     encode(expiration, bl);
     encode(noncur_expiration, bl);
     encode(mp_expiration, bl);
     encode(dm_expiration, bl);
     encode(filter, bl);
     encode(transitions, bl);
     encode(noncur_transitions, bl);
     ENCODE_FINISH(bl);
   }
   void decode(bufferlist::const_iterator& bl) {
     DECODE_START_LEGACY_COMPAT_LEN(6, 1, 1, bl);
     decode(id, bl);
     decode(prefix, bl);
     decode(status, bl);
     decode(expiration, bl);
     if (struct_v >=2) {
       decode(noncur_expiration, bl);
     }
     if (struct_v >= 3) {
       decode(mp_expiration, bl);
     }
     if (struct_v >= 4) {
        decode(dm_expiration, bl);
     }
     if (struct_v >= 5) {
       decode(filter, bl);
     }
     if (struct_v >= 6) {
       decode(transitions, bl);
       decode(noncur_transitions, bl);
     }
     DECODE_FINISH(bl);
   }
  void dump(Formatter *f) const;

  void init_simple_days_rule(std::string_view _id, std::string_view _prefix, int num_days);
};
WRITE_CLASS_ENCODER(LCRule)

struct transition_action
{
  int days;
  boost::optional<ceph::real_time> date;
  string storage_class;
  transition_action() : days(0) {}
};

struct lc_op
{
  bool status{false};
  bool dm_expiration{false};
  int expiration{0};
  int noncur_expiration{0};
  int mp_expiration{0};
  boost::optional<ceph::real_time> expiration_date;
  boost::optional<RGWObjTags> obj_tags;
  map<string, transition_action> transitions;
  map<string, transition_action> noncur_transitions;
  
  void dump(Formatter *f) const;
};

class RGWLifecycleConfiguration
{
protected:
  CephContext *cct;
  map<string, lc_op> prefix_map;
  multimap<string, LCRule> rule_map;
  bool _add_rule(const LCRule& rule);
  bool has_same_action(const lc_op& first, const lc_op& second);
public:
  explicit RGWLifecycleConfiguration(CephContext *_cct) : cct(_cct) {}
  RGWLifecycleConfiguration() : cct(NULL) {}

  void set_ctx(CephContext *ctx) {
    cct = ctx;
  }

  virtual ~RGWLifecycleConfiguration() {}

//  int get_perm(string& id, int perm_mask);
//  int get_group_perm(ACLGroupTypeEnum group, int perm_mask);
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(rule_map, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(rule_map, bl);
    multimap<string, LCRule>::iterator iter;
    for (iter = rule_map.begin(); iter != rule_map.end(); ++iter) {
      LCRule& rule = iter->second;
      _add_rule(rule);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWLifecycleConfiguration*>& o);

  void add_rule(const LCRule& rule);

  int check_and_add_rule(const LCRule& rule);

  bool valid();

  multimap<string, LCRule>& get_rule_map() { return rule_map; }
  map<string, lc_op>& get_prefix_map() { return prefix_map; }
/*
  void create_default(string id, string name) {
    ACLGrant grant;
    grant.set_canon(id, name, RGW_PERM_FULL_CONTROL);
    add_grant(&grant);
  }
*/
};
WRITE_CLASS_ENCODER(RGWLifecycleConfiguration)

class RGWLC : public DoutPrefixProvider {
  CephContext *cct;
  RGWRados *store;
  int max_objs{0};
  string *obj_names{nullptr};
  std::atomic<bool> down_flag = { false };
  string cookie;

  class LCWorker : public Thread {
    const DoutPrefixProvider *dpp;
    CephContext *cct;
    RGWLC *lc;
    Mutex lock;
    Cond cond;

  public:
    LCWorker(const DoutPrefixProvider* _dpp, CephContext *_cct, RGWLC *_lc) : dpp(_dpp), cct(_cct), lc(_lc), lock("LCWorker") {}
    void *entry() override;
    void stop();
    bool should_work(utime_t& now);
    int schedule_next_start_time(utime_t& start, utime_t& now);
  };
  
  public:
  LCWorker *worker;
  RGWLC() : cct(NULL), store(NULL), worker(NULL) {}
  ~RGWLC() {
    stop_processor();
    finalize();
  }

  void initialize(CephContext *_cct, RGWRados *_store);
  void finalize();

  int process();
  int process(int index, int max_secs);
  bool if_already_run_today(time_t& start_date);
  int list_lc_progress(const string& marker, uint32_t max_entries, map<string, int> *progress_map);
  int bucket_lc_prepare(int index);
  int bucket_lc_process(string& shard_id);
  int bucket_lc_post(int index, int max_lock_sec, pair<string, int >& entry, int& result);
  bool going_down();
  void start_processor();
  void stop_processor();
  int set_bucket_config(RGWBucketInfo& bucket_info,
                        const map<string, bufferlist>& bucket_attrs,
                        RGWLifecycleConfiguration *config);
  int remove_bucket_config(RGWBucketInfo& bucket_info,
                           const map<string, bufferlist>& bucket_attrs);

  CephContext *get_cct() const override { return store->ctx(); }
  unsigned get_subsys() const;
  std::ostream& gen_prefix(std::ostream& out) const;

  private:

  int handle_multipart_expiration(RGWRados::Bucket *target, const map<string, lc_op>& prefix_map);
};



#endif
