// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <map>
#include <array>
#include <string>
#include <iostream>

#include "common/debug.h"

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/iso_8601.h"
#include "common/Thread.h"
#include "rgw_common.h"
#include "cls/rgw/cls_rgw_types.h"
#include "rgw_tag.h"
#include "rgw_sal.h"

#include <atomic>
#include <tuple>

#define HASH_PRIME 7877
#define MAX_ID_LEN 255
static std::string lc_oid_prefix = "lc";
static std::string lc_index_lock_name = "lc_process";

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
  std::string days;
  //At present only current object has expiration date
  std::string date;
  std::string newer_noncurrent;
public:
  LCExpiration() {}
  LCExpiration(const std::string& _days, const std::string& _date) : days(_days), date(_date) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(4, 2, bl);
    encode(days, bl);
    encode(date, bl);
    encode(newer_noncurrent, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(4, 2, 2, bl);
    decode(days, bl);
    if (struct_v >= 3) {
      decode(date, bl);
      if (struct_v >= 4) {
	decode(newer_noncurrent, bl);
      }
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  //  static void generate_test_instances(list<ACLOwner*>& o);
  void set_days(const std::string& _days) { days = _days; }
  std::string get_days_str() const {
    return days;
  }
  int get_days() const {return atoi(days.c_str()); }
  bool has_days() const {
    return !days.empty();
  }
  void set_newer(const std::string& _newer) { newer_noncurrent = _newer; }
  int get_newer() const {return atoi(newer_noncurrent.c_str()); }
  bool has_newer() const {
    return !newer_noncurrent.empty();
  }
  void set_date(const std::string& _date) { date = _date; }
  std::string get_date() const {
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
  std::string days;
  std::string date;
  std::string storage_class;

public:
  int get_days() const {
    return atoi(days.c_str());
  }

  std::string get_date() const {
    return date;
  }

  std::string get_storage_class() const {
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
    } else if (!days.empty() && get_days() < 0) {
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
  void dump(Formatter *f) const {  
    f->dump_string("days", days);
    f->dump_string("date", date);
    f->dump_string("storage_class", storage_class);
  }
};
WRITE_CLASS_ENCODER(LCTransition)

enum class LCFlagType : uint16_t
{
  none = 0,
  ArchiveZone,
};

class LCFlag {
public:
  LCFlagType bit;
  const char* name;

  constexpr LCFlag(LCFlagType ord, const char* name) : bit(ord), name(name)
    {}
};

class LCFilter
{
 public:

  static constexpr uint32_t make_flag(LCFlagType type) {
    switch (type) {
    case LCFlagType::none:
      return 0;
      break;
    default:
      return 1 << (uint32_t(type) - 1);
    }
   }

  static constexpr std::array<LCFlag, 2> filter_flags =
  {
    LCFlag(LCFlagType::none, "none"),
    LCFlag(LCFlagType::ArchiveZone, "ArchiveZone"),
  };

protected:
  std::string prefix;
  std::string size_gt;
  std::string size_lt;
  RGWObjTags obj_tags;
  uint32_t flags;

public:

  LCFilter() : flags(make_flag(LCFlagType::none))
    {}

  const std::string& get_prefix() const {
    return prefix;
  }

  const RGWObjTags& get_tags() const {
    return obj_tags;
  }

  const uint32_t get_flags() const {
    return flags;
  }

  bool empty() const {
    return !(has_prefix() || has_tags() || has_flags() ||
	     has_size_rule());
  }

  // Determine if we need AND tag when creating xml
  bool has_multi_condition() const {
    if (obj_tags.count() + int(has_prefix()) + int(has_flags()) + int(has_size_rule()) > 1) {
	return true;
    }
    return false;
  }

  bool has_prefix() const {
    return !prefix.empty();
  }

  bool has_tags() const {
    return !obj_tags.empty();
  }

  bool has_size_gt() const {
    return !(size_gt.empty());
  }

  bool has_size_lt() const {
    return !(size_lt.empty());
  }

  bool has_size_rule() const {
    return (has_size_gt() || has_size_lt());
  }

  uint64_t get_size_gt() const {
    uint64_t sz{0};
    try {
      sz = uint64_t(std::stoull(size_gt));
    } catch (...) {}
    return sz;
  }

  uint64_t get_size_lt() const {
    uint64_t sz{0};
    try {
      sz = uint64_t(std::stoull(size_lt));
    } catch (...) {}
    return sz;
  }

  bool has_flags() const {
    return !(flags == uint32_t(LCFlagType::none));
  }

  bool have_flag(LCFlagType flag) const {
    return flags & make_flag(flag);
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(4, 1, bl);
    encode(prefix, bl);
    encode(obj_tags, bl);
    encode(flags, bl);
    encode(size_gt, bl);
    encode(size_lt, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(3, bl);
    decode(prefix, bl);
    if (struct_v >= 2) {
      decode(obj_tags, bl);
      if (struct_v >= 3) {
	decode(flags, bl);
	if (struct_v >= 4) {
	  decode(size_gt, bl);
	  decode(size_lt, bl);
	}
      }
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(LCFilter)

class LCRule
{
protected:
  std::string id;
  std::string prefix;
  std::string status;
  LCExpiration expiration;
  LCExpiration noncur_expiration;
  LCExpiration mp_expiration;
  LCFilter filter;
  std::map<std::string, LCTransition> transitions;
  std::map<std::string, LCTransition> noncur_transitions;
  bool dm_expiration = false;

public:

  LCRule(){};
  virtual ~LCRule() {}

  const std::string& get_id() const {
      return id;
  }

  const std::string& get_status() const {
      return status;
  }

  bool is_enabled() const {
    return status == "Enabled";
  }

  void set_enabled(bool flag) {
    status = (flag ? "Enabled" : "Disabled");
  }

  const std::string& get_prefix() const {
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

  const std::map<std::string, LCTransition>& get_transitions() const {
    return transitions;
  }

  const std::map<std::string, LCTransition>& get_noncur_transitions() const {
    return noncur_transitions;
  }

  void set_id(const std::string& _id) {
    id = _id;
  }

  void set_prefix(const std::string& _prefix) {
    prefix = _prefix;
  }

  void set_status(const std::string& _status) {
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
  std::string storage_class;
  transition_action() : days(0) {}
  void dump(Formatter *f) const {
    if (!date) {
      f->dump_int("days", days);
    } else {
      utime_t ut(*date);
      f->dump_stream("date") << ut;
    }
  }
};

/* XXX why not LCRule? */
struct lc_op
{
  std::string id;
  bool status{false};
  bool dm_expiration{false};
  int expiration{0};
  int noncur_expiration{0};
  uint64_t newer_noncurrent{0};
  int mp_expiration{0};
  boost::optional<uint64_t> size_gt;
  boost::optional<uint64_t> size_lt;
  boost::optional<ceph::real_time> expiration_date;
  boost::optional<RGWObjTags> obj_tags;
  std::map<std::string, transition_action> transitions;
  std::map<std::string, transition_action> noncur_transitions;
  uint32_t rule_flags;

  /* ctors are nice */
  lc_op() = delete;

  lc_op(const std::string id) : id(id)
    {}

  void dump(Formatter *f) const;
};

class RGWLifecycleConfiguration
{
protected:
  CephContext *cct;
  std::multimap<std::string, lc_op> prefix_map;
  std::multimap<std::string, LCRule> rule_map;
  bool _add_rule(const LCRule& rule);
public:
  explicit RGWLifecycleConfiguration(CephContext *_cct) : cct(_cct) {}
  RGWLifecycleConfiguration() : cct(NULL) {}

  void set_ctx(CephContext *ctx) {
    cct = ctx;
  }

  virtual ~RGWLifecycleConfiguration() {}

//  int get_perm(std::string& id, int perm_mask);
//  int get_group_perm(ACLGroupTypeEnum group, int perm_mask);
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(rule_map, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(rule_map, bl);
    std::multimap<std::string, LCRule>::iterator iter;
    for (iter = rule_map.begin(); iter != rule_map.end(); ++iter) {
      LCRule& rule = iter->second;
      _add_rule(rule);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWLifecycleConfiguration*>& o);

  void add_rule(const LCRule& rule);

  int check_and_add_rule(const LCRule& rule);

  bool valid();

  std::multimap<std::string, LCRule>& get_rule_map() { return rule_map; }
  std::multimap<std::string, lc_op>& get_prefix_map() { return prefix_map; }
/*
  void create_default(std::string id, std::string name) {
    ACLGrant grant;
    grant.set_canon(id, name, RGW_PERM_FULL_CONTROL);
    add_grant(&grant);
  }
*/
};
WRITE_CLASS_ENCODER(RGWLifecycleConfiguration)

class RGWLC : public DoutPrefixProvider {
  CephContext *cct;
  rgw::sal::Driver* driver;
  std::unique_ptr<rgw::sal::Lifecycle> sal_lc;
  int max_objs{0};
  std::string *obj_names{nullptr};
  std::atomic<bool> down_flag = { false };
  std::string cookie;

public:

  class WorkPool;

  class LCWorker : public Thread
  {
    const DoutPrefixProvider *dpp;
    CephContext *cct;
    RGWLC *lc;
    int ix;
    std::mutex lock;
    std::condition_variable cond;
    WorkPool* workpool{nullptr};
    /* save the target bucket names created as part of object transition
     * to cloud. This list is maintained for the duration of each RGWLC::process()
     * post which it is discarded. */
    std::set<std::string> cloud_targets;

  public:

    using lock_guard = std::lock_guard<std::mutex>;
    using unique_lock = std::unique_lock<std::mutex>;

    LCWorker(const DoutPrefixProvider* dpp, CephContext *_cct, RGWLC *_lc,
	     int ix);
    RGWLC* get_lc() { return lc; }

    std::string thr_name() {
      return std::string{"lc_thrd: "} + std::to_string(ix);
    }

    void *entry() override;
    void stop();
    bool should_work(utime_t& now);
    int schedule_next_start_time(utime_t& start, utime_t& now);
    std::set<std::string>& get_cloud_targets() { return cloud_targets; }
    virtual ~LCWorker() override;

    friend class RGWRados;
    friend class RGWLC;
    friend class WorkQ;
  }; /* LCWorker */

  friend class RGWRados;

  std::vector<std::unique_ptr<RGWLC::LCWorker>> workers;

  RGWLC() : cct(nullptr), driver(nullptr) {}
  virtual ~RGWLC() override;

  void initialize(CephContext *_cct, rgw::sal::Driver* _driver);
  void finalize();

  int process(LCWorker* worker,
	      const std::unique_ptr<rgw::sal::Bucket>& optional_bucket,
	      bool once);
  int advance_head(const std::string& lc_shard,
		   rgw::sal::Lifecycle::LCHead& head,
		   rgw::sal::Lifecycle::LCEntry& entry,
		   time_t start_date);
  int process(int index, int max_lock_secs, LCWorker* worker, bool once);
  int process_bucket(int index, int max_lock_secs, LCWorker* worker,
		     const std::string& bucket_entry_marker, bool once);
  bool expired_session(time_t started);
  time_t thread_stop_at();
  int list_lc_progress(std::string& marker, uint32_t max_entries,
		       std::vector<std::unique_ptr<rgw::sal::Lifecycle::LCEntry>>&,
		       int& index);
  int bucket_lc_process(std::string& shard_id, LCWorker* worker, time_t stop_at,
			bool once);
  int bucket_lc_post(int index, int max_lock_sec,
		     rgw::sal::Lifecycle::LCEntry& entry, int& result, LCWorker* worker);
  bool going_down();
  void start_processor();
  void stop_processor();
  int set_bucket_config(rgw::sal::Bucket* bucket,
                        const rgw::sal::Attrs& bucket_attrs,
                        RGWLifecycleConfiguration *config);
  int remove_bucket_config(rgw::sal::Bucket* bucket,
                           const rgw::sal::Attrs& bucket_attrs,
			   bool merge_attrs = true);

  CephContext *get_cct() const override { return cct; }
  rgw::sal::Lifecycle* get_lc() const { return sal_lc.get(); }
  unsigned get_subsys() const;
  std::ostream& gen_prefix(std::ostream& out) const;

  private:

  int handle_multipart_expiration(rgw::sal::Bucket* target,
				  const std::multimap<std::string, lc_op>& prefix_map,
				  LCWorker* worker, time_t stop_at, bool once);
};

namespace rgw::lc {

int fix_lc_shard_entry(const DoutPrefixProvider *dpp,
                       rgw::sal::Driver* driver,
		       rgw::sal::Lifecycle* sal_lc,
		       rgw::sal::Bucket* bucket);

std::string s3_expiration_header(
  DoutPrefixProvider* dpp,
  const rgw_obj_key& obj_key,
  const RGWObjTags& obj_tagset,
  const ceph::real_time& mtime,
  const std::map<std::string, buffer::list>& bucket_attrs);

bool s3_multipart_abort_header(
  DoutPrefixProvider* dpp,
  const rgw_obj_key& obj_key,
  const ceph::real_time& mtime,
  const std::map<std::string, buffer::list>& bucket_attrs,
  ceph::real_time& abort_date,
  std::string& rule_id);

} // namespace rgw::lc
