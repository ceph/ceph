// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "common/config.h"
#include "common/Formatter.h"
#include "common/errno.h"

#include "rgw_sal_rados.h"

#define RGW_ORPHAN_INDEX_OID "orphan.index"
#define RGW_ORPHAN_INDEX_PREFIX "orphan.scan"


enum RGWOrphanSearchStageId {
  ORPHAN_SEARCH_STAGE_UNKNOWN = 0,
  ORPHAN_SEARCH_STAGE_INIT = 1,
  ORPHAN_SEARCH_STAGE_LSPOOL = 2,
  ORPHAN_SEARCH_STAGE_LSBUCKETS = 3,
  ORPHAN_SEARCH_STAGE_ITERATE_BI = 4,
  ORPHAN_SEARCH_STAGE_COMPARE = 5,
};


struct RGWOrphanSearchStage {
  RGWOrphanSearchStageId stage;
  int shard;
  std::string marker;

  RGWOrphanSearchStage() : stage(ORPHAN_SEARCH_STAGE_UNKNOWN), shard(0) {}
  explicit RGWOrphanSearchStage(RGWOrphanSearchStageId _stage) : stage(_stage), shard(0) {}
  RGWOrphanSearchStage(RGWOrphanSearchStageId _stage, int _shard, const std::string& _marker) : stage(_stage), shard(_shard), marker(_marker) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode((int)stage, bl);
    encode(shard, bl);
    encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    int s;
    decode(s, bl);
    stage = (RGWOrphanSearchStageId)s;
    decode(shard, bl);
    decode(marker, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(RGWOrphanSearchStage)

struct RGWOrphanSearchInfo {
  std::string job_name;
  rgw_pool pool;
  uint16_t num_shards;
  utime_t start_time;

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(job_name, bl);
    encode(pool.to_str(), bl);
    encode(num_shards, bl);
    encode(start_time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(job_name, bl);
    std::string s;
    decode(s, bl);
    pool.from_str(s);
    decode(num_shards, bl);
    decode(start_time, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(RGWOrphanSearchInfo)

struct RGWOrphanSearchState {
  RGWOrphanSearchInfo info;
  RGWOrphanSearchStage stage;

  RGWOrphanSearchState() : stage(ORPHAN_SEARCH_STAGE_UNKNOWN) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(info, bl);
    encode(stage, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(info, bl);
    decode(stage, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(RGWOrphanSearchState)

class RGWOrphanStore {
  rgw::sal::RadosStore* store;
  librados::IoCtx ioctx;

  std::string oid;

public:
  explicit RGWOrphanStore(rgw::sal::RadosStore* _store) : store(_store), oid(RGW_ORPHAN_INDEX_OID) {}

  librados::IoCtx& get_ioctx() { return ioctx; }

  int init(const DoutPrefixProvider *dpp);

  int read_job(const std::string& job_name, RGWOrphanSearchState& state);
  int write_job(const std::string& job_name, const RGWOrphanSearchState& state);
  int remove_job(const std::string& job_name);
  int list_jobs(std::map<std::string,RGWOrphanSearchState> &job_list);


  int store_entries(const DoutPrefixProvider *dpp, const std::string& oid, const std::map<std::string, bufferlist>& entries);
  int read_entries(const std::string& oid, const std::string& marker, std::map<std::string, bufferlist> *entries, bool *truncated);
};


class RGWOrphanSearch {
  rgw::sal::RadosStore* store;

  RGWOrphanStore orphan_store;

  RGWOrphanSearchInfo search_info;
  RGWOrphanSearchStage search_stage;

  std::map<int, std::string> all_objs_index;
  std::map<int, std::string> buckets_instance_index;
  std::map<int, std::string> linked_objs_index;

  std::string index_objs_prefix;

  uint16_t max_concurrent_ios;
  uint64_t stale_secs;
  int64_t max_list_bucket_entries;

  bool detailed_mode;

  struct log_iter_info {
    std::string oid;
    std::list<std::string>::iterator cur;
    std::list<std::string>::iterator end;
  };

  int log_oids(const DoutPrefixProvider *dpp, std::map<int, std::string>& log_shards, std::map<int, std::list<std::string> >& oids);

#define RGW_ORPHANSEARCH_HASH_PRIME 7877
  int orphan_shard(const std::string& str) {
    return ceph_str_hash_linux(str.c_str(), str.size()) % RGW_ORPHANSEARCH_HASH_PRIME % search_info.num_shards;
  }

  int handle_stat_result(const DoutPrefixProvider *dpp, std::map<int, std::list<std::string> >& oids, RGWRados::Object::Stat::Result& result);
  int pop_and_handle_stat_op(const DoutPrefixProvider *dpp, std::map<int, std::list<std::string> >& oids, std::deque<RGWRados::Object::Stat>& ops);

  int remove_index(std::map<int, std::string>& index);
public:
  RGWOrphanSearch(rgw::sal::RadosStore* _store, int _max_ios, uint64_t _stale_secs) : store(_store), orphan_store(store), max_concurrent_ios(_max_ios), stale_secs(_stale_secs) {}

  int save_state() {
    RGWOrphanSearchState state;
    state.info = search_info;
    state.stage = search_stage;
    return orphan_store.write_job(search_info.job_name, state);
  }

  int init(const DoutPrefixProvider *dpp, const std::string& job_name, RGWOrphanSearchInfo *info, bool _detailed_mode=false);

  int create(const std::string& job_name, int num_shards);

  int build_all_oids_index(const DoutPrefixProvider *dpp);
  int build_buckets_instance_index(const DoutPrefixProvider *dpp);
  int build_linked_oids_for_bucket(const DoutPrefixProvider *dpp, const std::string& bucket_instance_id, std::map<int, std::list<std::string> >& oids);
  int build_linked_oids_index(const DoutPrefixProvider *dpp);
  int compare_oid_indexes(const DoutPrefixProvider *dpp);

  int run(const DoutPrefixProvider *dpp);
  int finish();
};


class RGWRadosList {

  /*
   * process_t describes how to process a irectory, we will either
   * process the whole thing (entire_container == true) or a portion
   * of it (entire_container == false). When we only process a
   * portion, we will list the specific keys and/or specific lexical
   * prefixes.
   */
  struct process_t {
    bool entire_container;
    std::set<rgw_obj_key> filter_keys;
    std::set<std::string> prefixes;

    process_t() :
      entire_container(false)
    {}
  };

  std::map<std::string,process_t> bucket_process_map;
  std::set<std::string> visited_oids;

  void add_bucket_entire(const std::string& bucket_name) {
    auto p = bucket_process_map.emplace(std::make_pair(bucket_name,
						       process_t()));
    p.first->second.entire_container = true;
  }

  void add_bucket_prefix(const std::string& bucket_name,
			 const std::string& prefix) {
    auto p = bucket_process_map.emplace(std::make_pair(bucket_name,
						       process_t()));
    p.first->second.prefixes.insert(prefix);
  }

  void add_bucket_filter(const std::string& bucket_name,
			 const rgw_obj_key& obj_key) {
    auto p = bucket_process_map.emplace(std::make_pair(bucket_name,
						       process_t()));
    p.first->second.filter_keys.insert(obj_key);
  }

  rgw::sal::RadosStore* store;

  uint16_t max_concurrent_ios;
  uint64_t stale_secs;
  std::string tenant_name;

  bool include_rgw_obj_name;
  std::string field_separator;

  int handle_stat_result(const DoutPrefixProvider *dpp,
			 RGWRados::Object::Stat::Result& result,
			 std::string& bucket_name,
			 rgw_obj_key& obj_key,
			 std::set<std::string>& obj_oids);
  int pop_and_handle_stat_op(const DoutPrefixProvider *dpp,
                             RGWObjectCtx& obj_ctx,
			     std::deque<RGWRados::Object::Stat>& ops);

public:

  RGWRadosList(rgw::sal::RadosStore* _store,
	       int _max_ios,
	       uint64_t _stale_secs,
	       const std::string& _tenant_name) :
    store(_store),
    max_concurrent_ios(_max_ios),
    stale_secs(_stale_secs),
    tenant_name(_tenant_name),
    include_rgw_obj_name(false)
  {}

  int process_bucket(const DoutPrefixProvider *dpp,
                     const std::string& bucket_instance_id,
		     const std::string& prefix,
		     const std::set<rgw_obj_key>& entries_filter);

  int do_incomplete_multipart(const DoutPrefixProvider *dpp,
			      rgw::sal::Bucket* bucket);

  int build_linked_oids_index();

  int run(const DoutPrefixProvider *dpp,
	  const std::string& bucket_name,
	  const std::string& object_name = std::string(),
	  const std::string& object_version = std::string(),
	  const bool silent_indexless = false);
  int run(const DoutPrefixProvider *dpp,
	  const bool yes_i_really_mean_it = false);

  // if there's a non-empty field separator, that means we'll display
  // bucket and object names
  void set_field_separator(const std::string& fs) {
    field_separator = fs;
    include_rgw_obj_name = !field_separator.empty();
  }
}; // class RGWRadosList
