// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#ifndef CEPH_RGW_ORPHAN_H
#define CEPH_RGW_ORPHAN_H

#include "common/config.h"
#include "common/Formatter.h"
#include "common/errno.h"

#include "rgw_rados.h"

#define dout_subsys ceph_subsys_rgw

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
  string marker;

  RGWOrphanSearchStage() : stage(ORPHAN_SEARCH_STAGE_UNKNOWN), shard(0) {}
  explicit RGWOrphanSearchStage(RGWOrphanSearchStageId _stage) : stage(_stage), shard(0) {}
  RGWOrphanSearchStage(RGWOrphanSearchStageId _stage, int _shard, const string& _marker) : stage(_stage), shard(_shard), marker(_marker) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode((int)stage, bl);
    ::encode(shard, bl);
    ::encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    int s;
    ::decode(s, bl);
    stage = (RGWOrphanSearchStageId)s;
    ::decode(shard, bl);
    ::decode(marker, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(RGWOrphanSearchStage)
  
struct RGWOrphanSearchInfo {
  string job_name;
  string pool;
  uint16_t num_shards;
  utime_t start_time;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(job_name, bl);
    ::encode(pool, bl);
    ::encode(num_shards, bl);
    ::encode(start_time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(job_name, bl);
    ::decode(pool, bl);
    ::decode(num_shards, bl);
    ::decode(start_time, bl);
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
    ::encode(info, bl);
    ::encode(stage, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(info, bl);
    ::decode(stage, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(RGWOrphanSearchState)

class RGWOrphanStore {
  RGWRados *store;
  librados::IoCtx ioctx;

  string oid;

public:
  explicit RGWOrphanStore(RGWRados *_store) : store(_store), oid(RGW_ORPHAN_INDEX_OID) {}

  librados::IoCtx& get_ioctx() { return ioctx; }

  int init();

  int read_job(const string& job_name, RGWOrphanSearchState& state);
  int write_job(const string& job_name, const RGWOrphanSearchState& state);
  int remove_job(const string& job_name);
  int list_jobs(map<string,RGWOrphanSearchState> &job_list);


  int store_entries(const string& oid, const map<string, bufferlist>& entries);
  int read_entries(const string& oid, const string& marker, map<string, bufferlist> *entries, bool *truncated);
};


class RGWOrphanSearch {
  RGWRados *store;

  RGWOrphanStore orphan_store;

  RGWOrphanSearchInfo search_info;
  RGWOrphanSearchStage search_stage;

  map<int, string> all_objs_index;
  map<int, string> buckets_instance_index;
  map<int, string> linked_objs_index;

  string index_objs_prefix;

  uint16_t max_concurrent_ios;
  uint64_t stale_secs;

  struct log_iter_info {
    string oid;
    list<string>::iterator cur;
    list<string>::iterator end;
  };

  int log_oids(map<int, string>& log_shards, map<int, list<string> >& oids);

#define RGW_ORPHANSEARCH_HASH_PRIME 7877
  int orphan_shard(const string& str) {
    return ceph_str_hash_linux(str.c_str(), str.size()) % RGW_ORPHANSEARCH_HASH_PRIME % search_info.num_shards;
  }

  int handle_stat_result(map<int, list<string> >& oids, RGWRados::Object::Stat::Result& result);
  int pop_and_handle_stat_op(map<int, list<string> >& oids, std::deque<RGWRados::Object::Stat>& ops);


  int remove_index(map<int, string>& index);
public:
  RGWOrphanSearch(RGWRados *_store, int _max_ios, uint64_t _stale_secs) : store(_store), orphan_store(store), max_concurrent_ios(_max_ios), stale_secs(_stale_secs) {}

  int save_state() {
    RGWOrphanSearchState state;
    state.info = search_info;
    state.stage = search_stage;
    return orphan_store.write_job(search_info.job_name, state);
  }

  int init(const string& job_name, RGWOrphanSearchInfo *info);

  int create(const string& job_name, int num_shards);

  int build_all_oids_index();
  int build_buckets_instance_index();
  int build_linked_oids_for_bucket(const string& bucket_instance_id, map<int, list<string> >& oids);
  int build_linked_oids_index();
  int compare_oid_indexes();

  int run();
  int finish();
};



#endif
