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


enum OrphanSearchState {
  ORPHAN_SEARCH_UNKNOWN = 0,
  ORPHAN_SEARCH_INIT = 1,
  ORPHAN_SEARCH_LSPOOL = 2,
  ORPHAN_SEARCH_LSBUCKETS = 3,
  ORPHAN_SEARCH_ITERATE_BI = 4,
  ORPHAN_SEARCH_DONE = 5,
};


struct RGWOrphanSearchInfo {
  string job_name;
  string pool;
  uint16_t num_shards;
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(job_name, bl);
    ::encode(pool, bl);
    ::encode(num_shards, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(job_name, bl);
    ::decode(pool, bl);
    ::decode(num_shards, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(RGWOrphanSearchInfo)

struct RGWOrphanSearchState {
  RGWOrphanSearchInfo info;
  OrphanSearchState state;
  bufferlist state_info;

  RGWOrphanSearchState() : state(ORPHAN_SEARCH_UNKNOWN) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(info, bl);
    ::encode((int)state, bl);
    ::encode(state_info, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(info, bl);
    int s;
    ::decode(s, bl);
    state = (OrphanSearchState)s;
    ::decode(state_info, bl);
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
  RGWOrphanStore(RGWRados *_store) : store(_store) {
    oid = RGW_ORPHAN_INDEX_OID;
  }

  int init();

  int read_job(const string& job_name, RGWOrphanSearchState& state);
  int write_job(const string& job_name, const RGWOrphanSearchState& state);


  int store_entries(const string& oid, const map<string, bufferlist>& entries);
  int read_entries(const string& oid, const string& marker, map<string, bufferlist> *entries, bool *truncated);
};


class RGWOrphanSearch {
  RGWRados *store;
  librados::IoCtx log_ioctx;

  RGWOrphanStore orphan_store;

  RGWOrphanSearchInfo search_info;
  OrphanSearchState search_state;

  map<int, string> all_objs_index;
  map<int, string> buckets_instance_index;
  map<int, string> linked_objs_index;

  string index_objs_prefix;

  uint16_t max_concurrent_ios;

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

public:
  RGWOrphanSearch(RGWRados *_store, int _max_ios) : store(_store), orphan_store(store), max_concurrent_ios(_max_ios) {}

  int save_state() {
    RGWOrphanSearchState state;
    state.info = search_info;
    state.state = search_state;
    return orphan_store.write_job(search_info.job_name, state);
  }

  int init(const string& job_name, RGWOrphanSearchInfo *info);

  int create(const string& job_name, int num_shards);

  int build_all_oids_index();
  int build_buckets_instance_index();
  int build_linked_oids_for_bucket(const string& bucket_instance_id);
  int build_linked_oids_index();

  int run();
};



#endif
