// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_SCRUB_RESULT_H
#define CEPH_SCRUB_RESULT_H

#include "SnapMapper.h"		// for OSDriver
#include "common/map_cacher.hpp"

namespace librados {
  struct object_id_t;
}

struct inconsistent_obj_wrapper;
struct inconsistent_snapset_wrapper;

namespace Scrub {

class Store {
public:
  ~Store();
  static Store* create(ObjectStore* store,
		       ObjectStore::Transaction* t,
		       const spg_t& pgid,
		       const coll_t& coll);
  void add_object_error(int64_t pool, const inconsistent_obj_wrapper& e);
  void add_snap_error(int64_t pool, const inconsistent_snapset_wrapper& e);
  bool empty() const;
  void flush(ObjectStore::Transaction *);
  void cleanup(ObjectStore::Transaction *);
  std::vector<bufferlist> get_snap_errors(ObjectStore* store,
					  int64_t pool,
					  const librados::object_id_t& start,
					  uint64_t max_return);
  std::vector<bufferlist> get_object_errors(ObjectStore* store,
					    int64_t pool,
					    const librados::object_id_t& start,
					    uint64_t max_return);
private:
  Store(const coll_t& coll, const ghobject_t& oid, ObjectStore* store);
  std::vector<bufferlist> get_errors(ObjectStore* store,
				     const string& start, const string& end,
				     uint64_t max_return);
private:
  const coll_t coll;
  const ghobject_t hoid;
  // a temp object holding mappings from seq-id to inconsistencies found in
  // scrubbing
  OSDriver driver;
  MapCacher::MapCacher<std::string, bufferlist> backend;
  map<string, bufferlist> results;
};
}

#endif // CEPH_SCRUB_RESULT_H
