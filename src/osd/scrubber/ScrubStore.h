// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_SCRUB_RESULT_H
#define CEPH_SCRUB_RESULT_H

#include "osd/SnapMapper.h"		// for OSDriver
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

  // and a variant-friendly interface:
  void add_error(int64_t pool, const inconsistent_obj_wrapper& e);
  void add_error(int64_t pool, const inconsistent_snapset_wrapper& e);

  bool empty() const;
  void flush(ObjectStore::Transaction *);
  void cleanup(ObjectStore::Transaction *);
  std::vector<ceph::buffer::list> get_snap_errors(int64_t pool,
					  const librados::object_id_t& start,
					  uint64_t max_return) const;
  std::vector<ceph::buffer::list> get_object_errors(int64_t pool,
					    const librados::object_id_t& start,
					    uint64_t max_return) const;
private:
  Store(const coll_t& coll, const ghobject_t& oid, ObjectStore* store);
  std::vector<ceph::buffer::list> get_errors(const std::string& start, const std::string& end,
				     uint64_t max_return) const;
private:
  const coll_t coll;
  const ghobject_t hoid;
  // a temp object holding mappings from seq-id to inconsistencies found in
  // scrubbing
  OSDriver driver;
  mutable MapCacher::MapCacher<std::string, ceph::buffer::list> backend;
  std::map<std::string, ceph::buffer::list> results;
};
}

#endif // CEPH_SCRUB_RESULT_H
