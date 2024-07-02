// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_SCRUB_RESULT_H
#define CEPH_SCRUB_RESULT_H

#include "common/LogClient.h"
#include "common/map_cacher.hpp"
#include "osd/SnapMapper.h"  // for OSDriver

namespace librados {
struct object_id_t;
}

struct inconsistent_obj_wrapper;
struct inconsistent_snapset_wrapper;

namespace Scrub {

/**
 * Storing errors detected during scrubbing.
 *
 * From both functional and internal perspectives, the store is a pair of key-value
 * databases: one maps objects to shallow errors detected during their scrubbing,
 * and other stores deep errors.
 * Note that the first store is updated in both shallow and in deep scrubs. The
 * second - only while deep scrubbing.
 *
 * The DBs can be consulted by the operator, when trying to list 'errors known
 * at this point in time'. Whenever a scrub starts - the relevant entries in the
 * DBs are removed. Specifically - the shallow errors DB is recreated each scrub,
 * while the deep errors DB is recreated only when a deep scrub starts.
 *
 * When queried - the data from both DBs is merged for each named object, and
 * returned to the operator.
 *
 * Implementation:
 * Each of the two DBs is implemented as OMAP entries of a single, uniquely named,
 * object. Both DBs are cached using the general KV Cache mechanism.
 */

class Store {
 public:
  ~Store();
  static Store* create(
      ObjectStore* store,
      ObjectStore::Transaction* t,
      const spg_t& pgid,
      const coll_t& coll,
      LoggerSinkSet& logger);

  void add_object_error(int64_t pool, const inconsistent_obj_wrapper& e);
  void add_snap_error(int64_t pool, const inconsistent_snapset_wrapper& e);

  // and a variant-friendly interface:
  void add_error(int64_t pool, const inconsistent_obj_wrapper& e);
  void add_error(int64_t pool, const inconsistent_snapset_wrapper& e);
  bool empty() const;
  void flush(ObjectStore::Transaction*);
  void cleanup(ObjectStore::Transaction*, scrub_level_t level);

  std::vector<ceph::buffer::list> get_snap_errors(
      int64_t pool,
      const librados::object_id_t& start,
      uint64_t max_return) const;

  std::vector<ceph::buffer::list> get_object_errors(
      int64_t pool,
      const librados::object_id_t& start,
      uint64_t max_return) const;

 private:
  // the collection (i.e. - the PG store) in which the errors are stored
  const coll_t coll;

  LoggerSinkSet& clog;

  // the machinery for storing the shallow errors: a fake object in the PG store,
  // caching mechanism, and the actual backend
  const ghobject_t shallow_hoid;
  OSDriver shallow_driver;  //< a temp object mapping seq-id to inconsistencies
  mutable MapCacher::MapCacher<std::string, ceph::buffer::list> shallow_backend;

  // same for all deep errors
  const ghobject_t deep_hoid;
  OSDriver deep_driver;
  mutable MapCacher::MapCacher<std::string, ceph::buffer::list> deep_backend;

  std::map<std::string, ceph::buffer::list> shallow_results;
  std::map<std::string, ceph::buffer::list> deep_results;
  using CacherPosData =
      MapCacher::MapCacher<std::string, ceph::buffer::list>::PosAndData;
  using ExpCacherPosData = tl::expected<CacherPosData, int>;

  Store(
      const coll_t& coll,
      const ghobject_t& oid,
      const ghobject_t& deep_oid,
      ObjectStore* store,
      LoggerSinkSet& logger);

  std::vector<ceph::buffer::list> get_errors(
      const std::string& start,
      const std::string& end,
      uint64_t max_return) const;

  void collect_specific_store(
      MapCacher::MapCacher<std::string, ceph::buffer::list>& backend,
      ExpCacherPosData& latest,
      std::vector<bufferlist>& errors,
      const std::string& end_key,
      uint64_t& max_return) const;
};
}  // namespace Scrub

#endif	// CEPH_SCRUB_RESULT_H
