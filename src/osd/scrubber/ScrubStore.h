// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>
#include <vector>

#include "common/map_cacher.hpp"
#include "osd/osd_types_fmt.h"
#include "osd/SnapMapper.h"  // for OSDriver

namespace librados {
struct object_id_t;
}

struct inconsistent_obj_wrapper;
struct inconsistent_snapset_wrapper;
class PgScrubber;

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

  Store(
      PgScrubber& scrubber,
      ObjectStore& osd_store,
      ObjectStore::Transaction* t,
      const spg_t& pgid,
      const coll_t& coll);


  /// mark down detected errors, either shallow or deep
  void add_object_error(int64_t pool, const inconsistent_obj_wrapper& e);

  void add_snap_error(int64_t pool, const inconsistent_snapset_wrapper& e);

  // and a variant-friendly interface:
  void add_error(int64_t pool, const inconsistent_obj_wrapper& e);
  void add_error(int64_t pool, const inconsistent_snapset_wrapper& e);

  [[nodiscard]] bool is_empty() const;
  void flush(ObjectStore::Transaction*);

  /// remove both shallow and deep errors DBs. Called on interval.
  void cleanup(ObjectStore::Transaction*);

  /**
   * prepare the Store object for a new scrub session.
   * This involves clearing one or both of the errors DBs, and resetting
   * the cache.
   *
   * @param level: the scrub level to prepare for. Whenever a deep scrub
   * is requested, both the shallow and deep errors DBs are cleared.
   * If, on the other hand, a shallow scrub is requested, only the shallow
   * errors DB is cleared.
   */
  void reinit(ObjectStore::Transaction* t, scrub_level_t level);

  std::vector<ceph::buffer::list> get_snap_errors(
    int64_t pool,
    const librados::object_id_t& start,
    uint64_t max_return) const;

  std::vector<ceph::buffer::list> get_object_errors(
    int64_t pool,
    const librados::object_id_t& start,
    uint64_t max_return) const;

  std::ostream& gen_prefix(std::ostream& out, std::string_view fn) const;

 private:
  /**
   * at_level_t
   *
   * The machinery for caching and storing errors at a specific scrub level.
   */
  struct at_level_t {
    at_level_t(const spg_t& pgid, const ghobject_t& err_obj, OSDriver&& drvr)
	: errors_hoid{err_obj}
	, driver{std::move(drvr)}
	, backend{&driver}
    {}

    /// the object in the PG store, where the errors are stored
    ghobject_t errors_hoid;

    /// abstracted key fetching
    OSDriver driver;

    /// a K,V cache for the errors that are detected during the scrub
    /// session. The errors marked for a specific object are stored as
    /// an OMap entry with the object's name as the key.
    MapCacher::MapCacher<std::string, ceph::buffer::list> backend;

    /// a temp object mapping seq-id to inconsistencies
    std::map<std::string, ceph::buffer::list> results;
  };

  using CacherPosData =
      MapCacher::MapCacher<std::string, ceph::buffer::list>::PosAndData;
  using ExpCacherPosData = tl::expected<CacherPosData, int>;

  /// access to the owning Scrubber object, for logging mostly
  PgScrubber& m_scrubber;

  /// the OSD's storage backend
  ObjectStore& object_store;

  /// the collection (i.e. - the PG store) in which the errors are stored
  const coll_t coll;

  scrub_level_t current_level;

  /**
   * the machinery (backend details, cache, etc.) for storing both levels
   * of errors (note: 'optional' to allow delayed creation w/o dynamic
   * allocations; and 'mutable', as the caching mechanism is used in const
   * methods)
   */
  mutable std::optional<at_level_t> shallow_db;
  mutable std::optional<at_level_t> deep_db;

  std::vector<ceph::buffer::list> get_errors(
      const std::string& start,
      const std::string& end,
      uint64_t max_return) const;

  void collect_specific_store(
      MapCacher::MapCacher<std::string, ceph::buffer::list>& backend,
      ExpCacherPosData& latest,
      std::vector<bufferlist>& errors,
      std::string_view end_key,
      uint64_t max_return) const;

  /**
   * Clear the DB of errors at a specific scrub level by performing an
   * omap_clear() on the DB object, and resetting the MapCacher.
   */
  void clear_level_db(
      ObjectStore::Transaction* t,
      at_level_t& db,
      std::string_view db_name);

  /**
   * merge the two error wrappers - fetched from both DBs for the same object.
   * Specifically, the object errors are or'ed, and so are the per-shard
   * entries.
   */
  bufferlist merge_encoded_error_wrappers(
      hobject_t obj,
      ExpCacherPosData& latest_sh,
      ExpCacherPosData& latest_dp) const;
};
}  // namespace Scrub
