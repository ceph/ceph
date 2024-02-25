// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/scrub_types.h"
#include "include/rados/rados_types.hpp"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"

#include "ScrubStore.h"

using std::ostringstream;
using std::string;
using std::vector;

using ceph::bufferlist;

namespace {
/**
 * create two special temp objects in the PG. Those are used to
 * hold the last/ongoing scrub errors detected. The errors are
 * coded as OMAP entries attached to the objects.
 * One of the objects stores detected shallow errors, and the other -
 * deep errors.
 */
auto make_scrub_objects(const spg_t& pgid)
{
  return std::pair{
      pgid.make_temp_ghobject(fmt::format("scrub_{}", pgid)),
      pgid.make_temp_ghobject(fmt::format("deep_scrub_{}", pgid))};
}

string first_object_key(int64_t pool)
{
  auto shallow_hoid = hobject_t(object_t(), "", 0, 0x00000000, pool, "");
  shallow_hoid.build_hash_cache();
  return "SCRUB_OBJ_" + shallow_hoid.to_str();
}

// the object_key should be unique across pools
string to_object_key(int64_t pool, const librados::object_id_t& oid)
{
  auto shallow_hoid = hobject_t(
      object_t(oid.name),
      oid.locator,  // key
      oid.snap,
      0,  // hash
      pool, oid.nspace);
  shallow_hoid.build_hash_cache();
  return "SCRUB_OBJ_" + shallow_hoid.to_str();
}

string last_object_key(int64_t pool)
{
  auto shallow_hoid = hobject_t(object_t(), "", 0, 0xffffffff, pool, "");
  shallow_hoid.build_hash_cache();
  return "SCRUB_OBJ_" + shallow_hoid.to_str();
}

string first_snap_key(int64_t pool)
{
  // scrub object is per spg_t object, so we can misuse the hash (pg.seed) for
  // the representing the minimal and maximum keys. and this relies on how
  // hobject_t::to_str() works: hex(pool).hex(revhash).
  auto shallow_hoid = hobject_t(object_t(), "", 0, 0x00000000, pool, "");
  shallow_hoid.build_hash_cache();
  return "SCRUB_SS_" + shallow_hoid.to_str();
}

string to_snap_key(int64_t pool, const librados::object_id_t& oid)
{
  auto shallow_hoid = hobject_t(
      object_t(oid.name),
      oid.locator,  // key
      oid.snap,
      0x77777777,  // hash
      pool, oid.nspace);
  shallow_hoid.build_hash_cache();
  return "SCRUB_SS_" + shallow_hoid.to_str();
}

string last_snap_key(int64_t pool)
{
  auto shallow_hoid = hobject_t(object_t(), "", 0, 0xffffffff, pool, "");
  shallow_hoid.build_hash_cache();
  return "SCRUB_SS_" + shallow_hoid.to_str();
}

uint64_t decode_errors(
    hobject_t o,
    const ceph::buffer::list& bl)
{
  inconsistent_obj_wrapper iow{o};
  auto sbi = bl.cbegin();
  // should add an iterator for r-value
  iow.decode(sbi);
  return iow.errors;
}

void reencode_errors(hobject_t o, uint64_t errors, ceph::buffer::list& bl)
{
  inconsistent_obj_wrapper iow{o};
  auto sbi = bl.cbegin();
  // should add an iterator for r-value
  iow.decode(sbi);
  iow.errors = errors;
  iow.encode(bl);
}

}  // namespace

namespace Scrub {

Store* Store::create(
    ObjectStore* store,
    ObjectStore::Transaction* t,
    const spg_t& pgid,
    const coll_t& coll,
    LoggerSinkSet& logger)
{
  ceph_assert(store);
  ceph_assert(t);
  auto [shallow_oid, deep_oid] = make_scrub_objects(pgid);
  t->touch(coll, shallow_oid);
  t->touch(coll, deep_oid);

  return new Store{coll, shallow_oid, deep_oid, store, logger};
}

Store::Store(
    const coll_t& coll,
    const ghobject_t& sh_oid,
    const ghobject_t& dp_oid,
    ObjectStore* store,
    LoggerSinkSet& logger)
    : coll(coll)
    , shallow_hoid(sh_oid)
    , shallow_driver(store, coll, sh_oid)
    , shallow_backend(&shallow_driver)
    , deep_hoid(dp_oid)
    , deep_driver(store, coll, dp_oid)
    , deep_backend(&deep_driver)
    , clog(logger)
{}

Store::~Store()
{
  ceph_assert(shallow_results.empty());
  ceph_assert(deep_results.empty());
}

void Store::add_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  add_object_error(pool, e);
}

void Store::add_object_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  bufferlist bl;
  e.encode(bl);
  if (e.has_deep_errors()) {
    deep_results[to_object_key(pool, e.object)] = bl;
  }
  shallow_results[to_object_key(pool, e.object)] = bl;
}

void Store::add_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  add_snap_error(pool, e);
}

void Store::add_snap_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  bufferlist bl;
  e.encode(bl);
  shallow_results[to_snap_key(pool, e.object)] = bl;
}

bool Store::empty() const
{
  return shallow_results.empty() && deep_results.empty();
}

void Store::flush(ObjectStore::Transaction* t)
{
  if (t) {
    OSDriver::OSTransaction txn = shallow_driver.get_transaction(t);
    shallow_backend.set_keys(shallow_results, &txn);

    OSDriver::OSTransaction deep_txn = deep_driver.get_transaction(t);
    deep_backend.set_keys(deep_results, &deep_txn);
  }

  shallow_results.clear();
  deep_results.clear();
}

void Store::cleanup(ObjectStore::Transaction* t, scrub_level_t level)
{
  // always clear the known shallow errors DB (as both shallow and deep scrubs
  // would recreate it)
  t->remove(coll, shallow_hoid);

  // only a deep scrub recreates the deep errors DB
  if (level == scrub_level_t::deep) {
    t->remove(coll, deep_hoid);
  }
}

std::vector<bufferlist> Store::get_snap_errors(
    int64_t pool,
    const librados::object_id_t& start,
    uint64_t max_return) const
{
  const string begin =
      (start.name.empty() ? first_snap_key(pool) : to_snap_key(pool, start));
  const string end = last_snap_key(pool);
  return get_errors(begin, end, max_return);
}

std::vector<bufferlist> Store::get_object_errors(
    int64_t pool,
    const librados::object_id_t& start,
    uint64_t max_return) const
{
  const string begin =
      (start.name.empty() ? first_object_key(pool)
			  : to_object_key(pool, start));
  const string end = last_object_key(pool);
  return get_errors(begin, end, max_return);
}

inline void decode(
    librados::inconsistent_obj_t& obj,
    ceph::buffer::list::const_iterator& bp)
{
  reinterpret_cast<inconsistent_obj_wrapper&>(obj).decode(bp);
}


void Store::collect_specific_store(
    MapCacher::MapCacher<std::string, ceph::buffer::list>& backend,
    Store::ExpCacherPosData& latest,
    std::vector<bufferlist>& errors,
    const std::string& end_key,
    uint64_t& max_return) const
{
  while (max_return && latest.has_value() &&
	 latest.value().last_key < end_key) {
    errors.push_back(latest->data);
    max_return--;
  }
}

// a better way to implement this: use two generators, one for each store.
// and sort-merge the results. Almost like a merge-sort, but with equal
// keys combined.

std::vector<bufferlist> Store::get_errors(
    const string& from_key,
    const string& end_key,
    uint64_t max_return) const
{
  vector<bufferlist> errors;

  // until enough errors are collected, merge the input from the
  // two sorted DBs

  ExpCacherPosData latest_sh = shallow_backend.get_1st_after_key(from_key);
  ExpCacherPosData latest_dp = deep_backend.get_1st_after_key(from_key);

  while (max_return) {
    clog.debug() << fmt::format(
	"{}: n:{} latest_sh: {}, latest_dp: {}", __func__, max_return,
	(latest_sh ? latest_sh->last_key : "(none)"),
	(latest_dp ? latest_dp->last_key : "(none)"));

    // returned keys that are greater than end_key are not interesting
    if (latest_sh.has_value() && latest_sh->last_key >= end_key) {
      latest_sh = tl::unexpected(-EINVAL);
    }
    if (latest_dp.has_value() && latest_dp->last_key >= end_key) {
      latest_dp = tl::unexpected(-EINVAL);
    }

    if (!latest_sh && !latest_dp) {
      // both stores are exhausted
      break;
    }
    if (!latest_sh.has_value()) {
      // continue with the deep store
      clog.debug() << fmt::format("{}: collecting from deep store", __func__);
      collect_specific_store(
	  deep_backend, latest_dp, errors, end_key, max_return);
      break;
    }
    if (!latest_dp.has_value()) {
      // continue with the shallow store
      clog.debug() << fmt::format(
	  "{}: collecting from shallow store", __func__);
      collect_specific_store(
	  shallow_backend, latest_sh, errors, end_key, max_return);
      break;
    }

    // we have results from both stores. Select the one with a lower key.
    // If the keys are equal, combine the errors.
    if (latest_sh->last_key == latest_dp->last_key) {

      uint64_t known_errs = decode_errors(shallow_hoid.hobj, latest_sh->data);
      uint64_t known_dp_errs = decode_errors(deep_hoid.hobj, latest_dp->data);
      known_errs |= (known_dp_errs & librados::obj_err_t::DEEP_ERRORS);
      clog.debug() << fmt::format(
	  "{}: == n:{} dp_errs: {}, known_errs: {}", __func__, max_return,
	  known_dp_errs, known_errs);

      reencode_errors(shallow_hoid.hobj, known_errs, latest_sh->data);
      errors.push_back(latest_sh->data);
      max_return--;
      latest_sh = shallow_backend.get_1st_after_key(latest_sh->last_key);
      latest_dp = deep_backend.get_1st_after_key(latest_dp->last_key);

    } else if (latest_sh->last_key < latest_dp->last_key) {
      clog.debug() << fmt::format(
	  "{}: shallow store element ({})", __func__, latest_sh->last_key);
      errors.push_back(latest_sh->data);
      latest_sh = shallow_backend.get_1st_after_key(latest_sh->last_key);
    } else {
      clog.debug() << fmt::format(
	  "{}: deep store element ({})", __func__, latest_dp->last_key);
      errors.push_back(latest_dp->data);
      latest_dp = deep_backend.get_1st_after_key(latest_dp->last_key);
    }
    max_return--;
  }
  clog.debug() << fmt::format(
      "{}: == n:{} #errors: {}", __func__, max_return, errors.size());

  return errors;
}

}  // namespace Scrub