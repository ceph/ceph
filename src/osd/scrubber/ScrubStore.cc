// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "ScrubStore.h"
#include "osd/osd_types.h"
#include "common/scrub_types.h"
#include "include/rados/rados_types.hpp"

using std::ostringstream;
using std::string;
using std::vector;

using ceph::bufferlist;

namespace {
ghobject_t make_scrub_object(const spg_t& pgid)
{
  ostringstream ss;
  ss << "scrub_" << pgid;
  return pgid.make_temp_ghobject(ss.str());
}

string first_object_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(),
			"",
			0,
			0x00000000,
			pool,
			"");
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

// the object_key should be unique across pools
string to_object_key(int64_t pool, const librados::object_id_t& oid)
{
  auto hoid = hobject_t(object_t(oid.name),
			oid.locator, // key
			oid.snap,
			0,		// hash
			pool,
			oid.nspace);
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

string last_object_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(),
			"",
			0,
			0xffffffff,
			pool,
			"");
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

string first_snap_key(int64_t pool)
{
  // scrub object is per spg_t object, so we can misuse the hash (pg.seed) for
  // the representing the minimal and maximum keys. and this relies on how
  // hobject_t::to_str() works: hex(pool).hex(revhash).
  auto hoid = hobject_t(object_t(),
			"",
			0,
			0x00000000,
			pool,
			"");
  hoid.build_hash_cache();
  return "SCRUB_SS_" + hoid.to_str();
}

string to_snap_key(int64_t pool, const librados::object_id_t& oid)
{
  auto hoid = hobject_t(object_t(oid.name),
			oid.locator, // key
			oid.snap,
			0x77777777, // hash
			pool,
			oid.nspace);
  hoid.build_hash_cache();
  return "SCRUB_SS_" + hoid.to_str();
}

string last_snap_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(),
			"",
			0,
			0xffffffff,
			pool,
			"");
  hoid.build_hash_cache();
  return "SCRUB_SS_" + hoid.to_str();
}
}

namespace Scrub {

Store::Store(
    ObjectStore& osd_store,
    ObjectStore::Transaction* t,
    const spg_t& pgid,
    const coll_t& coll)
    : object_store{osd_store}
    , coll{coll}
{
  ceph_assert(t);

  const auto err_obj = pgid.make_temp_ghobject(fmt::format("scrub_{}", pgid));
  t->touch(coll, err_obj);
  errors_db.emplace(pgid, err_obj, OSDriver{&object_store, coll, err_obj});
}


Store::~Store()
{
  ceph_assert(!errors_db || errors_db->results.empty());
}

void Store::add_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  add_object_error(pool, e);
}

void Store::add_object_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  const auto key = to_object_key(pool, e.object);
  bufferlist bl;
  e.encode(bl);
  errors_db->results[key] = bl;
}


void Store::add_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  add_snap_error(pool, e);
}

void Store::add_snap_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  bufferlist bl;
  e.encode(bl);
  errors_db->results[to_snap_key(pool, e.object)] = bl;
}

bool Store::empty() const
{
  return errors_db->results.empty();
}

void Store::flush(ObjectStore::Transaction* t)
{
  if (t) {
    OSDriver::OSTransaction txn = errors_db->driver.get_transaction(t);
    errors_db->backend.set_keys(errors_db->results, &txn);
  }
  errors_db->results.clear();
}


void Store::clear_level_db(
    ObjectStore::Transaction* t,
    at_level_t& db)
{
  // easiest way to guarantee that the object representing the DB exists
  t->touch(coll, db.errors_hoid);

  // remove all the keys in the DB
  t->omap_clear(coll, db.errors_hoid);

  // restart the 'in progress' part of the MapCacher
  db.backend.reset();
}


void Store::reinit(ObjectStore::Transaction* t, [[maybe_unused]] scrub_level_t level)
{
  // Note: only one caller, and it creates the transaction passed to reinit().
  // No need to assert on 't'

  if (errors_db) {
    clear_level_db(t, *errors_db);
  }
}


void Store::cleanup(ObjectStore::Transaction* t)
{
  ceph_assert(t);
  if (errors_db)
    t->remove(coll, errors_db->errors_hoid);
}

std::vector<bufferlist>
Store::get_snap_errors(int64_t pool,
		       const librados::object_id_t& start,
		       uint64_t max_return) const
{
  const string begin = (start.name.empty() ?
			first_snap_key(pool) : to_snap_key(pool, start));
  const string end = last_snap_key(pool);
  return get_errors(begin, end, max_return);
}

std::vector<bufferlist>
Store::get_object_errors(int64_t pool,
			 const librados::object_id_t& start,
			 uint64_t max_return) const
{
  const string begin = (start.name.empty() ?
			first_object_key(pool) : to_object_key(pool, start));
  const string end = last_object_key(pool);
  return get_errors(begin, end, max_return);
}

std::vector<bufferlist>
Store::get_errors(const string& begin,
		  const string& end,
		  uint64_t max_return) const
{
  vector<bufferlist> errors;
  if (!errors_db)
    return errors;

  auto next = std::make_pair(begin, bufferlist{});
  while (max_return && !errors_db->backend.get_next(next.first, &next)) {
    if (next.first >= end)
      break;
    errors.push_back(next.second);
    max_return--;
  }
  return errors;
}

} // namespace Scrub
