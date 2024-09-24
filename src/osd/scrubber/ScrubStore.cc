// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "./ScrubStore.h"
#include "osd/osd_types.h"
#include "common/scrub_types.h"
#include "include/rados/rados_types.hpp"

#include "pg_scrubber.h"

using std::ostringstream;
using std::string;
using std::vector;

using ceph::bufferlist;

namespace {
string first_object_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(), "", CEPH_NOSNAP, 0x00000000, pool, "");
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
  auto hoid = hobject_t(object_t(), "", CEPH_NOSNAP, 0xffffffff, pool, "");
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

string first_snap_key(int64_t pool)
{
  // scrub object is per spg_t object, so we can misuse the hash (pg.seed) for
  // representing the minimal and maximum keys. and this relies on how
  // hobject_t::to_str() works: hex(pool).hex(revhash).
  auto hoid = hobject_t(object_t(), "", 0, 0x00000000, pool, "");
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
  auto hoid = hobject_t(object_t(), "", 0, 0xffffffff, pool, "");
  hoid.build_hash_cache();
  return "SCRUB_SS_" + hoid.to_str();
}

}  // namespace

#undef dout_context
#define dout_context (m_scrubber.get_pg_cct())
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix_fn(_dout, this, __func__)

template <class T>
static std::ostream& _prefix_fn(std::ostream* _dout, T* t, std::string fn = "")
{
  return t->gen_prefix(*_dout, fn);
}

namespace Scrub {

Store::Store(
    PgScrubber& scrubber,
    ObjectStore& osd_store,
    ObjectStore::Transaction* t,
    const spg_t& pgid,
    const coll_t& coll)
    : m_scrubber{scrubber}
    , object_store{osd_store}
    , coll{coll}
{
  ceph_assert(t);

  // shallow errors DB object
  const auto sh_err_obj =
      pgid.make_temp_ghobject(fmt::format("scrub_{}", pgid));
  t->touch(coll, sh_err_obj);
  shallow_db.emplace(
      pgid, sh_err_obj, OSDriver{&object_store, coll, sh_err_obj});

  // and the DB for deep errors
  const auto dp_err_obj =
      pgid.make_temp_ghobject(fmt::format("deep_scrub_{}", pgid));
  t->touch(coll, dp_err_obj);
  deep_db.emplace(pgid, dp_err_obj, OSDriver{&object_store, coll, dp_err_obj});

  dout(20) << fmt::format(
		  "created Scrub::Store for pg[{}], shallow: {}, deep: {}",
		  pgid, sh_err_obj, dp_err_obj)
	   << dendl;
}


Store::~Store()
{
  ceph_assert(!shallow_db || shallow_db->results.empty());
  ceph_assert(!deep_db || deep_db->results.empty());
}


std::ostream& Store::gen_prefix(std::ostream& out, std::string_view fn) const
{
  if (fn.starts_with("operator")) {
    // it's a lambda, and __func__ is not available
    return m_scrubber.gen_prefix(out) << "Store::";
  } else {
    return m_scrubber.gen_prefix(out) << "Store::" << fn << ": ";
  }
}


void Store::add_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  add_object_error(pool, e);
}

namespace {

inconsistent_obj_wrapper create_filtered_copy(
    const inconsistent_obj_wrapper& obj,
    uint64_t obj_err_mask,
    uint64_t shard_err_mask)
{
  inconsistent_obj_wrapper dup = obj;
  dup.errors &= obj_err_mask;
  for (auto& [shard, si] : dup.shards) {
    si.errors &= shard_err_mask;
  }
  return dup;
}

}  // namespace


void Store::add_object_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  const auto key = to_object_key(pool, e.object);
  dout(20) << fmt::format(
		  "adding error for object {} ({}). Errors: {} ({}/{}) wr:{}",
		  e.object, key, librados::err_t{e.errors},
		  librados::err_t{e.errors & librados::err_t::SHALLOW_ERRORS},
		  librados::err_t{e.errors & librados::err_t::DEEP_ERRORS}, e)
	   << dendl;

  // divide the errors & shard errors into shallow and deep.
  {
    bufferlist bl;
    create_filtered_copy(
	e, librados::obj_err_t::SHALLOW_ERRORS, librados::err_t::SHALLOW_ERRORS)
	.encode(bl);
    shallow_db->results[key] = bl;
  }
  {
    bufferlist bl;
    create_filtered_copy(
	e, librados::obj_err_t::DEEP_ERRORS, librados::err_t::DEEP_ERRORS)
	.encode(bl);
    deep_db->results[key] = bl;
  }
}


void Store::add_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  add_snap_error(pool, e);
}


void Store::add_snap_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  // note: snap errors are only placed in the shallow store
  shallow_db->results[to_snap_key(pool, e.object)] = e.encode();
}


bool Store::is_empty() const
{
  return (!shallow_db || shallow_db->results.empty()) &&
	 (!deep_db || deep_db->results.empty());
}


void Store::flush(ObjectStore::Transaction* t)
{
  if (t) {
    auto txn = shallow_db->driver.get_transaction(t);
    shallow_db->backend.set_keys(shallow_db->results, &txn);
    txn = deep_db->driver.get_transaction(t);
    deep_db->backend.set_keys(deep_db->results, &txn);
  }

  shallow_db->results.clear();
  deep_db->results.clear();
}


void Store::clear_level_db(
    ObjectStore::Transaction* t,
    at_level_t& db,
    std::string_view db_name)
{
  dout(20) << fmt::format("removing (omap) entries for {} error DB", db_name)
	   << dendl;
  // easiest way to guarantee that the object representing the DB exists
  t->touch(coll, db.errors_hoid);

  // remove all the keys in the DB
  t->omap_clear(coll, db.errors_hoid);

  // restart the 'in progress' part of the MapCacher
  db.backend.reset();
}


void Store::reinit(
    ObjectStore::Transaction* t,
    scrub_level_t level)
{
  // Note: only one caller, and it creates the transaction passed to reinit().
  // No need to assert on 't'
  dout(20) << fmt::format(
		  "re-initializing the Scrub::Store (for {} scrub)",
		  (level == scrub_level_t::deep ? "deep" : "shallow"))
	   << dendl;

  // always clear the known shallow errors DB (as both shallow and deep scrubs
  // would recreate it)
  if (shallow_db) {
    clear_level_db(t, *shallow_db, "shallow");
  }
  // only a deep scrub recreates the deep errors DB
  if (level == scrub_level_t::deep && deep_db) {
    clear_level_db(t, *deep_db, "deep");
  }
}


void Store::cleanup(ObjectStore::Transaction* t)
{
  dout(20) << "discarding error DBs" << dendl;
  ceph_assert(t);
  if (shallow_db)
    t->remove(coll, shallow_db->errors_hoid);
  if (deep_db)
    t->remove(coll, deep_db->errors_hoid);
}


std::vector<bufferlist> Store::get_snap_errors(
    int64_t pool,
    const librados::object_id_t& start,
    uint64_t max_return) const
{
  vector<bufferlist> errors;
  const string begin =
      (start.name.empty() ? first_snap_key(pool) : to_snap_key(pool, start));
  const string end = last_snap_key(pool);

  // the snap errors are stored only in the shallow store
  ExpCacherPosData latest_sh = shallow_db->backend.get_1st_after_key(begin);

  while (max_return-- && latest_sh.has_value() && latest_sh->last_key < end) {
    errors.push_back(latest_sh->data);
    latest_sh = shallow_db->backend.get_1st_after_key(latest_sh->last_key);
  }

  return errors;
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
  dout(20) << fmt::format("fetching errors, from {} to {}", begin, end)
	   << dendl;
  return get_errors(begin, end, max_return);
}


inline void decode(
    librados::inconsistent_obj_t& obj,
    ceph::buffer::list::const_iterator& bp)
{
  reinterpret_cast<inconsistent_obj_wrapper&>(obj).decode(bp);
}


inconsistent_obj_wrapper decode_wrapper(
    hobject_t obj,
    ceph::buffer::list::const_iterator bp)
{
  inconsistent_obj_wrapper iow{obj};
  iow.decode(bp);
  return iow;
}


void Store::collect_specific_store(
    MapCacher::MapCacher<std::string, ceph::buffer::list>& backend,
    Store::ExpCacherPosData& latest,
    std::vector<bufferlist>& errors,
    std::string_view end_key,
    uint64_t max_return) const
{
  while (max_return-- && latest.has_value() &&
	 latest.value().last_key < end_key) {
    errors.push_back(latest->data);
    latest = backend.get_1st_after_key(latest->last_key);
  }
}


bufferlist Store::merge_encoded_error_wrappers(
    hobject_t obj,
    ExpCacherPosData& latest_sh,
    ExpCacherPosData& latest_dp) const
{
  // decode both error wrappers
  auto sh_wrap = decode_wrapper(obj, latest_sh->data.cbegin());
  auto dp_wrap = decode_wrapper(obj, latest_dp->data.cbegin());
  dout(20) << fmt::format(
		  "merging errors {}. Shallow: {}-({}), Deep: {}-({})",
		  sh_wrap.object, sh_wrap.errors, dp_wrap.errors, sh_wrap,
		  dp_wrap)
	   << dendl;

  // merge the object errors (a simple OR of the two error bit-sets)
  sh_wrap.errors |= dp_wrap.errors;

  // merge the two shard error maps
  for (const auto& [shard, si] : dp_wrap.shards) {
    dout(20) << fmt::format(
		    "shard {} dp-errors: {} sh-errors:{}", shard, si.errors,
		    sh_wrap.shards[shard].errors)
	     << dendl;
    // note: we may be creating the shallow shard entry here. This is OK
    sh_wrap.shards[shard].errors |= si.errors;
  }

  return sh_wrap.encode();
}


// a better way to implement get_errors(): use two generators, one for each store.
// and sort-merge the results. Almost like a merge-sort, but with equal
// keys combined. 'todo' once 'ranges' are really working.

std::vector<bufferlist> Store::get_errors(
    const std::string& from_key,
    const std::string& end_key,
    uint64_t max_return) const
{
  // merge the input from the two sorted DBs into 'errors' (until
  // enough errors are collected)
  vector<bufferlist> errors;
  dout(20) << fmt::format("getting errors from {} to {}", from_key, end_key)
	   << dendl;

  ceph_assert(shallow_db);
  ceph_assert(deep_db);
  ExpCacherPosData latest_sh = shallow_db->backend.get_1st_after_key(from_key);
  ExpCacherPosData latest_dp = deep_db->backend.get_1st_after_key(from_key);

  while (max_return) {
    dout(20) << fmt::format(
		    "n:{} latest_sh: {}, latest_dp: {}", max_return,
		    (latest_sh ? latest_sh->last_key : "(none)"),
		    (latest_dp ? latest_dp->last_key : "(none)"))
	     << dendl;

    // keys not smaller than end_key are not interesting
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
      dout(10) << fmt::format("collecting from deep store") << dendl;
      collect_specific_store(
	  deep_db->backend, latest_dp, errors, end_key, max_return);
      break;
    }
    if (!latest_dp.has_value()) {
      // continue with the shallow store
      dout(10) << fmt::format("collecting from shallow store") << dendl;
      collect_specific_store(
	  shallow_db->backend, latest_sh, errors, end_key, max_return);
      break;
    }

    // we have results from both stores. Select the one with a lower key.
    // If the keys are equal, combine the errors.
    if (latest_sh->last_key == latest_dp->last_key) {
      auto bl = merge_encoded_error_wrappers(
	  shallow_db->errors_hoid.hobj, latest_sh, latest_dp);
      errors.push_back(bl);
      latest_sh = shallow_db->backend.get_1st_after_key(latest_sh->last_key);
      latest_dp = deep_db->backend.get_1st_after_key(latest_dp->last_key);

    } else if (latest_sh->last_key < latest_dp->last_key) {
      dout(20) << fmt::format("shallow store element ({})", latest_sh->last_key)
	       << dendl;
      errors.push_back(latest_sh->data);
      latest_sh = shallow_db->backend.get_1st_after_key(latest_sh->last_key);
    } else {
      dout(20) << fmt::format("deep store element ({})", latest_dp->last_key)
	       << dendl;
      errors.push_back(latest_dp->data);
      latest_dp = deep_db->backend.get_1st_after_key(latest_dp->last_key);
    }
    max_return--;
  }

  dout(10) << fmt::format("{} errors reported", errors.size()) << dendl;
  return errors;
}
}  // namespace Scrub
