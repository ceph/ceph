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

/// the 'to_str()' representation of an hobject, if created with the specific
/// parameters, prefixed with 'prefix'.
std::string virtual_hobject_w_prefix(std::string_view prefix,
                                     const std::string& key,
                                     snapid_t snap,
                                     uint32_t hash,
                                     int64_t pool)
{
  return fmt::format("{}_{}", prefix,
                     hobject_t(object_t(), key, snap, hash, pool, "").to_str());
}

/// the 'to_str()' representation of an hobject, if created with the specific
/// parameters, prefixed with 'prefix'.
std::string virtual_hobject_w_prefix(std::string_view prefix,
                                     const librados::object_id_t& oid,
                                     uint32_t hash,
                                     int64_t pool) {
  return fmt::format("{}_{}", prefix,
                     hobject_t(object_t{oid.name}, oid.locator, oid.snap, hash,
                               pool, oid.nspace)
                         .to_str());
}

string first_object_key(int64_t pool)
{
  return virtual_hobject_w_prefix("SCRUB_OBJ", "", CEPH_NOSNAP, 0x00000000,
                                  pool);
}

// the object_key should be unique across pools
string to_object_key(int64_t pool, const librados::object_id_t& oid)
{
  return virtual_hobject_w_prefix("SCRUB_OBJ", oid, 0x00000000, pool);
}

string last_object_key(int64_t pool)
{
  return virtual_hobject_w_prefix("SCRUB_OBJ", "", CEPH_NOSNAP, 0xffffffff,
                                  pool);
}

string first_snap_key(int64_t pool)
{
  // scrub object is per spg_t object, so we can misuse the hash (pg.seed) for
  // representing the minimal and maximum keys. and this relies on how
  // hobject_t::to_str() works: hex(pool).hex(revhash).
  return virtual_hobject_w_prefix("SCRUB_SS", "", 0, 0x00000000, pool);
}

string to_snap_key(int64_t pool, const librados::object_id_t& oid)
{
  return virtual_hobject_w_prefix("SCRUB_SS", oid, 0x77777777, pool);
}

string last_snap_key(int64_t pool)
{
  return virtual_hobject_w_prefix("SCRUB_SS", "", 0, 0xffffffff, pool);
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


void Store::add_object_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  using librados::obj_err_t;
  const auto key = to_object_key(pool, e.object);
  dout(20) << fmt::format(
		  "{}: adding error for object {} ({}). Errors: {} ({}/{}) "
		  "unfiltered:{}",
		  (current_level == scrub_level_t::deep ? "deep" : "shallow"),
		  e.object, key, obj_err_t{e.errors},
		  obj_err_t{e.errors & obj_err_t::SHALLOW_ERRORS},
		  obj_err_t{e.errors & obj_err_t::DEEP_ERRORS}, e)
	   << dendl;

  if (current_level == scrub_level_t::deep) {
    // not overriding the deep errors DB during shallow scrubs
    deep_db->results[key] = e.encode();
  }

  // only shallow errors are stored in the shallow DB
  auto e_copy = e;
  e_copy.errors &= librados::obj_err_t::SHALLOW_ERRORS;
  e_copy.union_shards.errors &= librados::err_t::SHALLOW_ERRORS;
  shallow_db->results[key] = e_copy.encode();
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

  current_level = level;

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


/*
 * Implementation notes:
 * - see https://github.com/ceph/ceph/commit/df3ff6dafeadb3822b35c424a890db9a14d7f60f
 *   for why we encode the shard_info_t in the store.
 * - to maintain known shard_info-s created during a deep scrub (but only when
 *   needed), we use our knowledge of the level of the last scrub performed
 *   (current_level), and the object user version as encoded in the error
 *   structure.
 */
bufferlist Store::merge_encoded_error_wrappers(
    hobject_t obj,
    ExpCacherPosData& latest_sh,
    ExpCacherPosData& latest_dp) const
{
  // decode both error wrappers
  auto sh_wrap = decode_wrapper(obj, latest_sh->data.cbegin());
  auto dp_wrap = decode_wrapper(obj, latest_dp->data.cbegin());

  // note: the '20' level is just until we're sure the merging works as
  // expected
  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    dout(20) << fmt::format(
		    "merging errors {}. Deep: {:#x}-({})", sh_wrap.object,
		    dp_wrap.errors, dp_wrap)
	     << dendl;
    dout(20) << fmt::format(
		    "merging errors {}. Shallow: {:#x}-({})", sh_wrap.object,
		    sh_wrap.errors, sh_wrap)
	     << dendl;
    // dev: list the attributes:
    for (const auto& [shard, si] : sh_wrap.shards) {
      for (const auto& [attr, bl] : si.attrs) {
	dout(20) << fmt::format(" shallow: shard {} attr: {}", shard, attr)
		 << dendl;
      }
    }
    for (const auto& [shard, si] : dp_wrap.shards) {
      for (const auto& [attr, bl] : si.attrs) {
	dout(20) << fmt::format(" deep: shard {} attr: {}", shard, attr)
		 << dendl;
      }
    }
  }

  // Actual merging of the shard map entries is only performed if the
  // latest version is from the shallow scrub.
  // Otherwise, the deep scrub, which (for the shards info) contains all data,
  // and the shallow scrub is ignored.
  if (current_level == scrub_level_t::shallow) {
    // is the object data related to the same object version?
    if (sh_wrap.version == dp_wrap.version) {
      // combine the error information
      dp_wrap.errors |= sh_wrap.errors;
      for (const auto& [shard, si] : sh_wrap.shards) {
	if (dp_wrap.shards.contains(shard)) {
	  dout(20) << fmt::format(
			  "-----> {}-{}  combining: sh-errors: {} dp-errors:{}",
			  sh_wrap.object, shard, si, dp_wrap.shards[shard])
		   << dendl;
	  const auto saved_er = dp_wrap.shards[shard].errors;
	  dp_wrap.shards[shard].selected_oi = si.selected_oi;
	  dp_wrap.shards[shard].primary = si.primary;
	  dp_wrap.shards[shard].errors |= saved_er;

	  // the attributes:
	  for (const auto& [attr, bl] : si.attrs) {
	    if (!dp_wrap.shards[shard].attrs.contains(attr)) {
	      dout(20) << fmt::format(
			      "-----> {}-{}  copying shallow attr: attr: {}",
			      sh_wrap.object, shard, attr)
		       << dendl;
	      dp_wrap.shards[shard].attrs[attr] = bl;
	    }
	    // otherwise - we'll ignore the shallow attr buffer
	  }
	} else {
	  // the deep scrub data for this shard is missing. We take the shallow
	  // scrub data.
	  dp_wrap.shards[shard] = si;
	}
      }
    } else if (sh_wrap.version > dp_wrap.version) {
	if (false && dp_wrap.version == 0) {
	  // there was a read error in the deep scrub. The deep version
	  // shows as '0'. That's severe enough for us to ignore the shallow.
	  dout(10) << fmt::format("{} ignoring deep after read failure",
			  sh_wrap.object)
		   << dendl;
	} else {
	  // There is a new shallow version of the object results.
	  // The deep data is for an older version of that object.
	  // There are multiple possibilities here, but for now we ignore the
	  // deep data.
	  dp_wrap = sh_wrap;
	}
      }
  }

  return dp_wrap.encode();
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
