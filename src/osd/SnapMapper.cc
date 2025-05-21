// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "SnapMapper.h"

#include <fmt/printf.h>
#include <fmt/ranges.h>

#include "common/ceph_context.h"
#include "common/debug.h"
#include "global/global_context.h"
#include "osd/osd_types_fmt.h"
#include "SnapMapReaderI.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "snap_mapper "

using std::make_pair;
using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;

using ceph::decode;
using ceph::encode;
using ceph::timespan_str;
using result_t = Scrub::SnapMapReaderI::result_t;
using code_t = Scrub::SnapMapReaderI::result_t::code_t;


const string SnapMapper::MAPPING_PREFIX = "SNA_";
const string SnapMapper::OBJECT_PREFIX = "OBJ_";

const char *SnapMapper::PURGED_SNAP_PREFIX = "PSN_";

/*

  We have a bidirectional mapping, (1) from each snap+obj to object,
  sorted by snapshot, such that we can enumerate to identify all clones
  mapped to a particular snapshot, and (2) from object to snaps, so we
  can identify which reverse mappings exist for any given object (and,
  e.g., clean up on deletion).

  "SNA_"
  + ("%lld" % poolid)
  + "_"
  + ("%016x" % snapid)
  + "_"
  + (".%x" % shard_id)
  + "_"
  + hobject_t::to_str() ("%llx.%8x.%lx.name...." % pool, hash, snap)
  -> SnapMapping::Mapping { snap, hoid }

  "OBJ_" +
  + (".%x" % shard_id)
  + hobject_t::to_str()
   -> SnapMapper::object_snaps { oid, set<snapid_t> }

  */

#ifdef WITH_CRIMSON
#include "crimson/common/log.h"
#include "crimson/osd/pg_interval_interrupt_condition.h"
  template <typename ValuesT = void>
  using interruptible_future =
    ::crimson::interruptible::interruptible_future<
      ::crimson::osd::IOInterruptCondition, ValuesT>;
  using interruptor =
    ::crimson::interruptible::interruptor<
      ::crimson::osd::IOInterruptCondition>;

#define CRIMSON_DEBUG(FMT_MSG, ...) crimson::get_logger(ceph_subsys_).debug(FMT_MSG, ##__VA_ARGS__)
int OSDriver::get_keys(
  const std::set<std::string> &keys,
  std::map<std::string, ceph::buffer::list> *out)
{
  CRIMSON_DEBUG("OSDriver::{}", __func__);
  using crimson::os::FuturizedStore;
  return interruptor::green_get(os->omap_get_values(
    ch, hoid, keys
  ).safe_then([out] (FuturizedStore::Shard::omap_values_t&& vals) {
    // just the difference in comparator (`std::less<>` in omap_values_t`)
    reinterpret_cast<FuturizedStore::Shard::omap_values_t&>(*out) = std::move(vals);
    return 0;
  }, FuturizedStore::Shard::read_errorator::all_same_way([] (auto& e) {
    assert(e.value() > 0);
    return -e.value();
  }))); // this requires seastar::thread
}

int OSDriver::get_next(
  const std::string &key,
  std::pair<std::string, ceph::buffer::list> *next)
{
  CRIMSON_DEBUG("OSDriver::{} key {}", __func__, key);
  using crimson::os::FuturizedStore;
  return interruptor::green_get(os->omap_get_values(
    ch, hoid, key
  ).safe_then_unpack([&key, next] (bool, FuturizedStore::Shard::omap_values_t&& vals) {
    CRIMSON_DEBUG("OSDriver::get_next key {} got omap values", key);
    if (auto nit = std::begin(vals);
        nit == std::end(vals) || !SnapMapper::is_mapping(nit->first)) {
      CRIMSON_DEBUG("OSDriver::get_next key {} no more values", key);
      return -ENOENT;
    } else {
      CRIMSON_DEBUG("OSDriver::get_next returning next: {}, ", nit->first);
      assert(nit->first > key);
      *next = *nit;
      return 0;
    }
  }, FuturizedStore::Shard::read_errorator::all_same_way([] {
    CRIMSON_DEBUG("OSDriver::get_next saw error returning EINVAL");
    return -EINVAL;
  }))); // this requires seastar::thread
}

int OSDriver::get_next_or_current(
  const std::string &key,
  std::pair<std::string, ceph::buffer::list> *next_or_current)
{
  CRIMSON_DEBUG("OSDriver::{} key {}", __func__, key);
  using crimson::os::FuturizedStore;
  // let's try to get current first
  return interruptor::green_get(os->omap_get_values(
    ch, hoid, FuturizedStore::Shard::omap_keys_t{key}
  ).safe_then([&key, next_or_current] (FuturizedStore::Shard::omap_values_t&& vals) {
    CRIMSON_DEBUG("OSDriver::get_next_or_current returning {}", key);
    assert(vals.size() == 1);
    *next_or_current = std::make_pair(key, std::move(vals.begin()->second));
    return 0;
  }, FuturizedStore::Shard::read_errorator::all_same_way(
    [next_or_current, &key, this] {
    CRIMSON_DEBUG("OSDriver::get_next_or_current no current, try next {}", key);
    // no current, try next
    return get_next(key, next_or_current);
  }))); // this requires seastar::thread
}
#else
int OSDriver::get_keys(
  const std::set<std::string> &keys,
  std::map<std::string, ceph::buffer::list> *out)
{
  return os->omap_get_values(ch, hoid, keys, out);
}

int OSDriver::get_next(
  const std::string &seek_key,
  std::pair<std::string, ceph::buffer::list> *next)
{
  using omap_iter_seek_t = ObjectStore::omap_iter_seek_t;
  const auto result = os->omap_iterate(
    ch, hoid,
    ObjectStore::omap_iter_seek_t{
      .seek_position = seek_key,
      .seek_type = omap_iter_seek_t::UPPER_BOUND
    },
    [next] (std::string_view key, std::string_view value) mutable {
      next->first = key;
      next->second.clear();
      next->second.append(value);
      return ObjectStore::omap_iter_ret_t::STOP;
    });
  if (result < 0) {
    ceph_abort();
  } else if (!result) {
    return -ENOENT;
  } else {
    return 0; // found and STOPped
  }
}

int OSDriver::get_next_or_current(
  const std::string &seek_key,
  std::pair<std::string, ceph::buffer::list> *next_or_current)
{
  using omap_iter_seek_t = ObjectStore::omap_iter_seek_t;
  const auto result = os->omap_iterate(
    ch, hoid,
    ObjectStore::omap_iter_seek_t{
      .seek_position = seek_key,
      .seek_type = omap_iter_seek_t::LOWER_BOUND
    },
    [next_or_current] (std::string_view key, std::string_view value) mutable {
      next_or_current->first = key;
      next_or_current->second.clear();
      next_or_current->second.append(value);
      return ObjectStore::omap_iter_ret_t::STOP;
    });
  if (result < 0) {
    ceph_abort();
  } else if (!result) {
    return -ENOENT;
  } else {
    return 0; // found and STOPped
  }
}
#endif // WITH_CRIMSON

  SnapMapper::SnapMapper(
    CephContext* cct,
    MapCacher::StoreDriver<std::string, ceph::buffer::list> *driver,
    uint32_t match,  ///< [in] pgid
    uint32_t bits,   ///< [in] current split bits
    int64_t pool,    ///< [in] pool
    shard_id_t shard ///< [in] shard
    )
    : cct(cct), backend(driver), mask_bits(bits), match(match), pool(pool),
      shard(shard), shard_prefix(make_shard_prefix(shard)) {
    dout(10) << *this << __func__ << dendl;
    update_bits(mask_bits);
  }

string SnapMapper::get_prefix(int64_t pool, snapid_t snap)
{
  static_assert(sizeof(pool) == 8, "assumed by the formatting code");

  // note: the snap_id is to be formatted as a 64-bit hex number,
  // and not according to the text representation of snapid_t
  ceph_assert(snap != CEPH_NOSNAP && snap != CEPH_SNAPDIR);
  return fmt::sprintf("%s%lld_%.16X_",
		      MAPPING_PREFIX,
		      pool,
		      static_cast<uint64_t>(snap));
}

string SnapMapper::to_raw_key(
  const pair<snapid_t, hobject_t> &in) const
{
  return get_prefix(in.second.pool, in.first) + shard_prefix + in.second.to_str();
}

std::string SnapMapper::to_raw_key(snapid_t snap, const hobject_t &clone) const
{
  return get_prefix(clone.pool, snap) + shard_prefix + clone.to_str();
}

pair<string, ceph::buffer::list> SnapMapper::to_raw(
  const pair<snapid_t, hobject_t> &in) const
{
  ceph::buffer::list bl;
  encode(Mapping(in), bl);
  return make_pair(to_raw_key(in), bl);
}

pair<snapid_t, hobject_t> SnapMapper::from_raw(
  const pair<std::string, ceph::buffer::list> &image)
{
  using ceph::decode;
  Mapping map;
  ceph::buffer::list bl(image.second);
  auto bp = bl.cbegin();
  decode(map, bp);
  return make_pair(map.snap, map.hoid);
}

std::pair<snapid_t, hobject_t> SnapMapper::from_raw(
  const ceph::buffer::list &image)
{
  using ceph::decode;
  Mapping map;
  auto bp = image.cbegin();
  decode(map, bp);
  return make_pair(map.snap, map.hoid);
}

bool SnapMapper::is_mapping(const string &to_test)
{
  return to_test.substr(0, MAPPING_PREFIX.size()) == MAPPING_PREFIX;
}

string SnapMapper::to_object_key(const hobject_t &hoid) const
{
  return OBJECT_PREFIX + shard_prefix + hoid.to_str();
}

void SnapMapper::object_snaps::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(oid, bl);
  encode(snaps, bl);
  ENCODE_FINISH(bl);
}

void SnapMapper::object_snaps::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(oid, bl);
  decode(snaps, bl);
  DECODE_FINISH(bl);
}

void SnapMapper::object_snaps::dump(ceph::Formatter *f) const
{
  f->dump_stream("oid") << oid;
  f->dump_stream("snaps") << snaps;
}

void SnapMapper::object_snaps::generate_test_instances(
  std::list<object_snaps *> &o)
{
  o.push_back(new object_snaps);
  o.push_back(new object_snaps);
  o.back()->oid = hobject_t(sobject_t("name", CEPH_NOSNAP));
  o.back()->snaps.insert(1);
  o.back()->snaps.insert(2);
}

bool SnapMapper::check(const hobject_t &hoid) const
{
  if (hoid.match(mask_bits, match)) {
    return true;
  }
  derr << __func__ << " " << hoid << " mask_bits " << mask_bits
       << " match 0x" << std::hex << match << std::dec << " is false"
       << dendl;
  return false;
}

int SnapMapper::get_snaps(const hobject_t &oid, object_snaps *out) const
{
  auto snaps = get_snaps_common(oid);
  if (snaps) {
    *out = *snaps;
    return 0;
  }
  switch (auto e = snaps.error(); e.code) {
    case code_t::backend_error:
      return e.backend_error;
    case code_t::not_found:
      return -ENOENT;
    case code_t::inconsistent:
      // As this is a legacy interface, we cannot surprise the user with
      // a new error code here.
      return -ENOENT;
    default:
      // Can't happen. Just to keep the compiler happy.
      ceph_abort("get_snaps_common() returned invalid error code");
  }
}

tl::expected<std::set<snapid_t>, Scrub::SnapMapReaderI::result_t>
SnapMapper::get_snaps(const hobject_t &oid) const
{
  auto snaps = get_snaps_common(oid);
  if (snaps) {
    return snaps->snaps;
  }
  return tl::unexpected(snaps.error());
}

tl::expected<SnapMapper::object_snaps, Scrub::SnapMapReaderI::result_t>
SnapMapper::get_snaps_common(const hobject_t &oid) const
{
  ceph_assert(check(oid));
  set<string> keys{to_object_key(oid)};
  dout(20) << fmt::format("{}: key string: {} oid:{}", __func__, keys, oid)
	   << dendl;

  map<string, ceph::buffer::list> got;
  int r = backend.get_keys(keys, &got);
  if (r < 0) {
    dout(10) << __func__ << " " << oid << " got err " << r << dendl;
    return tl::unexpected(result_t{code_t::backend_error, r});
  }
  if (got.empty()) {
    dout(10) << __func__ << " " << oid << " got.empty()" << dendl;
    return tl::unexpected(result_t{code_t::not_found, -ENOENT});
  }

  object_snaps out;
  auto bp = got.begin()->second.cbegin();
  try {
    decode(out, bp);
  } catch (...) {
    dout(1) << __func__ << ": " << oid << " decode failed" << dendl;
    return tl::unexpected(result_t{code_t::backend_error, -EIO});
  }

  dout(20) << __func__ << " " << oid << " " << out.snaps << dendl;
  if (out.snaps.empty()) {
    dout(1) << __func__ << " " << oid << " empty snapset" << dendl;
    ceph_assert(!cct->_conf->osd_debug_verify_snaps);
  }
  return out;
}

std::set<std::string> SnapMapper::to_raw_keys(
  const hobject_t &clone,
  const std::set<snapid_t> &snaps) const
{
  std::set<std::string> keys;
  for (auto snap : snaps) {
    keys.insert(to_raw_key(snap, clone));
  }
  dout(20) << fmt::format(
		"{}: clone:{} snaps:{} -> keys: {}", __func__, clone, snaps,
		keys)
	   << dendl;
  return keys;
}

tl::expected<std::set<snapid_t>, result_t>
SnapMapper::get_snaps_check_consistency(const hobject_t &hoid) const
{
  // derive the set of snaps from the 'OBJ_' entry
  auto obj_snaps = get_snaps(hoid);
  if (!obj_snaps) {
    return obj_snaps;
  }

  // make sure we have the expected set of SNA_ entries:
  // we have the clone oid and the set of snaps relevant to this clone.
  // Let's construct all expected SNA_ key, then fetch them.

  auto mapping_keys = to_raw_keys(hoid, *obj_snaps);
  map<string, ceph::buffer::list> kvmap;
  auto r = backend.get_keys(mapping_keys, &kvmap);
  if (r < 0) {
    dout(10) << fmt::format(
		  "{}: backend error ({}) for cobject {}", __func__, r, hoid)
	     << dendl;
    // that's a backend error, but for the SNA_ entries. Let's treat it as an
    // internal consistency error (although a backend error would have made
    // sense too).
    return tl::unexpected(result_t{code_t::inconsistent, r});
  }

  std::set<snapid_t> snaps_from_mapping;
  for (auto &[k, v] : kvmap) {
    dout(20) << __func__ << " " << hoid << " " << k << dendl;
    // extract the object ID from the value fetched for an SNA mapping key
    auto [sn, obj] = SnapMapper::from_raw(v);
    if (obj != hoid) {
      dout(1) << fmt::format(
		   "{}: unexpected object ID {} for key{} (expected {})",
		   __func__, obj, k, hoid)
	      << dendl;
      return tl::unexpected(result_t{code_t::inconsistent});
    }
    snaps_from_mapping.insert(sn);
  }

  if (snaps_from_mapping != *obj_snaps) {
    dout(10) << fmt::format(
		  "{}: hoid:{} -> mapper internal inconsistency ({} vs {})",
		  __func__, hoid, *obj_snaps, snaps_from_mapping)
	     << dendl;
    return tl::unexpected(result_t{code_t::inconsistent});
  }
  dout(10) << fmt::format(
		"{}: snaps for {}: {}", __func__, hoid, snaps_from_mapping)
	   << dendl;
  return obj_snaps;
}

void SnapMapper::clear_snaps(
  const hobject_t &oid,
  MapCacher::Transaction<std::string, ceph::buffer::list> *t)
{
  dout(20) << __func__ << " " << oid << dendl;
  ceph_assert(check(oid));
  set<string> to_remove;
  to_remove.insert(to_object_key(oid));
  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    for (auto& i : to_remove) {
      dout(20) << __func__ << "::rm " << i << dendl;
    }
  }
  backend.remove_keys(to_remove, t);
}

void SnapMapper::set_snaps(
  const hobject_t &oid,
  const object_snaps &in,
  MapCacher::Transaction<std::string, ceph::buffer::list> *t)
{
  ceph_assert(check(oid));
  map<string, ceph::buffer::list> to_set;
  ceph::buffer::list bl;
  encode(in, bl);
  to_set[to_object_key(oid)] = bl;
  dout(20) << __func__ << " " << oid << " " << in.snaps << dendl;
  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    for (auto& i : to_set) {
      dout(20) << __func__ << "::set " << i.first << dendl;
    }
  }
  backend.set_keys(to_set, t);
}

void SnapMapper::update_bits(
  uint32_t new_bits)  ///< [in] new split bits
{
    dout(20) << *this << __func__ << " new_bits: " << new_bits << dendl;
    mask_bits = new_bits;
    std::set<std::string> _prefixes = hobject_t::get_prefixes(
      mask_bits,
      match,
      pool);
    prefixes.clear();
    for (auto i = _prefixes.begin(); i != _prefixes.end(); ++i) {
      prefixes.insert(shard_prefix + *i);
    }
    dout(20) << *this <<__func__ << " prefix updated" << dendl;

    reset_prefix_itr(CEPH_NOSNAP, "update_bits");
  }

int SnapMapper::update_snaps(
  const hobject_t &oid,
  const set<snapid_t> &new_snaps,
  const set<snapid_t> *old_snaps_check,
  MapCacher::Transaction<std::string, ceph::buffer::list> *t)
{
  dout(20) << *this << __func__ << " " << oid << " " << new_snaps
	   << " was " << (old_snaps_check ? *old_snaps_check : set<snapid_t>())
	   << dendl;
  ceph_assert(check(oid));
  if (new_snaps.empty())
    return remove_oid(oid, t);

  object_snaps out;
  int r = get_snaps(oid, &out);
  // Tolerate missing keys but not disk errors
  if (r < 0 && r != -ENOENT)
    return r;
  if (old_snaps_check)
    ceph_assert(out.snaps == *old_snaps_check);

  object_snaps in(oid, new_snaps);
  set_snaps(oid, in, t);

  set<string> to_remove;
  for (set<snapid_t>::iterator i = out.snaps.begin();
       i != out.snaps.end();
       ++i) {
    if (!new_snaps.count(*i)) {
      to_remove.insert(to_raw_key(make_pair(*i, oid)));
    }
  }
  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    for (auto& i : to_remove) {
      dout(20) << __func__ << " rm " << i << dendl;
    }
  }
  backend.remove_keys(to_remove, t);
  return 0;
}

void SnapMapper::add_oid(
  const hobject_t &oid,
  const set<snapid_t>& snaps,
  MapCacher::Transaction<std::string, ceph::buffer::list> *t)
{
  dout(20) << __func__ << " " << oid << " " << snaps << dendl;
  ceph_assert(!snaps.empty());
  ceph_assert(check(oid));
  {
    object_snaps out;
    int r = get_snaps(oid, &out);
    if (r != -ENOENT) {
      derr << *this << __func__ << " found existing snaps mapped on " << oid
	   << ", removing" << dendl;
      ceph_assert(!cct->_conf->osd_debug_verify_snaps);
      remove_oid(oid, t);
    }
  }

  object_snaps _snaps(oid, snaps);
  set_snaps(oid, _snaps, t);

  map<string, ceph::buffer::list> to_add;
  for (set<snapid_t>::iterator i = snaps.begin();
       i != snaps.end();
       ++i) {
    to_add.insert(to_raw(make_pair(*i, oid)));
  }
  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    for (auto& i : to_add) {
      dout(20) << *this << __func__ << " set " << i.first << dendl;
    }
  }
  backend.set_keys(to_add, t);
}

// reset the prefix iterator to the first prefix hash
void SnapMapper::reset_prefix_itr(snapid_t snap, const char *s)
{
  if (prefix_itr_snap == CEPH_NOSNAP) {
    dout(10) << *this << __func__ << "::from <CEPH_NOSNAP> to <" << snap << "> ::" << s << dendl;
  }
  else if (snap == CEPH_NOSNAP) {
    dout(10) << *this << __func__ << "::from <"<< prefix_itr_snap << "> to <CEPH_NOSNAP> ::" << s << dendl;
  }
  else if (prefix_itr_snap == snap) {
    dout(10) << *this << __func__ << "::with the same snapid <" << snap << "> ::" << s << dendl;
  }
  else {
    // This is unexpected!!
    dout(10) << *this << __func__ << "::from <"<< prefix_itr_snap << "> to <" << snap << "> ::" << s << dendl;
  }
  prefix_itr_snap = snap;
  prefix_itr      = prefixes.begin();
}

vector<hobject_t> SnapMapper::get_objects_by_prefixes(
  snapid_t snap,
  unsigned max)
{
  vector<hobject_t> out;

  /// maintain the prefix_itr between calls to avoid searching depleted prefixes
  for ( ; prefix_itr != prefixes.end(); prefix_itr++) {
    const string prefix(get_prefix(pool, snap) + *prefix_itr);
    string pos = prefix;
    while (out.size() < max) {
      pair<string, ceph::buffer::list> next;
      // access RocksDB (an expensive operation!)
      int r = backend.get_next(pos, &next);
      dout(20) << *this << __func__ << " get_next(" << pos << ") returns " << r
	       << " " << next.first << dendl;
      if (r != 0) {
	return out; // Done
      }

      ceph_assert(is_mapping(next.first));

      if (auto next_prefix = next.first.substr(0, prefix.size());
          next_prefix != prefix) {
	// TBD: we access the DB twice for the first object of each iterator...
	dout(20) << fmt::format("{}: breaking, prefix expected {} got {}",
	                        __func__, prefix, next_prefix)
	         << dendl;
	break; // Done with this prefix
      }

      dout(20) <<  *this << __func__ << " found " << next.first << dendl;
      pair<snapid_t, hobject_t> next_decoded(from_raw(next));
      ceph_assert(next_decoded.first == snap);
      ceph_assert(check(next_decoded.second));
      out.push_back(next_decoded.second);

      pos = next.first;
    }

    if (out.size() >= max) {
      dout(20) << *this << fmt::format("{}: reached max of: {} returning",
                                       __func__, out.size())
               << dendl;
      return out;
    }
  }
  return out;
}

std::optional<vector<hobject_t>> SnapMapper::get_next_objects_to_trim(
  snapid_t snap,
  unsigned max)
{
  dout(20) << *this << __func__ << "snapid=" << snap << dendl;

  // if max would be 0, we return ENOENT and the caller would mistakenly
  // trim the snaptrim queue
  ceph_assert(max > 0);

  // The prefix_itr is bound to a prefix_itr_snap so if we trim another snap
  // we must reset the prefix_itr (should not happen normally)
  if (prefix_itr_snap != snap) {
    if (prefix_itr_snap == CEPH_NOSNAP) {
      reset_prefix_itr(snap, "Trim begins");
    }
    else {
      reset_prefix_itr(snap, "Unexpected snap change");
    }
  }

  // when reaching the end of the DB reset the prefix_ptr and verify
  // we didn't miss objects which were added after we started trimming
  // This should never happen in reality because the snap was logically deleted
  // before trimming starts (and so no new clone-objects could be added)
  // For more info see PG::filter_snapc()
  //
  // We still like to be extra careful and run one extra loop over all prefixes
  auto objs = get_objects_by_prefixes(snap, max);
  if (unlikely(objs.size() == 0)) {
    reset_prefix_itr(snap, "Second pass trim");
    objs = get_objects_by_prefixes(snap, max);

    if (unlikely(objs.size() > 0)) {
      derr << *this << __func__ << " New Clone-Objects were added to Snap " << snap
	   << " after trimming was started" << dendl;
    }
    reset_prefix_itr(CEPH_NOSNAP, "Trim was completed successfully");
  }

  if (objs.size() == 0) {
    return std::nullopt;
  } else {
    return objs;
  }
}


int SnapMapper::remove_oid(
  const hobject_t &oid,
  MapCacher::Transaction<std::string, ceph::buffer::list> *t)
{
  dout(20) << *this << __func__ << " " << oid << dendl;
  ceph_assert(check(oid));
  return _remove_oid(oid, t);
}

int SnapMapper::_remove_oid(
  const hobject_t &oid,
  MapCacher::Transaction<std::string, ceph::buffer::list> *t)
{
  dout(20) << *this << __func__ << " " << oid << dendl;
  object_snaps out;
  int r = get_snaps(oid, &out);
  if (r < 0)
    return r;

  clear_snaps(oid, t);

  set<string> to_remove;
  for (set<snapid_t>::iterator i = out.snaps.begin();
       i != out.snaps.end();
       ++i) {
    to_remove.insert(to_raw_key(make_pair(*i, oid)));
  }
  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    for (auto& i : to_remove) {
      dout(20) << *this << __func__ << "::rm " << i << dendl;
    }
  }
  backend.remove_keys(to_remove, t);
  return 0;
}

int SnapMapper::get_snaps(
  const hobject_t &oid,
  std::set<snapid_t> *snaps) const
{
  ceph_assert(check(oid));
  object_snaps out;
  int r = get_snaps(oid, &out);
  if (r < 0)
    return r;
  if (snaps)
    snaps->swap(out.snaps);
  return 0;
}

void SnapMapper::update_snap_map(
  const pg_log_entry_t& i,
  MapCacher::Transaction<std::string, ceph::buffer::list> *_t)
{
  ceph_assert(i.soid.snap < CEPH_MAXSNAP);
  dout(20) << __func__ << " " << i << dendl;
  if (i.is_delete()) {
    int r = remove_oid(
      i.soid,
      _t);
    if (r)
      dout(20) << *this << __func__ << " remove_oid " << i.soid << " failed with " << r << dendl;
    // On removal tolerate missing key corruption
    ceph_assert(r == 0 || r == -ENOENT);
  } else if (i.is_update()) {
    ceph_assert(i.snaps.length() > 0);
    std::vector<snapid_t> snaps;
    bufferlist snapbl = i.snaps;
    auto p = snapbl.cbegin();
    try {
      decode(snaps, p);
    } catch (...) {
      dout(20) << *this << __func__ << " decode snaps failure on " << i << dendl;
      snaps.clear();
    }
    std::set<snapid_t> _snaps(snaps.begin(), snaps.end());

    if (i.is_clone() || i.is_promote()) {
      add_oid(
        i.soid,
        _snaps,
        _t);
    } else if (i.is_modify()) {
      int r = update_snaps(
        i.soid,
        _snaps,
        0,
        _t);
      ceph_assert(r == 0);
    } else {
      ceph_assert(i.is_clean());
    }
  }
}

// -- purged snaps --

string SnapMapper::make_purged_snap_key(int64_t pool, snapid_t last)
{
  ceph_assert(last != CEPH_NOSNAP && last != CEPH_SNAPDIR);
  return fmt::sprintf("%s_%lld_%016llx",
		      PURGED_SNAP_PREFIX,
		      pool,
		      static_cast<uint64_t>(last));
}

void SnapMapper::make_purged_snap_key_value(
  int64_t pool, snapid_t begin, snapid_t end, map<string,ceph::buffer::list> *m)
{
  string k = make_purged_snap_key(pool, end - 1);
  auto& v = (*m)[k];
  ceph::encode(pool, v);
  ceph::encode(begin, v);
  ceph::encode(end, v);
}

int SnapMapper::_lookup_purged_snap(
  CephContext *cct,
  OSDriver& backend,
  int64_t pool, snapid_t snap,
  snapid_t *begin, snapid_t *end)
{
  string k = make_purged_snap_key(pool, snap);
  std::pair<std::string, ceph::buffer::list> kv;
  if (auto ret = backend.get_next_or_current(k, &kv); ret == -ENOENT) {
    dout(20) << __func__ << " pool " << pool << " snap " << snap
	     << " key '" << k << "' lower_bound not found" << dendl;
    return -ENOENT;
  }
  if (kv.first.find(PURGED_SNAP_PREFIX) != 0) {
    dout(20) << __func__ << " pool " << pool << " snap " << snap
	     << " key '" << k << "' lower_bound got mismatched prefix '"
	     << kv.first << "'" << dendl;
    return -ENOENT;
  }
  ceph::buffer::list v = kv.second;
  auto p = v.cbegin();
  int64_t gotpool;
  decode(gotpool, p);
  decode(*begin, p);
  decode(*end, p);
  if (gotpool != pool) {
    dout(20) << __func__ << " got wrong pool " << gotpool << dendl;
    return -ENOENT;
  }
  if (snap < *begin || snap >= *end) {
    dout(20) << __func__ << " pool " << pool << " snap " << snap
	     << " found [" << *begin << "," << *end << "), no overlap" << dendl;
    return -ENOENT;
  }
  return 0;
}

void SnapMapper::record_purged_snaps(
  CephContext *cct,
  OSDriver& backend,
  OSDriver::OSTransaction&& txn,
  map<epoch_t,mempool::osdmap::map<int64_t,snap_interval_set_t>> purged_snaps)
{
  dout(10) << __func__ << " purged_snaps " << purged_snaps << dendl;
  map<string,ceph::buffer::list> m;
  set<string> rm;
  for (auto& [epoch, bypool] : purged_snaps) {
    // index by (pool, snap)
    for (auto& [pool, snaps] : bypool) {
      for (auto i = snaps.begin();
	   i != snaps.end();
	   ++i) {
	snapid_t begin = i.get_start();
	snapid_t end = i.get_end();
	snapid_t before_begin, before_end;
	snapid_t after_begin, after_end;
	int b = _lookup_purged_snap(cct, backend,
				    pool, begin - 1, &before_begin, &before_end);
	int a = _lookup_purged_snap(cct, backend,
				    pool, end, &after_begin, &after_end);
	if (!b && !a) {
	  dout(10) << __func__
		   << " [" << begin << "," << end << ") - joins ["
		   << before_begin << "," << before_end << ") and ["
		   << after_begin << "," << after_end << ")" << dendl;
	  // erase only the begin record; we'll overwrite the end one
	  rm.insert(make_purged_snap_key(pool, before_end - 1));
	  make_purged_snap_key_value(pool, before_begin, after_end, &m);
	} else if (!b) {
	  dout(10) << __func__
		   << " [" << begin << "," << end << ") - join with earlier ["
		   << before_begin << "," << before_end << ")" << dendl;
	  rm.insert(make_purged_snap_key(pool, before_end - 1));
	  make_purged_snap_key_value(pool, before_begin, end, &m);
	} else if (!a) {
	  dout(10) << __func__
		   << " [" << begin << "," << end << ") - join with later ["
		   << after_begin << "," << after_end << ")" << dendl;
	  // overwrite after record
	  make_purged_snap_key_value(pool, begin, after_end, &m);
	} else {
	  make_purged_snap_key_value(pool, begin, end, &m);
	}
      }
    }
  }
  txn.remove_keys(rm);
  txn.set_keys(m);
  dout(10) << __func__ << " rm " << rm.size() << " keys, set " << m.size()
	   << " keys" << dendl;
}


#ifndef WITH_CRIMSON
bool SnapMapper::Scrubber::_parse_p(std::string_view key, std::string_view value)
{
  if (key.find(PURGED_SNAP_PREFIX) != 0) {
    pool = -1;
    return false;
  }
  ceph::buffer::list v;
  v.append(value);
  auto p = v.cbegin();
  ceph::decode(pool, p);
  ceph::decode(begin, p);
  ceph::decode(end, p);
  dout(20) << __func__ << " purged_snaps pool " << pool
	   << " [" << begin << "," << end << ")" << dendl;
  return true;
}

bool SnapMapper::Scrubber::_parse_m(
  std::string_view key,
  std::string_view value)
{
  if (key.find(MAPPING_PREFIX) != 0) {
    return false;
  }
  ceph::bufferlist v;
  v.append(value); // create_static if decoding if anyhow visible a flamegraph
  auto p = v.cbegin();
  mapping.decode(p);

  {
    unsigned long long p, s;
    long sh;
    int r = sscanf(key.data(), "SNA_%lld_%llx.%lx", &p, &s, &sh);
    if (r != 1) {
      shard = shard_id_t::NO_SHARD;
    } else {
      shard = shard_id_t(sh);
    }
  }
  dout(20) << __func__ << " mapping pool " << mapping.hoid.pool
	   << " snap " << mapping.snap
	   << " shard " << shard
	   << " " << mapping.hoid << dendl;
  return true;
}

void SnapMapper::Scrubber::run()
{
  dout(10) << __func__ << dendl;

  store->omap_iterate(
    ch, mapping_hoid,
    ObjectStore::omap_iter_seek_t{
      .seek_position = MAPPING_PREFIX,
      .seek_type = ObjectStore::omap_iter_seek_t::UPPER_BOUND
    },
    [this] (std::string_view key, std::string_view value) mutable {
      if (!_parse_m(key, value)) {
        return ObjectStore::omap_iter_ret_t::STOP;
      }
      // advance to next purged_snaps range?
      const auto ret = store->omap_iterate(
        ch, purged_snaps_hoid,
        ObjectStore::omap_iter_seek_t{
          .seek_position = PURGED_SNAP_PREFIX,
          .seek_type = ObjectStore::omap_iter_seek_t::UPPER_BOUND
        },
        [this] (std::string_view key, std::string_view value) mutable {
          _parse_p(key, value);
          if (pool >= 0 &&
                 (mapping.hoid.pool > pool ||
                  (mapping.hoid.pool == pool && mapping.snap >= end))) {
            return ObjectStore::omap_iter_ret_t::NEXT;
	  } else {
            return ObjectStore::omap_iter_ret_t::STOP;
	  }
        });
      if (ret < 0) {
	// beware _parse_p() also modifies pool
	pool = -1;
	derr << "omap_iterate() on purged_snaps_hoid returns " << ret << dendl;
      } else if (const auto more = static_cast<bool>(ret); !more) {
	pool = -1;
      }
      if (pool < 0) {
        dout(10) << __func__ << " passed final purged_snaps interval, rest ok"
                 << dendl;
        return ObjectStore::omap_iter_ret_t::STOP;
      }
      if (mapping.hoid.pool < pool ||
          mapping.snap < begin) {
        // ok
        dout(20) << fmt::format(
                      "{} ok {} snap {} precedes pool {} purged_snaps [{}, {})",
                      __func__, mapping.hoid, mapping.snap, pool, begin, end)
                 << dendl;
      } else {
        assert(mapping.snap >= begin);
        assert(mapping.snap < end);
        assert(mapping.hoid.pool == pool);
        // invalid
        dout(10) << fmt::format(
                      "{} stray {} snap {} in pool {} shard {} purged_snaps[{}, {})",
                      __func__, mapping.hoid, mapping.snap, pool, shard, begin, end)
                 << dendl;
        stray.emplace_back(std::tuple<int64_t,snapid_t,uint32_t,shard_id_t>(
          		   pool, mapping.snap, mapping.hoid.get_hash(),
          		   shard
          		   ));
      }
      return ObjectStore::omap_iter_ret_t::NEXT;
    });
  dout(10) << __func__ << " end, found " << stray.size() << " stray" << dendl;
}
#endif // !WITH_CRIMSON
