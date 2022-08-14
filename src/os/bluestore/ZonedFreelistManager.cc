// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

//
// A freelist manager for zoned devices.  This iteration just keeps the write
// pointer per zone.  Following iterations will add enough information to enable
// cleaning of zones.
//
// Copyright (C) 2020 Abutalib Aghayev
//

#include "ZonedFreelistManager.h"
#include "bluestore_common.h"
#include "include/stringify.h"
#include "kv/KeyValueDB.h"
#include "os/kv.h"
#include "zoned_types.h"

#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "zoned freelist "

using std::string;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::decode;
using ceph::encode;

void ZonedFreelistManager::write_zone_state_delta_to_db(
  uint64_t zone_num,
  const zone_state_t &zone_state,
  KeyValueDB::Transaction txn)
{
  string key;
  _key_encode_u64(zone_num, &key);
  bufferlist bl;
  zone_state.encode(bl);
  txn->merge(info_prefix, key, bl);
}

void ZonedFreelistManager::write_zone_state_reset_to_db(
  uint64_t zone_num,
  const zone_state_t &zone_state,
  KeyValueDB::Transaction txn)
{
  string key;
  _key_encode_u64(zone_num, &key);
  bufferlist bl;
  zone_state.encode(bl);
  txn->set(info_prefix, key, bl);
}

void ZonedFreelistManager::load_zone_state_from_db(
  uint64_t zone_num,
  zone_state_t &zone_state,
  KeyValueDB::Iterator& it) const
{
  string k = it->key();
  uint64_t zone_num_from_db;
  _key_decode_u64(k.c_str(), &zone_num_from_db);
  ceph_assert(zone_num_from_db == zone_num);

  bufferlist bl = it->value();
  auto p = bl.cbegin();
  zone_state.decode(p);
}

void ZonedFreelistManager::init_zone_states(KeyValueDB::Transaction txn)
{
  dout(10) << __func__ << dendl;
  for (uint64_t zone_num = 0; zone_num < num_zones; ++zone_num) {
    zone_state_t zone_state;
    write_zone_state_reset_to_db(zone_num, zone_state, txn);
  }
}

void ZonedFreelistManager::setup_merge_operator(KeyValueDB *db, string prefix)
{
  std::shared_ptr<Int64ArrayMergeOperator> merge_op(
    new Int64ArrayMergeOperator);
  db->set_merge_operator(prefix, merge_op);
}

ZonedFreelistManager::ZonedFreelistManager(
  ObjectStore* store,
  string meta_prefix,
  string info_prefix)
  : FreelistManager(store),
    meta_prefix(meta_prefix),
    info_prefix(info_prefix),
    enumerate_zone_num(~0UL)
{
}

int ZonedFreelistManager::create(
  uint64_t new_size,
  uint64_t granularity,
  uint64_t new_zone_size,
  uint64_t first_sequential_zone,
  KeyValueDB::Transaction txn)
{
  size = new_size;
  bytes_per_block = granularity;
  zone_size = new_zone_size;
  num_zones = size / zone_size;
  starting_zone_num = first_sequential_zone;
  enumerate_zone_num = ~0UL;

  ceph_assert(size % zone_size == 0);

  dout(1) << __func__ << std::hex
	  << " size 0x" << size
	  << " bytes_per_block 0x" << bytes_per_block
	  << " zone size 0x " << zone_size
	  << " num_zones 0x" << num_zones
	  << " starting_zone 0x" << starting_zone_num << dendl;

  store->write_meta("zfm_size", stringify(size));
  store->write_meta("zfm_bytes_per_block", stringify(bytes_per_block));
  store->write_meta("zfm_zone_size", stringify(zone_size));
  store->write_meta("zfm_num_zones", stringify(num_zones));
  store->write_meta("zfm_starting_zone_num", stringify(starting_zone_num));
  init_zone_states(txn);

  return 0;
}

int ZonedFreelistManager::init(
  KeyValueDB *kvdb,
  bool db_in_read_only,
  cfg_reader_t cfg_reader)
{
  dout(1) << __func__ << dendl;
  int r = _read_cfg(cfg_reader);
  if (r != 0) {
    return r;
  }

  ceph_assert(num_zones == size / zone_size);

  dout(10) << __func__ << std::hex
	   << " size 0x" << size
	   << " bytes_per_block 0x" << bytes_per_block
	   << " zone size 0x" << zone_size
	   << " num_zones 0x" << num_zones
	   << " starting_zone 0x" << starting_zone_num
	   << std::dec << dendl;
  return 0;
}

void ZonedFreelistManager::sync(KeyValueDB* kvdb)
{
}

void ZonedFreelistManager::shutdown()
{
  dout(1) << __func__ << dendl;
}

void ZonedFreelistManager::enumerate_reset()
{
  std::lock_guard l(lock);

  dout(1) << __func__ << dendl;

  enumerate_p.reset();
  enumerate_zone_num = ~0UL;
}

// Currently, this just iterates over the list of zones and sets |offset| and
// |length| to the write pointer and the number of remaining free bytes in a
// given zone.  Hence, it can set |length| to 0 if a zone is full, and it can
// also return two contiguous empty zones in two calls.  This does not violate
// current semantics of the call and appears to work fine with the clients of
// this call.
bool ZonedFreelistManager::enumerate_next(
  KeyValueDB *kvdb,
  uint64_t *offset,
  uint64_t *length)
{
  std::lock_guard l(lock);

  // starting case
  if (enumerate_zone_num == ~0UL) {
    dout(30) << __func__ << " start" << dendl;
    enumerate_p = kvdb->get_iterator(info_prefix);
    enumerate_p->lower_bound(string());
    ceph_assert(enumerate_p->valid());
    enumerate_zone_num = 0;
  } else {
    enumerate_p->next();
    if (!enumerate_p->valid()) {
      dout(30) << __func__ << " end" << dendl;
      return false;
    }
    ++enumerate_zone_num;
  }

  zone_state_t zone_state;
  load_zone_state_from_db(enumerate_zone_num, zone_state, enumerate_p);

  *offset = enumerate_zone_num * zone_size + zone_state.get_write_pointer();
  *length = zone_size - zone_state.get_write_pointer();

  dout(30) << __func__ << std::hex << " 0x" << *offset << "~" << *length
	   << std::dec << dendl;

  return true;
}

void ZonedFreelistManager::dump(KeyValueDB *kvdb)
{
  enumerate_reset();
  uint64_t offset, length;
  while (enumerate_next(kvdb, &offset, &length)) {
    dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	     << std::dec << dendl;
  }
}

// Advances the write pointer and writes the updated write pointer to database.
void ZonedFreelistManager::allocate(
  uint64_t offset,
  uint64_t length,
  KeyValueDB::Transaction txn)
{
  while (length > 0) {
    uint64_t zone_num = offset / zone_size;
    uint64_t this_len = std::min(length, zone_size - offset % zone_size);
    dout(10) << __func__ << " 0x" << std::hex << offset << "~" << this_len
	     << " zone 0x" << zone_num << std::dec << dendl;
    zone_state_t zone_state;
    zone_state.increment_write_pointer(this_len);
    write_zone_state_delta_to_db(zone_num, zone_state, txn);
    offset += this_len;
    length -= this_len;
  }
}

// Increments the number of dead bytes in a zone and writes the updated value to
// database.  The dead bytes in the zone are not usable.  The cleaner will later
// copy live objects from the zone to another zone an make the zone writable
// again.  The number of dead bytes in a zone is used by the cleaner to select
// which zones to clean -- the ones with most dead bytes are good candidates
// since they require less I/O.
void ZonedFreelistManager::release(
  uint64_t offset,
  uint64_t length,
  KeyValueDB::Transaction txn)
{
  while (length > 0) {
    uint64_t zone_num = offset / zone_size;
    uint64_t this_len = std::min(length, zone_size - offset % zone_size);
    dout(10) << __func__ << " 0x" << std::hex << offset << "~" << this_len
	     << " zone 0x" << zone_num << std::dec << dendl;
    zone_state_t zone_state;
    zone_state.increment_num_dead_bytes(this_len);
    write_zone_state_delta_to_db(zone_num, zone_state, txn);
    length -= this_len;
    offset += this_len;
  }
}

void ZonedFreelistManager::get_meta(
  uint64_t target_size,
  std::vector<std::pair<string, string>>* res) const
{
  // We do not support expanding devices for now.
  ceph_assert(target_size == 0);
  res->emplace_back("zfm_size", stringify(size));
  res->emplace_back("zfm_bytes_per_block", stringify(bytes_per_block));
  res->emplace_back("zfm_zone_size", stringify(zone_size));
  res->emplace_back("zfm_num_zones", stringify(num_zones));
  res->emplace_back("zfm_starting_zone_num", stringify(starting_zone_num));
}

std::vector<zone_state_t> ZonedFreelistManager::get_zone_states(
  KeyValueDB *kvdb) const
{
  std::vector<zone_state_t> zone_states;
  auto p = kvdb->get_iterator(info_prefix);
  uint64_t zone_num = 0;
  for (p->lower_bound(string()); p->valid(); p->next(), ++zone_num) {
    zone_state_t zone_state;
    load_zone_state_from_db(zone_num, zone_state, p);
    zone_states.emplace_back(zone_state);
  }
  return zone_states;
}

// TODO: The following function is copied almost verbatim from
// BitmapFreelistManager.  Eliminate duplication.
int ZonedFreelistManager::_read_cfg(cfg_reader_t cfg_reader)
{
  dout(1) << __func__ << dendl;

  string err;

  const size_t key_count = 5;
  string keys[key_count] = {
    "zfm_size",
    "zfm_bytes_per_block",
    "zfm_zone_size",
    "zfm_num_zones",
    "zfm_starting_zone_num"
  };
  uint64_t* vals[key_count] = {
    &size,
    &bytes_per_block,
    &zone_size,
    &num_zones,
    &starting_zone_num};

  for (size_t i = 0; i < key_count; i++) {
    string val;
    int r = cfg_reader(keys[i], &val);
    if (r == 0) {
      *(vals[i]) = strict_iecstrtoll(val.c_str(), &err);
      if (!err.empty()) {
        derr << __func__ << " Failed to parse - "
          << keys[i] << ":" << val
          << ", error: " << err << dendl;
        return -EINVAL;
      }
    } else {
      // this is expected for legacy deployed OSDs
      dout(0) << __func__ << " " << keys[i] << " not found in bdev meta" << dendl;
      return r;
    }
  }
  return 0;
}

void ZonedFreelistManager::mark_zone_to_clean_free(
  uint64_t zone,
  KeyValueDB *kvdb)
{
  dout(10) << __func__ << " zone 0x" << std::hex << zone << std::dec << dendl;

  KeyValueDB::Transaction txn = kvdb->get_transaction();

  zone_state_t empty_zone_state;
  write_zone_state_reset_to_db(zone, empty_zone_state, txn);

  // block here until this commits so that we don't end up starting to allocate and
  // write to the new zone before this fully commits.
  kvdb->submit_transaction_sync(txn);
}
