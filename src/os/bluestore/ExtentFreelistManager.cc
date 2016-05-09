// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ExtentFreelistManager.h"
#include "kv/KeyValueDB.h"
#include "kv.h"

#include "common/debug.h"

#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "freelist "

int ExtentFreelistManager::init()
{
  dout(1) << __func__ << dendl;

  // load state from kvstore
  KeyValueDB::Transaction txn = kvdb->get_transaction();
  int fixed = 0;

  KeyValueDB::Iterator it = kvdb->get_iterator(prefix);
  it->lower_bound(string());
  uint64_t last_offset = 0;
  uint64_t last_length = 0;
  while (it->valid()) {
    uint64_t offset, length;
    string k = it->key();
    const char *p = _key_decode_u64(k.c_str(), &offset);
    assert(p);
    bufferlist bl = it->value();
    bufferlist::iterator bp = bl.begin();
    ::decode(length, bp);

    total_free += length;

    if (offset < last_offset + last_length) {
      derr << __func__ << " detected overlapping extent on load, had "
	   << last_offset << "~" << last_length
	   << " and got "
	   << offset << "~" << length
	   << dendl;
      return -EIO;
    }
    if (offset && offset == last_offset + last_length) {
      derr << __func__ << " detected contiguous extent on load, merging "
	   << last_offset << "~" << last_length << " with "
	   << offset << "~" << length
	   << dendl;
      kv_free.erase(last_offset);
      string key;
      _key_encode_u64(last_offset, &key);
      txn->rmkey(prefix, key);
      offset -= last_length;
      length += last_length;
      bufferlist value;
      ::encode(length, value);
      txn->set(prefix, key, value);
      fixed++;
    }

    kv_free[offset] = length;
    dout(20) << __func__ << "  " << offset << "~" << length << dendl;

    last_offset = offset;
    last_length = length;
    it->next();
  }

  if (fixed) {
    kvdb->submit_transaction_sync(txn);
    derr << " fixed " << fixed << " extents" << dendl;
  }

  dout(10) << __func__ << " loaded " << kv_free.size() << " extents" << dendl;
  return 0;
}

void ExtentFreelistManager::shutdown()
{
  dout(1) << __func__ << dendl;
}

void ExtentFreelistManager::dump()
{
  std::lock_guard<std::mutex> l(lock);
  _dump();
}

void ExtentFreelistManager::enumerate_reset()
{
  std::lock_guard<std::mutex> l(lock);
  enumerate_p = kv_free.begin();
}

bool ExtentFreelistManager::enumerate_next(uint64_t *offset, uint64_t *length)
{
  std::lock_guard<std::mutex> l(lock);
  if (enumerate_p == kv_free.end())
    return false;
  *offset = enumerate_p->first;
  *length = enumerate_p->second;
  ++enumerate_p;
  return true;
}

void ExtentFreelistManager::_dump()
{
  dout(30) << __func__ << " " << total_free
	   << " in " << kv_free.size() << " extents" << dendl;
  for (auto p = kv_free.begin();
       p != kv_free.end();
       ++p) {
    dout(30) << __func__ << "  " << p->first << "~" << p->second << dendl;
  }
}

void ExtentFreelistManager::_audit()
{
  uint64_t sum = 0;
  for (auto& p : kv_free) {
    sum += p.second;
  }
  if (total_free != sum) {
    derr << __func__ << " sum " << sum << " != total_free " << total_free
	 << dendl;
    derr << kv_free << dendl;
    assert(0 == "freelistmanager bug");
  }
}

void ExtentFreelistManager::allocate(
  uint64_t offset, uint64_t length,
  KeyValueDB::Transaction txn)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << offset << "~" << length << dendl;
  total_free -= length;
  auto p = kv_free.lower_bound(offset);
  if ((p == kv_free.end() || p->first > offset) &&
      p != kv_free.begin()) {
    --p;
  }
  if (p == kv_free.end() ||
      p->first > offset ||
      p->first + p->second < offset + length) {
    derr << " bad allocate " << offset << "~" << length << " - dne" << dendl;
    if (p != kv_free.end()) {
      derr << " existing extent " << p->first << "~" << p->second << dendl;
    }
    _dump();
    assert(0 == "bad allocate");
  }

  if (p->first == offset) {
    string key;
    _key_encode_u64(offset, &key);
    txn->rmkey(prefix, key);
    dout(20) << __func__ << "  rm " << p->first << "~" << p->second << dendl;
    if (p->second > length) {
      uint64_t newoff = offset + length;
      uint64_t newlen = p->second - length;
      string newkey;
      _key_encode_u64(newoff, &newkey);
      bufferlist newvalue;
      ::encode(newlen, newvalue);
      txn->set(prefix, newkey, newvalue);
      dout(20) << __func__ << "  set " << newoff << "~" << newlen
	       << " (remaining tail)" << dendl;
      kv_free.erase(p);
      kv_free[newoff] = newlen;
    } else {
      kv_free.erase(p);
    }
  } else {
    assert(p->first < offset);
    // shorten
    uint64_t newlen = offset - p->first;
    string key;
    _key_encode_u64(p->first, &key);
    bufferlist newvalue;
    ::encode(newlen, newvalue);
    txn->set(prefix, key, newvalue);
    dout(30) << __func__ << "  set " << p->first << "~" << newlen
	     << " (remaining head from " << p->second << ")" << dendl;
    if (p->first + p->second > offset + length) {
      // new trailing piece, too
      uint64_t tailoff = offset + length;
      uint64_t taillen = p->first + p->second - (offset + length);
      string tailkey;
      _key_encode_u64(tailoff, &tailkey);
      bufferlist tailvalue;
      ::encode(taillen, tailvalue);
      txn->set(prefix, tailkey, tailvalue);
      dout(20) << __func__ << "  set " << tailoff << "~" << taillen
	       << " (remaining tail from " << p->first << "~" << p->second << ")"
	       << dendl;
      p->second = newlen;
      kv_free[tailoff] = taillen;
    } else {
      p->second = newlen;
    }
  }
  if (g_conf->bluestore_debug_freelist)
    _audit();
}

void ExtentFreelistManager::release(
  uint64_t offset, uint64_t length,
  KeyValueDB::Transaction txn)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << offset << "~" << length << dendl;
  total_free += length;
  auto p = kv_free.lower_bound(offset);

  // contiguous with previous extent?
  if (p != kv_free.begin()) {
    --p;
    if (p->first + p->second == offset) {
      string prevkey;
      _key_encode_u64(p->first, &prevkey);
      txn->rmkey(prefix, prevkey);
      dout(20) << __func__ << "  rm " << p->first << "~" << p->second
	       << " (merge with previous)" << dendl;
      length += p->second;
      offset = p->first;
      if (map_t_has_stable_iterators) {
	kv_free.erase(p++);
      } else {
	p = kv_free.erase(p);
      }
    } else if (p->first + p->second > offset) {
      derr << __func__ << " bad release " << offset << "~" << length
	   << " overlaps with " << p->first << "~" << p->second << dendl;
      _dump();
      assert(0 == "bad release overlap");
    } else {
      dout(30) << __func__ << " previous extent " << p->first << "~" << p->second
	       << " is not contiguous" << dendl;
      ++p;
    }
  }

  // contiguous with next extent?
  if (p != kv_free.end()) {
    if (p->first == offset + length) {
      string tailkey;
      _key_encode_u64(p->first, &tailkey);
      txn->rmkey(prefix, tailkey);
      dout(20) << __func__ << "  rm " << p->first << "~" << p->second
	       << " (merge with next)" << dendl;
      length += p->second;
      kv_free.erase(p);
    } else if (p->first < offset + length) {
      derr << __func__ << " bad release " << offset << "~" << length
	   << " overlaps with " << p->first << "~" << p->second << dendl;
      _dump();
      assert(0 == "bad release overlap");
    } else {
      dout(30) << __func__ << " next extent " << p->first << "~" << p->second
	       << " is not contiguous" << dendl;
    }
  }

  string key;
  _key_encode_u64(offset, &key);
  bufferlist value;
  ::encode(length, value);
  txn->set(prefix, key, value);
  dout(20) << __func__ << "  set " << offset << "~" << length << dendl;

  kv_free[offset] = length;

  if (g_conf->bluestore_debug_freelist)
    _audit();
}
