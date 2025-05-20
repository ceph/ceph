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

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "snap_mapper."

using std::string;

const string SnapMapper::MAPPING_PREFIX = "MAP_";
const string SnapMapper::OBJECT_PREFIX = "OBJ_";

int OSDriver::get_keys(
  const std::set<std::string> &keys,
  std::map<std::string, bufferlist> *out)
{
  return os->omap_get_values(ch, hoid, keys, out);
}

int OSDriver::get_next(
  const std::string &key,
  pair<std::string, bufferlist> *next,
  bool with_lower_bound)
{
  ObjectMap::ObjectMapIterator iter =
    os->get_omap_iterator(ch, hoid, with_lower_bound);
  if (!iter) {
    ceph_abort();
    return -EINVAL;
  }
  iter->upper_bound(key);
  if (iter->valid()) {
    if (next)
      *next = make_pair(iter->key(), iter->value());
    return 0;
  } else {
    return -ENOENT;
  }
}

struct Mapping {
  snapid_t snap;
  hobject_t hoid;
  explicit Mapping(const pair<snapid_t, hobject_t> &in)
    : snap(in.first), hoid(in.second) {}
  Mapping() : snap(0) {}
  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(snap, bl);
    encode(hoid, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(snap, bl);
    decode(hoid, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(Mapping)

string SnapMapper::get_prefix(snapid_t snap)
{
  char buf[100];
  int len = snprintf(
    buf, sizeof(buf),
    "%.*X_", (int)(sizeof(snap)*2),
    static_cast<unsigned>(snap));
  return MAPPING_PREFIX + string(buf, len);
}

string SnapMapper::to_raw_key(
  const pair<snapid_t, hobject_t> &in)
{
  return get_prefix(in.first) + shard_prefix + in.second.to_str();
}

pair<string, bufferlist> SnapMapper::to_raw(
  const pair<snapid_t, hobject_t> &in)
{
  bufferlist bl;
  encode(Mapping(in), bl);
  return make_pair(
    to_raw_key(in),
    bl);
}

pair<snapid_t, hobject_t> SnapMapper::from_raw(
  const pair<std::string, bufferlist> &image)
{
  Mapping map;
  bufferlist bl(image.second);
  auto bp = bl.cbegin();
  decode(map, bp);
  return make_pair(map.snap, map.hoid);
}

bool SnapMapper::is_mapping(const string &to_test)
{
  return to_test.substr(0, MAPPING_PREFIX.size()) == MAPPING_PREFIX;
}

string SnapMapper::to_object_key(const hobject_t &hoid)
{
  return OBJECT_PREFIX + shard_prefix + hoid.to_str();
}

void SnapMapper::object_snaps::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(oid, bl);
  encode(snaps, bl);
  ENCODE_FINISH(bl);
}

void SnapMapper::object_snaps::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(oid, bl);
  decode(snaps, bl);
  DECODE_FINISH(bl);
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

int SnapMapper::get_snaps(
  const hobject_t &oid,
  object_snaps *out)
{
  ceph_assert(check(oid));
  set<string> keys;
  map<string, bufferlist> got;
  keys.insert(to_object_key(oid));
  int r = backend.get_keys(keys, &got);
  if (r < 0) {
    dout(20) << __func__ << " " << oid << " got err " << r << dendl;
    return r;
  }
  if (got.empty()) {
    dout(20) << __func__ << " " << oid << " got.empty()" << dendl;
    return -ENOENT;
  }
  if (out) {
    auto bp = got.begin()->second.cbegin();
    decode(*out, bp);
    dout(20) << __func__ << " " << oid << " " << out->snaps << dendl;
    if (out->snaps.empty()) {
      dout(1) << __func__ << " " << oid << " empty snapset" << dendl;
      ceph_assert(!cct->_conf->osd_debug_verify_snaps);
    }
  } else {
    dout(20) << __func__ << " " << oid << " (out == NULL)" << dendl;
  }
  return 0;
}

void SnapMapper::clear_snaps(
  const hobject_t &oid,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  dout(20) << __func__ << " " << oid << dendl;
  ceph_assert(check(oid));
  set<string> to_remove;
  to_remove.insert(to_object_key(oid));
  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    for (auto& i : to_remove) {
      dout(20) << __func__ << " rm " << i << dendl;
    }
  }
  backend.remove_keys(to_remove, t);
}

void SnapMapper::set_snaps(
  const hobject_t &oid,
  const object_snaps &in,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  ceph_assert(check(oid));
  map<string, bufferlist> to_set;
  bufferlist bl;
  encode(in, bl);
  to_set[to_object_key(oid)] = bl;
  dout(20) << __func__ << " " << oid << " " << in.snaps << dendl;
  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    for (auto& i : to_set) {
      dout(20) << __func__ << " set " << i.first << dendl;
    }
  }
  backend.set_keys(to_set, t);
}

int SnapMapper::update_snaps(
  const hobject_t &oid,
  const set<snapid_t> &new_snaps,
  const set<snapid_t> *old_snaps_check,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  dout(20) << __func__ << " " << oid << " " << new_snaps
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
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  dout(20) << __func__ << " " << oid << " " << snaps << dendl;
  ceph_assert(!snaps.empty());
  ceph_assert(check(oid));
  {
    object_snaps out;
    int r = get_snaps(oid, &out);
    if (r != -ENOENT) {
      derr << __func__ << " found existing snaps mapped on " << oid
	   << ", removing" << dendl;
      ceph_assert(!cct->_conf->osd_debug_verify_snaps);
      remove_oid(oid, t);
    }
  }

  object_snaps _snaps(oid, snaps);
  set_snaps(oid, _snaps, t);

  map<string, bufferlist> to_add;
  for (set<snapid_t>::iterator i = snaps.begin();
       i != snaps.end();
       ++i) {
    to_add.insert(to_raw(make_pair(*i, oid)));
  }
  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    for (auto& i : to_add) {
      dout(20) << __func__ << " set " << i.first << dendl;
    }
  }
  backend.set_keys(to_add, t);
}

int SnapMapper::get_next_objects_to_trim(
  snapid_t snap,
  unsigned max,
  vector<hobject_t> *out,
  string &last_key,
  std::set<string>& last_prefixes)
{
  ceph_assert(out);
  ceph_assert(out->empty());
  int r = 0;
  size_t prefix_snap = get_prefix(snap).size();
  size_t prefix_pg = (*prefixes.begin()).size();
  if(last_prefixes != prefixes) {
    // pg may split or merge, reset last_key
    last_key = string();
    last_prefixes = prefixes;
  }
  string target = (prefix_snap + prefix_pg ) < last_key.size() ? last_key.substr(prefix_snap, prefix_pg) : "";
  auto i = prefixes.lower_bound(target);
  for (; i != prefixes.end() && out->size() < max && r == 0;
       ++i) {
    // Construct the prefix of the snapshot object to be deleted.
    string prefix(get_prefix(snap) + *i);
    // last_key.size() = 0, pg may split or merge, pos use the prefix.
    string pos = last_key.size() > 0 ? last_key : prefix;
    while (out->size() < max) {
      pair<string, bufferlist> next;
      r = backend.get_next(pos, &next, false);
      last_key = string();
      dout(20) << __func__ << " get_next(" << pos << ") returns " << r
	       << " " << next << dendl;
      if (r != 0) {
	break; // Done
      }

      if (next.first.substr(0, prefix.size()) !=
	  prefix) {
	break; // Done with this prefix
      }

      ceph_assert(is_mapping(next.first));

      dout(20) << __func__ << " " << next.first << dendl;
      pair<snapid_t, hobject_t> next_decoded(from_raw(next));
      ceph_assert(next_decoded.first == snap);
      ceph_assert(check(next_decoded.second));

      out->push_back(next_decoded.second);
      last_key = pos = next.first;
    }
  }
  if (out->size() == 0) {
    return -ENOENT;
  } else {
    return 0;
  }
}


int SnapMapper::remove_oid(
  const hobject_t &oid,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  dout(20) << __func__ << " " << oid << dendl;
  ceph_assert(check(oid));
  return _remove_oid(oid, t);
}

int SnapMapper::_remove_oid(
  const hobject_t &oid,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  dout(20) << __func__ << " " << oid << dendl;
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
      dout(20) << __func__ << " rm " << i << dendl;
    }
  }
  backend.remove_keys(to_remove, t);
  return 0;
}

int SnapMapper::get_snaps(
  const hobject_t &oid,
  std::set<snapid_t> *snaps)
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
