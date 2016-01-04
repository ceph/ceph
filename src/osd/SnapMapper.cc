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
  return os->omap_get_values(cid, hoid, keys, out);
}

int OSDriver::get_next(
  const std::string &key,
  pair<std::string, bufferlist> *next)
{
  ObjectMap::ObjectMapIterator iter =
    os->get_omap_iterator(cid, hoid);
  if (!iter) {
    assert(0);
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
  Mapping(const pair<snapid_t, hobject_t> &in)
    : snap(in.first), hoid(in.second) {}
  Mapping() : snap(0) {}
  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(snap, bl);
    ::encode(hoid, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(snap, bl);
    ::decode(hoid, bl);
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
  ::encode(Mapping(in), bl);
  return make_pair(
    to_raw_key(in),
    bl);
}

pair<snapid_t, hobject_t> SnapMapper::from_raw(
  const pair<std::string, bufferlist> &image)
{
  Mapping map;
  bufferlist bl(image.second);
  bufferlist::iterator bp(bl.begin());
  ::decode(map, bp);
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
  ::encode(oid, bl);
  ::encode(snaps, bl);
  ENCODE_FINISH(bl);
}

void SnapMapper::object_snaps::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(oid, bl);
  ::decode(snaps, bl);
  DECODE_FINISH(bl);
}

int SnapMapper::get_snaps(
  const hobject_t &oid,
  object_snaps *out)
{
  assert(check(oid));
  set<string> keys;
  map<string, bufferlist> got;
  keys.insert(to_object_key(oid));
  int r = backend.get_keys(keys, &got);
  if (r < 0)
    return r;
  if (got.empty())
    return -ENOENT;
  if (out) {
    bufferlist::iterator bp = got.begin()->second.begin();
    ::decode(*out, bp);
    dout(20) << __func__ << " " << oid << " " << out->snaps << dendl;
    assert(!out->snaps.empty());
  } else {
    dout(20) << __func__ << " " << oid << " (out == NULL)" << dendl;
  }
  return 0;
}

void SnapMapper::clear_snaps(
  const hobject_t &oid,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  assert(check(oid));
  set<string> to_remove;
  to_remove.insert(to_object_key(oid));
  backend.remove_keys(to_remove, t);
}

void SnapMapper::set_snaps(
  const hobject_t &oid,
  const object_snaps &in,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  assert(check(oid));
  map<string, bufferlist> to_set;
  bufferlist bl;
  ::encode(in, bl);
  to_set[to_object_key(oid)] = bl;
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
  assert(check(oid));
  if (new_snaps.empty())
    return remove_oid(oid, t);

  object_snaps out;
  int r = get_snaps(oid, &out);
  if (r < 0)
    return r;
  if (old_snaps_check)
    assert(out.snaps == *old_snaps_check);

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
  backend.remove_keys(to_remove, t);
  return 0;
}

void SnapMapper::add_oid(
  const hobject_t &oid,
  const set<snapid_t>& snaps,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  dout(20) << __func__ << " " << oid << " " << snaps << dendl;
  assert(check(oid));
  {
    object_snaps out;
    int r = get_snaps(oid, &out);
    assert(r == -ENOENT);
  }

  object_snaps _snaps(oid, snaps);
  set_snaps(oid, _snaps, t);

  map<string, bufferlist> to_add;
  for (set<snapid_t>::iterator i = snaps.begin();
       i != snaps.end();
       ++i) {
    to_add.insert(to_raw(make_pair(*i, oid)));
  }
  backend.set_keys(to_add, t);
}

int SnapMapper::get_next_object_to_trim(
  snapid_t snap,
  hobject_t *hoid)
{
  for (set<string>::iterator i = prefixes.begin();
       i != prefixes.end();
       ++i) {
    string list_after(get_prefix(snap) + *i);

    pair<string, bufferlist> next;
    int r = backend.get_next(list_after, &next);
    if (r < 0) {
      break; // Done
    }

    if (next.first.substr(0, list_after.size()) !=
	list_after) {
      continue; // Done with this prefix
    }

    assert(is_mapping(next.first));

    pair<snapid_t, hobject_t> next_decoded(from_raw(next));
    assert(next_decoded.first == snap);
    assert(check(next_decoded.second));

    if (hoid)
      *hoid = next_decoded.second;
    return 0;
  }
  return -ENOENT;
}


int SnapMapper::remove_oid(
  const hobject_t &oid,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  dout(20) << __func__ << " " << oid << dendl;
  assert(check(oid));
  return _remove_oid(oid, t);
}

int SnapMapper::_remove_oid(
  const hobject_t &oid,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
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
  backend.remove_keys(to_remove, t);
  return 0;
}

int SnapMapper::get_snaps(
  const hobject_t &oid,
  std::set<snapid_t> *snaps)
{
  assert(check(oid));
  object_snaps out;
  int r = get_snaps(oid, &out);
  if (r < 0)
    return r;
  if (snaps)
    snaps->swap(out.snaps);
  return 0;
}
string PGScrubResult::inconsistent_info_t::object_errors_string(bool deep) const
{
  ostringstream oss;
  if (errors & READ_ERROR)
    oss << "read_error+";
  if (errors & SIZE_MISMATCH)
    oss << "size_mismatch+";
  if (errors & ATTR_MISMATCH) {
    oss << "attr_mismatch[";
    set<string>::const_iterator p = attr_mismatch.begin();
    oss << *p;
    for (p++; p != attr_mismatch.end(); p++)
      oss << ", " << *p;
    oss << "]+";
  }
  if (errors & ATTR_MISSING) {
    oss << "attr_missing[";
    set<string>::const_iterator p = attr_missing.begin();
    oss << *p;
    for (p++; p != attr_missing.end(); p++)
      oss << ", " << *p;
    oss << "]+";
  }
  if (errors & ATTR_EXTRA) {
    oss << "attr_extra[";
    set<string>::const_iterator p = attr_extra.begin();
    oss << *p;
    for (p++; p != attr_extra.end(); p++)
      oss << ", " << *p;
    oss << "]+";
  }
  if (errors & OI_CORRUPTION)
    oss << "oi_corruption+";
  if (deep) {
    if (errors & DIGEST_MISMATCH)
      oss << "digest_mismatch+";
    if (errors & OMAP_DIGEST_MISMATCH)
      oss << "omap_digest_mismatch+";
  }
  string ret(oss.str());
  if (ret.length() > 0)
    ret.resize(ret.length()-1);
  return ret;
}

void PGScrubResult::inconsistent_info_t::encode(bufferlist &bl) const
{
  ::encode(errors, bl);
  ::encode(version, bl);
  ::encode(attr_mismatch, bl);
  ::encode(attr_missing, bl);
  ::encode(attr_extra, bl);
}

void PGScrubResult::inconsistent_info_t::decode(bufferlist::iterator &bp)
{
  ::decode(errors, bp);
  ::decode(version, bp);
  ::decode(attr_mismatch, bp);
  ::decode(attr_missing, bp);
  ::decode(attr_extra, bp);
}

void PGScrubResult::object_scrub_info_t::encode(bufferlist &bl) const
{
  ::encode(hoid, bl);
  ::encode(epoch, bl);
  ::encode(time, bl);
  ::encode(deep, bl);
  ::encode(no_auth, bl);
  ::encode(version, bl);
  ::encode(auth, bl);
  ::encode(missing, bl);
  ::encode(inconsistent, bl);
}

void PGScrubResult::object_scrub_info_t::decode(bufferlist::iterator &bp)
{
  ::decode(hoid, bp);
  ::decode(epoch, bp);
  ::decode(time, bp);
  ::decode(deep, bp);
  ::decode(no_auth, bp);
  ::decode(version, bp);
  ::decode(auth, bp);
  ::decode(missing, bp);
  ::decode(inconsistent, bp);
}

void PGScrubResult::object_scrub_info_t::dump(Formatter *f) const
{
  f->dump_stream("hoid") << hoid;
  f->dump_unsigned("epoch", epoch);
  f->dump_stream("time") << time;
  f->dump_bool("deep-scrub", deep);
  f->dump_bool("no_auth", no_auth);
  f->dump_stream("eversion") << version;
  f->open_array_section("auth");
  for (set<pg_shard_t>::const_iterator p = auth.begin(); p != auth.end(); p++)
    f->dump_stream("pg_shard") << *p;
  f->close_section();
  f->open_array_section("missing");
  for (set<pg_shard_t>::const_iterator p = missing.begin(); p != missing.end(); p++)
    f->dump_stream("pg_shard") << *p;
  f->close_section();
  f->open_array_section("inconsistent");
  for (map<pg_shard_t, inconsistent_info_t>::const_iterator p = inconsistent.begin(); p != inconsistent.end(); p++) {
    f->dump_stream("pg_shard") << p->first;
    f->dump_string("inconsistent_info", p->second.object_errors_string(deep));
  }
  f->close_section();
}

string PGScrubResult::to_key(const hobject_t &oid)
{
  stringstream ss;
  ss << prefix << oid;
  return ss.str();
}

void PGScrubResult::set_object_scrub_info(
  const hobject_t &oid,
  const object_scrub_info_t &in,
  MapCacher::Transaction<string, bufferlist> *t)
{
  map<string, bufferlist> to_set;
  bufferlist bl;
  ::encode(in, bl);
  to_set[to_key(oid)] = bl;
  backend.set_keys(to_set, t);
}

int PGScrubResult::get_object_scrub_info(
  const hobject_t &oid,
  object_scrub_info_t *out)
{
  string key = to_key(oid);
  return get_object_scrub_info(key, out);
}

int PGScrubResult::get_object_scrub_info(
  const string &oid,
  object_scrub_info_t *out)
{
  set<string> keys;
  map<string, bufferlist> got;
  keys.insert(oid);
  int r = backend.get_keys(keys, &got);
  if (r < 0)
    return r;
  if (got.empty())
    return -ENOENT;
  if (out) {
    bufferlist::iterator bp = got.begin()->second.begin();
    ::decode(*out, bp);
  }
  return 0;
}

void PGScrubResult::clear_object_scrub_info(
  const hobject_t &oid,
  MapCacher::Transaction<string, bufferlist> *t)
{
  set<string> to_remove;
  to_remove.insert(to_key(oid));
  backend.remove_keys(to_remove, t);
}

int PGScrubResult::get_all_keys(set<string> *out)
{
  string list_after(prefix);
  pair<string, bufferlist> next;
  int r = backend.get_next(list_after, &next);
  while (r == 0) {
    if (next.first.compare(0, prefix.size(), prefix) != 0)
      break;
    out->insert(next.first);
    list_after = next.first;
    r = backend.get_next(list_after, &next);
  }
  return 0;
}

void  PGScrubResult::clear_all(MapCacher::Transaction<string, bufferlist> *t)
{
  set<string> keys;
  get_all_keys(&keys);
  backend.remove_keys(keys, t);
}
