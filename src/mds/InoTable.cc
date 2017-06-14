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

#include "InoTable.h"
#include "MDSRank.h"

#include "include/types.h"

#include "common/config.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << rank << "." << table_name << ": "

void InoTable::reset_state()
{
  // use generic range. FIXME THIS IS CRAP
  free.clear();
  //#ifdef __LP64__
  uint64_t start = (uint64_t)(rank+1) << 40;
  uint64_t len = (uint64_t)1 << 40;
  //#else
  //# warning this looks like a 32-bit system, using small inode numbers.
  //  uint64_t start = (uint64_t)(mds->get_nodeid()+1) << 25;
  //  uint64_t end = ((uint64_t)(mds->get_nodeid()+2) << 25) - 1;
  //#endif
  free.insert(start, len);

  projected_free = free;
}

inodeno_t InoTable::project_alloc_id(inodeno_t id) 
{
  dout(10) << "project_alloc_id " << id << " to " << projected_free << "/" << free << dendl;
  assert(is_active());
  if (!id)
    id = projected_free.range_start();
  projected_free.erase(id);
  ++projected_version;
  return id;
}
void InoTable::apply_alloc_id(inodeno_t id)
{
  dout(10) << "apply_alloc_id " << id << " to " << projected_free << "/" << free << dendl;
  free.erase(id);
  ++version;
}

void InoTable::project_alloc_ids(interval_set<inodeno_t>& ids, int want) 
{
  assert(is_active());
  while (want > 0) {
    inodeno_t start = projected_free.range_start();
    inodeno_t end = projected_free.end_after(start);
    inodeno_t num = end - start;
    if (num > (inodeno_t)want)
      num = want;
    projected_free.erase(start, num);
    ids.insert(start, num);
    want -= num;
  }
  dout(10) << "project_alloc_ids " << ids << " to " << projected_free << "/" << free << dendl;
  ++projected_version;
}
void InoTable::apply_alloc_ids(interval_set<inodeno_t>& ids)
{
  dout(10) << "apply_alloc_ids " << ids << " to " << projected_free << "/" << free << dendl;
  free.subtract(ids);
  ++version;
}


void InoTable::project_release_ids(interval_set<inodeno_t>& ids) 
{
  dout(10) << "project_release_ids " << ids << " to " << projected_free << "/" << free << dendl;
  projected_free.insert(ids);
  ++projected_version;
}
void InoTable::apply_release_ids(interval_set<inodeno_t>& ids) 
{
  dout(10) << "apply_release_ids " << ids << " to " << projected_free << "/" << free << dendl;
  free.insert(ids);
  ++version;
}


//

void InoTable::replay_alloc_id(inodeno_t id) 
{
  assert(mds);  // Only usable in online mode

  dout(10) << "replay_alloc_id " << id << dendl;
  if (free.contains(id)) {
    free.erase(id);
    projected_free.erase(id);
  } else {
    mds->clog->error() << "journal replay alloc " << id
      << " not in free " << free << "\n";
  }
  projected_version = ++version;
}
void InoTable::replay_alloc_ids(interval_set<inodeno_t>& ids) 
{
  assert(mds);  // Only usable in online mode

  dout(10) << "replay_alloc_ids " << ids << dendl;
  interval_set<inodeno_t> is;
  is.intersection_of(free, ids);
  if (is == ids) {
    free.subtract(ids);
    projected_free.subtract(ids);
  } else {
    mds->clog->error() << "journal replay alloc " << ids << ", only "
	<< is << " is in free " << free << "\n";
    free.subtract(is);
    projected_free.subtract(is);
  }
  projected_version = ++version;
}
void InoTable::replay_release_ids(interval_set<inodeno_t>& ids) 
{
  dout(10) << "replay_release_ids " << ids << dendl;
  free.insert(ids);
  projected_free.insert(ids);
  projected_version = ++version;
}


void InoTable::replay_reset()
{
  dout(10) << "replay_reset " << free << dendl;
  skip_inos(inodeno_t(10000000));  // a lot!
  projected_free = free;
  projected_version = ++version;
}


void InoTable::skip_inos(inodeno_t i)
{
  dout(10) << "skip_inos was " << free << dendl;
  inodeno_t first = free.range_start();
  interval_set<inodeno_t> s;
  s.insert(first, i);
  s.intersection_of(free);
  free.subtract(s);
  projected_free = free;
  projected_version = ++version;
  dout(10) << "skip_inos now " << free << dendl;
}

void InoTable::dump(Formatter *f) const
{
  f->open_object_section("inotable");

  f->open_array_section("projected_free");
  for (interval_set<inodeno_t>::const_iterator i = projected_free.begin(); i != projected_free.end(); ++i) {
    f->open_object_section("range");
    f->dump_int("start", (*i).first);
    f->dump_int("len", (*i).second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("free");
  for (interval_set<inodeno_t>::const_iterator i = free.begin(); i != free.end(); ++i) {
    f->open_object_section("range");
    f->dump_int("start", (*i).first);
    f->dump_int("len", (*i).second);
    f->close_section();
  }
  f->close_section();

  f->close_section();
}


void InoTable::generate_test_instances(list<InoTable*>& ls)
{
  ls.push_back(new InoTable());
}

bool InoTable::intersects_free(
    const interval_set<inodeno_t> &other,
    interval_set<inodeno_t> *intersection)
{
  interval_set<inodeno_t> i;
  i.intersection_of(free, other);
  if (intersection != nullptr) {
    *intersection = i;
  }
  return !(i.empty());
}
