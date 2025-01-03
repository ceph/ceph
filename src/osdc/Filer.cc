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


#include <mutex>
#include <algorithm>
#include "Filer.h"
#include "osd/OSDMap.h"
#include "Striper.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#include "msg/Messenger.h"

#include "include/Context.h"

#include "common/Finisher.h"
#include "common/config.h"

#define dout_subsys ceph_subsys_filer
#undef dout_prefix
#define dout_prefix *_dout << objecter->messenger->get_myname() << ".filer "

using std::hex;
using std::dec;
using std::vector;

class Filer::C_Probe : public Context {
public:
  Filer *filer;
  Probe *probe;
  object_t oid;
  uint64_t size;
  ceph::real_time mtime;
  C_Probe(Filer *f, Probe *p, object_t o) : filer(f), probe(p), oid(o),
					    size(0) {}
  void finish(int r) override {
    if (r == -ENOENT) {
      r = 0;
      ceph_assert(size == 0);
    }

    bool probe_complete;
    {
      Probe::unique_lock pl(probe->lock);
      if (r != 0) {
	probe->err = r;
      }

      probe_complete = filer->_probed(probe, oid, size, mtime, pl);
      ceph_assert(!pl.owns_lock());
    }
    if (probe_complete) {
      probe->onfinish->complete(probe->err);
      delete probe;
    }
  }
};

int Filer::probe(inodeno_t ino,
		 const file_layout_t *layout,
		 snapid_t snapid,
		 uint64_t start_from,
		 uint64_t *end, // LB, when !fwd
		 ceph::real_time *pmtime,
		 bool fwd,
		 int flags,
		 Context *onfinish)
{
  ldout(cct, 10) << "probe " << (fwd ? "fwd ":"bwd ")
	   << hex << ino << dec
	   << " starting from " << start_from
	   << dendl;

  ceph_assert(snapid);  // (until there is a non-NOSNAP write)

  Probe *probe = new Probe(ino, *layout, snapid, start_from, end, pmtime,
			   flags, fwd, onfinish);

  return probe_impl(probe, layout, start_from, end);
}

int Filer::probe(inodeno_t ino,
		 const file_layout_t *layout,
		 snapid_t snapid,
		 uint64_t start_from,
		 uint64_t *end, // LB, when !fwd
		 utime_t *pmtime,
		 bool fwd,
		 int flags,
		 Context *onfinish)
{
  ldout(cct, 10) << "probe " << (fwd ? "fwd ":"bwd ")
	   << hex << ino << dec
	   << " starting from " << start_from
	   << dendl;

  ceph_assert(snapid);  // (until there is a non-NOSNAP write)

  Probe *probe = new Probe(ino, *layout, snapid, start_from, end, pmtime,
			   flags, fwd, onfinish);
  return probe_impl(probe, layout, start_from, end);
}

int Filer::probe_impl(Probe* probe, const file_layout_t *layout,
		      uint64_t start_from, uint64_t *end) // LB, when !fwd
{
  // period (bytes before we jump unto a new set of object(s))
  uint64_t period = layout->get_period();

  // start with 1+ periods.
  probe->probing_len = period;
  if (probe->fwd) {
    if (start_from % period)
      probe->probing_len += period - (start_from % period);
  } else {
    ceph_assert(start_from > *end);
    if (start_from % period)
      probe->probing_len -= period - (start_from % period);
    probe->probing_off -= probe->probing_len;
  }

  Probe::unique_lock pl(probe->lock);
  _probe(probe, pl);
  ceph_assert(!pl.owns_lock());

  return 0;
}



/**
 * probe->lock must be initially locked, this function will release it
 */
void Filer::_probe(Probe *probe, Probe::unique_lock& pl)
{
  ceph_assert(pl.owns_lock() && pl.mutex() == &probe->lock);

  ldout(cct, 10) << "_probe " << hex << probe->ino << dec
		 << " " << probe->probing_off << "~" << probe->probing_len
		 << dendl;

  // map range onto objects
  probe->known_size.clear();
  probe->probing.clear();
  Striper::file_to_extents(cct, probe->ino, &probe->layout, probe->probing_off,
			   probe->probing_len, 0, probe->probing);

  std::vector<ObjectExtent> stat_extents;
  for (auto p = probe->probing.begin(); p != probe->probing.end(); ++p) {
    ldout(cct, 10) << "_probe  probing " << p->oid << dendl;
    probe->ops.insert(p->oid);
    stat_extents.push_back(*p);
  }

  pl.unlock();
  for (std::vector<ObjectExtent>::iterator i = stat_extents.begin();
       i != stat_extents.end(); ++i) {
    C_Probe *c = new C_Probe(this, probe, i->oid);
    objecter->stat(i->oid, i->oloc, probe->snapid, &c->size, &c->mtime,
		   probe->flags | CEPH_OSD_FLAG_RWORDERED,
		   new C_OnFinisher(c, finisher));
  }
}

/**
 * probe->lock must be initially held, and will be released by this function.
 *
 * @return true if probe is complete and Probe object may be freed.
 */
bool Filer::_probed(Probe *probe, const object_t& oid, uint64_t size,
		    ceph::real_time mtime, Probe::unique_lock& pl)
{
  ceph_assert(pl.owns_lock() && pl.mutex() == &probe->lock);

  ldout(cct, 10) << "_probed " << probe->ino << " object " << oid
	   << " has size " << size << " mtime " << mtime << dendl;

  probe->known_size[oid] = size;
  if (mtime > probe->max_mtime)
    probe->max_mtime = mtime;

  ceph_assert(probe->ops.count(oid));
  probe->ops.erase(oid);

  if (!probe->ops.empty()) {
    pl.unlock();
    return false;  // waiting for more!
  }

  if (probe->err) { // we hit an error, propagate back up
    pl.unlock();
    return true;
  }

  // analyze!
  uint64_t end = 0;

  if (!probe->fwd) {
    std::reverse(probe->probing.begin(), probe->probing.end());
  }

  for (auto p = probe->probing.begin(); p != probe->probing.end(); ++p) {
    uint64_t shouldbe = p->length + p->offset;
    ldout(cct, 10) << "_probed  " << probe->ino << " object " << hex
		   << p->oid << dec << " should be " << shouldbe
		   << ", actual is " << probe->known_size[p->oid]
		   << dendl;

    if (!probe->found_size) {
      ceph_assert(probe->known_size[p->oid] <= shouldbe);

      if ((probe->fwd && probe->known_size[p->oid] == shouldbe) ||
	  (!probe->fwd && probe->known_size[p->oid] == 0 &&
	   probe->probing_off > 0))
	continue;  // keep going

      // aha, we found the end!
      // calc offset into buffer_extent to get distance from probe->from.
      uint64_t oleft = probe->known_size[p->oid] - p->offset;
      for (auto i = p->buffer_extents.begin();
	   i != p->buffer_extents.end();
	   ++i) {
	if (oleft <= (uint64_t)i->second) {
	  end = probe->probing_off + i->first + oleft;
	  ldout(cct, 10) << "_probed  end is in buffer_extent " << i->first
			 << "~" << i->second << " off " << oleft
			 << ", from was " << probe->probing_off << ", end is "
			 << end << dendl;

	  probe->found_size = true;
	  ldout(cct, 10) << "_probed found size at " << end << dendl;
	  *probe->psize = end;

	  if (!probe->pmtime &&
	      !probe->pumtime)  // stop if we don't need mtime too
	    break;
	}
	oleft -= i->second;
      }
    }
    break;
  }

  if (!probe->found_size || (probe->probing_off && (probe->pmtime ||
						    probe->pumtime))) {
    // keep probing!
    ldout(cct, 10) << "_probed probing further" << dendl;

    uint64_t period = probe->layout.get_period();
    if (probe->fwd) {
      probe->probing_off += probe->probing_len;
      ceph_assert(probe->probing_off % period == 0);
      probe->probing_len = period;
    } else {
      // previous period.
      ceph_assert(probe->probing_off % period == 0);
      probe->probing_len = period;
      probe->probing_off -= period;
    }
    _probe(probe, pl);
    ceph_assert(!pl.owns_lock());
    return false;
  } else if (probe->pmtime) {
    ldout(cct, 10) << "_probed found mtime " << probe->max_mtime << dendl;
    *probe->pmtime = probe->max_mtime;
  } else if (probe->pumtime) {
    ldout(cct, 10) << "_probed found mtime " << probe->max_mtime << dendl;
    *probe->pumtime = ceph::real_clock::to_ceph_timespec(probe->max_mtime);
  }
  // done!
  pl.unlock();
  return true;
}


// -----------------------

struct PurgeRange {
  std::mutex lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  inodeno_t ino;
  file_layout_t layout;
  SnapContext snapc;
  uint64_t first, num;
  ceph::real_time mtime;
  int flags;
  Context *oncommit;
  int uncommitted;
  int err = 0;
  PurgeRange(inodeno_t i, const file_layout_t& l, const SnapContext& sc,
	     uint64_t fo, uint64_t no, ceph::real_time t, int fl,
	     Context *fin)
    : ino(i), layout(l), snapc(sc), first(fo), num(no), mtime(t), flags(fl),
      oncommit(fin), uncommitted(0) {}
};

int Filer::purge_range(inodeno_t ino,
		       const file_layout_t *layout,
		       const SnapContext& snapc,
		       uint64_t first_obj, uint64_t num_obj,
		       ceph::real_time mtime,
		       int flags,
		       Context *oncommit)
{
  ceph_assert(num_obj > 0);

  // single object?  easy!
  if (num_obj == 1) {
    object_t oid = file_object_t(ino, first_obj);
    object_locator_t oloc = OSDMap::file_to_object_locator(*layout);
    ldout(cct, 10) << "purge_range removing " << oid << dendl;
    objecter->remove(oid, oloc, snapc, mtime, flags, oncommit);
    return 0;
  }

  PurgeRange *pr = new PurgeRange(ino, *layout, snapc, first_obj,
				  num_obj, mtime, flags, oncommit);

  _do_purge_range(pr, 0, 0);
  return 0;
}

struct C_PurgeRange : public Context {
  Filer *filer;
  PurgeRange *pr;
  C_PurgeRange(Filer *f, PurgeRange *p) : filer(f), pr(p) {}
  void finish(int r) override {
    filer->_do_purge_range(pr, 1, r);
  }
};

void Filer::_do_purge_range(PurgeRange *pr, int fin, int err)
{
  PurgeRange::unique_lock prl(pr->lock);
  if (err && err != -ENOENT)
    pr->err = err;
  pr->uncommitted -= fin;
  ldout(cct, 10) << "_do_purge_range " << pr->ino << " objects " << pr->first
		 << "~" << pr->num << " uncommitted " << pr->uncommitted
		 << dendl;

  if (pr->num == 0 && pr->uncommitted == 0) {
    pr->oncommit->complete(pr->err);
    prl.unlock();
    delete pr;
    return;
  }

  std::vector<object_t> remove_oids;

  int max = cct->_conf->filer_max_purge_ops - pr->uncommitted;
  while (pr->num > 0 && max > 0) {
    remove_oids.push_back(file_object_t(pr->ino, pr->first));
    pr->uncommitted++;
    pr->first++;
    pr->num--;
    max--;
  }
  prl.unlock();

  // Issue objecter ops outside pr->lock to avoid lock dependency loop
  for (const auto& oid : remove_oids) {
    object_locator_t oloc = OSDMap::file_to_object_locator(pr->layout);
    objecter->remove(oid, oloc, pr->snapc, pr->mtime, pr->flags,
		     new C_OnFinisher(new C_PurgeRange(this, pr), finisher));
  }
}

// -----------------------
struct TruncRange {
  std::mutex lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  inodeno_t ino;
  file_layout_t layout;
  SnapContext snapc;
  ceph::real_time mtime;
  int flags;
  Context *oncommit;
  int uncommitted;
  uint64_t offset;
  uint64_t length;
  uint32_t truncate_seq;
  TruncRange(inodeno_t i, const file_layout_t& l, const SnapContext& sc,
	     ceph::real_time t, int fl, Context *fin,
	     uint64_t off, uint64_t len, uint32_t ts)
    : ino(i), layout(l), snapc(sc), mtime(t), flags(fl), oncommit(fin),
      uncommitted(0), offset(off), length(len), truncate_seq(ts) {}
};

void Filer::truncate(inodeno_t ino,
		     const file_layout_t *layout,
		     const SnapContext& snapc,
		     uint64_t offset,
		     uint64_t len,
		     __u32 truncate_seq,
		     ceph::real_time mtime,
		     int flags,
		     Context *oncommit)
{
  uint64_t period = layout->get_period();
  uint64_t num_objs = Striper::get_num_objects(*layout, len + (offset % period));
  if (num_objs == 1) {
    vector<ObjectExtent> extents;
    Striper::file_to_extents(cct, ino, layout, offset, len, 0, extents);
    osdc_opvec ops(1);
    ops[0].op.op = CEPH_OSD_OP_TRIMTRUNC;
    ops[0].op.extent.truncate_seq = truncate_seq;
    ops[0].op.extent.truncate_size = extents[0].offset;
    objecter->_modify(extents[0].oid, extents[0].oloc, ops, mtime, snapc,
		      flags, oncommit);
    return;
  }

  if (len > 0 && (offset + len) % period)
    len += period - ((offset + len) % period);

  TruncRange *tr = new TruncRange(ino, *layout, snapc, mtime, flags, oncommit,
				  offset, len, truncate_seq);
  _do_truncate_range(tr, 0);
}

struct C_TruncRange : public Context {
  Filer *filer;
  TruncRange *tr;
  C_TruncRange(Filer *f, TruncRange *t) : filer(f), tr(t) {}
  void finish(int r) override {
    filer->_do_truncate_range(tr, 1);
  }
};

void Filer::_do_truncate_range(TruncRange *tr, int fin)
{
  TruncRange::unique_lock trl(tr->lock);
  tr->uncommitted -= fin;
  ldout(cct, 10) << "_do_truncate_range " << tr->ino << " objects " << tr->offset
		 << "~" << tr->length << " uncommitted " << tr->uncommitted
		 << dendl;

  if (tr->length == 0 && tr->uncommitted == 0) {
    tr->oncommit->complete(0);
    trl.unlock();
    delete tr;
    return;
  }

  vector<ObjectExtent> extents;

  int max = cct->_conf->filer_max_truncate_ops - tr->uncommitted;
  if (max > 0 && tr->length > 0) {
    uint64_t len = tr->layout.get_period() * max;
    if (len > tr->length)
      len = tr->length;

    uint64_t offset = tr->offset + tr->length - len;
    Striper::file_to_extents(cct, tr->ino, &tr->layout, offset, len, 0, extents);
    tr->uncommitted += extents.size();
    tr->length -= len;
  }

  trl.unlock();

  // Issue objecter ops outside tr->lock to avoid lock dependency loop
  for (const auto& p : extents) {
    osdc_opvec ops(1);
    ops[0].op.op = CEPH_OSD_OP_TRIMTRUNC;
    ops[0].op.extent.truncate_size = p.offset;
    ops[0].op.extent.truncate_seq = tr->truncate_seq;
    objecter->_modify(p.oid, p.oloc, ops, tr->mtime, tr->snapc, tr->flags,
		      new C_OnFinisher(new C_TruncRange(this, tr), finisher));
  }
}
