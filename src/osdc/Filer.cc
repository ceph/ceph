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


#include "Filer.h"
#include "osd/OSDMap.h"
#include "Striper.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#include "msg/Messenger.h"

#include "include/Context.h"

#include "common/config.h"

#define dout_subsys ceph_subsys_filer
#undef dout_prefix
#define dout_prefix *_dout << objecter->messenger->get_myname() << ".filer "

class Filer::C_Probe : public Context {
public:
  Filer *filer;
  Probe *probe;
  object_t oid;
  uint64_t size;
  utime_t mtime;
  C_Probe(Filer *f, Probe *p, object_t o) : filer(f), probe(p), oid(o), size(0) {}
  void finish(int r) {
    if (r == -ENOENT) {
      r = 0;
      assert(size == 0);
    }

    // TODO: handle this error.
    if (r != 0)
      probe->err = r;

    filer->_probed(probe, oid, size, mtime);
  }  
};

int Filer::probe(inodeno_t ino,
		 ceph_file_layout *layout,
		 snapid_t snapid,
		 uint64_t start_from,
		 uint64_t *end,           // LB, when !fwd
		 utime_t *pmtime,
		 bool fwd,
		 int flags,
		 Context *onfinish) 
{
  ldout(cct, 10) << "probe " << (fwd ? "fwd ":"bwd ")
	   << hex << ino << dec
	   << " starting from " << start_from
	   << dendl;

  assert(snapid);  // (until there is a non-NOSNAP write)

  Probe *probe = new Probe(ino, *layout, snapid, start_from, end, pmtime, flags, fwd, onfinish);
  
  // period (bytes before we jump unto a new set of object(s))
  uint64_t period = (uint64_t)layout->fl_stripe_count * (uint64_t)layout->fl_object_size;
  
  // start with 1+ periods.
  probe->probing_len = period;
  if (probe->fwd) {
    if (start_from % period)
      probe->probing_len += period - (start_from % period);
  } else {
    assert(start_from > *end);
    if (start_from % period)
      probe->probing_len -= period - (start_from % period);
    probe->probing_off -= probe->probing_len;
  }
  
  _probe(probe);
  return 0;
}


void Filer::_probe(Probe *probe)
{
  ldout(cct, 10) << "_probe " << hex << probe->ino << dec 
	   << " " << probe->probing_off << "~" << probe->probing_len 
	   << dendl;
  
  // map range onto objects
  probe->known_size.clear();
  probe->probing.clear();
  Striper::file_to_extents(cct, probe->ino, &probe->layout,
			   probe->probing_off, probe->probing_len, probe->probing);
  
  for (vector<ObjectExtent>::iterator p = probe->probing.begin();
       p != probe->probing.end();
       ++p) {
    ldout(cct, 10) << "_probe  probing " << p->oid << dendl;
    C_Probe *c = new C_Probe(this, probe, p->oid);
    objecter->stat(p->oid, p->oloc, probe->snapid, &c->size, &c->mtime, 
		   probe->flags | CEPH_OSD_FLAG_RWORDERED, c);
    probe->ops.insert(p->oid);
  }
}

void Filer::_probed(Probe *probe, const object_t& oid, uint64_t size, utime_t mtime)
{
  ldout(cct, 10) << "_probed " << probe->ino << " object " << oid
	   << " has size " << size << " mtime " << mtime << dendl;

  probe->known_size[oid] = size;
  if (mtime > probe->max_mtime)
    probe->max_mtime = mtime;

  assert(probe->ops.count(oid));
  probe->ops.erase(oid);

  if (!probe->ops.empty()) 
    return;  // waiting for more!

  if (probe->err) { // we hit an error, propagate back up
    probe->onfinish->finish(probe->err);
    delete probe->onfinish;
    delete probe;
    return;
  }

  // analyze!
  uint64_t end = 0;

  if (!probe->fwd) {
    // reverse
    vector<ObjectExtent> r;
    for (vector<ObjectExtent>::reverse_iterator p = probe->probing.rbegin();
	 p != probe->probing.rend();
	 ++p)
      r.push_back(*p);
    probe->probing.swap(r);
  }

  for (vector<ObjectExtent>::iterator p = probe->probing.begin();
       p != probe->probing.end();
       ++p) {
    uint64_t shouldbe = p->length + p->offset;
    ldout(cct, 10) << "_probed  " << probe->ino << " object " << hex << p->oid << dec
	     << " should be " << shouldbe
	     << ", actual is " << probe->known_size[p->oid]
	     << dendl;

    if (!probe->found_size) {
      assert(probe->known_size[p->oid] <= shouldbe);

      if ((probe->fwd && probe->known_size[p->oid] == shouldbe) ||
	  (!probe->fwd && probe->known_size[p->oid] == 0 && probe->probing_off > 0))
	continue;  // keep going
      
      // aha, we found the end!
      // calc offset into buffer_extent to get distance from probe->from.
      uint64_t oleft = probe->known_size[p->oid] - p->offset;
      for (vector<pair<uint64_t, uint64_t> >::iterator i = p->buffer_extents.begin();
	   i != p->buffer_extents.end();
	   ++i) {
	if (oleft <= (uint64_t)i->second) {
	  end = probe->probing_off + i->first + oleft;
	  ldout(cct, 10) << "_probed  end is in buffer_extent " << i->first << "~" << i->second << " off " << oleft 
		   << ", from was " << probe->probing_off << ", end is " << end 
		   << dendl;
	  
	  probe->found_size = true;
	  ldout(cct, 10) << "_probed found size at " << end << dendl;
	  *probe->psize = end;
	  
	  if (!probe->pmtime)  // stop if we don't need mtime too
	    break;
	}
	oleft -= i->second;
      }
    }
    break;
  }

  if (!probe->found_size || (probe->probing_off && probe->pmtime)) {
    // keep probing!
    ldout(cct, 10) << "_probed probing further" << dendl;

    uint64_t period = (uint64_t)probe->layout.fl_stripe_count * (uint64_t)probe->layout.fl_object_size;
    if (probe->fwd) {
      probe->probing_off += probe->probing_len;
      assert(probe->probing_off % period == 0);
      probe->probing_len = period;
    } else {
      // previous period.
      assert(probe->probing_off % period == 0);
      probe->probing_len = period;
      probe->probing_off -= period;
    }
    _probe(probe);
    return;
  }

  if (probe->pmtime) {
    ldout(cct, 10) << "_probed found mtime " << probe->max_mtime << dendl;
    *probe->pmtime = probe->max_mtime;
  }

  // done!  finish and clean up.
  probe->onfinish->finish(probe->err);
  delete probe->onfinish;
  delete probe;
}


// -----------------------

struct PurgeRange {
  inodeno_t ino;
  ceph_file_layout layout;
  SnapContext snapc;
  uint64_t first, num;
  utime_t mtime;
  int flags;
  Context *oncommit;
  int uncommitted;
};

int Filer::purge_range(inodeno_t ino,
		       ceph_file_layout *layout,
		       const SnapContext& snapc,
		       uint64_t first_obj, uint64_t num_obj,
		       utime_t mtime,
		       int flags,
		       Context *oncommit) 
{
  assert(num_obj > 0);

  // single object?  easy!
  if (num_obj == 1) {
    object_t oid = file_object_t(ino, first_obj);
    object_locator_t oloc = objecter->osdmap->file_to_object_locator(*layout);
    objecter->remove(oid, oloc, snapc, mtime, flags, NULL, oncommit);
    return 0;
  }

  // lots!  let's do this in pieces.
  PurgeRange *pr = new PurgeRange;
  pr->ino = ino;
  pr->layout = *layout;
  pr->snapc = snapc;
  pr->first = first_obj;
  pr->num = num_obj;
  pr->mtime = mtime;
  pr->flags = flags;
  pr->oncommit = oncommit;
  pr->uncommitted = 0;

  _do_purge_range(pr, 0);
  return 0;
}

struct C_PurgeRange : public Context {
  Filer *filer;
  PurgeRange *pr;
  C_PurgeRange(Filer *f, PurgeRange *p) : filer(f), pr(p) {}
  void finish(int r) {
    filer->_do_purge_range(pr, 1);
  }
};

void Filer::_do_purge_range(PurgeRange *pr, int fin)
{
  pr->uncommitted -= fin;
  ldout(cct, 10) << "_do_purge_range " << pr->ino << " objects " << pr->first << "~" << pr->num
	   << " uncommitted " << pr->uncommitted << dendl;

  if (pr->num == 0 && pr->uncommitted == 0) {
    pr->oncommit->finish(0);
    delete pr->oncommit;
    delete pr;
    return;
  }

  int max = 10 - pr->uncommitted;
  while (pr->num > 0 && max > 0) {
    object_t oid = file_object_t(pr->ino, pr->first);
    object_locator_t oloc = objecter->osdmap->file_to_object_locator(pr->layout);
    objecter->remove(oid, oloc, pr->snapc, pr->mtime, pr->flags,
		     NULL, new C_PurgeRange(this, pr));
    pr->uncommitted++;
    pr->first++;
    pr->num--;
    max--;
  }
}


