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

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#include "msg/Messenger.h"

#include "include/Context.h"

#include "config.h"

#define DOUT_SUBSYS filer
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << objecter->messenger->get_myname() << ".filer "


class Filer::C_Probe : public Context {
public:
  Filer *filer;
  Probe *probe;
  object_t oid;
  __u64 size;
  C_Probe(Filer *f, Probe *p, object_t o) : filer(f), probe(p), oid(o), size(0) {}
  void finish(int r) {
    filer->_probed(probe, oid, size);    
  }  
};

int Filer::probe(inodeno_t ino,
		 ceph_file_layout *layout,
		 snapid_t snapid,
		 __u64 start_from,
		 __u64 *end,           // LB, when !fwd
		 bool fwd,
		 int flags,
		 Context *onfinish) 
{
  dout(10) << "probe " << (fwd ? "fwd ":"bwd ")
	   << hex << ino << dec
	   << " starting from " << start_from
	   << dendl;

  assert(snapid);  // (until there is a non-NOSNAP write)

  Probe *probe = new Probe(ino, *layout, snapid, start_from, end, flags, fwd, onfinish);
  
  // period (bytes before we jump unto a new set of object(s))
  __u64 period = ceph_file_layout_period(*layout);
  
  // start with 1+ periods.
  probe->probing_len = period;
  if (probe->fwd) {
    if (start_from % period)
      probe->probing_len += period - (start_from % period);
  } else {
    assert(start_from > *end);
    if (start_from % period)
      probe->probing_len -= period - (start_from % period);
    probe->from -= probe->probing_len;
  }
  
  _probe(probe);
  return 0;
}


void Filer::_probe(Probe *probe)
{
  dout(10) << "_probe " << hex << probe->ino << dec 
	   << " " << probe->from << "~" << probe->probing_len 
	   << dendl;
  
  // map range onto objects
  file_to_extents(probe->ino, &probe->layout, probe->from, probe->probing_len, probe->probing);
  
  for (vector<ObjectExtent>::iterator p = probe->probing.begin();
       p != probe->probing.end();
       p++) {
    dout(10) << "_probe  probing " << p->oid << dendl;
    C_Probe *c = new C_Probe(this, probe, p->oid);
    probe->ops[p->oid] = objecter->stat(p->oid, p->layout, probe->snapid, &c->size, probe->flags, c);
  }
}

void Filer::_probed(Probe *probe, object_t oid, __u64 size)
{
  dout(10) << "_probed " << probe->ino << " object " << hex << oid << dec << " has size " << size << dendl;

  probe->known[oid] = size;
  assert(probe->ops.count(oid));
  probe->ops.erase(oid);

  if (!probe->ops.empty()) 
    return;  // waiting for more!

  // analyze!
  bool found = false;
  __u64 end = 0;

  if (!probe->fwd) {
    // reverse
    vector<ObjectExtent> r;
    for (vector<ObjectExtent>::reverse_iterator p = probe->probing.rbegin();
	 p != probe->probing.rend();
	 p++)
      r.push_back(*p);
    probe->probing.swap(r);
  }

  for (vector<ObjectExtent>::iterator p = probe->probing.begin();
       p != probe->probing.end();
       p++) {
    __u64 shouldbe = p->length + p->offset;
    dout(10) << "_probed  " << probe->ino << " object " << hex << p->oid << dec
	     << " should be " << shouldbe
	     << ", actual is " << probe->known[p->oid]
	     << dendl;

    if (probe->known[p->oid] < 0) { end = -1; break; } // error!

    assert(probe->known[p->oid] <= shouldbe);
    if (shouldbe == probe->known[p->oid] && probe->fwd)
      continue;  // keep going
   
    // aha, we found the end!
    // calc offset into buffer_extent to get distance from probe->from.
    __u64 oleft = probe->known[p->oid] - p->offset;
    for (map<__u32,__u32>::iterator i = p->buffer_extents.begin();
	 i != p->buffer_extents.end();
	 i++) {
      if (oleft <= (__u64)i->second) {
	end = probe->from + i->first + oleft;
	found = true;
	dout(10) << "_probed  end is in buffer_extent " << i->first << "~" << i->second << " off " << oleft 
		 << ", from was " << probe->from << ", end is " << end 
		 << dendl;
	break;
      }
      oleft -= i->second;
    }
    break;
  }

  if (!found) {
    // keep probing!
    dout(10) << "_probed didn't find end, probing further" << dendl;
    __u64 period = ceph_file_layout_period(probe->layout);
    if (probe->fwd) {
      probe->from += probe->probing_len;
      assert(probe->from % period == 0);
      probe->probing_len = period;
    } else {
      // previous period.
      assert(probe->from % period == 0);
      probe->probing_len -= period;
      probe->from -= period;
    }
    _probe(probe);
    return;
  }

  if (end < 0) {
    dout(10) << "_probed encountered an error while probing" << dendl;
    *probe->end = -1;
  } else {
    // hooray!
    dout(10) << "_probed found end at " << end << dendl;
    *probe->end = end;
  }

  // done!  finish and clean up.
  probe->onfinish->finish(end >= 0 ? 0:-1);
  delete probe->onfinish;
  delete probe;
}


void Filer::file_to_extents(inodeno_t ino, ceph_file_layout *layout,
                            __u64 offset, size_t len,
                            vector<ObjectExtent>& extents)
{
  dout(10) << "file_to_extents " << offset << "~" << len 
           << " on " << hex << ino << dec
           << dendl;
  assert(len > 0);

  /* we want only one extent per object!
   * this means that each extent we read may map into different bits of the 
   * final read buffer.. hence OSDExtent.buffer_extents
   */
  map< object_t, ObjectExtent > object_extents;
  
  __u32 object_size = ceph_file_layout_object_size(*layout);
  __u32 su = ceph_file_layout_su(*layout);
  __u32 stripe_count = ceph_file_layout_stripe_count(*layout);
  assert(object_size >= su);
  __u64 stripes_per_object = object_size / su;
  dout(20) << " stripes_per_object " << stripes_per_object << dendl;

  __u64 cur = offset;
  __u64 left = len;
  while (left > 0) {
    // layout into objects
    __u64 blockno = cur / su;          // which block
    __u64 stripeno = blockno / stripe_count;    // which horizontal stripe        (Y)
    __u64 stripepos = blockno % stripe_count;   // which object in the object set (X)
    __u64 objectsetno = stripeno / stripes_per_object;       // which object set
    __u64 objectno = objectsetno * stripe_count + stripepos;  // object id
    
    // find oid, extent
    ObjectExtent *ex = 0;
    object_t oid( ino, objectno );
    if (object_extents.count(oid)) 
      ex = &object_extents[oid];
    else {
      ex = &object_extents[oid];
      ex->oid = oid;
      ex->layout = objecter->osdmap->file_to_object_layout( oid, *layout );
    }
    
    // map range into object
    __u64 block_start = (stripeno % stripes_per_object)*su;
    __u64 block_off = cur % su;
    __u64 max = su - block_off;
    
    __u64 x_offset = block_start + block_off;
    __u64 x_len;
    if (left > max)
      x_len = max;
    else
      x_len = left;
    
    if (ex->offset + (__u64)ex->length == x_offset) {
      // add to extent
      ex->length += x_len;
    } else {
      // new extent
      assert(ex->length == 0);
      assert(ex->offset == 0);
      ex->offset = x_offset;
      ex->length = x_len;
    }
    ex->buffer_extents[cur-offset] = x_len;
        
    dout(15) << "file_to_extents  " << *ex << " in " << ex->layout << dendl;
    //dout(0) << "map: ino " << ino << " oid " << ex.oid << " osd " << ex.osd << " offset " << ex.offset << " len " << ex.len << " ... left " << left << dendl;
    
    left -= x_len;
    cur += x_len;
  }
  
  // make final list
  for (map<object_t, ObjectExtent>::iterator it = object_extents.begin();
       it != object_extents.end();
       it++) {
    extents.push_back(it->second);
  }
}
