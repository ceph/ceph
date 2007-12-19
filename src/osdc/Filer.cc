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


#include <assert.h>

#include "Filer.h"
#include "osd/OSDMap.h"

//#include "messages/MOSDRead.h"
//#include "messages/MOSDReadReply.h"
//#include "messages/MOSDWrite.h"
//#include "messages/MOSDWriteReply.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#include "msg/Messenger.h"

#include "include/Context.h"

#include "config.h"

#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_filer) *_dout << dbeginl << g_clock.now() << " " << objecter->messenger->get_myname() << ".filer "


class Filer::C_Probe : public Context {
public:
  Filer *filer;
  Probe *probe;
  object_t oid;
  off_t size;
  C_Probe(Filer *f, Probe *p, object_t o) : filer(f), probe(p), oid(o), size(0) {}
  void finish(int r) {
    filer->_probed(probe, oid, size);    
  }  
};

int Filer::probe_fwd(inode_t& inode,
		     off_t start_from,
		     off_t *end,
		     Context *onfinish) 
{
  dout(10) << "probe_fwd " << hex << inode.ino << dec << " starting from " << start_from << dendl;

  Probe *probe = new Probe(inode, start_from, end, onfinish);

  // period (bytes before we jump unto a new set of object(s))
  off_t period = ceph_file_layout_period(inode.layout);

  // start with 1+ periods.
  probe->probing_len = period;
  if (start_from % period) 
    probe->probing_len += period - (start_from % period);
  
  _probe(probe);
  return 0;
}

void Filer::_probe(Probe *probe)
{
  dout(10) << "_probe " << hex << probe->inode.ino << dec << " " << probe->from << "~" << probe->probing_len << dendl;
  
  // map range onto objects
  file_to_extents(probe->inode, probe->from, probe->probing_len, probe->probing);
  
  for (list<ObjectExtent>::iterator p = probe->probing.begin();
       p != probe->probing.end();
       p++) {
    dout(10) << "_probe  probing " << p->oid << dendl;
    C_Probe *c = new C_Probe(this, probe, p->oid);
    probe->ops[p->oid] = objecter->stat(p->oid, &c->size, p->layout, c);
  }
}

void Filer::_probed(Probe *probe, object_t oid, off_t size)
{
  dout(10) << "_probed " << probe->inode.ino << " object " << hex << oid << dec << " has size " << size << dendl;

  probe->known[oid] = size;
  assert(probe->ops.count(oid));
  probe->ops.erase(oid);

  if (!probe->ops.empty()) 
    return;  // waiting for more!

  // analyze!
  off_t end = 0;
  for (list<ObjectExtent>::iterator p = probe->probing.begin();
       p != probe->probing.end();
       p++) {
    off_t shouldbe = p->length+p->start;
    dout(10) << "_probed  " << probe->inode.ino << " object " << hex << p->oid << dec
	     << " should be " << shouldbe
	     << ", actual is " << probe->known[p->oid]
	     << dendl;

    if (probe->known[p->oid] < 0) { end = -1; break; } // error!

    assert(probe->known[p->oid] <= shouldbe);
    if (shouldbe == probe->known[p->oid]) continue;  // keep going
   
    // aha, we found the end!
    // calc offset into buffer_extent to get distance from probe->from.
    off_t oleft = probe->known[p->oid] - p->start;
    for (map<size_t,size_t>::iterator i = p->buffer_extents.begin();
	 i != p->buffer_extents.end();
	 i++) {
      if (oleft <= (off_t)i->second) {
	end = probe->from + i->first + oleft;
	dout(10) << "_probed  end is in buffer_extent " << i->first << "~" << i->second << " off " << oleft 
		 << ", from was " << probe->from << ", end is " << end 
		 << dendl;
	break;
      }
      oleft -= i->second;
    }
    break;
  }

  if (end == 0) {
    // keep probing!
    dout(10) << "_probed didn't find end, probing further" << dendl;
    off_t period = probe->inode.layout.fl_object_size * probe->inode.layout.fl_stripe_count;
    probe->from += probe->probing_len;
    probe->probing_len = period;
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
  probe->onfinish->finish(end > 0 ? 0:-1);
  delete probe->onfinish;
  delete probe;
}


void Filer::file_to_extents(inode_t inode,
                            off_t offset, size_t len,
                            list<ObjectExtent>& extents,
			    objectrev_t rev) 
{
  dout(10) << "file_to_extents " << offset << "~" << len 
           << " on " << hex << inode.ino << dec
           << dendl;

  /* we want only one extent per object!
   * this means that each extent we read may map into different bits of the 
   * final read buffer.. hence OSDExtent.buffer_extents
   */
  map< object_t, ObjectExtent > object_extents;
  
  assert(inode.layout.fl_object_size >= inode.layout.fl_stripe_unit);
  off_t stripes_per_object = inode.layout.fl_object_size / inode.layout.fl_stripe_unit;
  dout(20) << " stripes_per_object " << stripes_per_object << dendl;

  off_t cur = offset;
  off_t left = len;
  while (left > 0) {
    // layout into objects
    off_t blockno = cur / inode.layout.fl_stripe_unit;          // which block
    off_t stripeno = blockno / inode.layout.fl_stripe_count;    // which horizontal stripe        (Y)
    off_t stripepos = blockno % inode.layout.fl_stripe_count;   // which object in the object set (X)
    off_t objectsetno = stripeno / stripes_per_object;       // which object set
    off_t objectno = objectsetno * inode.layout.fl_stripe_count + stripepos;  // object id
    
    // find oid, extent
    ObjectExtent *ex = 0;
    object_t oid( inode.ino, objectno, rev );
    if (object_extents.count(oid)) 
      ex = &object_extents[oid];
    else {
      ex = &object_extents[oid];
      ex->oid = oid;
      ex->layout = objecter->osdmap->file_to_object_layout( oid, inode.layout );
    }
    
    // map range into object
    off_t block_start = (stripeno % stripes_per_object)*inode.layout.fl_stripe_unit;
    off_t block_off = cur % inode.layout.fl_stripe_unit;
    off_t max = inode.layout.fl_stripe_unit - block_off;
    
    off_t x_offset = block_start + block_off;
    off_t x_len;
    if (left > max)
      x_len = max;
    else
      x_len = left;
    
    if (ex->start + (off_t)ex->length == x_offset) {
      // add to extent
      ex->length += x_len;
    } else {
      // new extent
      assert(ex->length == 0);
      assert(ex->start == 0);
      ex->start = x_offset;
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
