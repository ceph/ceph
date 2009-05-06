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


#ifndef __FILER_H
#define __FILER_H

/*** Filer
 *
 * stripe file ranges onto objects.
 * build list<ObjectExtent> for the objecter or objectcacher.
 *
 * also, provide convenience methods that call objecter for you.
 *
 * "files" are identified by ino. 
 */

#include "include/types.h"

#include "osd/OSDMap.h"
#include "Objecter.h"

class Context;
class Messenger;
class OSDMap;


/**** Filer interface ***/

class Filer {
  Objecter   *objecter;
  
  // probes
  struct Probe {
    inodeno_t ino;
    ceph_file_layout layout;
    snapid_t snapid;
    __u64 from;        // for !fwd, this is start of extent we are probing, thus possibly < our endpoint.
    __u64 *end;
    int flags;

    bool fwd;

    Context *onfinish;
    
    vector<ObjectExtent> probing;
    __u64 probing_len;
    
    map<object_t, __u64> known;
    map<object_t, tid_t> ops;

    Probe(inodeno_t i, ceph_file_layout &l, snapid_t sn,
	  __u64 f, __u64 *e, int fl, bool fw, Context *c) : 
      ino(i), layout(l), snapid(sn),
      from(f), end(e), flags(fl), fwd(fw), onfinish(c), probing_len(0) {}
  };
  
  class C_Probe;

  void _probe(Probe *p);
  void _probed(Probe *p, object_t oid, __u64 size);

 public:
  Filer(Objecter *o) : objecter(o) {}
  ~Filer() {}

  bool is_active() {
    return objecter->is_active(); // || (oc && oc->is_active());
  }


  /***** mapping *****/

  /*
   * map (ino, layout, offset, len) to a (list of) OSDExtents (byte
   * ranges in objects on (primary) osds)
   */
  void file_to_extents(inodeno_t ino, ceph_file_layout *layout,
		       __u64 offset, size_t len,
		       vector<ObjectExtent>& extents);


  

  /*** async file interface.  scatter/gather as needed. ***/

  int read(inodeno_t ino,
	   ceph_file_layout *layout,
	   snapid_t snap,
           __u64 offset, 
           size_t len, 
           bufferlist *bl,   // ptr to data
	   int flags,
           Context *onfinish) {
    assert(snap);  // (until there is a non-NOSNAP write)
    vector<ObjectExtent> extents;
    file_to_extents(ino, layout, offset, len, extents);
    objecter->sg_read(extents, snap, bl, flags, onfinish);
    return 0;
  }


  int write(inodeno_t ino,
	    ceph_file_layout *layout,
	    const SnapContext& snapc,
	    __u64 offset, 
            size_t len, 
            bufferlist& bl,
	    utime_t mtime,
            int flags, 
            Context *onack,
            Context *oncommit) {
    vector<ObjectExtent> extents;
    file_to_extents(ino, layout, offset, len, extents);
    objecter->sg_write(extents, snapc, bl, mtime, flags, onack, oncommit);
    return 0;
  }

  int truncate(inodeno_t ino,
	       ceph_file_layout *layout,
	       const SnapContext& snapc,
	       __u64 offset,
	       size_t len,
	       __u32 truncate_seq,
	       utime_t mtime,
	       int flags,
	       Context *onack,
	       Context *oncommit) {
    bufferlist bl;
    vector<ObjectExtent> extents;
    file_to_extents(ino, layout, offset, len, extents);
    if (extents.size() == 1) {
      vector<ceph_osd_op> ops(1);
      memset(&ops[0], 0, sizeof(ops[0]));
      ops[0].op = CEPH_OSD_OP_TRIMTRUNC;
      ops[0].truncate_seq = truncate_seq;
      ops[0].truncate_size = extents[0].offset;
      objecter->modify(extents[0].oid, extents[0].layout, ops, snapc, bl, mtime, flags, onack, oncommit);
    } else {
      C_Gather *gack = 0, *gcom = 0;
      if (onack)
	gack = new C_Gather(onack);
      if (oncommit)
	gcom = new C_Gather(oncommit);
      for (vector<ObjectExtent>::iterator p = extents.begin(); p != extents.end(); p++) {
	vector<ceph_osd_op> ops(1);
	memset(&ops[0], 0, sizeof(ops[0]));
	ops[0].op = CEPH_OSD_OP_TRIMTRUNC;
	ops[0].truncate_size = p->offset;
	ops[0].truncate_seq = truncate_seq;
	objecter->modify(extents[0].oid, p->layout, ops, snapc, bl, mtime, flags,
			 gack ? gack->new_sub():0,
			 gcom ? gcom->new_sub():0);
      }
    }
    return 0;
  }

  int zero(inodeno_t ino,
	   ceph_file_layout *layout,
	   const SnapContext& snapc,
	   __u64 offset,
           size_t len,
	   utime_t mtime,
	   int flags,
           Context *onack,
           Context *oncommit) {
    vector<ObjectExtent> extents;
    file_to_extents(ino, layout, offset, len, extents);
    if (extents.size() == 1) {
      objecter->zero(extents[0].oid, extents[0].layout, extents[0].offset, extents[0].length, 
		     snapc, mtime, flags, onack, oncommit);
    } else {
      C_Gather *gack = 0, *gcom = 0;
      if (onack)
	gack = new C_Gather(onack);
      if (oncommit)
	gcom = new C_Gather(oncommit);
      for (vector<ObjectExtent>::iterator p = extents.begin(); p != extents.end(); p++) {
	objecter->zero(p->oid, p->layout, p->offset, p->length, 
		       snapc, mtime, flags,
		       gack ? gack->new_sub():0,
		       gcom ? gcom->new_sub():0);
      }
    }
    return 0;
  }

  int remove(inodeno_t ino,
	     ceph_file_layout *layout,
	     const SnapContext& snapc,
	     __u64 offset,
	     size_t len,
	     utime_t mtime,
	     int flags,
	     Context *onack,
	     Context *oncommit) {
    vector<ObjectExtent> extents;
    file_to_extents(ino, layout, offset, len, extents);
    if (extents.size() == 1) {
      objecter->remove(extents[0].oid, extents[0].layout,
		       snapc, mtime, flags, onack, oncommit);
    } else {
      C_Gather *gack = 0, *gcom = 0;
      if (onack)
	gack = new C_Gather(onack);
      if (oncommit)
	gcom = new C_Gather(oncommit);
      for (vector<ObjectExtent>::iterator p = extents.begin(); p != extents.end(); p++)
	objecter->remove(p->oid, p->layout,
			 snapc, mtime, flags,
			 gack ? gack->new_sub():0,
			 gcom ? gcom->new_sub():0);
    }
    return 0;
  }

  /*
   * probe 
   *  specify direction,
   *  and whether we stop when we find data, or hole.
   */
  int probe(inodeno_t ino,
	    ceph_file_layout *layout,
	    snapid_t snapid,
	    __u64 start_from,
	    __u64 *end,
	    bool fwd,
	    int flags,
	    Context *onfinish);

};



#endif
