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

    __u64 *psize;
    utime_t *pmtime;

    int flags;

    bool fwd;

    Context *onfinish;
    
    vector<ObjectExtent> probing;
    __u64 probing_off, probing_len;
    
    map<object_t, __u64> known_size;
    utime_t max_mtime;

    map<object_t, tid_t> ops;

    int err;
    bool found_size;

    Probe(inodeno_t i, ceph_file_layout &l, snapid_t sn,
	  __u64 f, __u64 *e, utime_t *m, int fl, bool fw, Context *c) : 
      ino(i), layout(l), snapid(sn),
      psize(e), pmtime(m), flags(fl), fwd(fw), onfinish(c),
      probing_off(f), probing_len(0),
      err(0), found_size(false) {}
  };
  
  class C_Probe;

  void _probe(Probe *p);
  void _probed(Probe *p, const object_t& oid, __u64 size, utime_t mtime);

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
		       __u64 offset, __u64 len,
		       vector<ObjectExtent>& extents);


  

  /*** async file interface.  scatter/gather as needed. ***/

  int read(inodeno_t ino,
	   ceph_file_layout *layout,
	   snapid_t snap,
           __u64 offset, 
           __u64 len, 
           bufferlist *bl,   // ptr to data
	   int flags,
           Context *onfinish) {
    assert(snap);  // (until there is a non-NOSNAP write)
    vector<ObjectExtent> extents;
    file_to_extents(ino, layout, offset, len, extents);
    objecter->sg_read(extents, snap, bl, flags, onfinish);
    return 0;
  }

  int read_trunc(inodeno_t ino,
	   ceph_file_layout *layout,
	   snapid_t snap,
           __u64 offset, 
           __u64 len, 
           bufferlist *bl,   // ptr to data
	   int flags,
	   __u64 truncate_size,
	   __u32 truncate_seq,
           Context *onfinish) {
    assert(snap);  // (until there is a non-NOSNAP write)
    vector<ObjectExtent> extents;
    file_to_extents(ino, layout, offset, len, extents);
    objecter->sg_read_trunc(extents, snap, bl, flags,
			    truncate_size, truncate_seq, onfinish);
    return 0;
  }

  int write(inodeno_t ino,
	    ceph_file_layout *layout,
	    const SnapContext& snapc,
	    __u64 offset, 
            __u64 len, 
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

  int write_trunc(inodeno_t ino,
	    ceph_file_layout *layout,
	    const SnapContext& snapc,
	    __u64 offset, 
            __u64 len, 
            bufferlist& bl,
	    utime_t mtime,
            int flags, 
	   __u64 truncate_size,
	   __u32 truncate_seq,
            Context *onack,
            Context *oncommit) {
    vector<ObjectExtent> extents;
    file_to_extents(ino, layout, offset, len, extents);
    objecter->sg_write_trunc(extents, snapc, bl, mtime, flags,
		       truncate_size, truncate_seq, onack, oncommit);
    return 0;
  }

  int truncate(inodeno_t ino,
	       ceph_file_layout *layout,
	       const SnapContext& snapc,
	       __u64 offset,
	       __u64 len,
	       __u32 truncate_seq,
	       utime_t mtime,
	       int flags,
	       Context *onack,
	       Context *oncommit) {
    vector<ObjectExtent> extents;
    file_to_extents(ino, layout, offset, len, extents);
    if (extents.size() == 1) {
      vector<OSDOp> ops(1);
      ops[0].op.op = CEPH_OSD_OP_TRIMTRUNC;
      ops[0].op.extent.truncate_seq = truncate_seq;
      ops[0].op.extent.truncate_size = extents[0].offset;
      objecter->_modify(extents[0].oid, extents[0].layout, ops, mtime, snapc, flags, onack, oncommit);
    } else {
      C_Gather *gack = 0, *gcom = 0;
      if (onack)
	gack = new C_Gather(onack);
      if (oncommit)
	gcom = new C_Gather(oncommit);
      for (vector<ObjectExtent>::iterator p = extents.begin(); p != extents.end(); p++) {
	vector<OSDOp> ops(1);
	ops[0].op.op = CEPH_OSD_OP_TRIMTRUNC;
	ops[0].op.extent.truncate_size = p->offset;
	ops[0].op.extent.truncate_seq = truncate_seq;
	objecter->_modify(extents[0].oid, p->layout, ops, mtime, snapc, flags,
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
           __u64 len,
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

  // purge range of ino.### objects
  int purge_range(inodeno_t ino,
		  ceph_file_layout *layout,
		  const SnapContext& snapc,
		  __u64 first_obj, __u64 num_obj,
		  utime_t mtime,
		  int flags,
		  Context *oncommit);
  void _do_purge_range(class PurgeRange *pr, int fin);

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
	    utime_t *mtime,
	    bool fwd,
	    int flags,
	    Context *onfinish);
};



#endif
