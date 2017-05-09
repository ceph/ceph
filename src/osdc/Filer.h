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


#ifndef CEPH_FILER_H
#define CEPH_FILER_H

/*** Filer
 *
 * stripe file ranges onto objects.
 * build list<ObjectExtent> for the objecter or objectcacher.
 *
 * also, provide convenience methods that call objecter for you.
 *
 * "files" are identified by ino.
 */


#include <mutex>

#include "include/types.h"

#include "common/ceph_time.h"

#include "osd/OSDMap.h"
#include "Objecter.h"
#include "Striper.h"

class Context;
class Messenger;
class OSDMap;
class Finisher;


/**** Filer interface ***/

class Filer {
  CephContext *cct;
  Objecter   *objecter;
  Finisher   *finisher;

  // probes
  class Probe;
  class C_Probe;

  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;

  void _probe(Probe *p, unique_lock& pl);
  bool _probed(Probe *p, const object_t& oid, uint64_t size,
	       ceph::real_time mtime, unique_lock& pl);

 public:
  Filer(const Filer& other);
  const Filer operator=(const Filer& other);

  Filer(Objecter *o, Finisher *f) : cct(o->cct), objecter(o), finisher(f) {}
  ~Filer() {}

  bool is_active() {
    return objecter->is_active(); // || (oc && oc->is_active());
  }


  /*** async file interface.  scatter/gather as needed. ***/

  void read(inodeno_t ino,
	   file_layout_t *layout,
	   snapid_t snap,
	   uint64_t offset,
	   uint64_t len,
	   bufferlist *bl,   // ptr to data
	   int flags,
	   Context *onfinish,
	   int op_flags = 0) {
    assert(snap);  // (until there is a non-NOSNAP write)
    vector<ObjectExtent> extents;
    Striper::file_to_extents(cct, ino, layout, offset, len, 0, extents);
    objecter->sg_read(extents, snap, bl, flags, onfinish, op_flags);
  }

  void read_trunc(inodeno_t ino,
		 file_layout_t *layout,
		 snapid_t snap,
		 uint64_t offset,
		 uint64_t len,
		 bufferlist *bl, // ptr to data
		 int flags,
		 uint64_t truncate_size,
		 __u32 truncate_seq,
		 Context *onfinish,
		 int op_flags = 0) {
    assert(snap);  // (until there is a non-NOSNAP write)
    vector<ObjectExtent> extents;
    Striper::file_to_extents(cct, ino, layout, offset, len, truncate_size,
			     extents);
    objecter->sg_read_trunc(extents, snap, bl, flags,
			    truncate_size, truncate_seq, onfinish, op_flags);
  }

  void write(inodeno_t ino,
	    file_layout_t *layout,
	    const SnapContext& snapc,
	    uint64_t offset,
	    uint64_t len,
	    bufferlist& bl,
	    ceph::real_time mtime,
	    int flags,
	    Context *oncommit,
	    int op_flags = 0) {
    vector<ObjectExtent> extents;
    Striper::file_to_extents(cct, ino, layout, offset, len, 0, extents);
    objecter->sg_write(extents, snapc, bl, mtime, flags, oncommit, op_flags);
  }

  void write_trunc(inodeno_t ino,
		  file_layout_t *layout,
		  const SnapContext& snapc,
		  uint64_t offset,
		  uint64_t len,
		  bufferlist& bl,
		  ceph::real_time mtime,
		  int flags,
		  uint64_t truncate_size,
		  __u32 truncate_seq,
		  Context *oncommit,
		  int op_flags = 0) {
    vector<ObjectExtent> extents;
    Striper::file_to_extents(cct, ino, layout, offset, len, truncate_size,
			     extents);
    objecter->sg_write_trunc(extents, snapc, bl, mtime, flags,
		       truncate_size, truncate_seq, oncommit, op_flags);
  }

  void truncate(inodeno_t ino,
	       file_layout_t *layout,
	       const SnapContext& snapc,
	       uint64_t offset,
	       uint64_t len,
	       __u32 truncate_seq,
	       ceph::real_time mtime,
	       int flags,
	       Context *oncommit) {
    vector<ObjectExtent> extents;
    Striper::file_to_extents(cct, ino, layout, offset, len, 0, extents);
    if (extents.size() == 1) {
      vector<OSDOp> ops(1);
      ops[0].op.op = CEPH_OSD_OP_TRIMTRUNC;
      ops[0].op.extent.truncate_seq = truncate_seq;
      ops[0].op.extent.truncate_size = extents[0].offset;
      objecter->_modify(extents[0].oid, extents[0].oloc, ops, mtime, snapc,
			flags, oncommit);
    } else {
      C_GatherBuilder gcom(cct, oncommit);
      for (vector<ObjectExtent>::iterator p = extents.begin();
	   p != extents.end();
	   ++p) {
	vector<OSDOp> ops(1);
	ops[0].op.op = CEPH_OSD_OP_TRIMTRUNC;
	ops[0].op.extent.truncate_size = p->offset;
	ops[0].op.extent.truncate_seq = truncate_seq;
	objecter->_modify(p->oid, p->oloc, ops, mtime, snapc, flags,
			  oncommit ? gcom.new_sub():0);
      }
      gcom.activate();
    }
  }

  void zero(inodeno_t ino,
	   const file_layout_t *layout,
	   const SnapContext& snapc,
	   uint64_t offset,
	   uint64_t len,
	   ceph::real_time mtime,
	   int flags,
	   bool keep_first,
	   Context *oncommit) {
    vector<ObjectExtent> extents;
    Striper::file_to_extents(cct, ino, layout, offset, len, 0, extents);
    if (extents.size() == 1) {
      if (extents[0].offset == 0 && extents[0].length == layout->object_size
	  && (!keep_first || extents[0].objectno != 0))
	objecter->remove(extents[0].oid, extents[0].oloc,
			 snapc, mtime, flags, oncommit);
      else
	objecter->zero(extents[0].oid, extents[0].oloc, extents[0].offset,
		       extents[0].length, snapc, mtime, flags, oncommit);
    } else {
      C_GatherBuilder gcom(cct, oncommit);
      for (vector<ObjectExtent>::iterator p = extents.begin();
	   p != extents.end();
	   ++p) {
	if (p->offset == 0 && p->length == layout->object_size &&
	    (!keep_first || p->objectno != 0))
	  objecter->remove(p->oid, p->oloc,
			   snapc, mtime, flags,
			   oncommit ? gcom.new_sub():0);
	else
	  objecter->zero(p->oid, p->oloc, p->offset, p->length,
			 snapc, mtime, flags,
			 oncommit ? gcom.new_sub():0);
      }
      gcom.activate();
    }
  }

  void zero(inodeno_t ino,
	   file_layout_t *layout,
	   const SnapContext& snapc,
	   uint64_t offset,
	   uint64_t len,
	   ceph::real_time mtime,
	   int flags,
	   Context *oncommit) {
    zero(ino, layout,
         snapc, offset,
         len, mtime,
         flags, false,
         oncommit);
  }
  // purge range of ino.### objects
  int purge_range(inodeno_t ino,
		  const file_layout_t *layout,
		  const SnapContext& snapc,
		  uint64_t first_obj, uint64_t num_obj,
		  ceph::real_time mtime,
		  int flags, Context *oncommit);
  void _do_purge_range(struct PurgeRange *pr, int fin);

  /*
   * probe
   *  specify direction,
   *  and whether we stop when we find data, or hole.
   */
  int probe(inodeno_t ino,
	    file_layout_t *layout,
	    snapid_t snapid,
	    uint64_t start_from,
	    uint64_t *end,
	    ceph::real_time *mtime,
	    bool fwd,
	    int flags,
	    Context *onfinish);

  int probe(inodeno_t ino,
	    file_layout_t *layout,
	    snapid_t snapid,
	    uint64_t start_from,
	    uint64_t *end,
	    bool fwd,
	    int flags,
	    Context *onfinish) {
    return probe(ino, layout, snapid, start_from, end,
		 (ceph::real_time* )0, fwd, flags, onfinish);
  }

  int probe(inodeno_t ino,
	    file_layout_t *layout,
	    snapid_t snapid,
	    uint64_t start_from,
	    uint64_t *end,
	    utime_t *mtime,
	    bool fwd,
	    int flags,
	    Context *onfinish);

private:
  int probe_impl(Probe* probe, file_layout_t *layout,
		 uint64_t start_from, uint64_t *end);
};

#endif // !CEPH_FILER_H
