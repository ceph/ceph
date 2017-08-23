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
  struct Probe {
    std::mutex lock;
    typedef std::lock_guard<std::mutex> lock_guard;
    typedef std::unique_lock<std::mutex> unique_lock;
    inodeno_t ino;
    file_layout_t layout;
    snapid_t snapid;

    uint64_t *psize;
    ceph::real_time *pmtime;
    utime_t *pumtime;

    int flags;

    bool fwd;

    Context *onfinish;

    vector<ObjectExtent> probing;
    uint64_t probing_off, probing_len;

    map<object_t, uint64_t> known_size;
    ceph::real_time max_mtime;

    set<object_t> ops;

    int err;
    bool found_size;

    Probe(inodeno_t i, file_layout_t &l, snapid_t sn,
	  uint64_t f, uint64_t *e, ceph::real_time *m, int fl, bool fw,
	  Context *c) :
      ino(i), layout(l), snapid(sn),
      psize(e), pmtime(m), pumtime(nullptr), flags(fl), fwd(fw), onfinish(c),
      probing_off(f), probing_len(0),
      err(0), found_size(false) {}

    Probe(inodeno_t i, file_layout_t &l, snapid_t sn,
	  uint64_t f, uint64_t *e, utime_t *m, int fl, bool fw,
	  Context *c) :
      ino(i), layout(l), snapid(sn),
      psize(e), pmtime(nullptr), pumtime(m), flags(fl), fwd(fw),
      onfinish(c), probing_off(f), probing_len(0),
      err(0), found_size(false) {}
  };

  class C_Probe;

  void _probe(Probe *p, Probe::unique_lock& pl);
  bool _probed(Probe *p, const object_t& oid, uint64_t size,
	       ceph::real_time mtime, Probe::unique_lock& pl);

 public:
  Filer(const Filer& other);
  const Filer operator=(const Filer& other);

  Filer(Objecter *o, Finisher *f) : cct(o->cct), objecter(o), finisher(f) {}
  ~Filer() {}

  bool is_active() {
    return objecter->is_active(); // || (oc && oc->is_active());
  }


  /*** async file interface.  scatter/gather as needed. ***/

  int read(inodeno_t ino,
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
    return 0;
  }

  int read_trunc(inodeno_t ino,
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
    return 0;
  }

  int write(inodeno_t ino,
	    file_layout_t *layout,
	    const SnapContext& snapc,
	    uint64_t offset,
	    uint64_t len,
	    bufferlist& bl,
	    ceph::real_time mtime,
	    int flags,
	    Context *onack,
	    Context *oncommit,
	    int op_flags = 0) {
    vector<ObjectExtent> extents;
    Striper::file_to_extents(cct, ino, layout, offset, len, 0, extents);
    objecter->sg_write(extents, snapc, bl, mtime, flags, onack, oncommit,
		       op_flags);
    return 0;
  }

  int write_trunc(inodeno_t ino,
		  file_layout_t *layout,
		  const SnapContext& snapc,
		  uint64_t offset,
		  uint64_t len,
		  bufferlist& bl,
		  ceph::real_time mtime,
		  int flags,
		  uint64_t truncate_size,
		  __u32 truncate_seq,
		  Context *onack,
		  Context *oncommit,
		  int op_flags = 0) {
    vector<ObjectExtent> extents;
    Striper::file_to_extents(cct, ino, layout, offset, len, truncate_size,
			     extents);
    objecter->sg_write_trunc(extents, snapc, bl, mtime, flags,
		       truncate_size, truncate_seq, onack, oncommit, op_flags);
    return 0;
  }

  void truncate(inodeno_t ino,
	       file_layout_t *layout,
	       const SnapContext& snapc,
	       uint64_t offset,
	       uint64_t len,
	       __u32 truncate_seq,
	       ceph::real_time mtime,
	       int flags,
	       Context *oncommit);
  void _do_truncate_range(struct TruncRange *pr, int fin);

  int zero(inodeno_t ino,
	   file_layout_t *layout,
	   const SnapContext& snapc,
	   uint64_t offset,
	   uint64_t len,
	   ceph::real_time mtime,
	   int flags,
	   bool keep_first,
	   Context *onack,
	   Context *oncommit) {
    vector<ObjectExtent> extents;
    Striper::file_to_extents(cct, ino, layout, offset, len, 0, extents);
    if (extents.size() == 1) {
      if (extents[0].offset == 0 && extents[0].length == layout->object_size
	  && (!keep_first || extents[0].objectno != 0))
	objecter->remove(extents[0].oid, extents[0].oloc,
			 snapc, mtime, flags, onack, oncommit);
      else
	objecter->zero(extents[0].oid, extents[0].oloc, extents[0].offset,
		       extents[0].length, snapc, mtime, flags, onack,
		       oncommit);
    } else {
      C_GatherBuilder gack(cct, onack);
      C_GatherBuilder gcom(cct, oncommit);
      for (vector<ObjectExtent>::iterator p = extents.begin();
	   p != extents.end();
	   ++p) {
	if (p->offset == 0 && p->length == layout->object_size &&
	    (!keep_first || p->objectno != 0))
	  objecter->remove(p->oid, p->oloc,
			   snapc, mtime, flags,
			   onack ? gack.new_sub():0,
			   oncommit ? gcom.new_sub():0);
	else
	  objecter->zero(p->oid, p->oloc, p->offset, p->length,
			 snapc, mtime, flags,
			 onack ? gack.new_sub():0,
			 oncommit ? gcom.new_sub():0);
      }
      gack.activate();
      gcom.activate();
    }
    return 0;
  }

  int zero(inodeno_t ino,
	   file_layout_t *layout,
	   const SnapContext& snapc,
	   uint64_t offset,
	   uint64_t len,
	   ceph::real_time mtime,
	   int flags,
	   Context *onack,
	   Context *oncommit) {

    return zero(ino, layout,
		snapc, offset,
		len, mtime,
		flags, false,
		onack, oncommit);
  }
  // purge range of ino.### objects
  int purge_range(inodeno_t ino,
		  file_layout_t *layout,
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
