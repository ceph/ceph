// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_SNAPREALM_H
#define CEPH_CLIENT_SNAPREALM_H

#include "include/types.h"
#include "common/snap_types.h"
#include "include/xlist.h"

struct Inode;

struct SnapRealm {
  inodeno_t ino;
  int nref;
  snapid_t created;
  snapid_t seq;
  
  inodeno_t parent;
  snapid_t parent_since;
  vector<snapid_t> prior_parent_snaps;  // snaps prior to parent_since
  vector<snapid_t> my_snaps;

  SnapRealm *pparent;
  set<SnapRealm*> pchildren;

private:
  SnapContext cached_snap_context;  // my_snaps + parent snaps + past_parent_snaps
  friend ostream& operator<<(ostream& out, const SnapRealm& r);

public:
  xlist<Inode*> inodes_with_caps;

  explicit SnapRealm(inodeno_t i) :
    ino(i), nref(0), created(0), seq(0),
    pparent(NULL) { }

  void build_snap_context();
  void invalidate_cache() {
    cached_snap_context.clear();
  }

  const SnapContext& get_snap_context() {
    if (cached_snap_context.seq == 0)
      build_snap_context();
    return cached_snap_context;
  }

  void dump(Formatter *f) const;
};

inline ostream& operator<<(ostream& out, const SnapRealm& r) {
  return out << "snaprealm(" << r.ino << " nref=" << r.nref << " c=" << r.created << " seq=" << r.seq
	     << " parent=" << r.parent
	     << " my_snaps=" << r.my_snaps
	     << " cached_snapc=" << r.cached_snap_context
	     << ")";
}

#endif
