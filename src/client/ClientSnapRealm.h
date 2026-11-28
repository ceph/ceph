// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_CLIENT_SNAPREALM_H
#define CEPH_CLIENT_SNAPREALM_H

#include <atomic>
#include <iostream>
#include <memory>
#include <set>
#include <vector>

#include "include/container_ios.h"
#include "include/types.h"
#include "common/snap_types.h"
#include "include/xlist.h"

struct Inode;

// A shared_ptr that threads load and store atomically: std::atomic of it
// where the library has that (libstdc++), else the atomic_load() and
// atomic_store() of a shared_ptr, which C++20 deprecates for it (libc++,
// as in the Windows build)
template <typename T>
class AtomicSharedPtr {
#ifdef __cpp_lib_atomic_shared_ptr
  std::atomic<std::shared_ptr<T>> p;
public:
  std::shared_ptr<T> load() const { return p.load(); }
  void store(std::shared_ptr<T> v) { p.store(std::move(v)); }
#else
  std::shared_ptr<T> p;
public:
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  std::shared_ptr<T> load() const { return std::atomic_load(&p); }
  void store(std::shared_ptr<T> v) { std::atomic_store(&p, std::move(v)); }
#pragma GCC diagnostic pop
#endif
};

struct SnapRealm {
  inodeno_t ino;
  int nref;
  snapid_t created;
  snapid_t seq;
  
  inodeno_t parent;
  snapid_t parent_since;
  std::vector<snapid_t> prior_parent_snaps;  // snaps prior to parent_since
  std::vector<snapid_t> my_snaps;

  SnapRealm *pparent;
  std::set<SnapRealm*> pchildren;
  utime_t last_modified;
  uint64_t change_attr;
  bool is_snapdir_visible = true;

private:
  SnapContext cached_snap_context;  // my_snaps + parent snaps + past_parent_snaps
  // a copy of cached_snap_context for readers without client_lock (the
  // write fast path), or null when the cache is invalid
  AtomicSharedPtr<const SnapContext> published_snapc;
  friend std::ostream& operator<<(std::ostream& out, const SnapRealm& r);

public:
  xlist<Inode*> inodes_with_caps;

  explicit SnapRealm(inodeno_t i) :
    ino(i), nref(0), created(0), seq(0),
    pparent(NULL), last_modified(utime_t()), change_attr(0) { }

  void build_snap_context();
  void invalidate_cache() {
    cached_snap_context.clear();
    published_snapc.store(nullptr);
  }

  // the snap context, if it is cached; may be called without client_lock
  std::shared_ptr<const SnapContext> get_published_snap_context() const {
    return published_snapc.load();
  }

  const SnapContext& get_snap_context() {
    if (cached_snap_context.seq == 0)
      build_snap_context();
    return cached_snap_context;
  }

  void dump(Formatter *f) const;
};

inline std::ostream& operator<<(std::ostream& out, const SnapRealm& r) {
  return out << "snaprealm(" << r.ino << " nref=" << r.nref << " c=" << r.created << " seq=" << r.seq
	     << " parent=" << r.parent
	     << " my_snaps=" << r.my_snaps
	     << " cached_snapc=" << r.cached_snap_context
	     << " last_modified=" << r.last_modified
	     << " change_attr=" << r.change_attr
             << " is_snapdir_visible=" << r.is_snapdir_visible
	     << ")";
}

#endif
