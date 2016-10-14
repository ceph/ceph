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


#ifndef DAMAGE_TABLE_H_
#define DAMAGE_TABLE_H_

#include "mdstypes.h"
#include "auth/Crypto.h"

class CDir;

typedef uint64_t damage_entry_id_t;

typedef enum
{
  DAMAGE_ENTRY_DIRFRAG,
  DAMAGE_ENTRY_DENTRY,
  DAMAGE_ENTRY_BACKTRACE

} damage_entry_type_t;

class DamageEntry
{
  public:
  damage_entry_id_t id;
  utime_t reported_at;

  DamageEntry()
  {
    id = get_random(0, 0xffffffff);
    reported_at = ceph_clock_now(g_ceph_context);
  }

  virtual damage_entry_type_t get_type() const = 0;

  virtual ~DamageEntry();

  virtual void dump(Formatter *f) const = 0;
};


typedef ceph::shared_ptr<DamageEntry> DamageEntryRef;

/**
 * Record damage to a particular dirfrag, implicitly affecting
 * any dentries within it.
 */
class DirFragDamage : public DamageEntry
{
  public:
  inodeno_t ino;
  frag_t frag;

  DirFragDamage(inodeno_t ino_, frag_t frag_)
    : ino(ino_), frag(frag_)
  {}

  virtual damage_entry_type_t get_type() const
  {
    return DAMAGE_ENTRY_DIRFRAG;
  }

  void dump(Formatter *f) const
  {
    f->open_object_section("dir_frag_damage");
    f->dump_string("damage_type", "dir_frag");
    f->dump_int("id", id);
    f->dump_int("ino", ino);
    f->dump_stream("frag") << frag;
    f->close_section();
  }
};


/**
 * Record damage to a particular dname within a particular dirfrag
 */
class DentryDamage : public DamageEntry
{
  public:
  inodeno_t ino;
  frag_t frag;
  std::string dname;
  snapid_t snap_id;

  DentryDamage(
      inodeno_t ino_,
      frag_t frag_,
      std::string dname_,
      snapid_t snap_id_)
    : ino(ino_), frag(frag_), dname(dname_), snap_id(snap_id_)
  {}

  virtual damage_entry_type_t get_type() const
  {
    return DAMAGE_ENTRY_DENTRY;
  }

  void dump(Formatter *f) const
  {
    f->open_object_section("dentry_damage");
    f->dump_string("damage_type", "dentry");
    f->dump_int("id", id);
    f->dump_int("ino", ino);
    f->dump_stream("frag") << frag;
    f->dump_string("dname", dname);
    f->dump_stream("snap_id") << snap_id;
    f->close_section();
  }
};


/**
 * Record damage to our ability to look up an ino by number
 */
class BacktraceDamage : public DamageEntry
{
  public:
  inodeno_t ino;

  BacktraceDamage(inodeno_t ino_)
    : ino(ino_)
  {}

  virtual damage_entry_type_t get_type() const
  {
    return DAMAGE_ENTRY_BACKTRACE;
  }

  void dump(Formatter *f) const
  {
    f->open_object_section("backtrace_damage");
    f->dump_string("damage_type", "backtrace");
    f->dump_int("id", id);
    f->dump_int("ino", ino);
    f->close_section();
  }
};

class DirFragIdent
{
  public:
  inodeno_t ino;
  frag_t frag;

  bool operator<(const DirFragIdent &rhs) const
  {
    if (ino == rhs.ino) {
      return frag < rhs.frag;
    } else {
      return ino < rhs.ino;
    }
  }

  DirFragIdent(inodeno_t ino_, frag_t frag_)
    : ino(ino_), frag(frag_)
  {}
};

class DentryIdent
{
  public:
  std::string dname;
  snapid_t snap_id;

  bool operator<(const DentryIdent &rhs) const
  {
    if (dname == rhs.dname) {
      return snap_id < rhs.snap_id;
    } else {
      return dname < rhs.dname;
    }
  }

  DentryIdent(const std::string &dname_, snapid_t snap_id_)
    : dname(dname_), snap_id(snap_id_)
  {}
};

/**
 * Registry of in-RADOS metadata damage identified
 * during forward scrub or during normal fetches.
 *
 * Used to indicate damage to the administrator, and
 * to cache known-bad paths so that we don't hit them
 * repeatedly.
 *
 * Callers notifying damage must check return code; if
 * an fatal condition is indicated then they should mark the MDS
 * rank damaged.
 *
 * An artificial limit on the number of damage entries
 * is imposed to avoid this structure growing indefinitely.  If
 * a notification causes the limit to be exceeded, the fatal
 * condition will be indicated in the return code and the MDS
 * rank should be marked damaged.
 *
 * Protected by MDS::mds_lock
 */
class DamageTable
{
protected:

  // Map of all dirfrags reported damaged
  std::map<DirFragIdent, DamageEntryRef> dirfrags;

  // Store dentries in a map per dirfrag, so that we can
  // readily look up all the bad dentries in a particular
  // dirfrag
  std::map<DirFragIdent, std::map<DentryIdent, DamageEntryRef> > dentries;

  // Map of all inodes which could not be resolved remotely
  // (i.e. have probably/possibly missing backtraces)
  std::map<inodeno_t, DamageEntryRef> remotes;

  // All damage, by ID.  This is a secondary index
  // to the dirfrag, dentry, remote maps.  It exists
  // to enable external tools to unambiguously operate
  // on particular entries.
  std::map<damage_entry_id_t, DamageEntryRef> by_id;

  // I need to know my MDS rank so that I can check if
  // metadata items are part of my mydir.
  const mds_rank_t rank;

  bool oversized() const;

public:

  /**
   * Return true if no damage entries exist
   */
  bool empty() const
  {
    return by_id.empty();
  }

  /**
   * Indicate that a dirfrag cannot be loaded.
   *
   * @return true if fatal
   */
  bool notify_dirfrag(inodeno_t ino, frag_t frag);

  /**
   * Indicate that a particular dentry cannot be loaded.
   *
   * @return true if fatal
   */
  bool notify_dentry(
    inodeno_t ino, frag_t frag,
    snapid_t snap_id, const std::string &dname);

  /**
   * Indicate that a particular Inode could not be loaded by number
   */
  bool notify_remote_damaged(
      inodeno_t ino);

  bool is_dentry_damaged(
      const CDir *dir_frag,
      const std::string &dname,
      const snapid_t snap_id) const;

  bool is_dirfrag_damaged(
      const CDir *dir_frag) const;

  bool is_remote_damaged(
      const inodeno_t ino) const;


  DamageTable(const mds_rank_t rank_)
    : rank(rank_)
  {
    assert(rank_ != MDS_RANK_NONE);
  }

  void dump(Formatter *f) const;

  void erase(damage_entry_id_t damage_id);
};

#endif // DAMAGE_TABLE_H_

