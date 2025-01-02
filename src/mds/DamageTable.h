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

#include <string_view>

#include "mdstypes.h"
#include "include/random.h"

class CDir;
class CInode;

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
    DamageEntry()
    {
      id = ceph::util::generate_random_number<damage_entry_id_t>(0, 0xffffffff);
      reported_at = ceph_clock_now();
    }

    virtual ~DamageEntry();

    virtual damage_entry_type_t get_type() const = 0;
    virtual void dump(Formatter *f) const = 0;

    damage_entry_id_t id;
    utime_t reported_at;

    // path is optional, advisory.  Used to give the admin an idea of what
    // part of his tree the damage affects.
    std::string path;
};

typedef std::shared_ptr<DamageEntry> DamageEntryRef;

class DirFragIdent
{
  public:
    DirFragIdent(inodeno_t ino_, frag_t frag_)
      : ino(ino_), frag(frag_)
    {}

    bool operator<(const DirFragIdent &rhs) const
    {
      if (ino == rhs.ino) {
        return frag < rhs.frag;
      } else {
        return ino < rhs.ino;
      }
    }

    inodeno_t ino;
    frag_t frag;
};

class DentryIdent
{
  public:
    DentryIdent(std::string_view dname_, snapid_t snap_id_)
      : dname(dname_), snap_id(snap_id_)
    {}

    bool operator<(const DentryIdent &rhs) const
    {
      if (dname == rhs.dname) {
        return snap_id < rhs.snap_id;
      } else {
        return dname < rhs.dname;
      }
    }

    std::string dname;
    snapid_t snap_id;
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
  public:
    explicit DamageTable(const mds_rank_t rank_)
      : rank(rank_)
    {
      ceph_assert(rank_ != MDS_RANK_NONE);
    }

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
    bool notify_dirfrag(inodeno_t ino, frag_t frag, std::string_view path);

    /**
     * Indicate that a particular dentry cannot be loaded.
     *
     * @return true if fatal
     */
    bool notify_dentry(
      inodeno_t ino, frag_t frag,
      snapid_t snap_id, std::string_view dname, std::string_view path);

    /**
     * Indicate that a particular Inode could not be loaded by number
     */
    bool notify_remote_damaged(inodeno_t ino, std::string_view path);

    void remove_dentry_damage_entry(CDir *dir);

    void remove_dirfrag_damage_entry(CDir *dir);

    void remove_backtrace_damage_entry(inodeno_t ino);

    bool is_dentry_damaged(
      const CDir *dir_frag,
      std::string_view dname,
      const snapid_t snap_id) const;

    bool is_dirfrag_damaged(const CDir *dir_frag) const;

    bool is_remote_damaged(const inodeno_t ino) const;

    void dump(Formatter *f) const;

    void erase(damage_entry_id_t damage_id);

  protected:
    // I need to know my MDS rank so that I can check if
    // metadata items are part of my mydir.
    const mds_rank_t rank;

    bool oversized() const;

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
};
#endif // DAMAGE_TABLE_H_
