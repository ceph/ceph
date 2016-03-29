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


#ifndef CEPH_INOTABLE_H
#define CEPH_INOTABLE_H

#include "MDSTable.h"
#include "include/interval_set.h"

class MDSRank;

class InoTable : public MDSTable {
  interval_set<inodeno_t> free;   // unused ids
  interval_set<inodeno_t> projected_free;

 public:
  explicit InoTable(MDSRank *m) : MDSTable(m, "inotable", true) { }

  inodeno_t project_alloc_id(inodeno_t id=0);
  void apply_alloc_id(inodeno_t id);

  void project_alloc_ids(interval_set<inodeno_t>& inos, int want);
  void apply_alloc_ids(interval_set<inodeno_t>& inos);

  void project_release_ids(interval_set<inodeno_t>& inos);
  void apply_release_ids(interval_set<inodeno_t>& inos);

  void replay_alloc_id(inodeno_t ino);
  void replay_alloc_ids(interval_set<inodeno_t>& inos);
  void replay_release_ids(interval_set<inodeno_t>& inos);
  void replay_reset();

  void reset_state();
  void encode_state(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(free, bl);
    ENCODE_FINISH(bl);
  }
  void decode_state(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    ::decode(free, bl);
    projected_free = free;
    DECODE_FINISH(bl);
  }

  // To permit enc/decoding in isolation in dencoder
  InoTable() : MDSTable(NULL, "inotable", true) {}
  void encode(bufferlist& bl) const {
    encode_state(bl);
  }
  void decode(bufferlist::iterator& bl) {
    decode_state(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<InoTable*>& ls);

  void skip_inos(inodeno_t i);

  /**
   * If the specified inode is marked as free, mark it as used.
   * For use in tools, not normal operations.
   *
   * @returns true if the inode was previously marked as free
   */
  bool force_consume(inodeno_t ino)
  {
    if (free.contains(ino)) {
      free.erase(ino);
      return true;
    } else {
      return false;
    }
  }

  /**
   * If this ino is in this rank's range, consume up to and including it.
   * For use in tools, when we know the max ino in use and want to make
   * sure we're only allocating new inodes from above it.
   *
   * @return true if the table was modified
   */
  bool force_consume_to(inodeno_t ino)
  {
    if (free.contains(ino)) {
      inodeno_t min = free.begin().get_start();
      std::cerr << "Erasing 0x" << std::hex << min << " to 0x" << ino << std::dec << std::endl;
      free.erase(min, ino - min + 1);
      projected_free = free;
      projected_version = ++version;
      return true;
    } else {
      return false;
    }
  }
};

#endif
