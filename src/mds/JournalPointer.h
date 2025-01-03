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


#ifndef JOURNAL_POINTER_H
#define JOURNAL_POINTER_H

#include "include/encoding.h"
#include "mdstypes.h"

class Objecter;

// This always lives in the same location for a given MDS
// instance, it tells the daemon where to look for the journal.
class JournalPointer {
  public:
  JournalPointer(int node_id_, int64_t pool_id_) : node_id(node_id_), pool_id(pool_id_) {}
  JournalPointer() {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(front, bl);
    encode(back, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(front, bl);
    decode(back, bl);
    DECODE_FINISH(bl);
  }

  int load(Objecter *objecter);
  int save(Objecter *objecter) const;
  void save(Objecter *objecter, Context *completion) const;

  bool is_null() const {
    return front == 0 && back == 0;
  }

  void dump(Formatter *f) const {
    f->open_object_section("journal_pointer");
    {
      f->dump_unsigned("front", front);
      f->dump_unsigned("back", back);
    }
    f->close_section(); // journal_header
  }

  static void generate_test_instances(std::list<JournalPointer*> &ls)
  {
    ls.push_back(new JournalPointer());
    ls.push_back(new JournalPointer());
    ls.back()->front = 0xdeadbeef;
    ls.back()->back = 0xfeedbead;
  }

  // The currently active journal
  inodeno_t front = 0;
  // The backup journal, if any (may be 0)
  inodeno_t back = 0;

  private:
  // MDS rank
  int node_id = -1;
  // Metadata pool ID
  int64_t pool_id = -1;

  std::string get_object_id() const;
};
WRITE_CLASS_ENCODER(JournalPointer)

#endif // JOURNAL_POINTER_H
