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

struct JournalPointer
{
  inodeno_t front = 0;
  inodeno_t back = 0;

  bool is_null() const
  {
    return front == 0 && back == 0;
  }

  void dump(Formatter* f) const
  {
    f->open_object_section("journal_pointer");
    {
      f->dump_unsigned("front", front);
      f->dump_unsigned("back", back);
    }
    f->close_section(); // journal_header
  }

  void encode(bufferlist& bl) const
  {
    ENCODE_START(1, 1, bl);
    encode(front, bl);
    encode(back, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(front, bl);
    decode(back, bl);
    DECODE_FINISH(bl);
  }

  void rotate() {
    std::swap(front, back);
  }

  static void generate_test_instances(std::list<JournalPointer*> &ls)
  {
    ls.push_back(new JournalPointer());
    ls.push_back(new JournalPointer());
    ls.back()->front = 0xdeadbeef;
    ls.back()->back = 0xfeedbead;
  }
};
WRITE_CLASS_ENCODER(JournalPointer)

// This always lives in the same location for a given MDS
// instance, it tells the daemon where to look for the journal.
struct JournalPointerStore {
  JournalPointer pointer;

  virtual int load() = 0;
  virtual int save() const = 0;
  virtual void save(Context *completion) const = 0;
  virtual ~JournalPointerStore() {}
};

#endif // JOURNAL_POINTER_H
