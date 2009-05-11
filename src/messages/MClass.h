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

#ifndef __MCLASS_H
#define __MCLASS_H

#include "include/ClassEntry.h"

class MClass : public Message {
public:
  ceph_fsid_t fsid;
  deque<ClassLibraryIncremental> entries;
  version_t last;
  __s32 action;

  enum {
     CLASS_NOOP = 0,
     CLASS_ADD,
     CLASS_REMOVE,
     CLASS_GET,
  };
  
  MClass() : Message(MSG_CLASS) {}
  MClass(ceph_fsid_t& f, deque<ClassLibraryIncremental>& e) : Message(MSG_CLASS), fsid(f), entries(e), last(0), action(0) { }
  MClass(ceph_fsid_t& f, version_t l) : Message(MSG_CLASS), fsid(f), last(l) {}

  const char *get_type_name() { return "class"; }
  void print(ostream& out) {
    out << "class(";
    if (entries.size())
      out << entries.size() << " entries";
    if (last)
      out << "last " << last;
    out << ")";
  }

  void encode_payload() {
    ::encode(fsid, payload);
    ::encode(entries, payload);
    ::encode(last, payload);
    ::encode(action, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(entries, p);
    ::decode(last, p);
    ::decode(action, payload);
  }
};

#endif
