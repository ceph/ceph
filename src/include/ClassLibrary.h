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

#ifndef __CLASSENTRY_H
#define __CLASSENTRY_H

#include "include/types.h"
#include "include/encoding.h"

struct ClassImpl {
  bufferlist binary;
  utime_t stamp;
  version_t seq;

  void encode(bufferlist& bl) const {
    ::encode(binary, bl);
    ::encode(seq, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(binary, bl);
    ::decode(seq, bl);
  }
};


WRITE_CLASS_ENCODER(ClassImpl)


struct ClassInfo {
  string name;
  version_t version;

  void encode(bufferlist& bl) const {
    ::encode(name, bl);
    ::encode(version, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(name, bl);
    ::decode(version, bl);
  }
};

WRITE_CLASS_ENCODER(ClassInfo)

struct ClassLibraryIncremental {
   bool add;
   bufferlist info;
   bufferlist impl;

  void encode(bufferlist& bl) const {
    ::encode(add, bl);
    ::encode(info, bl);
    ::encode(impl, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(add, bl);
    ::decode(info, bl);
    ::decode(impl, bl);
  }

  void decode_impl(ClassImpl& i) {
     bufferlist::iterator iter = impl.begin();
     ::decode(i, iter);
  }

  void decode_info(ClassInfo& l) {
     bufferlist::iterator iter = info.begin();
     ::decode(l, iter);
  }
};

WRITE_CLASS_ENCODER(ClassLibraryIncremental)

struct ClassLibrary {
  version_t version;
  map<string, ClassInfo> library_map;

  ClassLibrary() : version(0) {}

  void add(const string& name, const version_t version) {
    ClassInfo library;
    library.version = version;
    library.name = name;
    add(library);
  }

  void add(ClassInfo& library) {
    library_map[library.name] = library;
  }

  void remove(const string& name, const version_t version) {
    /* fixme */
  }

  bool contains(string& name) {
    return (library_map.find(name) != library_map.end());
  }

  bool get_ver(string& name, version_t *ver) {
    map<string, ClassInfo>::iterator iter = library_map.find(name);
    if (iter == library_map.end())
      return false;
    *ver = (iter->second).version;
    return true;
  }

  void encode(bufferlist& bl) const {
    ::encode(version, bl);
    ::encode(library_map, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(version, bl);
    ::decode(library_map, bl);
  }
};
WRITE_CLASS_ENCODER(ClassLibrary)

inline ostream& operator<<(ostream& out, const ClassInfo& e)
{
  return out << e.name << " (v" << e.version << ")";
}

#endif
