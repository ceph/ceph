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

#include "common/ClassVersion.h"

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
  ClassVersion version;

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

typedef enum {
  INC_NOP,
  INC_ADD,
  INC_DEL,
  INC_ACTIVATE,
} ClassLibraryIncOp;

struct ClassLibraryIncremental {
   ClassLibraryIncOp op;
   bufferlist info;
   bufferlist impl;

  void encode(bufferlist& bl) const {
    __u32 _op = (__u32)op;
    ::encode(_op, bl);
    ::encode(info, bl);
    ::encode(impl, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u32 _op;
    ::decode(_op, bl);
    op = (ClassLibraryIncOp)_op;
    assert( op >= INC_NOP && op <= INC_ACTIVATE);
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

typedef map<ClassVersion, ClassInfo> tClassVersionMap;
class ClassVersionMap
{
public:
  tClassVersionMap m;
  string default_ver;

  void encode(bufferlist& bl) const {
    ::encode(m, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(m, bl);
  }

  tClassVersionMap::iterator begin() { return m.begin(); }
  tClassVersionMap::iterator end() { return m.end(); }

  void add(ClassInfo& library) {
    m[library.version] = library;
   if (default_ver.length() == 0)
    default_ver = library.version.ver;
  }

  void remove(ClassInfo& library) {
    tClassVersionMap::iterator iter;
    iter = m.find(library.version);
    if (iter != m.end()) {
      m.erase(iter);
    }
  }

  ClassInfo *get(ClassVersion& ver);
  void set_default(string ver) { default_ver = ver; }
};
WRITE_CLASS_ENCODER(ClassVersionMap)

struct ClassLibrary {
  version_t version;
  map<string, ClassVersionMap> library_map;

  ClassLibrary() : version(0) {}

  void add(const string& name, const ClassVersion& version) {
    ClassInfo library;
    library.version = version;
    library.name = name;
    add(library);
  }

  void add(ClassInfo& library) {
    ClassVersionMap& vmap = library_map[library.name];
    vmap.add(library);
  }

  void remove(const string& name, const ClassVersion& version) {
    map<string, ClassVersionMap>::iterator mapiter = library_map.find(name);
    if (mapiter == library_map.end())
      return;
    library_map.erase(mapiter);
  }

  bool contains(string& name) {
    return (library_map.find(name) != library_map.end());
  }
  bool get_ver(string& name, ClassVersion& reqver, ClassVersion *ver) {
    map<string, ClassVersionMap>::iterator mapiter = library_map.find(name);
    if (mapiter == library_map.end())
      return false;
    string ver_str;
    ClassVersionMap& map = mapiter->second;
    ClassInfo *info = map.get(reqver);
    if (info)
      *ver = info->version;
    
    return (info != NULL);
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
