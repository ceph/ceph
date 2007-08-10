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


#ifndef __CLIENT_TRACE_H
#define __CLIENT_TRACE_H

#include <cassert>
#include <list>
#include <string>
using namespace std;

/*

 this class is more like an iterator over a constant tokenlist (which 
 is protected by a mutex, see Trace.cc)

 */

class Trace {
  class TokenList *tl;
  int _line;

 public:
  Trace(const char* filename);
  ~Trace();

  int get_line() { return _line; }
  list<const char*>& get_list();

  list<const char*>::iterator _cur;
  list<const char*>::iterator _end;

  void start() {
    _cur = get_list().begin();
    _end = get_list().end();
    _line = 1;
  }

  const char *get_string(char *buf, const char *prefix) {
    assert(_cur != _end);
    const char *s = *_cur;
    _cur++; _line++;
    if (prefix) {
      if (strstr(s, "/prefix") == s ||
          strstr(s, "/prefix") == s+1) {
        strcpy(buf, prefix);
        strcpy(buf + strlen(prefix),
               s + strlen("/prefix"));
        s = (const char*)buf;
      }
    } 
    return s;
  }
  __int64_t get_int() {
    char buf[20];
    return atoll(get_string(buf, 0));
  }
  bool end() {
    return _cur == _end;
  }
};

#endif
