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

#ifndef CEPH_BITMAPPER_H
#define CEPH_BITMAPPER_H

class bitmapper {
  char *_data;
  int _len;

 public:
  bitmapper() : _data(0), _len(0) { }
  bitmapper(char *data, int len) : _data(data), _len(len) { }

  void set_data(char *data, int len) { _data = data; _len = len; }

  int bytes() const { return _len; }
  int bits() const { return _len * 8; }

  bool operator[](int b) const {
    return get(b);
  }
  bool get(int b) const {
    return _data[b >> 3] & (1 << (b&7));
  }
  void set(int b) {
    _data[b >> 3] |= 1 << (b&7);
  }
  void clear(int b) {
    _data[b >> 3] &= ~(1 << (b&7));
  }
  void toggle(int b) {
    _data[b >> 3] ^= 1 << (b&7);
  }
};

#endif
