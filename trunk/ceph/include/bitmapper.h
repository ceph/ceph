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


#ifndef __BITMAPPER_H
#define __BITMAPPER_H

class bitmapper {
  char *_data;

 public:
  bitmapper(char *data) : _data(data) { }

  bool operator[](int b) {
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
