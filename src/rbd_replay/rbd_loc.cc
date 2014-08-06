// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Adam Crume <adamcrume@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "rbd_loc.hpp"
#include "include/assert.h"


using namespace std;
using namespace rbd_replay;


rbd_loc::rbd_loc() {
}

rbd_loc::rbd_loc(string pool, string image, string snap)
  : pool(pool),
    image(image),
    snap(snap) {
}

bool rbd_loc::parse(string name_string) {
  int field = 0;
  string fields[3];
  bool read_slash = false;
  bool read_at = false;
  for (size_t i = 0, n = name_string.length(); i < n; i++) {
    char c = name_string[i];
    switch (c) {
    case '/':
      if (read_slash || read_at) {
	return false;
      }
      assert(field == 0);
      field++;
      read_slash = true;
      break;
    case '@':
      if (read_at) {
	return false;
      }
      assert(field < 2);
      field++;
      read_at = true;
      break;
    case '\\':
      if (i == n - 1) {
	return false;
      }
      fields[field].push_back(name_string[++i]);
      break;
    default:
      fields[field].push_back(c);
    }
  }

  if (read_slash) {
    pool = fields[0];
    image = fields[1];
    // note that if read_at is false, then fields[2] is the empty string,
    // so this is still correct
    snap = fields[2];
  } else {
    pool = "";
    image = fields[0];
    // note that if read_at is false, then fields[1] is the empty string,
    // so this is still correct
    snap = fields[1];
  }
  return true;
}


static void write(const string &in, string *out) {
  for (size_t i = 0, n = in.length(); i < n; i++) {
    char c = in[i];
    if (c == '@' || c == '/' || c == '\\') {
      out->push_back('\\');
    }
    out->push_back(c);
  }
}

string rbd_loc::str() const {
  string out;
  if (!pool.empty()) {
    write(pool, &out);
    out.push_back('/');
  }
  write(image, &out);
  if (!snap.empty()) {
    out.push_back('@');
    write(snap, &out);
  }
  return out;
}

int rbd_loc::compare(const rbd_loc& rhs) const {
  int c = pool.compare(rhs.pool);
  if (c) {
    return c;
  }
  c = image.compare(rhs.image);
  if (c) {
    return c;
  }
  c = snap.compare(rhs.snap);
  if (c) {
    return c;
  }
  return 0;
}

bool rbd_loc::operator==(const rbd_loc& rhs) const {
  return compare(rhs) == 0;
}

bool rbd_loc::operator<(const rbd_loc& rhs) const {
  return compare(rhs) < 0;
}
