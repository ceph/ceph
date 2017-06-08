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


#ifndef CEPH_DISTRIBUTION_H
#define CEPH_DISTRIBUTION_H

#include <vector>
using namespace std;

class Distribution {
  vector<float> p;
  vector<int> v;

 public:
  //Distribution() { 
  //}
  
  unsigned get_width() {
    return p.size();
  }

  void clear() {
    p.clear();
    v.clear();
  }
  void add(int val, float pr) {
    p.push_back(pr);
    v.push_back(val);
  }

  void random() {
    float sum = 0.0;
    for (unsigned i=0; i<p.size(); i++) {
      p[i] = (float)(rand() % 10000);
      sum += p[i];
    }
    for (unsigned i=0; i<p.size(); i++) 
      p[i] /= sum;
  }

  int sample() {
    float s = (float)(rand() % 10000) / 10000.0;
    for (unsigned i=0; i<p.size(); i++) {
      if (s < p[i]) return v[i];
      s -= p[i];
    }
    assert(0);
    return v[p.size() - 1];  // hmm.  :/
  }

  float normalize() {
    float s = 0.0;
    for (unsigned i=0; i<p.size(); i++)
      s += p[i];
    for (unsigned i=0; i<p.size(); i++)
      p[i] /= s;
    return s;
  }

};

#endif
