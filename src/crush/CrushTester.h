// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CRUSH_TESTER_H
#define CEPH_CRUSH_TESTER_H

#include "crush/CrushWrapper.h"

class CrushTester {
  CrushWrapper& crush;
  ostream& err;
  int verbose;

  map<int, int> device_weight;
  int min_rule, max_rule;
  int min_x, max_x;
  int min_rep, max_rep;

public:
  CrushTester(CrushWrapper& c, ostream& eo, int verbosity=0)
    : crush(c), err(eo), verbose(verbosity),
      min_rule(-1), max_rule(-1),
      min_x(-1), max_x(-1),
      min_rep(-1), max_rep(-1)
  { }

  void set_verbosity(int v) {
    verbose = v;
  }
  void set_device_weight(int dev, float f);

  void set_min_rep(int r) {
    min_rep = r;
  }
  void set_max_rep(int r) {
    max_rep = r;
  }
  void set_num_rep(int r) {
    min_rep = max_rep = r;
  }
  
  void set_min_x(int x) {
    min_x = x;
  }
  void set_max_x(int x) {
    max_x = x;
  }
  void set_x(int x) {
    min_x = max_x = x;
  }

  void set_min_rule(int rule) {
    min_rule = rule;
  }
  void set_max_rule(int rule) {
    max_rule = rule;
  }
  void set_rule(int rule) {
    min_rule = max_rule = rule;
  }

  int test();
};

#endif
