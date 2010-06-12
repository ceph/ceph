// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_AGER_H
#define CEPH_AGER_H

#include "include/types.h"
#include "include/Distribution.h"
#include "os/ObjectStore.h"
#include "common/Clock.h"

#include <list>
#include <vector>
using namespace std;

class Ager {
  ObjectStore *store;

 private:
  list<file_object_t>           age_free_oids;
  file_object_t                 age_cur_oid;
  vector< list<file_object_t> > age_objects;
  Distribution file_size_distn; //kb
  bool         did_distn;

  void age_empty(float pc);
  uint64_t age_fill(float pc, utime_t until);
  ssize_t age_pick_size();
  file_object_t age_get_oid();

 public:
  Ager(ObjectStore *s) : store(s), did_distn(false) {} 

  void age(int time,
           float high_water,    // fill to this %
          float low_water,     // then empty to this %
          int count,         // this many times
          float final_water,   // and end here ( <= low_water)
          int fake_size_mb=0);

  void save_freelist(int);
  void load_freelist();
};

#endif
