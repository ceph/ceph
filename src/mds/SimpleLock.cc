// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include "SimpleLock.h"
#include "Mutation.h"

void SimpleLock::dump(Formatter *f) const {
  assert(f != NULL);
  if (is_sync_and_unlocked()) {
    return;
  }

  f->open_array_section("gather_set");
  if (have_more()) {
    for(std::set<int32_t>::iterator i = more()->gather_set.begin();
        i != more()->gather_set.end(); ++i) {
      f->dump_int("rank", *i);
    }
  }
  f->close_section();

  f->dump_int("num_client_lease", num_client_lease);
  f->dump_int("num_rdlocks", get_num_rdlocks());
  f->dump_int("num_wrlocks", get_num_wrlocks());
  f->dump_int("num_xlocks", get_num_xlocks());
  f->open_object_section("xlock_by");
  if (get_xlock_by()) {
    get_xlock_by()->dump(f);
  }
  f->close_section();
}
