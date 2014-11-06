// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Inktank Storage, Inc.
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 */
#ifndef CEPH_UTIL_H
#define CEPH_UTIL_H

// is buf~len completely zero (in 8-byte chunks)

#include "common/Formatter.h"
#include "include/types.h"

bool buf_is_zero(const char *buf, size_t len);

int64_t unit_to_bytesize(string val, ostream *pss);

struct ceph_data_stats
{
  uint64_t byte_total;
  uint64_t byte_used;
  uint64_t byte_avail;
  int avail_percent;

  void dump(Formatter *f) const {
    assert(f != NULL);
    f->dump_int("total", byte_total);
    f->dump_int("used", byte_used);
    f->dump_int("avail", byte_avail);
    f->dump_int("avail_percent", avail_percent);
  }
};
typedef struct ceph_data_stats ceph_data_stats_t;

int get_fs_stats(ceph_data_stats_t &stats, const char *path);
#endif /* CEPH_UTIL_H */
