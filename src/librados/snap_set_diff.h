// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_OSDC_SNAP_SET_DIFF_H
#define __CEPH_OSDC_SNAP_SET_DIFF_H

class CephContext;
#include "include/rados/rados_types.hpp"
#include "include/interval_set.h"

void calc_snap_set_diff(CephContext *cct,
			const librados::snap_set_t& snap_set,
			librados::snap_t start, librados::snap_t end,
			interval_set<uint64_t> *diff, uint64_t *end_size,
			bool *end_exists, librados::snap_t *clone_end_snap_id);

#endif
