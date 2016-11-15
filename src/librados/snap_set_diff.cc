// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <vector>

#include "snap_set_diff.h"
#include "common/ceph_context.h"
#include "include/rados/librados.hpp"
#include "include/interval_set.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_rados

/**
 * calculate intervals/extents that vary between two snapshots
 */
void calc_snap_set_diff(CephContext *cct, const librados::snap_set_t& snap_set,
			librados::snap_t start, librados::snap_t end,
			interval_set<uint64_t> *diff, uint64_t *end_size,
                        bool *end_exists)
{
  ldout(cct, 10) << "calc_snap_set_diff start " << start << " end " << end
		 << ", snap_set seq " << snap_set.seq << dendl;
  bool saw_start = false;
  uint64_t start_size = 0;
  diff->clear();
  *end_size = 0;
  *end_exists = false;

  for (vector<librados::clone_info_t>::const_iterator r = snap_set.clones.begin();
       r != snap_set.clones.end();
       ) {
    // make an interval, and hide the fact that the HEAD doesn't
    // include itself in the snaps list
    librados::snap_t a, b;
    if (r->cloneid == librados::SNAP_HEAD) {
      // head is valid starting from right after the last seen seq
      a = snap_set.seq + 1;
      b = librados::SNAP_HEAD;
    } else {
      a = r->snaps[0];
      // note: b might be < r->cloneid if a snap has been trimmed.
      b = r->snaps[r->snaps.size()-1];
    }
    ldout(cct, 20) << " clone " << r->cloneid << " snaps " << r->snaps
		   << " -> [" << a << "," << b << "]"
		   << " size " << r->size << " overlap to next " << r->overlap << dendl;

    if (b < start) {
      // this is before start
      ++r;
      continue;
    }

    if (!saw_start) {
      if (start < a) {
	ldout(cct, 20) << "  start, after " << start << dendl;
	// this means the object didn't exist at start
	if (r->size)
	  diff->insert(0, r->size);
	start_size = 0;
      } else {
	ldout(cct, 20) << "  start" << dendl;
	start_size = r->size;
      }
      saw_start = true;
    }

    *end_size = r->size;
    if (end < a) {
      ldout(cct, 20) << " past end " << end << ", end object does not exist" << dendl;
      *end_exists = false;
      diff->clear();
      if (start_size) {
	diff->insert(0, start_size);
      }
      break;
    }
    if (end <= b) {
      ldout(cct, 20) << " end" << dendl;
      *end_exists = true;
      break;
    }

    // start with the max(this size, next size), and subtract off any
    // overlap
    const vector<pair<uint64_t, uint64_t> > *overlap = &r->overlap;
    interval_set<uint64_t> diff_to_next;
    uint64_t max_size = r->size;
    ++r;
    if (r != snap_set.clones.end()) {
      if (r->size > max_size)
	max_size = r->size;
    }
    if (max_size)
      diff_to_next.insert(0, max_size);
    for (vector<pair<uint64_t, uint64_t> >::const_iterator p = overlap->begin();
	 p != overlap->end();
	 ++p) {
      diff_to_next.erase(p->first, p->second);
    }
    ldout(cct, 20) << "  diff_to_next " << diff_to_next << dendl;
    diff->union_of(diff_to_next);
    ldout(cct, 20) << "  diff now " << *diff << dendl;
  }
}
