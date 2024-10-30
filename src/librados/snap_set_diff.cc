// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <vector>

#include "snap_set_diff.h"
#include "common/ceph_context.h"
#include "include/rados/librados.hpp"
#include "include/interval_set.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_rados

using namespace std;
/**
 * calculate intervals/extents that vary between two snapshots
 */
void calc_snap_set_diff(CephContext *cct, const librados::snap_set_t& snap_set,
			librados::snap_t start, librados::snap_t end,
			interval_set<uint64_t> *diff, uint64_t *end_size,
                        bool *end_exists, librados::snap_t *clone_end_snap_id,
                        bool *whole_object)
{
  ldout(cct, 10) << "calc_snap_set_diff start " << start << " end " << end
		 << ", snap_set seq " << snap_set.seq << dendl;
  bool saw_start = false;
  uint64_t start_size = 0;
  diff->clear();
  *end_size = 0;
  *end_exists = false;
  *clone_end_snap_id = 0;
  *whole_object = false;

  auto r = snap_set.clones.begin();
  while (r != snap_set.clones.end()) {
    // make an interval, and hide the fact that the HEAD doesn't
    // include itself in the snaps list
    librados::snap_t a, b;
    if (r->cloneid == librados::SNAP_HEAD) {
      // head is valid starting from right after the last seen seq
      a = snap_set.seq + 1;
      b = librados::SNAP_HEAD;
    } else if (r->snaps.empty()) {
      ldout(cct, 1) << "clone " << r->cloneid
                    << ": empty snaps, return whole object" << dendl;
      diff->clear();
      *whole_object = true;
      return;
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

    if (end < a) {
      break;
    }
    if (end <= b) {
      ldout(cct, 20) << " end" << dendl;
      *end_size = r->size;
      *end_exists = true;
      *clone_end_snap_id = b;
      return;
    }

    // start with the largest possible diff to next, and subtract off
    // any overlap
    const vector<pair<uint64_t, uint64_t> > *overlap = &r->overlap;
    interval_set<uint64_t> diff_to_next;
    uint64_t diff_boundary;
    uint64_t prev_size = r->size;
    ++r;
    if (r != snap_set.clones.end()) {
      if (r->size >= prev_size) {
        diff_boundary = r->size;
      } else if (prev_size <= start_size) {
        // truncated range below size at start
        diff_boundary = prev_size;
      } else {
        // truncated range (partially) above size at start -- drop that
        // part from the running diff
        diff_boundary = std::max(r->size, start_size);
        ldout(cct, 20) << "  no more diff beyond " << diff_boundary << dendl;
        diff->erase(diff_boundary, prev_size - diff_boundary);
      }
      if (diff_boundary) {
        diff_to_next.insert(0, diff_boundary);
      }
      for (auto p = overlap->begin(); p != overlap->end(); ++p) {
        diff_to_next.erase(p->first, p->second);
      }
      ldout(cct, 20) << "  diff_to_next " << diff_to_next << dendl;
      diff->union_of(diff_to_next);
      ldout(cct, 20) << "  diff now " << *diff << dendl;
    }
  }

  if (r != snap_set.clones.end()) {
    ldout(cct, 20) << " past end " << end
                   << ", end object does not exist" << dendl;
  } else {
    ldout(cct, 20) << " ran out of clones before reaching end " << end
                   << ", end object does not exist" << dendl;
  }
  diff->clear();
  if (start_size) {
    diff->insert(0, start_size);
  }
}
