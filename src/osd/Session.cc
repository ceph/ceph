// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PG.h"
#include "Session.h"

#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd

void Session::clear_backoffs()
{
  map<hobject_t,set<BackoffRef>,hobject_t::BitwiseComparator> ls;
  {
    Mutex::Locker l(backoff_lock);
    ls.swap(backoffs);
    backoff_count = 0;
  }
  for (auto& p : ls) {
    for (auto& b : p.second) {
      Mutex::Locker l(b->lock);
      if (b->pg) {
	assert(b->session == this);
	assert(b->is_new() || b->is_acked());
	b->pg->rm_backoff(b);
	b->pg.reset();
	b->session.reset();
      } else if (b->session) {
	assert(b->session == this);
	assert(b->is_deleting());
	b->session.reset();
      }
    }
  }
}

void Session::ack_backoff(
  CephContext *cct,
  uint64_t id,
  const hobject_t& begin,
  const hobject_t& end)
{
  Mutex::Locker l(backoff_lock);
  // NOTE that ack may be for [a,c] but osd may now have [a,b) and
  // [b,c) due to a PG split.
  auto p = backoffs.lower_bound(begin);
  while (p != backoffs.end()) {
    // note: must still examine begin=end=p->first case
    int r = cmp_bitwise(p->first, end);
    if (r > 0 || (r == 0 && cmp_bitwise(begin, end) < 0)) {
      break;
    }
    auto q = p->second.begin();
    while (q != p->second.end()) {
      Backoff *b = (*q).get();
      if (b->id == id) {
	if (b->is_new()) {
	  b->state = Backoff::STATE_ACKED;
	  dout(20) << __func__ << " now " << *b << dendl;
	} else if (b->is_deleting()) {
	  dout(20) << __func__ << " deleting " << *b << dendl;
	  q = p->second.erase(q);
	  continue;
	}
      }
      ++q;
    }
    if (p->second.empty()) {
      dout(20) << __func__ << " clearing bin " << p->first << dendl;
      p = backoffs.erase(p);
      --backoff_count;
    } else {
      ++p;
    }
  }
  assert(backoff_count == (int)backoffs.size());
}
