// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PG.h"
#include "Session.h"

#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd

void Session::clear_backoffs()
{
  map<spg_t,map<hobject_t,set<BackoffRef>>> ls;
  {
    Mutex::Locker l(backoff_lock);
    ls.swap(backoffs);
    backoff_count = 0;
  }
  for (auto& i : ls) {
    for (auto& p : i.second) {
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
}

void Session::ack_backoff(
  CephContext *cct,
  spg_t pgid,
  uint64_t id,
  const hobject_t& begin,
  const hobject_t& end)
{
  Mutex::Locker l(backoff_lock);
  auto p = backoffs.find(pgid);
  if (p == backoffs.end()) {
    dout(20) << __func__ << " " << pgid << " " << id << " [" << begin << ","
	     << end << ") pg not found" << dendl;
    return;
  }
  auto q = p->second.find(begin);
  if (q == p->second.end()) {
    dout(20) << __func__ << " " << pgid << " " << id << " [" << begin << ","
	     << end << ") begin not found" << dendl;
    return;
  }
  for (auto i = q->second.begin(); i != q->second.end(); ++i) {
    Backoff *b = (*i).get();
    if (b->id == id) {
      if (b->is_new()) {
	b->state = Backoff::STATE_ACKED;
	dout(20) << __func__ << " now " << *b << dendl;
      } else if (b->is_deleting()) {
	dout(20) << __func__ << " deleting " << *b << dendl;
	q->second.erase(i);
	--backoff_count;
      }
      break;
    }
  }
  if (q->second.empty()) {
    dout(20) << __func__ << " clearing begin bin " << q->first << dendl;
    p->second.erase(q);
    if (p->second.empty()) {
      dout(20) << __func__ << " clearing pg bin " << p->first << dendl;
      backoffs.erase(p);
    }
  }
  assert(!backoff_count == backoffs.empty());
}

bool Session::check_backoff(
  CephContext *cct, spg_t pgid, const hobject_t& oid, const Message *m)
{
  BackoffRef b(have_backoff(pgid, oid));
  if (b) {
    dout(10) << __func__ << " session " << this << " has backoff " << *b
	     << " for " << *m << dendl;
    assert(!b->is_acked() || !g_conf->osd_debug_crash_on_ignored_backoff);
    return true;
  }
  // we may race with ms_handle_reset.  it clears session->con before removing
  // backoffs, so if we see con is cleared here we have to abort this
  // request.
  if (!con) {
    dout(10) << __func__ << " session " << this << " disconnected" << dendl;
    return true;
  }
  return false;
}
