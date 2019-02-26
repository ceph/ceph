// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MonSub.h"

bool MonSub::have_new() const {
  return !sub_new.empty();
}

bool MonSub::need_renew() const
{
  return ceph::coarse_mono_clock::now() > renew_after;
}

void MonSub::renewed()
{
  if (clock::is_zero(renew_sent)) {
    renew_sent = clock::now();
  }
  // update sub_sent with sub_new
  sub_new.insert(sub_sent.begin(), sub_sent.end());
  std::swap(sub_new, sub_sent);
  sub_new.clear();
}

void MonSub::acked(uint32_t interval)
{
  if (!clock::is_zero(renew_sent)) {
    // NOTE: this is only needed for legacy (infernalis or older)
    // mons; see MonClient::tick().
    renew_after = renew_sent;
    renew_after += ceph::make_timespan(interval / 2.0);
    renew_sent = clock::zero();
  }
}

bool MonSub::reload()
{
  for (auto& [what, sub] : sub_sent) {
    if (sub_new.count(what) == 0) {
      sub_new[what] = sub;
    }
  }
  return have_new();
}

void MonSub::got(const std::string& what, version_t have)
{
  if (auto i = sub_new.find(what); i != sub_new.end()) {
    auto& sub = i->second;
    if (sub.start <= have) {
      if (sub.flags & CEPH_SUBSCRIBE_ONETIME) {
        sub_new.erase(i);
      } else {
        sub.start = have + 1;
      }
    }
  } else if (auto i = sub_sent.find(what); i != sub_sent.end()) {
    auto& sub = i->second;
    if (sub.start <= have) {
      if (sub.flags & CEPH_SUBSCRIBE_ONETIME) {
        sub_sent.erase(i);
      } else {
        sub.start = have + 1;
      }
    }
  }
}

bool MonSub::want(const std::string& what, version_t start, unsigned flags)
{
  if (auto sub = sub_new.find(what);
      sub != sub_new.end() &&
      sub->second.start == start &&
      sub->second.flags == flags) {
    return false;
  } else if (auto sub = sub_sent.find(what);
      sub != sub_sent.end() &&
      sub->second.start == start &&
      sub->second.flags == flags) {
	return false;
  } else {
    sub_new[what].start = start;
    sub_new[what].flags = flags;
    return true;
  }
}

bool MonSub::inc_want(const std::string& what, version_t start, unsigned flags)
{
  if (auto sub = sub_new.find(what); sub != sub_new.end()) {
    if (sub->second.start >= start) {
      return false;
    } else {
      sub->second.start = start;
      sub->second.flags = flags;
      return true;
    }
  } else if (auto sub = sub_sent.find(what);
             sub == sub_sent.end() || sub->second.start < start) {
    auto& item = sub_new[what];
    item.start = start;
    item.flags = flags;
    return true;
  } else {
    return false;
  }
}

void MonSub::unwant(const std::string& what)
{
  sub_sent.erase(what);
  sub_new.erase(what);
}
