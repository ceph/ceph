// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <grp.h>
#include <pwd.h>
#include <unistd.h>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/dout.h"
#include "common/errno.h"
#include "impersonate.h"
#include "name_resolve.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

int nsswitch_lookup(const std::string& name, ResolvedName& out)
{
  long hint = sysconf(_SC_GETPW_R_SIZE_MAX);
  std::vector<char> buf((hint > 0) ? hint : 1024);
  struct passwd pw;
  struct passwd* result = nullptr;
  for (;;) {
    const int err = getpwnam_r(name.c_str(), &pw, buf.data(), buf.size(),
			       &result);
    if (err == ERANGE) {
      buf.resize(buf.size() * 2);
      continue;
    }
    if (err != 0) {
      return -err;
    }
    break;
  }
  /* getpwnam_r(3):  a name no source has is success with no result */
  if (! result) {
    return -ENOENT;
  }
  out.uid = pw.pw_uid;
  out.gid = pw.pw_gid;
  return 0;
}

int nsswitch_user(uint32_t uid, std::string& name, uint32_t& gid)
{
  long hint = sysconf(_SC_GETPW_R_SIZE_MAX);
  std::vector<char> buf((hint > 0) ? hint : 1024);
  struct passwd pw;
  struct passwd* result = nullptr;
  for (;;) {
    const int err = getpwuid_r(uid, &pw, buf.data(), buf.size(), &result);
    if (err == ERANGE) {
      buf.resize(buf.size() * 2);
      continue;
    }
    if (err != 0) {
      return -err;
    }
    break;
  }
  if (! result) {
    return -ENOENT;
  }
  name = pw.pw_name;
  gid = pw.pw_gid;
  return 0;
}

int nsswitch_groups(const std::string& name, uint32_t gid,
		    std::vector<uint32_t>& out)
{
  /* getgrouplist(3) returns -1 and sets `n` to the count needed when
   * the buffer is short */
  std::vector<gid_t> buf(64);
  for (;;) {
    int n = static_cast<int>(buf.size());
    if (getgrouplist(name.c_str(), gid, buf.data(), &n) >= 0) {
      out.assign(buf.begin(), buf.begin() + n);
      return 0;
    }
    if (n <= static_cast<int>(buf.size())) {
      return errno ? -errno : -EIO;
    }
    buf.resize(n);
  }
}

NsswitchCache::NsswitchCache(CephContext* cct)
  : cct(cct), lookup(nsswitch_lookup), group_lookup(nsswitch_groups),
    user_lookup(nsswitch_user)
{}

NsswitchCache::~NsswitchCache()
{
  stop();
}

int NsswitchCache::look_up(const std::string& name, ResolvedName& out) const
{
  NameLookupFn fn;
  {
    std::lock_guard l(mtx);
    fn = lookup;
  }
  return fn(name, out);
}

bool NsswitchCache::store(const std::string& name, int ret,
		      const ResolvedName& rn)
{
  std::lock_guard l(mtx);
  auto [i, inserted] = entries.try_emplace(name);
  const bool changed = inserted || (i->second.ret != ret)
    || ((ret == 0) && !(i->second.name == rn));
  i->second.ret = ret;
  i->second.name = (ret == 0) ? rn : ResolvedName{};
  if (ret != 0) {
    i->second.groups.reset();
  }
  return changed;
}

int NsswitchCache::look_up_groups(const std::string& name, uint32_t gid,
				  std::vector<uint32_t>& out) const
{
  GroupLookupFn fn;
  {
    std::lock_guard l(mtx);
    fn = group_lookup;
  }
  return fn(name, gid, out);
}

int NsswitchCache::look_up_uid(uint32_t uid, UidEntry& out) const
{
  UserLookupFn fn;
  {
    std::lock_guard l(mtx);
    fn = user_lookup;
  }
  out.ret = fn(uid, out.name, out.gid);
  if (out.ret < 0) {
    return out.ret;
  }
  return look_up_groups(out.name, out.gid, out.groups);
}

bool NsswitchCache::drop(const std::string& name)
{
  std::lock_guard l(mtx);
  return entries.erase(name) > 0;
}

int NsswitchCache::resolve(const DoutPrefixProvider* dpp,
		       const std::string& name, ResolvedName& out,
		       optional_yield y)
{
  {
    std::lock_guard l(mtx);
    if (auto i = entries.find(name); i != entries.end()) {
      out = i->second.name;
      return i->second.ret;
    }
  }

  ResolvedName rn;
  int ret;
  if (y) {
    ret = run_on_helper(dpp, [&] { return look_up(name, rn); }, y);
  } else {
    ret = look_up(name, rn);
  }
  if ((ret != 0) && (ret != -ENOENT)) {
    ldpp_dout(dpp, 0) << "ERROR: nsswitch lookup of " << name
      << " failed: " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  /* A first answer invalidates nothing:  no personality can have been
   * registered from an answer this cache did not hold. */
  store(name, ret, rn);
  out = rn;
  return ret;
}

int NsswitchCache::groups_by_name(const DoutPrefixProvider* dpp,
				  const std::string& name,
				  std::vector<uint32_t>& out, optional_yield y)
{
  ResolvedName rn;
  int ret = resolve(dpp, name, rn, y);
  if (ret < 0) {
    return ret;
  }
  {
    std::lock_guard l(mtx);
    if (auto i = entries.find(name);
	(i != entries.end()) && i->second.groups) {
      out = *i->second.groups;
      return 0;
    }
  }
  std::vector<uint32_t> g;
  if (y) {
    ret = run_on_helper(dpp, [&] { return look_up_groups(name, rn.gid, g); },
			y);
  } else {
    ret = look_up_groups(name, rn.gid, g);
  }
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: nsswitch group lookup of " << name
      << " failed: " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  {
    std::lock_guard l(mtx);
    if (auto i = entries.find(name); i != entries.end()) {
      i->second.groups = g;
    }
  }
  out = std::move(g);
  return 0;
}

int NsswitchCache::groups_by_uid(const DoutPrefixProvider* dpp, uint32_t uid,
				 std::vector<uint32_t>& out, optional_yield y)
{
  {
    std::lock_guard l(mtx);
    if (auto i = uid_entries.find(uid); i != uid_entries.end()) {
      if (i->second.ret < 0) {
	return i->second.ret;
      }
      out = i->second.groups;
      return 0;
    }
  }
  UidEntry e;
  int ret;
  if (y) {
    ret = run_on_helper(dpp, [&] { return look_up_uid(uid, e); }, y);
  } else {
    ret = look_up_uid(uid, e);
  }
  if ((ret < 0) && (ret != -ENOENT)) {
    ldpp_dout(dpp, 0) << "ERROR: nsswitch lookup of uid " << uid
      << " failed: " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  {
    std::lock_guard l(mtx);
    uid_entries[uid] = e;
  }
  if (ret < 0) {
    return ret;
  }
  out = std::move(e.groups);
  return 0;
}

bool NsswitchCache::dynamic_groups() const
{
  const int o = dynamic_override.load(std::memory_order_relaxed);
  if (o >= 0) {
    return o != 0;
  }
  return cct->_conf.get_val<bool>("rgw_nsfs_dynamic_supplemental_groups");
}

void NsswitchCache::override_dynamic_groups(std::optional<bool> on)
{
  dynamic_override.store(on ? (*on ? 1 : 0) : -1, std::memory_order_relaxed);
  note_all_identities_changed();
}

bool NsswitchCache::forget_uid(uint32_t uid)
{
  bool had;
  {
    std::lock_guard l(mtx);
    had = uid_entries.erase(uid) > 0;
  }
  if (had) {
    note_all_identities_changed();
  }
  return had;
}

std::vector<std::string> NsswitchCache::names_for_uid(uint32_t uid) const
{
  std::vector<std::string> out;
  std::lock_guard l(mtx);
  for (const auto& [name, e] : entries) {
    if ((e.ret == 0) && (e.name.uid == uid)) {
      out.push_back(name);
    }
  }
  return out;
}

/* The cache is updated before the generation moves.  A request reads the
 * generation and then resolves (pin_personality_for()), so in this order
 * the worst case is one that reads the old generation and the new
 * answer, and re-registers once.  The reverse would let one read the new
 * generation and the old answer, and keep it. */
int NsswitchCache::refresh(const DoutPrefixProvider* dpp,
		       const std::string& name)
{
  bool had_groups = false;
  {
    std::lock_guard l(mtx);
    if (auto i = entries.find(name); i != entries.end()) {
      had_groups = i->second.groups.has_value();
    }
  }
  ResolvedName rn;
  int ret = look_up(name, rn);
  std::vector<uint32_t> g;
  if ((ret == 0) && had_groups) {
    ret = look_up_groups(name, rn.gid, g);
  }
  bool changed;
  if ((ret != 0) && (ret != -ENOENT)) {
    ldpp_dout(dpp, 0) << "ERROR: nsswitch lookup of " << name
      << " failed: " << cpp_strerror(-ret)
      << ";  dropping its resolution" << dendl;
    changed = drop(name);
  } else {
    changed = store(name, ret, rn);
    if ((ret == 0) && had_groups) {
      std::lock_guard l(mtx);
      auto& e = entries[name];
      changed = changed || !e.groups || (*e.groups != g);
      e.groups = std::move(g);
    }
  }
  if (changed) {
    ldpp_dout(dpp, 4) << "resolution of " << name << " changed;"
      "  invalidating identities" << dendl;
    note_all_identities_changed();
  }
  if ((ret != 0) && (ret != -ENOENT)) {
    return ret;
  }
  return changed ? 1 : 0;
}

bool NsswitchCache::forget(const std::string& name)
{
  if (! drop(name)) {
    return false;
  }
  note_all_identities_changed();
  return true;
}

int NsswitchCache::refresh_uid(const DoutPrefixProvider* dpp, uint32_t uid)
{
  UidEntry e;
  const int ret = look_up_uid(uid, e);
  bool changed;
  {
    std::lock_guard l(mtx);
    if ((ret < 0) && (ret != -ENOENT)) {
      changed = uid_entries.erase(uid) > 0;
    } else {
      auto [i, inserted] = uid_entries.try_emplace(uid);
      const auto& h = i->second;
      changed = inserted || (h.ret != e.ret)
	|| ((e.ret == 0) && ((h.name != e.name) || (h.gid != e.gid)
			     || (h.groups != e.groups)));
      i->second = std::move(e);
    }
  }
  if ((ret < 0) && (ret != -ENOENT)) {
    ldpp_dout(dpp, 0) << "ERROR: nsswitch lookup of uid " << uid
      << " failed: " << cpp_strerror(-ret)
      << ";  dropping its resolution" << dendl;
  }
  if (changed) {
    ldpp_dout(dpp, 4) << "resolution of uid " << uid << " changed;"
      "  invalidating identities" << dendl;
    note_all_identities_changed();
  }
  if ((ret < 0) && (ret != -ENOENT)) {
    return ret;
  }
  return changed ? 1 : 0;
}

int NsswitchCache::sweep(const DoutPrefixProvider* dpp)
{
  std::vector<std::string> names;
  std::vector<uint32_t> uids;
  {
    std::lock_guard l(mtx);
    for (const auto& [name, e] : entries) {
      names.push_back(name);
    }
    for (const auto& [uid, e] : uid_entries) {
      uids.push_back(uid);
    }
  }
  int changed = 0;
  for (const auto& name : names) {
    const int r = refresh(dpp, name);
    if ((r == 1) || (r < 0)) {
      ++changed;
    }
  }
  for (const auto uid : uids) {
    const int r = refresh_uid(dpp, uid);
    if ((r == 1) || (r < 0)) {
      ++changed;
    }
  }
  return changed;
}

void NsswitchCache::sweep_loop(NameSource source)
{
  NoDoutPrefix dpp(cct, dout_subsys);
  if (source) {
    const Table t = source(&dpp);
    for (const auto& name : t.names) {
      ResolvedName rn;
      if (resolve(&dpp, name, rn, null_yield) == -ENOENT) {
	ldpp_dout(&dpp, 0) << "WARNING: an identity names " << name
	  << ", which nsswitch does not know;  it is refused until it"
	  " resolves" << dendl;
      }
    }
    for (const auto uid : t.uids) {
      std::vector<uint32_t> g;
      if (groups_by_uid(&dpp, uid, g, null_yield) == -ENOENT) {
	ldpp_dout(&dpp, 0) << "WARNING: an identity with no group list runs "
	  "as uid " << uid << ", which nsswitch does not know;  under "
	  "rgw_nsfs_dynamic_supplemental_groups it is refused until it "
	  "resolves" << dendl;
      }
    }
  }
  std::unique_lock l(sweep_mtx);
  while (! stopping) {
    const auto ttl = std::chrono::seconds(
      cct->_conf.get_val<uint64_t>("rgw_nsfs_identity_resolve_ttl"));
    if (sweep_cv.wait_for(l, ttl, [this] { return stopping; })) {
      break;
    }
    l.unlock();
    sweep(&dpp);
    l.lock();
  }
}

void NsswitchCache::start(NameSource source)
{
  std::lock_guard l(sweep_mtx);
  if (sweeper.joinable()) {
    return;
  }
  stopping = false;
  ldout(cct, 1) << "nsfs: nsswitch-resolved identities swept every "
    << cct->_conf.get_val<uint64_t>("rgw_nsfs_identity_resolve_ttl")
    << " s" << dendl;
  sweeper = std::thread([this, source = std::move(source)] {
    sweep_loop(source);
  });
}

void NsswitchCache::stop()
{
  {
    std::lock_guard l(sweep_mtx);
    stopping = true;
  }
  sweep_cv.notify_all();
  if (sweeper.joinable()) {
    sweeper.join();
  }
}

void NsswitchCache::set_lookup_for_test(NameLookupFn fn)
{
  std::lock_guard l(mtx);
  lookup = std::move(fn);
  entries.clear();
}

void NsswitchCache::set_group_lookups_for_test(GroupLookupFn groups,
					       UserLookupFn user)
{
  std::lock_guard l(mtx);
  group_lookup = std::move(groups);
  user_lookup = std::move(user);
  entries.clear();
  uid_entries.clear();
}

}}} // namespace rgw::sal::nsfs
