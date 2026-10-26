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

#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/async/yield_context.h"
#include "include/common_fwd.h"

class DoutPrefixProvider;

namespace rgw { namespace sal { namespace nsfs {

/* What nsswitch returned for a name:  the user's uid and primary gid. */
struct ResolvedName {
  uint32_t uid{0};
  uint32_t gid{0};

  bool operator==(const ResolvedName&) const = default;
};

/* One lookup of a name:  0 and `out`, -ENOENT for a name no source
 * knows, or another -errno where the lookup itself failed. */
using NameLookupFn =
  std::function<int(const std::string& name, ResolvedName& out)>;

/* getpwnam_r(3), which is what NooBaa calls for `distinguished_name`
 * (native_fs_utils.js:555-566).  Whatever nsswitch is configured with
 * returns -- `files` here, sssd in front of a directory on Scale.  No
 * directory client is written here. */
int nsswitch_lookup(const std::string& name, ResolvedName& out);

/* The resolutions of an identity row's `distinguished_name`, kept for
 * a bounded time.
 *
 * NooBaa calls getpwnam each time it builds a request context, so its
 * staleness is sssd's cache.  A personality is a snapshot taken once
 * per registration, and a hit on one resolves nothing, so the bound
 * cannot come from the request path.  The sweep supplies it:  every
 * `rgw_nsfs_identity_resolve_ttl` seconds it looks up each name held
 * here again, and where any outcome differs -- a new uid or gid, a name
 * that has gone, one that has appeared, a lookup that failed -- it
 * calls note_all_identities_changed(), so personalities registered from
 * the old answer are re-resolved on their next use.  Hits pay nothing,
 * and nothing re-registers unless a resolution changed.
 *
 * Every outcome is bumped for all identities because the cache knows
 * names, not the rows that carry them;  a change in the directory is
 * rare enough for that to be the right price.
 *
 * An entry is never dropped for disuse.  A warm personality never calls
 * resolve(), so disuse here says nothing about whether the identity is
 * still being served, and dropping its entry would stop it being swept.
 * The cache is bounded by the names the identity table has carried.
 *
 * A lookup that fails rather than returning an answer is not cached:
 * the next resolve() tries again, and the caller is refused meanwhile,
 * as NooBaa's request is. */
class NsswitchCache {
public:
  explicit NsswitchCache(CephContext* cct);
  ~NsswitchCache();

  NsswitchCache(const NsswitchCache&) = delete;
  NsswitchCache& operator=(const NsswitchCache&) = delete;

  /* The resolution of `name`:  from the cache, else from the lookup.
   *
   * A lookup with a coroutine in `y` runs on the impersonation pool as
   * the gateway, and the coroutine suspends meanwhile, so a frontend
   * thread is not held for a directory round trip.  With `null_yield`
   * it runs on the calling thread.
   *
   *   0        `out` is filled
   *   -ENOENT  no source knows the name;  cached, like an answer
   *   other    the lookup failed;  not cached
   */
  int resolve(const DoutPrefixProvider* dpp, const std::string& name,
	      ResolvedName& out, optional_yield y);

  /* The names currently resolved to `uid`, for a credential built from
   * a file, which carries a uid and no name. */
  std::vector<std::string> names_for_uid(uint32_t uid) const;

  /* Look every held name up again now.  Returns how many outcomes
   * changed;  identities were invalidated if that is non-zero. */
  int sweep(const DoutPrefixProvider* dpp);

  /* The same for one name, for an operator or provisioning that knows
   * a name changed and will not wait for the sweep.  Returns 1 if the
   * outcome changed, 0 if not, or the lookup's -errno, in which case
   * the entry is dropped and identities are invalidated. */
  int refresh(const DoutPrefixProvider* dpp, const std::string& name);

  /* Drop `name`'s entry, so the next resolve() looks it up.  Where there
   * was one, identities are invalidated, so a warm personality resolves
   * again too.  Returns whether there was.  For the `nsswitch-cache` driver
   * hint;  the bound does not rely on anyone calling it. */
  bool forget(const std::string& name);

  /* The names to resolve when the sweep starts. */
  using NameSource =
    std::function<std::vector<std::string>(const DoutPrefixProvider*)>;

  /* The sweep's thread.  Its first act is to resolve every name `source`
   * returns, logging each nsswitch does not know;  that runs on the
   * thread so it delays nothing.  Idempotent;  stop() is also the
   * destructor's. */
  void start(NameSource source = {});
  void stop();

  /* Internal:  replace the nsswitch call, for the unit tests.  The
   * entries held are dropped, since they came from the old one. */
  void set_lookup_for_test(NameLookupFn fn);

private:
  struct Entry {
    int ret{0};			/* 0 or -ENOENT */
    ResolvedName name;
  };

  CephContext* const cct;
  mutable std::mutex mtx;
  std::map<std::string, Entry> entries;
  NameLookupFn lookup;

  std::mutex sweep_mtx;
  std::condition_variable sweep_cv;
  std::thread sweeper;
  bool stopping{false};

  int look_up(const std::string& name, ResolvedName& out) const;
  /* Store an outcome:  true if it differs from what was held. */
  bool store(const std::string& name, int ret, const ResolvedName& rn);
  bool drop(const std::string& name);
  void sweep_loop(NameSource source);
};

}}} // namespace rgw::sal::nsfs
