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
#include <linux/capability.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "common/ceph_context.h"
#include <array>
#include <atomic>

#include "common/errno.h"
#include "global/global_context.h"

#include "driver/posix/posix_io_uring.h"

#include "impersonate.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

bool impersonation_enabled()
{
  return g_conf().get_val<bool>("rgw_nsfs_impersonate");
}

bool have_credential_capabilities(std::string* missing)
{
  /* capget(2) directly rather than libcap, which Ceph does not link.
   * Version 3 is the current ABI and reports two 32-bit words; the
   * capabilities we need are both in the first. */
  struct __user_cap_header_struct hdr = {};
  struct __user_cap_data_struct data[2] = {};

  hdr.version = _LINUX_CAPABILITY_VERSION_3;
  hdr.pid = 0;				/* this task */

  if (::syscall(SYS_capget, &hdr, data) < 0) {
    /* An older kernel answers by rewriting hdr.version rather than
     * filling data.  Either way we cannot tell, and cannot tell is
     * not the same as absent -- say so rather than guess. */
    if (missing) {
      *missing = "capget() failed; capabilities could not be determined";
    }
    return false;
  }

  const uint32_t effective = data[0].effective;
  const bool setuid = effective & (1u << CAP_SETUID);
  const bool setgid = effective & (1u << CAP_SETGID);

  if (setuid && setgid) {
    return true;
  }
  if (missing) {
    if (!setuid && !setgid) {
      *missing = "CAP_SETUID and CAP_SETGID";
    } else {
      *missing = setuid ? "CAP_SETGID" : "CAP_SETUID";
    }
  }
  return false;
}

} } } // namespace rgw::sal::nsfs

namespace rgw { namespace sal { namespace nsfs {

namespace {

/* Save and restore of the calling task's credentials.
 *
 * The raw syscalls, not glibc's wrappers.  POSIX requires
 * setresuid(2) and friends to apply process-wide, and glibc honours
 * that by signalling every other thread -- which would make a
 * momentary change here a momentary change everywhere.  The raw
 * syscalls are per-task, which is what confines this to the one
 * thread for the one call.  NooBaa's ThreadScope bypasses libc for
 * exactly this reason (src/native/util/os_linux.cpp).
 */
class ScopedCredentials {
  std::vector<gid_t> saved_groups;
  uid_t saved_uid{0};
  gid_t saved_gid{0};
  __user_cap_data_struct saved_caps[2] = {};
  bool armed{false};
  bool caps_cleared{false};

  /* Clear the effective capability set, keeping permitted.
   *
   * Measured 2026-10-01 (probes/results-capprobe-2026-10-01.txt):
   * io_uring_register_personality() copies current_cred() whole,
   * and a setresuid between two non-zero uids clears nothing --
   * so a personality registered here would carry the gateway's
   * capabilities as well as the identity's uid.  With
   * CAP_DAC_READ_SEARCH held for listings, every impersonated read
   * would then bypass DAC while appearing to enforce it.
   *
   * Clearing effective leaves permitted intact, so it can always
   * be raised again -- which is what restore() does. */
  int clear_effective() {
    struct __user_cap_header_struct hdr = {};
    hdr.version = _LINUX_CAPABILITY_VERSION_3;
    hdr.pid = 0;

    if (::syscall(SYS_capget, &hdr, saved_caps) < 0) {
      return -errno;
    }
    __user_cap_data_struct cleared[2];
    cleared[0] = saved_caps[0];
    cleared[1] = saved_caps[1];
    cleared[0].effective = 0;
    cleared[1].effective = 0;
    if (::syscall(SYS_capset, &hdr, cleared) < 0) {
      return -errno;
    }
    caps_cleared = true;
    return 0;
  }

  void restore_effective() {
    if (!caps_cleared) {
      return;
    }
    struct __user_cap_header_struct hdr = {};
    hdr.version = _LINUX_CAPABILITY_VERSION_3;
    hdr.pid = 0;
    ::syscall(SYS_capset, &hdr, saved_caps);
    caps_cleared = false;
  }

public:
  int push(const Credentials& cred) {
    int n = ::getgroups(0, nullptr);
    if (n < 0) {
      return -errno;
    }
    saved_groups.resize(n);
    if (n > 0 && ::getgroups(n, saved_groups.data()) < 0) {
      return -errno;
    }
    saved_uid = ::geteuid();
    saved_gid = ::getegid();

    /* Groups first, then gid, then uid:  each step gives up
     * privilege, so the order is the only one where every call is
     * still permitted when it runs. */
    if (::syscall(SYS_setgroups, cred.groups.size(),
		  cred.groups.empty() ? nullptr : cred.groups.data()) < 0) {
      return -errno;
    }
    armed = true;		/* groups changed:  must restore */
    if (::syscall(SYS_setresgid, -1, cred.gid, -1) < 0) {
      return -errno;
    }
    if (::syscall(SYS_setresuid, -1, cred.uid, -1) < 0) {
      return -errno;
    }

    /* Last, and only after the identity change:  assuming the
     * identity needs CAP_SETUID and CAP_SETGID, so the capabilities
     * cannot go before this. */
    return clear_effective();
  }

  ~ScopedCredentials() {
    if (!armed) {
      return;
    }
    /* capabilities first:  restoring the group vector needs
     * CAP_SETGID, which was just cleared. */
    restore_effective();
    /* then uid:  dropping it last would leave no privilege to
     * restore the others with. */
    ::syscall(SYS_setresuid, -1, saved_uid, -1);
    ::syscall(SYS_setresgid, -1, saved_gid, -1);
    ::syscall(SYS_setgroups, saved_groups.size(),
	      saved_groups.empty() ? nullptr : saved_groups.data());
  }
};

/* Registration against this thread's ring.  The table knows nothing
 * about io_uring and this knows nothing about identities. */
class RingRegistrar : public PersonalityTable::Registrar {
public:
  int register_personality(const DoutPrefixProvider* dpp,
			   const Credentials& cred) override {
    ScopedCredentials scope;
    const int ret = scope.push(cred);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not assume uid " << cred.uid
	<< " gid " << cred.gid << " to register a personality: "
	<< cpp_strerror(-ret) << dendl;
      return ret;
    }
    return posix_uring_register_personality(dpp);
  }

  void unregister_personality(const DoutPrefixProvider* dpp,
			      uint16_t id) override {
    posix_uring_unregister_personality(dpp, id);
  }
};

thread_local RingRegistrar tl_registrar;

} // namespace

/* The generation shards.
 *
 * 256 counters, indexed by the same XXH3 hash the personality table
 * uses for its map, so two identities collide here only if they
 * collide in the low byte of that hash.  Relaxed ordering is enough:
 * a reader that sees the old value serves one more request under the
 * old credentials and sees the new value on the next lookup, which
 * is the same window an operator already has between issuing the
 * Admin Ops call and the next request arriving. */
static constexpr size_t GENERATION_SHARDS = 256;
static std::array<std::atomic<uint64_t>, GENERATION_SHARDS> generations{};

static size_t generation_shard(const rgw_user& key)
{
  return ankerl::unordered_dense::hash<rgw_user>{}(key) %
      GENERATION_SHARDS;
}

void note_identity_changed(const rgw_user& key)
{
  generations[generation_shard(key)].fetch_add(1, std::memory_order_relaxed);
}

namespace {
struct ShardedGenerations : GenerationSource {
  uint64_t generation(const rgw_user& key) const override {
    return generations[generation_shard(key)].load(
	std::memory_order_relaxed);
  }
};
ShardedGenerations tl_generations;
} // namespace

PersonalityTable& thread_personality_table()
{
  /* Sized from configuration on first use, which is the first time
   * this thread serves an impersonated request. */
  thread_local PersonalityTable table{
      g_conf().get_val<uint64_t>("rgw_nsfs_personality_table_size"),
      &tl_registrar, &tl_generations};
  return table;
}

thread_local bool tl_impersonating;

/* The pool behind run_as().
 *
 * Deliberately plain:  it exists to be deleted once the kernel can
 * express listxattr and removexattr on a ring (see the header, and
 * docs/IMPERSONATED_XATTR.md), so it is not worth more machinery
 * than it takes to be correct.  One thread per concurrent call, up
 * to a bound;  callers block.
 */
class ImpersonationPool {
  struct Job {
    const Credentials* cred;
    const ImpersonatedFn* fn;
    int result{0};
    bool done{false};
    std::mutex m;
    std::condition_variable cv;
  };

  std::mutex mtx;
  std::condition_variable work;
  std::deque<Job*> queue;
  std::vector<std::thread> threads;
  bool stopping{false};
  size_t idle{0};
  size_t max_threads{0};

  void worker() {
    for (;;) {
      Job* job = nullptr;
      {
	std::unique_lock lock(mtx);
	++idle;
	work.wait(lock, [this] { return stopping || !queue.empty(); });
	--idle;
	if (stopping && queue.empty()) {
	  return;
	}
	job = queue.front();
	queue.pop_front();
      }

      int ret;
      {
	ScopedCredentials scope;
	ret = scope.push(*job->cred);
	if (ret == 0) {
	  tl_impersonating = true;
	  ret = (*job->fn)();
	  tl_impersonating = false;
	}
      }
      {
	std::lock_guard lock(job->m);
	job->result = ret;
	job->done = true;
      }
      job->cv.notify_one();
    }
  }

public:
  ImpersonationPool() {
    max_threads = g_conf().get_val<uint64_t>(
	"rgw_nsfs_impersonate_helper_threads");
    if (max_threads == 0) {
      max_threads = 1;
    }
  }

  ~ImpersonationPool() {
    {
      std::lock_guard lock(mtx);
      stopping = true;
    }
    work.notify_all();
    for (auto& t : threads) {
      if (t.joinable()) {
	t.join();
      }
    }
  }

  int run(const Credentials& cred, const ImpersonatedFn& fn) {
    Job job;
    job.cred = &cred;
    job.fn = &fn;
    {
      std::lock_guard lock(mtx);
      queue.push_back(&job);
      /* grow only when nobody is free to take it */
      if (idle == 0 && threads.size() < max_threads) {
	threads.emplace_back([this] { worker(); });
      }
    }
    work.notify_one();

    std::unique_lock lock(job.m);
    job.cv.wait(lock, [&job] { return job.done; });
    return job.result;
  }
};

/* Set while a pool thread is running somebody else's work.
 *
 * Without it a nested call -- an operation that wraps itself and
 * then calls something that wraps itself -- would enqueue to the
 * pool from a pool thread and wait for it.  Enough of those at once
 * and every thread is blocked waiting for a job no thread is free
 * to take.  A thread already running as the identity simply makes
 * the call. */
int run_as(const DoutPrefixProvider* dpp, const Credentials& cred,
	   const ImpersonatedFn& fn)
{
  if (tl_impersonating) {
    return fn();
  }
  static ImpersonationPool pool;
  const int ret = pool.run(cred, fn);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "impersonated call as uid " << cred.uid
      << " returned " << cpp_strerror(-ret) << dendl;
  }
  return ret;
}

int with_identity(const DoutPrefixProvider* dpp, const FSIdentity& id,
		  const ImpersonatedFn& fn)
{
  if (! id.active()) {
    return fn();
  }
  return run_as(dpp, id.cred, fn);
}

int find_personality(const rgw_user& key, PersonalityRef* out,
		     const DoutPrefixProvider* dpp)
{
  return thread_personality_table().find(key, out, dpp);
}

int acquire_personality(const DoutPrefixProvider* dpp, const rgw_user& key,
			const Credentials& cred, PersonalityRef* out)
{
  auto& table = thread_personality_table();
  if (table.find(key, out) == 0) {
    return 0;
  }
  return table.insert(dpp, key, cred, out);
}

} } } // namespace rgw::sal::nsfs
