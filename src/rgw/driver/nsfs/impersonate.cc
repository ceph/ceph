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

#include "common/code_environment.h"
#include "common/errno.h"
#include "global/global_context.h"
#include "include/ceph_assert.h"
#include "common/async/yield_waiter.h"

#include "driver/posix/posix_io_uring.h"

#include "rgw_sal.h"

#include "impersonate.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

bool impersonation_configured()
{
  return g_conf().get_val<bool>("rgw_nsfs_impersonate");
}

bool impersonation_enabled()
{
  if (! impersonation_configured()) {
    return false;
  }

  /* Configured is not the same as in effect, and the difference is
   * not cosmetic:  the callers of this refuse a read when no
   * personality is pinned, because serving it as the daemon is the
   * escalation the design exists to prevent.  In a process where
   * nothing will ever pin one, that refusal denies everything.
   *
   * Two such processes.  radosgw-admin runs as the operator and
   * serves no authenticated identity.  An embedding library binds one
   * uid at mount -- `RGWLibFS` -- and likewise has no per-request
   * identity;  it needs its own test because librgw initialises with
   * CODE_ENVIRONMENT_DAEMON and `g_code_env` cannot see it.
   *
   * The startup gate asks `impersonation_configured()` instead, so a
   * gateway missing the capabilities still refuses to start rather
   * than quietly serving every request as itself. */
  if (g_code_env != CODE_ENVIRONMENT_DAEMON) {
    return false;
  }
  return ! rgw::sal::embedded();
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
    /* An older kernel responds by rewriting hdr.version rather than
     * filling data.  Either way we cannot tell, and cannot tell is
     * not the same as absent -- say so rather than guess. */
    if (missing) {
      *missing = "capget() failed; capabilities could not be determined";
    }
    return false;
  }

  /* Permitted, not effective.  Nothing starts effective here -- the
   * capabilities are granted `+p` so that no thread begins able to
   * bypass DAC -- and permitted is what makes a raise possible, so
   * it is the right question:  can this process assume an identity
   * when it comes to it?  Asking about effective refused to start a
   * correctly configured gateway (run 2026-10-16). */
  const uint32_t permitted = data[0].permitted;
  const bool setuid = permitted & (1u << CAP_SETUID);
  const bool setgid = permitted & (1u << CAP_SETGID);

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

    /* Only if nothing has saved the set yet.  raise_switch_caps()
     * runs first and holds the thread's own set;  re-reading here
     * would save the *raised* one, and the restore would then leave
     * CAP_SETUID and CAP_SETGID effective on a thread that started
     * with neither. */
    if (!caps_cleared && ::syscall(SYS_capget, &hdr, saved_caps) < 0) {
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

  /* Put CAP_SETUID and CAP_SETGID in the effective set, from
   * permitted, saving what was there first.
   *
   * Permitted survives a change between two non-root uids -- measured
   * in probes/results-capprobe-2026-10-01.txt -- so the restore can
   * raise them again after the identity has been assumed. */
  int raise_switch_caps() {
    struct __user_cap_header_struct hdr = {};
    hdr.version = _LINUX_CAPABILITY_VERSION_3;
    hdr.pid = 0;

    if (!caps_cleared && ::syscall(SYS_capget, &hdr, saved_caps) < 0) {
      return -errno;
    }
    __user_cap_data_struct raised[2];
    raised[0] = saved_caps[0];
    raised[1] = saved_caps[1];
    raised[0].effective |= (1u << CAP_SETUID) | (1u << CAP_SETGID);
    raised[0].effective &= raised[0].permitted;
    raised[1].effective &= raised[1].permitted;
    if (::syscall(SYS_capset, &hdr, raised) < 0) {
      return -errno;
    }
    caps_cleared = true;		/* saved_caps is authoritative */
    return 0;
  }

  /* Returns 0, or -errno from the call that failed. */
  int restore_effective() {
    if (!caps_cleared) {
      return 0;
    }
    struct __user_cap_header_struct hdr = {};
    hdr.version = _LINUX_CAPABILITY_VERSION_3;
    hdr.pid = 0;
    if (::syscall(SYS_capset, &hdr, saved_caps) < 0) {
      return -errno;
    }
    caps_cleared = false;
    return 0;
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

    /* The switch needs CAP_SETUID and CAP_SETGID *effective*, and
     * nothing is effective here:  the gateway's capabilities are
     * granted permitted-only, so that no thread starts able to
     * bypass DAC.  Raise the two for the three calls below, and
     * clear them again at the end -- which is the same clear that
     * has always had to happen before a personality is registered. */
    if (int ret = raise_switch_caps(); ret < 0) {
      return ret;
    }

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
    /* capabilities first:  putting the credentials back needs
     * CAP_SETUID and CAP_SETGID effective, and the call that assumed
     * the identity cleared them. */
    int ret = raise_switch_caps();
    /* then uid:  dropping it last would leave no privilege to
     * restore the others with. */
    if (ret == 0 && ::syscall(SYS_setresuid, -1, saved_uid, -1) < 0) {
      ret = -errno;
    }
    if (ret == 0 && ::syscall(SYS_setresgid, -1, saved_gid, -1) < 0) {
      ret = -errno;
    }
    if (ret == 0 &&
	::syscall(SYS_setgroups, saved_groups.size(),
		  saved_groups.empty() ? nullptr : saved_groups.data()) < 0) {
      ret = -errno;
    }
    /* and the effective set back to what this thread had, which is
     * empty but for whatever it holds */
    if (ret == 0) {
      ret = restore_effective();
    }
    if (ret < 0) {
      /* The thread is still running as the identity, and whatever
       * runs on it next runs as that identity too:  another request
       * on this frontend worker, another job on this pool thread.
       * That is one identity served as another, which is what the
       * switch exists to prevent, and it would be silent.
       *
       * There is nothing to fall back to.  The calls that would put
       * the credentials back are the ones that just failed, and they
       * cannot normally fail here -- the capabilities they need were
       * restored first. */
      ceph_abort_msg(std::string("nsfs: could not restore this thread's "
				 "credentials: ") + cpp_strerror(-ret));
    }
  }
};

/* Raise and drop, around one call.
 *
 * The same shape as ScopedCredentials and for the same reason:  the
 * change is per-task, it lasts exactly as long as the object, and it
 * is put back by a destructor rather than by every return path. */
class ScopedCapabilities {
  __user_cap_data_struct saved[2] = {};
  bool armed{false};

public:
  /* 0, -EPERM when a requested capability is not permitted, or
   * -errno from capget/capset. */
  int push(uint64_t caps, uint64_t* missing) {
    struct __user_cap_header_struct hdr = {};
    hdr.version = _LINUX_CAPABILITY_VERSION_3;
    hdr.pid = 0;
    if (::syscall(SYS_capget, &hdr, saved) < 0) {
      return -errno;
    }

    const uint32_t lo = static_cast<uint32_t>(caps & 0xffffffffull);
    const uint32_t hi = static_cast<uint32_t>(caps >> 32);
    const uint32_t absent_lo = lo & ~saved[0].permitted;
    const uint32_t absent_hi = hi & ~saved[1].permitted;
    if (absent_lo || absent_hi) {
      if (missing) {
	*missing = (static_cast<uint64_t>(absent_hi) << 32) | absent_lo;
      }
      return -EPERM;
    }

    __user_cap_data_struct raised[2];
    raised[0] = saved[0];
    raised[1] = saved[1];
    raised[0].effective |= lo;
    raised[1].effective |= hi;
    if (::syscall(SYS_capset, &hdr, raised) < 0) {
      return -errno;
    }
    armed = true;
    return 0;
  }

  ~ScopedCapabilities() {
    if (!armed) {
      return;
    }
    struct __user_cap_header_struct hdr = {};
    hdr.version = _LINUX_CAPABILITY_VERSION_3;
    hdr.pid = 0;
    if (::syscall(SYS_capset, &hdr, saved) < 0) {
      /* The thread keeps the capability, and so does everything that
       * runs on it next:  another request on this frontend worker.
       * That is the gateway's whole authority handed to an ordinary
       * call, silently.  There is nothing to fall back to -- the call
       * that would put the set back is the one that just failed. */
      ceph_abort_msg(std::string("nsfs: could not drop raised "
				 "capabilities: ") + cpp_strerror(errno));
    }
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

void note_all_identities_changed()
{
  for (auto& g : generations) {
    g.fetch_add(1, std::memory_order_relaxed);
  }
}

uint64_t identity_generation(const rgw_user& key)
{
  return generations[generation_shard(key)].load(std::memory_order_relaxed);
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

/* Which identity that thread assumed.
 *
 * `tl_impersonating` says only *that* a pool thread is running
 * somebody's work.  A nested run_as() asking for a *different*
 * identity then took the fast path below and ran as the outer one,
 * silently:  the operation succeeded, under the wrong uid, and
 * nothing said so.  Recording who makes the two cases
 * distinguishable.
 *
 * A pointer, not a copy:  the job outlives the call it is running,
 * and copying the group vector on every nested call would cost more
 * than the comparison it serves. */
thread_local const Credentials* tl_current_cred;

namespace {
bool same_identity(const Credentials& a, const Credentials& b)
{
  return (a.uid == b.uid) && (a.gid == b.gid) && (a.groups == b.groups);
}
}

/* The pool behind run_as().
 *
 * Deliberately plain:  it exists to be deleted once the kernel can
 * express listxattr and removexattr on a ring (see the header, and
 * docs/IMPERSONATED_XATTR.md), so it is not worth more machinery
 * than it takes to be correct.  One thread per concurrent call, up
 * to a bound.  A caller with a coroutine suspends while its call
 * runs, so its frontend thread serves other requests;  one without
 * blocks.
 */
class ImpersonationPool {
  struct Job {
    const Credentials* cred;
    const ImpersonatedFn* fn;
    int result{0};
    bool done{false};
    std::mutex m;
    ceph::async::yield_waiter<int> waiter;	/* a caller that suspends */
    std::condition_variable cv;			/* one that blocks */
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
	  tl_current_cred = job->cred;
	  ret = (*job->fn)();
	  tl_current_cred = nullptr;
	  tl_impersonating = false;
	}
      }
      /* Wake the caller under the lock, and touch nothing after it.
       * `job` is a local of run(), so once the lock is released the
       * caller may have seen `done`, returned and destroyed it.
       * Completing the waiter under the lock is safe because this is
       * not an executor thread:  complete() posts the handler, which
       * takes the lock again before the coroutine resumes. */
      {
	std::lock_guard lock(job->m);
	job->result = ret;
	job->done = true;
	if (job->waiter) {
	  job->waiter.complete(boost::system::error_code{}, ret);
	}
	job->cv.notify_one();
      }
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

  int run(const Credentials& cred, const ImpersonatedFn& fn,
	  optional_yield y) {
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
    if (! job.done) {
      /* Suspend only where nothing can cancel the wait.  `fn`
       * captures the caller's frame, and a cancelled wait would
       * unwind that frame while a helper is still running `fn` in
       * it.  The frontend spawns requests with no cancellation slot,
       * so this is the path they take;  a coroutine that has one
       * blocks instead, which is safe. */
      if (y && ! y.get_yield_context().get_cancellation_slot().is_connected()) {
	job.result = job.waiter.async_wait(lock, y.get_yield_context());
      } else {
	job.cv.wait(lock, [&job] { return job.done; });
      }
    }
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
	   const ImpersonatedFn& fn, optional_yield y)
{
  if (tl_impersonating) {
    /* The fast path is only correct for the identity in force.  For
     * any other it would run the work as the wrong one, so refuse:
     * enqueueing instead is what the comment above rules out. */
    if (tl_current_cred && same_identity(*tl_current_cred, cred)) {
      return fn();
    }
    ldpp_dout(dpp, 0) << "ERROR: refusing to run as uid " << cred.uid
      << " on a thread already running as uid "
      << (tl_current_cred ? tl_current_cred->uid : (uid_t)-1)
      << ";  the work would have run as the wrong identity" << dendl;
    return -EDEADLK;
  }
  static ImpersonationPool pool;
  const int ret = pool.run(cred, fn, y);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "impersonated call as uid " << cred.uid
      << " returned " << cpp_strerror(-ret) << dendl;
  }
  return ret;
}

int with_identity(const DoutPrefixProvider* dpp, const FSIdentity& id,
		  const ImpersonatedFn& fn, optional_yield y)
{
  if (! id.active()) {
    return fn();
  }
  return run_as(dpp, id.cred, fn, y);
}

bool capability_is_effective(int cap)
{
  struct __user_cap_header_struct hdr = {};
  struct __user_cap_data_struct data[2] = {};
  hdr.version = _LINUX_CAPABILITY_VERSION_3;
  hdr.pid = 0;
  if (::syscall(SYS_capget, &hdr, data) < 0) {
    return false;
  }
  const int word = cap >> 5;
  return (data[word].effective & (1u << (cap & 31))) != 0;
}

/* Set once this thread holds CAP_DAC_READ_SEARCH, so the common path
 * is a branch rather than two syscalls. */
static thread_local bool tl_holds_read_search;

int hold_thread_capabilities(const DoutPrefixProvider* dpp)
{
  if (tl_holds_read_search) {
    return 0;
  }

  struct __user_cap_header_struct hdr = {};
  struct __user_cap_data_struct data[2] = {};
  hdr.version = _LINUX_CAPABILITY_VERSION_3;
  hdr.pid = 0;
  if (::syscall(SYS_capget, &hdr, data) < 0) {
    return -errno;
  }

  const uint32_t want = 1u << CAP_DAC_READ_SEARCH;
  if (!(data[0].permitted & want)) {
    /* not granted at all.  Not an error here:  a gateway may run
     * without it and refuse the reads that need it, which is what
     * the open's own error says. */
    tl_holds_read_search = true;
    return 0;
  }
  if (data[0].effective & want) {
    tl_holds_read_search = true;
    return 0;
  }

  data[0].effective |= want;
  if (::syscall(SYS_capset, &hdr, data) < 0) {
    return -errno;
  }
  tl_holds_read_search = true;
  ldpp_dout(dpp, 20) << "nsfs: this thread now holds CAP_DAC_READ_SEARCH"
    << dendl;
  return 0;
}

int with_capabilities(const DoutPrefixProvider* dpp, uint64_t caps,
		      const ImpersonatedFn& fn)
{
  if (caps == 0) {
    return fn();
  }

  if (tl_impersonating) {
    ldpp_dout(dpp, 0) << "ERROR: refusing to raise capabilities 0x"
      << std::hex << caps << std::dec << " on a thread running as uid "
      << (tl_current_cred ? tl_current_cred->uid : (uid_t)-1)
      << ";  the capability would be the identity's" << dendl;
    return -EPERM;
  }

  ScopedCapabilities raised;
  uint64_t missing = 0;
  const int ret = raised.push(caps, &missing);
  if (ret == -EPERM) {
    ldpp_dout(dpp, 0) << "ERROR: capabilities 0x" << std::hex << missing
      << std::dec << " are not in this gateway's permitted set, so they "
      "cannot be raised;  the gateway was started without them (see "
      "have_credential_capabilities() and the setcap step)" << dendl;
    return -EPERM;
  }
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not raise capabilities 0x"
      << std::hex << caps << std::dec << ": " << cpp_strerror(-ret)
      << dendl;
    return -EIO;
  }

  return fn();
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

int open_as(const DoutPrefixProvider* dpp, const FSIdentity& id,
	    int dirfd, const char* path, uint64_t flags, uint64_t mode)
{
  PersonalityRef ref;
  const int ret = acquire_personality(dpp, id.key, id.cred, &ref);
  if (ret < 0) {
    return ret;
  }
  return rgw::sal::posix_uring_openat2(dpp, dirfd, path, flags, mode,
				       ref.id());
}

int IdentityPersonality::acquire(const DoutPrefixProvider* dpp, uint16_t* out)
{
  const int ret = acquire_personality(dpp, id.key, id.cred, &ref);
  if (ret < 0) {
    return ret;
  }
  *out = ref.id();
  return 0;
}

int IdentityPersonality::run_as(const DoutPrefixProvider* dpp,
				fu2::unique_function<int() const> fn,
				optional_yield y)
{
  return nsfs::run_as(dpp, id.cred, fn, y);
}

} } } // namespace rgw::sal::nsfs
