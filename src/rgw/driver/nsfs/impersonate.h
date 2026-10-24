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

#include <string>

#include "include/function2.hpp"
#include "common/dout.h"
#include "driver/posix/posix_io_uring.h"

#include "access_strategy.h"
#include "personality.h"

namespace rgw { namespace sal { namespace nsfs {

/* No identity in `o`:  the empty user, which is what
 * rgw::sal::no_authenticated_user holds, or an empty account id. */
inline bool owner_empty(const rgw_owner& o)
{
  return std::visit([](const auto& id) { return id.empty(); }, o);
}

/* Whether this task may change its own credentials.
 *
 * Registering an io_uring personality copies the credentials of the
 * calling task, so minting one for an identity means becoming that
 * identity for the length of one syscall.  That needs CAP_SETUID and
 * CAP_SETGID.
 *
 * Checked rather than assumed, because the alternative is a gateway
 * that starts cleanly, serves every request unimpersonated, and
 * looks correct while giving each of them the daemon's filesystem
 * reach.
 *
 * Capabilities are per-thread, and a thread inherits the creating
 * thread's sets, so holding them here -- before the frontend workers
 * exist -- is what makes them available to the workers that will
 * register.  `missing` names what is absent, for the operator.
 *
 * The *permitted* set is what this asks about.  Nothing starts
 * effective -- see hold_thread_capabilities() -- and permitted is
 * what a thread raises from, so permitted is the question that
 * matters:  can this process assume an identity when it comes to it?
 * Asking about effective kept a correctly configured gateway from
 * starting (run 2026-10-16).
 */
bool have_credential_capabilities(std::string* missing);

/* Whether requests are served as their own POSIX identity:
 * `rgw_nsfs_impersonate`, as configured.  The switch is the hinge, not
 * the presence of identity records:  records arrive and depart over
 * Admin Ops at any time, a deployment's intent does not.
 *
 * A process that will never pin a personality -- radosgw-admin, or an
 * embedding library that binds one uid at mount -- is not configured
 * with it.  The callers of this deny a read when no personality is
 * pinned, so such a process configured with it denies every read.
 *
 * Followed at runtime, and cheap:  an attribute read asks on every
 * call. */
bool impersonation_enabled();

/* The personality table for the calling thread's io_uring ring.
 *
 * One per ring, because an id names a credential on the ring it was
 * registered against and means nothing on any other.  No locking:
 * a ring has a single submitting task, so its table does too. */
PersonalityTable& thread_personality_table();

/* Obtain a pinned personality for `key`, registering it if this
 * ring has not seen it.
 *
 * Registration becomes `cred` for the length of one syscall, which
 * is the only moment any thread here changes identity -- and it is
 * per identity per ring, not per request.
 *
 *   0        `out` is pinned;  out.id() goes on the SQE
 *   -EBUSY   every slot is held by a live operation
 *   < 0      the registration failed
 */
int acquire_personality(const DoutPrefixProvider* dpp, const rgw_owner& key,
			const Credentials& cred, PersonalityRef* out);

/* Say that an identity's record has changed.
 *
 * Called by whatever writes the identity table -- the Admin Ops PUT
 * and DELETE -- so that personalities already registered from the
 * old record stop being served.  Cheap enough to call
 * unconditionally;  it is one relaxed atomic increment.
 *
 * NOT global.  The counters are a small sharded array indexed by a
 * hash of the key, so a change to one identity leaves most others
 * alone.  A collision costs one re-registration of an unrelated
 * identity, which is the same thing a single global counter would
 * cost everybody, so sharding is strictly the weaker hammer at the
 * same price:  the read on the request path is one relaxed load
 * either way. */
void note_identity_changed(const rgw_owner& key);

/* Every identity's record may have changed.
 *
 * For a change to an *account's* row.  Personalities are cached under
 * the user a request authenticated as, and a user with no row of its
 * own was resolved through its account's, so bumping the account's
 * own shard reaches none of them.  Nothing records which users went
 * through which account, so every shard is bumped:  each thread then
 * re-resolves an identity once, on its next use.  Account rows change
 * by operator action, which is rare enough for that to be the right
 * price. */
void note_all_identities_changed();

/* The current generation `key` is read at, for tests.  A personality
 * registered at one generation is re-resolved once this moves. */
uint64_t identity_generation(const rgw_owner& key);

/* The level-1 lookup on its own:  a pinned personality if this ring
 * already has one for `key`, without resolving credentials.
 *
 * Exposed separately because resolving them is a database read on
 * every request, and a hit needs none -- the slot carries the
 * credentials it was registered with.
 *
 *   0        `out` is pinned;  out.credentials() is usable
 *   -ENOENT  this ring has not seen `key`
 */
int find_personality(const rgw_owner& key, PersonalityRef* out,
		     const DoutPrefixProvider* dpp = nullptr);

/* Run one filesystem call as `cred`, on a thread that does nothing
 * else.
 *
 * **For the operations io_uring cannot express.**  Three of them,
 * with three different owners -- see docs/IMPERSONATED_XATTR.md:
 *
 *   - listxattr and removexattr have no opcode.  Two opcodes in
 *     io_uring/xattr.c would close it;  that is for the nsfs work
 *     to propose upstream, and a loadable module offering them
 *     through uring_cmd could ship in the meantime.
 *   - On GPFS the attribute path does not use the xattr syscalls at
 *     all:  GPFSStrategy builds a gpfsFcntlHeader_t and calls
 *     gpfs_fcntl(), a vendor ioctl no opcode will ever cover.  The
 *     mechanism that would is f_op->uring_cmd on GPFS files, and
 *     that is IBM's to implement.
 *
 * So this is not a stopgap to be deleted on the next kernel.  On
 * the target platform it carries every attribute operation until
 * that last item exists.  It is still worth keeping narrow:  route
 * a call here only when the ring genuinely cannot express it.
 *
 * Why a separate pool rather than the calling thread:  a request
 * thread that changed identity would be somebody else while other
 * coroutines resumed on it.  A pool thread serves one call at a
 * time and has no other work to contaminate, so the hazard does not
 * arise -- which is the same reasoning that lets registration
 * briefly assume an identity.
 *
 * **The caller suspends rather than blocks** when `y` is a coroutine:
 * its frontend thread serves other requests while the helper works,
 * which matters most where the work is long -- a copy of a whole
 * object, on a filesystem with no range clone.  With `null_yield` it
 * blocks, which is what a caller with no coroutine -- librgw,
 * startup, a driver hint -- already does for every syscall.
 *
 * `fn` runs on the helper, so it must not use `y` or anything else
 * that would suspend the caller's coroutine:  that coroutine is not
 * running on the helper's thread.  A nested with_identity() inside
 * `fn` passes `null_yield`;  it takes the fast path below and never
 * waits.
 *
 * Returns what `fn` returned, or -errno if the credentials could
 * not be assumed.
 */
/* The shape of an impersonated operation.
 *
 * The `const` is inside the signature because these are invoked
 * through a const reference;  see `split_func_t` in rgw_auth_s3.cc
 * for the same spelling. */
using ImpersonatedFn = fu2::unique_function<int() const>;

int run_as(const DoutPrefixProvider* dpp, const Credentials& cred,
	   const ImpersonatedFn& fn, optional_yield y);

/* What an operation needs to be performed as somebody.
 *
 * Who, not a personality id.  An id names a credential on one
 * thread's ring, and a request coroutine may resume on another thread
 * after any yield, so an id is looked up where it is used --
 * acquire_personality() on `key`, registering from `cred` on a miss --
 * and held only until that submission returns.  A call io_uring
 * cannot express is run by a helper thread that assumes `cred`
 * itself. */
struct FSIdentity {
  rgw_owner key;
  Credentials cred;

  bool active() const { return !owner_empty(key); }

  /* No identity:  the gateway's own credentials.  A reference to bind,
   * where `FSIdentity{}` in a ternary would be a temporary and the
   * reference would dangle. */
  static const FSIdentity& none() {
    static const FSIdentity n;
    return n;
  }
};

/* openat2 through the ring as `id`.  The personality is looked up on
 * this thread, because an id names a credential only on the ring it was
 * registered on, and is held only until the submission returns.  A
 * descriptor, or -errno. */
int open_as(const DoutPrefixProvider* dpp, const FSIdentity& id,
	    int dirfd, const char* path, uint64_t flags, uint64_t mode);

/* An FSIdentity as an io_uring window's transfers need it.
 *
 * acquire() looks the identity up in the calling thread's table, so
 * the id is valid on the ring the window is about to submit to;
 * release() unpins it.  `id` must outlive this. */
class IdentityPersonality : public rgw::sal::UringPersonality {
  const FSIdentity& id;
  PersonalityRef ref;
public:
  explicit IdentityPersonality(const FSIdentity& id) : id(id) {}
  int acquire(const DoutPrefixProvider* dpp, uint16_t* out) override;
  void release() override { ref.reset(); }
  int run_as(const DoutPrefixProvider* dpp,
	     fu2::unique_function<int() const> fn,
	     optional_yield y) override;
};

/* Perform `fn` as this identity.
 *
 * Takes the whole operation rather than a single call, deliberately.
 * The operations that cannot go on the ring cost a thread hop, and
 * an attribute read is a listing plus one fetch per name -- paying
 * the hop once for the operation rather than once per syscall is
 * the difference between acceptable and not.
 *
 * With no identity it simply calls `fn`, so an unimpersonated
 * gateway runs exactly the code it ran before.
 *
 * This is the one place that decides *how* something is
 * impersonated.  Callers say what they want done. */
int with_identity(const DoutPrefixProvider* dpp, const FSIdentity& id,
		  const ImpersonatedFn& fn, optional_yield y);

/* Put `CAP_DAC_READ_SEARCH` in this thread's effective set, once.
 *
 * **Nothing starts effective.**  The gateway's capabilities are
 * granted permitted-only -- `setcap ...+p`, no `e` -- because a
 * capability set carries one effective flag for the whole of it, so
 * `+ep` would make all six effective on every thread and (n)'s
 * *"the capabilities sit in the permitted set"* would be false.
 * **Measured 2026-10-16:**  with `+ep` and a lowering at startup,
 * the main thread's `CapEff` was `0xc4` and all 31 `io_context_pool`
 * threads' was `0xcf` -- they are created before the driver
 * initialises, and capabilities are per-thread, so every request
 * thread held `CAP_DAC_OVERRIDE` for its whole life.  Granting
 * permitted-only removes the ordering question rather than winning
 * it.
 *
 * `CAP_DAC_READ_SEARCH` is the one capability the gateway *holds*
 * rather than raises:  the authorisation pass reads an ACL off an
 * object it does not own, and a listing row reads that object's
 * attributes, both with no identity (`File::open()`'s read-only
 * branch).  Holding it per thread is the design, not a concession;
 * what is new is that the thread takes it on first use instead of
 * inheriting it from exec.  A thread that never calls this simply
 * does not have it, and a read fails -- which is the direction a
 * mistake should fail in.
 *
 * Idempotent and cheap:  a thread_local flag after the first call.
 * Returns 0, or -errno.  `ScopedCredentials` clears it for the
 * length of an impersonated call, and the flag is not consulted
 * there -- the restore puts back whatever was found. */
int hold_thread_capabilities(const DoutPrefixProvider* dpp);

/* fgetxattr(2), for an attribute read.
 *
 * The kernel checks read permission on a user.* attribute at each
 * call, not at open, and against the calling thread.  A descriptor
 * opened on a thread that held CAP_DAC_READ_SEARCH does not carry it
 * to the thread a coroutine resumes on, so each read takes it first,
 * through hold_thread_capabilities():  a branch once the thread holds
 * it, and nothing on a thread running as an identity.  Returns what
 * the syscall returns, and leaves errno as it set it.
 *
 * Listing the names is not checked, so flistxattr(2) needs nothing
 * (measured 2026-10-20 on xfs, tmpfs and GPFS). */
ssize_t fgetxattr(int fd, const char* name, void* value, size_t size);

/* statx(2) and fstatat(2), for a gateway stat inside an identity's tree.
 *
 * A key's directories are its requester's at `0770`, so the gateway can
 * search them only holding CAP_DAC_READ_SEARCH -- and holds it only on
 * a thread that took it, which a coroutine resumed elsewhere is not.
 * Each stat takes it first, as fgetxattr() does.  Metadata only:  a
 * gateway open of a file is decided at its site, because read-search
 * would also hand the gateway the identity's bytes.  Returns what the
 * syscall returns, and leaves errno as it set it. */
int statx(int dirfd, const char* path, int flags, unsigned int mask,
	  struct ::statx* buf);
int fstatat(int dirfd, const char* path, struct ::stat* buf, int flags);

/* Whether `cap` is in this thread's effective set, for tests and for
 * the `raise` driver hint. */
bool capability_is_effective(int cap);

/* Perform `fn` with `caps` raised in the effective set.
 *
 * The raise primitive.  A capability in the permitted set does
 * nothing until it is in the effective set, and the gateway keeps its
 * effective set empty of the five it may raise so that an ordinary
 * call cannot use one by accident.  This is the only way one becomes
 * effective, and it is effective for the length of `fn` on this
 * thread alone -- capabilities are per-task, as credentials are.
 *
 * `caps` is a mask of `1ull << CAP_*` bits.  An empty mask runs `fn`
 * and touches nothing.
 *
 * Three rejections, all of them before `fn` runs:
 *
 *   -EPERM    a requested capability is not in the permitted set.
 *             Fails closed:  a gateway started without the file
 *             capabilities would otherwise run the raised path as an
 *             ordinary unprivileged call and report whatever the
 *             kernel said, which reads as a permission bug rather
 *             than a deployment one.
 *   -EPERM    the calling thread is impersonating.  Raising there
 *             would hand the capability to the *identity*, which is
 *             the escalation this design exists to prevent;  (n)
 *             raises as the gateway and never while impersonating.
 *             NooBaa does the opposite -- CAP_DAC_READ_SEARCH raised
 *             after its setresuid -- and the asymmetry is deliberate.
 *   -EIO      capget() or capset() failed, which cannot be ignored:
 *             running `fn` after a failed raise would report a
 *             permission error for a capability we never installed.
 *
 * Synchronous work only.  The raise is per-thread and `fn` must not
 * yield:  beast multiplexes coroutines on one thread, so a suspended
 * raise would be another request's privilege.  That cannot be
 * enforced here, so it is a rule about what may be passed.
 *
 * Never on the ring, either.  An SQE with no personality takes the
 * submitter's credentials at issue (probe 2), so a raise held on a
 * ring-driving thread reaches other requests' submissions;  the ring
 * route is a registered personality, not this.
 *
 * Failing to *drop* the capability again aborts the process, as
 * failing to restore credentials does:  a thread that returns to its
 * pool holding CAP_DAC_OVERRIDE serves every later request with it,
 * and there is nothing this function could return that would stop
 * that.
 */
int with_capabilities(const DoutPrefixProvider* dpp, uint64_t caps,
		      const ImpersonatedFn& fn);

/* The mask for one capability, for callers assembling one. */
constexpr uint64_t cap_bit(int cap) { return 1ull << cap; }

/* Perform `fn` as `id`, and where the filesystem denies it and `model`
 * permits `what`, perform it again as the gateway, raising `caps` for
 * the call:  a model's raise.
 *
 * `what` is the class of the operation (access_strategy.h), named by
 * the caller and never assumed here.  Every current caller passes
 * AccessClass::data:  creating, linking, renaming or unlinking in a key
 * directory or its `.versions` that is another identity's is a change a
 * side-loading client can observe.  Under s3-native S3 has authorised
 * it, and the gateway performs it as S3's agent.
 *
 * `fn` must not yield, as for with_capabilities():  the second call
 * runs on this thread, raised.  It runs plain syscalls, which are the
 * identity's on the helper and the gateway's on the second call.
 *
 * `*raised` is set where the gateway performed it and it succeeded:  a
 * caller that created something then gives it its owner.  An inactive
 * `id` is the gateway already, and `fn` runs once. */
int with_identity_or_raise(const DoutPrefixProvider* dpp,
			   const FSIdentity& id, const AccessStrategy* model,
			   AccessClass what, uint64_t caps,
			   const ImpersonatedFn& fn,
			   optional_yield y, bool* raised = nullptr);

/* open_as(), and as the gateway where with_identity_or_raise() would
 * raise:  the open through the ring as `id`, then a plain openat(2)
 * raising `caps`, where `model` permits `what`.  An inactive `id` is a
 * plain openat(2).  A descriptor, or -errno. */
int open_as_or_raise(const DoutPrefixProvider* dpp, const FSIdentity& id,
		     const AccessStrategy* model, AccessClass what,
		     uint64_t caps,
		     int dirfd, const char* path, uint64_t flags,
		     uint64_t mode, bool* raised = nullptr);

/* How many operations the two above performed as the gateway, for the
 * `access-model` hint. */
uint64_t raised_operations();

/* The same, for operations that return something other than an
 * errno -- the compare-and-swap link and unlink return a verdict
 * struct.  A failure to assume the identity cannot be expressed in
 * that return, so it is reported and the callable is not run,
 * leaving the caller its default-constructed result. */
template <typename F>
auto with_identity_r(const DoutPrefixProvider* dpp, const FSIdentity& id,
		     F&& fn, optional_yield y) -> decltype(fn())
{
  if (! id.active()) {
    return fn();
  }
  decltype(fn()) result{};
  run_as(dpp, id.cred, [&]() -> int { result = fn(); return 0; }, y);
  return result;
}

} } } // namespace rgw::sal::nsfs
