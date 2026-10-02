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

#include "personality.h"

namespace rgw { namespace sal { namespace nsfs {

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
 */
bool have_credential_capabilities(std::string* missing);

/* Whether impersonation is configured on.  The switch is the hinge,
 * not the presence of identity records:  records arrive and depart
 * over Admin Ops at any time, a deployment's intent does not. */
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
int acquire_personality(const DoutPrefixProvider* dpp, const rgw_user& key,
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
void note_identity_changed(const rgw_user& key);

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
int find_personality(const rgw_user& key, PersonalityRef* out,
		     const DoutPrefixProvider* dpp = nullptr);

/* Run one filesystem call as `cred`, on a thread that does nothing
 * else.
 *
 * **For the operations io_uring cannot express.**  Three of them,
 * with three different owners -- see docs/IMPERSONATED_XATTR.md:
 *
 *   - listxattr and removexattr have no opcode.  Two opcodes in
 *     io_uring/xattr.c would close it;  that is ours to propose
 *     upstream, and a loadable module offering them through
 *     uring_cmd is ours to ship in the meantime.
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
	   const ImpersonatedFn& fn);

/* What an operation needs to be performed as somebody.
 *
 * Both halves, because the two mechanisms want different things:
 * a submission entry names the `personality`, while a call that
 * io_uring cannot express is run by a helper thread that has to
 * assume `cred` itself. */
struct FSIdentity {
  uint16_t personality{0};
  Credentials cred;

  bool active() const { return personality != 0; }
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
		  const ImpersonatedFn& fn);

/* The same, for operations that answer with something other than an
 * errno -- the compare-and-swap link and unlink return a verdict
 * struct.  A failure to assume the identity cannot be expressed in
 * that return, so it is reported and the callable is not run,
 * leaving the caller its default-constructed answer. */
template <typename F>
auto with_identity_r(const DoutPrefixProvider* dpp, const FSIdentity& id,
		     F&& fn) -> decltype(fn())
{
  if (! id.active()) {
    return fn();
  }
  decltype(fn()) result{};
  run_as(dpp, id.cred, [&]() -> int { result = fn(); return 0; });
  return result;
}

} } } // namespace rgw::sal::nsfs
