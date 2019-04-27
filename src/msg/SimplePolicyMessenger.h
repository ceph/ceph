// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef SIMPLE_POLICY_MESSENGER_H
#define SIMPLE_POLICY_MESSENGER_H

#include "Messenger.h"
#include "Policy.h"

class SimplePolicyMessenger : public Messenger
{
private:
  /// lock protecting policy
  Mutex policy_lock;
  // entity_name_t::type -> Policy
  ceph::net::PolicySet<Throttle> policy_set;

public:

  SimplePolicyMessenger(CephContext *cct, entity_name_t name,
			string mname, uint64_t _nonce)
    : Messenger(cct, name),
      policy_lock("SimplePolicyMessenger::policy_lock")
    {
    }

    /**
   * Get the Policy associated with a type of peer.
   * @param t The peer type to get the default policy for.
   *
   * @return A const Policy reference.
   */
  Policy get_policy(int t) override {
    Mutex::Locker l(policy_lock);
    return policy_set.get(t);
  }

  Policy get_default_policy() override {
    Mutex::Locker l(policy_lock);
    return policy_set.get_default();
  }

  /**
   * Set a policy which is applied to all peers who do not have a type-specific
   * Policy.
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param p The Policy to apply.
   */
  void set_default_policy(Policy p) override {
    Mutex::Locker l(policy_lock);
    policy_set.set_default(p);
  }
  /**
   * Set a policy which is applied to all peers of the given type.
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param type The peer type this policy applies to.
   * @param p The policy to apply.
   */
  void set_policy(int type, Policy p) override {
    Mutex::Locker l(policy_lock);
    policy_set.set(type, p);
  }

  /**
   * Set a Throttler which is applied to all Messages from the given
   * type of peer.
   * This is an init-time function and cannot be called after calling
   * start() or bind().
   *
   * @param type The peer type this Throttler will apply to.
   * @param t The Throttler to apply. The messenger does not take
   * ownership of this pointer, but you must not destroy it before
   * you destroy messenger.
   */
  void set_policy_throttlers(int type,
			     Throttle* byte_throttle,
			     Throttle* msg_throttle) override {
    Mutex::Locker l(policy_lock);
    policy_set.set_throttlers(type, byte_throttle, msg_throttle);
  }

}; /* SimplePolicyMessenger */

#endif /* SIMPLE_POLICY_MESSENGER_H */
