// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "MDSRank.h"
#include "MDCache.h"
#include "Mutation.h"
#include "SessionMap.h"
#include "common/Finisher.h"

#include "common/config.h"
#include "common/errno.h"
#include "include/assert.h"
#include "include/stringify.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "Session "

/**
 * Calculate the length of the `requests` member list,
 * because elist does not have a size() method.
 *
 * O(N) runtime.  This would be const, but elist doesn't
 * have const iterators.
 */
size_t Session::get_request_count()
{
  size_t result = 0;

  elist<MDRequestImpl*>::iterator p = requests.begin(
      member_offset(MDRequestImpl, item_session_request));
  while (!p.end()) {
    ++result;
    ++p;
  }

  return result;
}

/**
 * Capped in response to a CEPH_MSG_CLIENT_CAPRELEASE message,
 * with n_caps equal to the number of caps that were released
 * in the message.  Used to update state about how many caps a
 * client has released since it was last instructed to RECALL_STATE.
 */
void Session::notify_cap_release(size_t n_caps)
{
  if (!recalled_at.is_zero()) {
    recall_release_count += n_caps;
    if (recall_release_count >= recall_count) {
      recalled_at = utime_t();
      recall_count = 0;
      recall_release_count = 0;
    }
  }
}

/**
 * Called when a CEPH_MSG_CLIENT_SESSION->CEPH_SESSION_RECALL_STATE
 * message is sent to the client.  Update our recall-related state
 * in order to generate health metrics if the session doesn't see
 * a commensurate number of calls to ::notify_cap_release
 */
void Session::notify_recall_sent(int const new_limit)
{
  if (recalled_at.is_zero()) {
    // Entering recall phase, set up counters so we can later
    // judge whether the client has respected the recall request
    recalled_at = ceph_clock_now(g_ceph_context);
    assert (new_limit < caps.size());  // Behaviour of Server::recall_client_state
    recall_count = caps.size() - new_limit;
    recall_release_count = 0;
  }
}

void Session::set_client_metadata(map<string, string> const &meta)
{
  info.client_metadata = meta;

  _update_human_name();
}

/**
 * Use client metadata to generate a somewhat-friendlier
 * name for the client than its session ID.
 *
 * This is *not* guaranteed to be unique, and any machine
 * consumers of session-related output should always use
 * the session ID as a primary capacity and use this only
 * as a presentation hint.
 */
void Session::_update_human_name()
{
  if (info.client_metadata.count("hostname")) {
    // Happy path, refer to clients by hostname
    human_name = info.client_metadata["hostname"];
    if (!info.auth_name.has_default_id()) {
      // When a non-default entity ID is set by the user, assume they
      // would like to see it in references to the client, if it's
      // reasonable short.  Limit the length because we don't want
      // to put e.g. uuid-generated names into a "human readable"
      // rendering.
      const int arbitrarily_short = 16;
      if (info.auth_name.get_id().size() < arbitrarily_short) {
        human_name += std::string(":") + info.auth_name.get_id();
      }
    }
  } else {
    // Fallback, refer to clients by ID e.g. client.4567
    human_name = stringify(info.inst.name.num());
  }
}

void Session::decode(bufferlist::iterator &p)
{
  info.decode(p);

  _update_human_name();
}

int Session::check_access(CInode *in, unsigned mask,
			  int caller_uid, int caller_gid,
			  int new_uid, int new_gid)
{
  return 0;
}
