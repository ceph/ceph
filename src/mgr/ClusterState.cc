// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 John Spray <john.spray@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "messages/MMgrDigest.h"

#include "mgr/ClusterState.h"


ClusterState::ClusterState(MonClient *monc_, Objecter *objecter_)
  : monc(monc_), objecter(objecter_), lock("ClusterState")
{}

void ClusterState::set_objecter(Objecter *objecter_)
{
  Mutex::Locker l(lock);

  objecter = objecter_;
}

void ClusterState::set_fsmap(FSMap const &new_fsmap)
{
  Mutex::Locker l(lock);

  fsmap = new_fsmap;
}

void ClusterState::load_digest(MMgrDigest *m)
{
  pg_summary_json = std::move(m->pg_summary_json);
  health_json = std::move(m->health_json);
  mon_status_json = std::move(m->mon_status_json);
}

