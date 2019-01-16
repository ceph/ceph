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

#ifndef MDS_UTILITY_H_
#define MDS_UTILITY_H_

#include "osdc/Objecter.h"
#include "mds/FSMap.h"
#include "messages/MFSMap.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"
#include "auth/Auth.h"
#include "common/Finisher.h"
#include "common/Timer.h"

/// MDS Utility
/**
 * This class is the parent for MDS utilities, i.e. classes that
 * need access the objects belonging to the MDS without actually
 * acting as an MDS daemon themselves.
 */
class MDSUtility : public Dispatcher {
protected:
  Objecter *objecter;
  FSMap *fsmap;
  Messenger *messenger;
  MonClient *monc;

  Mutex lock;
  Finisher finisher;

  Context *waiting_for_mds_map;

  bool inited;
public:
  MDSUtility();
  ~MDSUtility() override;

  void handle_fs_map(MFSMap* m);
  bool ms_dispatch(Message *m) override;
  bool ms_handle_reset(Connection *con) override { return false; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override { return false; }
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer) override;
  int init();
  void shutdown();
};

#endif /* MDS_UTILITY_H_ */
