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
  SafeTimer timer;
  Finisher finisher;

  Context *waiting_for_mds_map;

public:
  MDSUtility();
  ~MDSUtility();

  void handle_fs_map(MFSMap* m);
  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new);
  int init();
  void shutdown();
};

#endif /* MDS_UTILITY_H_ */
