// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include "common/async/context_pool.h"
#include "common/Finisher.h"
#include "common/Timer.h"

struct tee_streambuf : public std::streambuf {
  std::streambuf* sb1;
  std::streambuf* sb2;
protected:
  int overflow(int c) override {
    if (c == EOF) return !EOF;
    int r1 = sb1->sputc(c);
    int r2 = sb2->sputc(c);
    return (r1 == EOF || r2 == EOF) ? EOF : c;
  }
  int sync() override {
    int r1 = sb1->pubsync();
    int r2 = sb2->pubsync();
    return (r1 || r2) ? -1 : 0;
  }
public:
  tee_streambuf(std::streambuf* sb1_, std::streambuf* sb2_)
  : sb1(sb1_), sb2(sb2_) {}
};

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

  ceph::mutex lock = ceph::make_mutex("MDSUtility::lock");
  Finisher finisher;
  ceph::async::io_context_pool poolctx;

  Context *waiting_for_mds_map;

  bool inited;

  std::ostringstream audit_buf;
  tee_streambuf tee_sb{std::cerr.rdbuf(), audit_buf.rdbuf()};
  std::ostream err{&tee_sb};
public:
  MDSUtility();
  ~MDSUtility() override;

  void handle_fs_map(MFSMap* m);
  bool ms_dispatch(Message *m) override;
  bool ms_handle_reset(Connection *con) override { return false; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override { return false; }
  int init();
  void shutdown();
  std::string get_audit_status() const {
    return audit_buf.str();
  }
};

#endif /* MDS_UTILITY_H_ */
