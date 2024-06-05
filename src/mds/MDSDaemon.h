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

#ifndef CEPH_MDS_H
#define CEPH_MDS_H

#include <string_view>

#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MGenericMessage.h"
#include "messages/MMDSMap.h"
#include "messages/MMonCommand.h"

#include "common/LogClient.h"
#include "common/ceph_mutex.h"
#include "common/fair_mutex.h"
#include "common/Timer.h"
#include "include/Context.h"
#include "include/types.h"
#include "mgr/MgrClient.h"
#include "msg/Dispatcher.h"

#include "Beacon.h"
#include "MDSMap.h"
#include "MDSRank.h"

#define CEPH_MDS_PROTOCOL    36 /* cluster internal */

class Messenger;
class MonClient;

class MDSDaemon : public Dispatcher {
 public:
  MDSDaemon(std::string_view n, Messenger *m, MonClient *mc,
	    boost::asio::io_context& ioctx);

  ~MDSDaemon() override;

  mono_time get_starttime() const {
    return starttime;
  }
  std::chrono::duration<double> get_uptime() const {
    mono_time now = mono_clock::now();
    return std::chrono::duration<double>(now-starttime);
  }

  // handle a signal (e.g., SIGTERM)
  void handle_signal(int signum);

  int init();

  /**
   * Hint at whether we were shutdown gracefully (i.e. we were only
   * in standby, or our rank was stopped).  Should be removed once
   * we handle shutdown properly (e.g. clear out all message queues)
   * such that deleting xlists doesn't assert.
   */
  bool is_clean_shutdown();

  /* Global MDS lock: every time someone takes this, they must
   * also check the `stopping` flag.  If stopping is true, you
   * must either do nothing and immediately drop the lock, or
   * never drop the lock again (i.e. call respawn()) */
  ceph::fair_mutex mds_lock{"MDSDaemon::mds_lock"};
  bool stopping = false;

  class CommonSafeTimer<ceph::fair_mutex> timer;
  std::string gss_ktfile_client{};

  int orig_argc;
  const char **orig_argv;


 protected:
  // admin socket handling
  friend class MDSSocketHook;

  // special message types
  friend class C_MDS_Send_Command_Reply;

  void reset_tick();
  void wait_for_omap_osds();

  void set_up_admin_socket();
  void clean_up_admin_socket();
  void check_ops_in_flight(); // send off any slow ops to monitor
  void asok_command(
    std::string_view command,
    const cmdmap_t& cmdmap,
    Formatter *f,
    const bufferlist &inbl,
    asok_finisher on_finish);

  void dump_status(Formatter *f);

  /**
   * Terminate this daemon process.
   *
   * This function will return, but once it does so the calling thread
   * must do no more work as all subsystems will have been shut down.
   */
  void suicide();

  /**
   * Start a new daemon process with the same command line parameters that
   * this process was run with, then terminate this process
   */
  void respawn();

  void tick();

  bool handle_core_message(const cref_t<Message> &m);
  
  void handle_command(const cref_t<MCommand> &m);
  void handle_mds_map(const cref_t<MMDSMap> &m);

  Beacon beacon;

  std::string name;

  Messenger    *messenger;
  MonClient    *monc;
  boost::asio::io_context& ioctx;
  MgrClient     mgrc;
  std::unique_ptr<MDSMap> mdsmap;
  LogClient    log_client;
  LogChannelRef clog;

  MDSRankDispatcher *mds_rank = nullptr;

  // tick and other timer fun
  Context *tick_event = nullptr;
  class MDSSocketHook *asok_hook = nullptr;

 private:
  bool ms_dispatch2(const ref_t<Message> &m) override;
  int ms_handle_fast_authentication(Connection *con) override;
  void ms_handle_accept(Connection *con) override;
  void ms_handle_connect(Connection *con) override;
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override;
  bool ms_handle_refused(Connection *con) override;

  bool parse_caps(const AuthCapsInfo&, MDSAuthCaps&);

  mono_time starttime = mono_clock::zero();
};

#endif
