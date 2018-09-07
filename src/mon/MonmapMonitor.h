// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

/*
 * The Monmap Monitor is used to track the monitors in the cluster.
 */

#ifndef CEPH_MONMAPMONITOR_H
#define CEPH_MONMAPMONITOR_H

#include <map>
#include <set>
#include <memory>

#include "include/types.h"
#include "msg/Messenger.h"

#include "PaxosService.h"
#include "MonMap.h"
#include "MonitorDBStore.h"

#include "command_multiplexer.h"

class MMonGetMap;
class MMonMap;
class MMonCommand;
class MMonJoin;

namespace ceph::mon_cmds {

class failure final 
{
    const int error_code_;
    char what_[128] = {};

    public:
    failure(const int error_code, const std::string msg)
     : error_code_(error_code)
    {
        std::snprintf(what_, sizeof(what_), "error %d: %s", error_code, msg.c_str());
    };

    virtual ~failure() {}

    public:
    const int error_code() const noexcept   { return error_code_; }
    const char *what() const noexcept       { return what_; }
};

struct mon_cmd_ctx
{
 MonOpRequestRef                op;
 MonmapMonitor&                 monmap_monitor;
 Monitor&                       mon;

 MMonCommand&                   mmon_command;   // aka "m"

 cmdmap_t                       cmdmap;
 std::string                    prefix;            // "the command" itself, e.g. "osd set"

 // many functions expect this to be available:
 std::string                    format;
 std::unique_ptr<Formatter>     f;

 // output by side-effect:
 mutable int                    r = 0;      // a result code
 mutable std::stringstream      ss;         // a string message
 mutable ceph::bufferlist       rdata;      // a result buffer

 public:
 mon_cmd_ctx(MonOpRequestRef op,
             MonmapMonitor& monmap_monitor,
             Monitor& mon);
};

enum class mon_cmd_result : int { failure, success };

using mon_cmd          = ceph::command<mon_cmd_ctx, mon_cmd_result>;
using mon_cmd_registry = ceph::command_registry<mon_cmd>;

void register_commands(mon_cmd_registry& registry);

} // namespace ceph::mon_cmds

>>>>>>> 429e38fadb... mon: monmapmonitor plumbing for command dispatch
class MonmapMonitor : public PaxosService {
 public:
  MonmapMonitor(Monitor *mn, Paxos *p, const string& service_name);

 public:
  MonMap pending_map; //the pending map awaiting passage

  void create_initial() override;

  void update_from_paxos(bool *need_bootstrap) override;

  void create_pending() override;

  void encode_pending(MonitorDBStore::TransactionRef t) override;
  // we always encode the full map; we have no use for full versions
  void encode_full(MonitorDBStore::TransactionRef t) override { }

  void on_active() override;
  void apply_mon_features(const mon_feature_t& features,
			  int min_mon_release);

  void dump_info(Formatter *f);

  bool preprocess_query(MonOpRequestRef op) override;
  bool prepare_update(MonOpRequestRef op) override;

  bool preprocess_join(MonOpRequestRef op);
  bool prepare_join(MonOpRequestRef op);

  bool preprocess_command(MonOpRequestRef op);
  [[deprecated("migrating to new dispatch multiplexer")]]
  bool legacy_preprocess_command(MonOpRequestRef op);

  bool prepare_command(MonOpRequestRef op);
  [[deprecated("migrating to new dispatch multiplexer")]]
  bool legacy_prepare_command(MonOpRequestRef op);

  int get_monmap(bufferlist &bl);

  /*
   * Since monitors are pretty
   * important, this implementation will just write 0.0.
   */
  bool should_propose(double& delay) override;

  void check_sub(Subscription *sub);

  void tick() override;

 private:
  void check_subs();
  bufferlist monmap_bl;

 private:
  ceph::mon_cmds::mon_cmd_registry commands;
};
  
#endif
