// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_TOOLS_COMMON_DOT_H
#define CEPH_TOOLS_COMMON_DOT_H

#include <iosfwd>
#include <stdint.h>
#include <map>

#include "common/Cond.h"
#include "common/Mutex.h"
#include "mon/MonClient.h"
#include "mon/PGMap.h"
#include "mds/MDSMap.h"
#include "osd/OSDMap.h"
#include "common/Timer.h"


#include "common/LogEntry.h"
#include "mon/mon_types.h"
#include "messages/MOSDMap.h"
#include "messages/MLog.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MMonCommandAck.h"


#define OSD_MON_UPDATE	    (1<<0)
#define MDS_MON_UPDATE	    (1<<1)
#define PG_MON_UPDATE	    (1<<2)
#define MON_MON_UPDATE	    (1<<3)
#define EVERYTHING_UPDATE   0xffffffff

class CephContext;
class CephToolCtx;

enum ceph_tool_mode_t {
  CEPH_TOOL_MODE_CLI_INPUT = 0,
  CEPH_TOOL_MODE_WATCH = 1,
  CEPH_TOOL_MODE_STATUS = 2,
  CEPH_TOOL_MODE_GUI = 3
};

struct Subscriptions {
  CephToolCtx *ctx;
  version_t last_known_version;
  string name;

  Subscriptions(CephToolCtx *c) : ctx(c), last_known_version(0) { }

  void handle_log(MLog *m);
};

class Admin : public Dispatcher {
private:
  CephToolCtx *ctx;
public:
  Subscriptions subs;

  Admin(CephToolCtx *ctx_)
    : Dispatcher(g_ceph_context),
      ctx(ctx_), subs(ctx_)
  {
  }

  bool ms_dispatch(Message *m);
  void ms_handle_connect(Connection *con);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con) {}

  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new);
};


// tool/ceph.cc
class CephToolCtx
{
public:
  CephContext *cct;
  PGMap pgmap;
  MDSMap mdsmap;
  OSDMap osdmap;
  MonClient mc;

  // Which aspects of the cluster have been updated recently?
  uint32_t updates;

  // The main log for ceph-tool
  std::ostream *log;

  // Used by the GUI to read from the log.
  // NULL if there is no GUI active.
  std::ostringstream *slog;

  // The ceph-tool lock
  Mutex lock;
  SafeTimer timer;

  // A condition variable used to wake up the GUI thread
  Cond gui_cond;

  bool concise;

  Admin *dispatcher;

  CephToolCtx(CephContext *cct_, bool concise_) :
    cct(cct_),
    mc(g_ceph_context),
    updates(EVERYTHING_UPDATE),
    log(&std::cout),
    slog(NULL),
    lock("ceph.cc lock"), timer(cct_, lock),
    concise(concise_),
    dispatcher(NULL)
  {
  }

  ~CephToolCtx() {
    delete dispatcher;
  }
};

// tool/ceph.cc
int ceph_tool_do_cli(CephToolCtx *data);
int run_command(CephToolCtx *data, const char *line);
CephToolCtx* ceph_tool_common_init(ceph_tool_mode_t mode, bool concise);
int do_command(CephToolCtx *ctx,
	       vector<string>& cmd, bufferlist& bl, bufferlist& rbl);
int ceph_tool_messenger_shutdown();
int ceph_tool_common_shutdown(CephToolCtx *ctx);

#endif
