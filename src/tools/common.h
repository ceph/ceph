#ifndef CEPH_TOOLS_COMMON_DOT_H
#define CEPH_TOOLS_COMMON_DOT_H

#include "common/Cond.h"
#include "common/Mutex.h"
#include "mon/MonClient.h"
#include "mon/PGMap.h"
#include "mds/MDSMap.h"
#include "osd/OSDMap.h"
#include "common/Timer.h"

#include <iosfwd>
#include <stdint.h>

#define OSD_MON_UPDATE	    (1<<0)
#define MDS_MON_UPDATE	    (1<<1)
#define PG_MON_UPDATE	    (1<<2)
#define MON_MON_UPDATE	    (1<<3)
#define EVERYTHING_UPDATE   0xffffffff

class CephContext;

enum ceph_tool_mode_t {
  CEPH_TOOL_MODE_CLI_INPUT = 0,
  CEPH_TOOL_MODE_OBSERVER = 1,
  CEPH_TOOL_MODE_ONE_SHOT_OBSERVER = 2,
  CEPH_TOOL_MODE_GUI = 3
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

  Dispatcher *dispatcher;

  CephToolCtx(CephContext *cct_, bool concise_) :
    cct(cct_),
    mc(&g_ceph_context),
    updates(EVERYTHING_UPDATE),
    log(&std::cout),
    slog(NULL),
    lock("ceph.cc lock"), timer(lock),
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
void send_observe_requests(CephToolCtx *ctx);
CephToolCtx* ceph_tool_common_init(ceph_tool_mode_t mode, bool concise);
int ceph_tool_cli_input(CephToolCtx *ctx, std::vector<std::string> &cmd, 
			const char *outfile, bufferlist &indata);
int ceph_tool_messenger_shutdown();
int ceph_tool_common_shutdown(CephToolCtx *ctx);

#endif
