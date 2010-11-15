#ifndef CEPH_TOOL_H
#define CEPH_TOOL_H

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

// tool/ceph.cc
struct ceph_tool_data
{
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

  ceph_tool_data() :
    updates(EVERYTHING_UPDATE),
    log(&std::cout),
    slog(NULL),
    lock("ceph.cc lock"), timer(lock)
  {
  }
};

// tool/ceph.cc
extern struct ceph_tool_data g;
int run_command(const char *line);

// tool/gyi.cc
int run_gui(int argc, char **argv);

#endif
