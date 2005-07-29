
#ifndef __OSD_H
#define __OSD_H

#include "msg/Dispatcher.h"

#include "common/Mutex.h"
#include "common/ThreadPool.h"

#include <map>
using namespace std;


class Messenger;
class Message;

typedef __uint64_t version_t;


/** RGImport
 * state associated with import of RG contents from another 
 * OSD.
 */
#define RG_IMPORT_STATE_STARTING    1   // fetching object, delete lists.
#define RG_IMPORT_STATE_IMPORTING   2   // fetching replicas.
#define RG_IMPORT_STATE_FINISHING   3   // got everything; telling old guy to hose residual state

struct RGImport {
  int                      peer;               // the peer
  int                      peer_role;          // peer's role.  if <0, we should delete strays.
  int                      import_state;

  map<object_t,version_t>  remaining_objects;  // remote object list
  map<object_t,version_t>  stray_objects;      // imported but not deleted. 

  // FIXME: add destructive vs non-destructive.  maybe peer is a replica!
};


/** RGPeer
 * state associated with (possibly old) RG peers.
 * only used by primary?
 *
 */

// by primary
#define RG_PEER_STATE_ACTIVE    1   // active peer
#define RG_PEER_STATE_COMPLETE  2   // peer has everything replicated

struct RGPeer {
  int       peer;
  int       role;    // 0 primary, 1+ replica, -1 residual
  int       state;

  // used by primary for syncing (old) replicas
  map<object_t,version_t>  objects;        // remote object list
  map<object_t,version_t>  deleted;        // remote delete list
  map<object_t,version_t>  fetching;       // objects i'm reading from replica
  map<object_t,version_t>  stray;          // objects that need to be deleted
  
  // used by primary for normal replication stuff
  map<object_t,version_t>  writing;        // objects i've written to replica
  map<object_t,version_t>  flushing;       // objects i've written to remote buffer cache only
};



/** RG - Replica Group
 *
 */

// bits used on any
#define RG_STATE_COMPLETE    1  // i have full RG contents locally.
#define RG_STATE_PEERED      2  // i have contacted prior primary and all
                                // replica osds and/or fetched their 
                                // content lists, and thus know what's up.
                                // or, i have check in w/ new primary (on replica)

// on primary or old-primary only
#define RG_STATE_CLEAN       4  // i am fully replicated

class RG {
 public:
  repgroup_t rg;
  int        role;    // 0 = primary, 1 = secondary, etc.  -1=undef/none.
  int        state;   
  
  map<int, RGPeer>          peers;

  // for unstable states,
  map<object_t, version_t>  deleted_objects;  // locally

 public:  
  RG(repgroup_t rg);
  
  repgroup_t get_rg() { return rg; }
  int        get_role() { return role; }
  int        get_state() { return state; }
  
  void enumerate_objects(list<object_t>& ls);
};


/** Onode
 * per-object OSD metadata
 */
class Onode {
  object_t            oid;
  version_t           version;

  map<int, version_t> stray_replicas;   // osds w/ stray replicas.

 public:

};


class OSD : public Dispatcher {
 protected:
  Messenger *messenger;
  int whoami;

  class OSDMap  *osdmap;
  class ObjectStore *store;
  class HostMonitor *monitor;
  class Logger      *logger;
  class ThreadPool<class OSD, class MOSDOp>  *threadpool;

  list<class MOSDOp*> waiting_for_osdmap;

  // replica hack
  __uint64_t                     last_tid;
  Mutex                          replica_write_lock;
  map<MOSDOp*, Cond*>            replica_write_cond;
  map<MOSDOp*, set<__uint64_t> > replica_write_tids;
  map<__uint64_t, MOSDOp*>       replica_writes;

  // global lock
  Mutex osd_lock;


 public:
  OSD(int id, Messenger *m);
  ~OSD();
  
  // startup/shutdown
  int init();
  int shutdown();

  // OSDMap
  void update_osd_map(__uint64_t ocv, bufferlist& blist);

  void queue_op(class MOSDOp *m);
  void do_op(class MOSDOp *m);
  static void doop(OSD *o, MOSDOp *op) {
      o->do_op(op);
    };


  // messages
  virtual void dispatch(Message *m);

  void handle_ping(class MPing *m);
  void handle_getmap_ack(class MOSDGetMapAck *m);
  void handle_op(class MOSDOp *m);
  void op_read(class MOSDOp *m);
  void op_write(class MOSDOp *m);
  void op_mkfs(class MOSDOp *m);
  void op_delete(class MOSDOp *m);
  void op_truncate(class MOSDOp *m);
  void op_stat(class MOSDOp *m);

  // for replication
  void handle_op_reply(class MOSDOpReply *m);
};

#endif
