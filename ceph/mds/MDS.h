
#ifndef __MDS_H
#define __MDS_H

#include <list>
#include <vector>
#include <set>
#include <map>
#include <ostream>
using namespace std;

#include <ext/hash_map>
#include <ext/rope>
using namespace __gnu_cxx;

#include "msg/Dispatcher.h"
#include "include/types.h"
#include "include/Context.h"
#include "common/DecayCounter.h"
#include "common/Logger.h"
#include "common/Mutex.h"

typedef __uint64_t object_t;



#define MDS_PORT_MAIN     0
#define MDS_PORT_SERVER   1
#define MDS_PORT_CACHE    101
#define MDS_PORT_STORE    102
#define MDS_PORT_BALANCER 103

#define MDS_PORT_ANCHORMGR 200

#define MDS_PORT_OSDMON    300

#define MDS_INO_ROOT              1
#define MDS_INO_LOG_OFFSET        100
#define MDS_INO_IDS_OFFSET        200
#define MDS_INO_INODEFILE_OFFSET  300
#define MDS_INO_ANCHORTABLE       400
#define MDS_INO_BASE              1000

#define MDS_MKFS_FAST      1   // fake new root inode+dir
#define MDS_MKFS_FULL      2   // wipe osd's too

#define MDS_TRAVERSE_FORWARD       1
#define MDS_TRAVERSE_DISCOVER      2    // skips permissions checks etc.
#define MDS_TRAVERSE_DISCOVERXLOCK 3    // succeeds on (foreign?) null, xlocked dentries.
#define MDS_TRAVERSE_FAIL          4


class filepath;

class OSDCluster;
class Filer;

class AnchorTable;
class OSDMonitor;
class MDCluster;
class CInode;
class CDir;
class CDentry;
class MDCache;
class MDStore;
class MDLog;
class Messenger;
class Message;
class MClientRequest;
class MClientReply;
class MDBalancer;
class LogEvent;
class IdAllocator;


// types

class MDS;

void split_path(string& path, 
				vector<string>& bits);


class MDS : public Dispatcher {
 protected:
  int          whoami;

  Mutex        mds_lock;

  MDCluster    *mdcluster;
 public:
  OSDCluster   *osdcluster;
  Filer        *filer;       // for reading/writing to/from osds
  AnchorTable  *anchormgr;
  OSDMonitor   *osdmonitor;
 protected:


  // shutdown crap
  bool         shutting_down;
  set<int>     did_shut_down;
  bool         shut_down;

  bool         mds_paused;
  list<Context*> waiting_for_unpause;
  friend class C_MDS_Unpause;

  // ino's and fh's
 public:
  class IdAllocator  *idalloc;
 protected:

  friend class MDStore;

  // stats
  DecayCounter stat_req;
  DecayCounter stat_read;
  DecayCounter stat_write;
  
  set<int>     mounted_clients;

  
  
 public:
  list<Context*> finished_queue;

 public:
  // sub systems
  MDCache      *mdcache;    // cache
  MDStore      *mdstore;    // storage interface
  Messenger    *messenger;    // message processing
  MDLog        *mdlog;
  MDBalancer   *balancer;

  Logger       *logger, *logger2;

 protected:
  __uint64_t   stat_ops;
  timepair_t   last_balancer_heartbeat;
  
 public:
  MDS(MDCluster *mdc, int whoami, Messenger *m);
  ~MDS();

  // who am i etc
  int get_nodeid() { return whoami; }
  MDCluster *get_cluster() { return mdcluster; }
  MDCluster *get_mds_cluster() { return mdcluster; }
  OSDCluster *get_osd_cluster() { return osdcluster; }

  mds_load_t get_load();

  // start up, shutdown
  bool is_shutting_down() { return shutting_down; }
  bool is_shut_down(int who=-1) { 
	if (who<0)
	  return shut_down; 
	return did_shut_down.count(who);
  }

  int init();
  int shutdown_start();
  int shutdown_final();

  // messages
  void proc_message(Message *m);
  virtual void dispatch(Message *m);
  void my_dispatch(Message *m);

  // generic request helpers
  void reply_request(MClientRequest *req, int r = 0, CInode *tracei = 0);
  void reply_request(MClientRequest *req, MClientReply *reply, CInode *tracei);
  void commit_request(MClientRequest *req,
					  MClientReply *reply,
					  CInode *tracei,
					  LogEvent *event,
					  LogEvent *event2 = 0);
  
  // special message types
  void handle_ping(class MPing *m);

  void handle_shutdown_start(Message *m);
  void handle_shutdown_finish(Message *m);

  // osds
  void handle_osd_getcluster(Message *m);

  // clients
  void handle_client_mount(class MClientMount *m);
  void handle_client_unmount(Message *m);

  void handle_client_request(MClientRequest *m);
  void handle_client_request_2(MClientRequest *req, 
							   vector<CDentry*>& trace,
							   int r);
  
  // fs ops
  void handle_client_fstat(MClientRequest *req);

  // requests
  void dispatch_request(Message *m, CInode *ref);

  // inode request *req, CInode *ref;
  void handle_client_stat(MClientRequest *req, CInode *ref);
  void handle_client_utime(MClientRequest *req, CInode *ref);
  void handle_client_inode_soft_update_2(MClientRequest *req,
										 MClientReply *reply,
										 CInode *ref);
  void handle_client_chmod(MClientRequest *req, CInode *ref);
  void handle_client_chown(MClientRequest *req, CInode *ref);
  void handle_client_inode_hard_update_2(MClientRequest *req,
										 MClientReply *reply,
										 CInode *ref);

  // namespace
  void handle_client_readdir(MClientRequest *req, CInode *ref);
  void handle_client_mknod(MClientRequest *req, CInode *ref);
  void handle_client_link(MClientRequest *req, CInode *ref);
  void handle_client_link_2(int r, MClientRequest *req, CInode *ref, vector<CDentry*>& trace);
  void handle_client_link_finish(MClientRequest *req, CInode *ref,
								 CDentry *dn, CInode *targeti);

  void handle_client_unlink(MClientRequest *req, CInode *ref);
  void handle_client_rename(MClientRequest *req, CInode *ref);
  void handle_client_rename_2(MClientRequest *req,
							  CInode *ref,
							  CInode *srcdiri,
							  CDir *srcdir,
							  CDentry *srcdn,
							  filepath& destpath,
							  vector<CDentry*>& trace,
							  int r);
  void handle_client_rename_local(MClientRequest *req, CInode *ref,
								  string& srcpath, CInode *srcdiri, CDentry *srcdn, 
								  string& destpath, CDir *destdir, CDentry *destdn, string& name);

  void handle_client_mkdir(MClientRequest *req, CInode *ref);
  void handle_client_rmdir(MClientRequest *req, CInode *ref);
  void handle_client_symlink(MClientRequest *req, CInode *ref);

  // file
  void handle_client_open(MClientRequest *req, CInode *ref);
  void handle_client_openc(MClientRequest *req, CInode *ref);
  void handle_client_close(MClientRequest *req, CInode *in);
  void handle_client_truncate(MClientRequest *req, CInode *in);
  void handle_client_fsync(MClientRequest *req, CInode *in);

  CInode *mknod(MClientRequest *req, CInode *ref, bool okexist=false);  // used by mknod, symlink, mkdir, openc


  void queue_finished(list<Context*>& ls) {
	finished_queue.splice( finished_queue.end(), ls );
  }
};


class C_MDS_RetryRequest : public Context {
  MDS *mds;
  Message *req;   // MClientRequest or MLock
  CInode *ref;
 public:
  C_MDS_RetryRequest(MDS *mds, Message *req, CInode *ref) {
	assert(ref);
	this->mds = mds;
	this->req = req;
	this->ref = ref;
  }
  virtual void finish(int r) {
	mds->dispatch_request(req, ref);
  }
  
  virtual bool can_redelegate() {
	return true;
  }
};


class C_MDS_RetryMessage : public Context {
  Message *m;
  MDS *mds;
public:
  C_MDS_RetryMessage(MDS *mds, Message *m) {
	assert(m);
	this->m = m;
	this->mds = mds;
  }
  virtual void finish(int r) {
	mds->my_dispatch(m);
  }
  
  virtual bool can_redelegate() {
	return true;
  }
  
  virtual void redelegate(MDS *mds, int newmds);
};


ostream& operator<<(ostream& out, MDS& mds);


//extern MDS *g_mds;


#endif
