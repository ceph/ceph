
#ifndef __MDS_H
#define __MDS_H

#include <list>
#include <ext/hash_map>
#include <vector>
#include <set>
#include <ostream>
#include <ext/rope>

#include "include/types.h"
#include "include/Context.h"
#include "include/Dispatcher.h"
#include "include/DecayCounter.h"
#include "include/Logger.h"

typedef __uint64_t object_t;

using namespace std;


#define MDS_PORT_MAIN     1
#define MDS_PORT_SERVER   5
#define MDS_PORT_CACHE    10
#define MDS_PORT_STORE    11
#define MDS_PORT_BALANCER 20


// md ops
#define MDS_OP_STAT    100  //
#define MDS_OP_READDIR 101  //

#define MDS_OP_OPENRD  111  //
#define MDS_OP_OPENWR  112  //
#define MDS_OP_OPENWRC 113  //
#define MDS_OP_CLOSE   119

#define MDS_OP_TOUCH   200  // utime, actually
#define MDS_OP_CHMOD   201  // chmod

#define MDS_OP_RENAME  211
#define MDS_OP_UNLINK  212
#define MDS_OP_LINK    213

#define MDS_OP_MKDIR   220
#define MDS_OP_RMDIR   221

#define MDS_TRAVERSE_FORWARD  1
#define MDS_TRAVERSE_DISCOVER 2
#define MDS_TRAVERSE_FAIL     3



class MDCluster;
class CInode;
class CDir;
class MDCache;
class MDStore;
class MDLog;
class Messenger;
class Message;
class MClientRequest;
class MClientReply;
class MDBalancer;

// types

typedef struct {
  crope *buffer;
  Context *context;
} PendingOSDRead_t;


class MDS;

void split_path(string& path, 
				vector<string>& bits);


class MDS : public Dispatcher {
 protected:
  int          whoami;

  MDCluster    *mdcluster;

  bool         shutting_down;
  set<int>     did_shut_down;
  bool         shut_down;
  
  // import/export
  list<CInode*>      import_list;
  list<CInode*>      export_list;
  
  // osd interface
  __uint64_t         osd_last_tid;
  hash_map<__uint64_t,PendingOSDRead_t*>  osd_reads;
  hash_map<__uint64_t,Context*> osd_writes;   

  friend class MDStore;

  // stats
  DecayCounter stat_req;
  DecayCounter stat_read;
  DecayCounter stat_write;
  
  set<int>     done_clients;

 public:
  // sub systems
  MDCache      *mdcache;    // cache
  MDStore      *mdstore;    // storage interface
  Messenger    *messenger;    // message processing
  MDLog        *mdlog;
  MDBalancer   *balancer;

  Logger       *logger;

 protected:
  __uint64_t   stat_ops;
  __uint64_t   last_heartbeat;

  
 public:
  MDS(MDCluster *mdc, int whoami, Messenger *m);
  ~MDS();

  int get_nodeid() { return whoami; }
  MDCluster *get_cluster() { return mdcluster; }

  mds_load_t get_load();

  bool is_shutting_down() { return shutting_down; }
  bool is_shut_down(int who=-1) { 
	if (who<0)
	  return shut_down; 
	return did_shut_down.count(who);
  }

  int init();
  int shutdown_start();
  int shutdown_final();

  void proc_message(Message *m);
  virtual void dispatch(Message *m);

  bool open_root(Context *c);
  bool open_root_2(int result, Context *c);

  void reply_request(MClientRequest *req, int r = 0);


  void handle_ping(class MPing *m);
  void handle_client_done(Message *m);
  void handle_shutdown_start(Message *m);
  void handle_shutdown_finish(Message *m);

  int handle_client_request(MClientRequest *m);
  
  MClientReply *handle_client_readdir(MClientRequest *req,
									  CInode *cur);
  MClientReply *handle_client_stat(MClientRequest *req,
								   CInode *cur);
  
  MClientReply *handle_client_touch(MClientRequest *req,
									CInode *cur);
  void handle_client_touch_2(MClientRequest *req,
							 MClientReply *reply,
							 CInode *cur);
  
  MClientReply *handle_client_chmod(MClientRequest *req,
									CInode *cur);
  void handle_client_chmod_2(MClientRequest *req,
							 MClientReply *reply,
							 CInode *cur);

  MClientReply *handle_client_openrd(MClientRequest *req,
									 CInode *cur);
  MClientReply *handle_client_openwr(MClientRequest *req,
									 CInode *cur);
  void handle_client_openwrc(MClientRequest *req);
  void handle_client_close(MClientRequest *req);

  void handle_client_unlink(MClientRequest *req,
							CInode *cur);
  void handle_client_unlink_2(MClientRequest *req, 
							  CInode *cur);

  void handle_client_mkdir(MClientRequest *req);
  void handle_client_rmdir(MClientRequest *req, CInode *cur);

  void handle_client_rename(MClientRequest *req, CInode *cur);
  void handle_client_rename_file(MClientRequest *req, CInode *cur, CDir *destdir, string& name);
  void handle_client_rename_dir(MClientRequest *req, CInode *cur, CDir *destdir, string& name);


  int do_stat(MClientRequest *m);



  // osd fun
  int osd_read(int osd, 
			 object_t oid, 
			 size_t len, 
			 size_t offset, 
			 crope *buffer, 
			 Context *c);
  int osd_read_finish(Message *m);

  int osd_write(int osd, 
				object_t oid, 
				size_t len, 
				size_t offset, 
				crope& buffer, 
				int flags, 
				Context *c);
  int osd_write_finish(Message *m);



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
	mds->dispatch(m);
  }
  
  virtual bool can_redelegate() {
	return true;
  }
  
  virtual void redelegate(MDS *mds, int newmds);
};


ostream& operator<<(ostream& out, MDS& mds);


//extern MDS *g_mds;


#endif
