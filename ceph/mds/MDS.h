
#ifndef __MDS_H
#define __MDS_H

#include <list>
#include <ext/hash_map>
#include <vector>

#include "include/types.h"
#include "include/Context.h"
#include "include/Dispatcher.h"


typedef __uint64_t object_t;

using namespace std;


#define MDS_PORT_MAIN   1
#define MDS_PORT_SERVER 5
#define MDS_PORT_CACHE  10
#define MDS_PORT_STORE  11


// md ops
#define MDS_OP_STAT    100
#define MDS_OP_READDIR 101

#define MDS_OP_OPEN    111
#define MDS_OP_CLOSE   112

#define MDS_OP_TOUCH   200

#define MDS_OP_RENAME  211
#define MDS_OP_UNLINK  212
#define MDS_OP_LINK    213

#define MDS_TRAVERSE_FORWARD  1
#define MDS_TRAVERSE_DISCOVER 2
#define MDS_TRAVERSE_FAIL     3

class MDCluster;
class CInode;
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
  char **bufptr;
  size_t *bytesread;
  char *buf;
  Context *context;
} PendingOSDRead_t;




void split_path(string& path, 
				vector<string>& bits);


class MDS : public Dispatcher {
 protected:
  int          whoami;

  MDCluster    *mdcluster;

  // import/export
  list<CInode*>      import_list;
  list<CInode*>      export_list;
  
  // osd interface
  __uint64_t         osd_last_tid;
  hash_map<__uint64_t,PendingOSDRead_t*>  osd_reads;
  hash_map<__uint64_t,Context*> osd_writes;   

  friend class MDStore;

 public:
  // sub systems
  MDCache      *mdcache;    // cache
  MDStore      *mdstore;    // storage interface
  Messenger    *messenger;    // message processing
  MDLog        *mdlog;
  MDBalancer   *balancer;
 


  
 public:
  MDS(MDCluster *mdc, Messenger *m);
  ~MDS();

  int get_nodeid() { return whoami; }
  MDCluster *get_cluster() { return mdcluster; }

  int init();
  int shutdown();

  void proc_message(Message *m);
  virtual void dispatch(Message *m);

  bool open_root(Context *c);
  bool open_root_2(int result, Context *c);


  void handle_ping(class MPing *m);

  int handle_client_request(MClientRequest *m);
  
  MClientReply *handle_client_readdir(MClientRequest *req,
									  CInode *cur);
  MClientReply *handle_client_stat(MClientRequest *req,
								   CInode *cur);
  MClientReply *handle_client_touch(MClientRequest *req,
									CInode *cur);


  int do_stat(MClientRequest *m);



  // osd fun
  int osd_read(int osd, object_t oid, size_t len, size_t offset, char *bufptr, size_t *bytesread, Context *c);
  int osd_read(int osd, object_t oid, size_t len, size_t offset, char **bufptr, size_t *bytesread, Context *c);
  int osd_read_finish(Message *m);

  int osd_write(int osd, object_t oid, size_t len, size_t offset, char *buf, int flags, Context *c);
  int osd_write_finish(Message *m);



};


class C_MDS_RetryMessage : public Context {
  Message *m;
  MDS *mds;
public:
  C_MDS_RetryMessage(MDS *mds, Message *m) {
	this->m = m;
	this->mds = mds;
  }
  virtual void finish(int r) {
	mds->dispatch(m);
  }
};



//extern MDS *g_mds;


#endif
