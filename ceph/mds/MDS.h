
#ifndef __MDS_H
#define __MDS_H

#include <list>
#include <ext/hash_map>

#include "include/types.h"
#include "include/Context.h"
#include "include/Dispatcher.h"


typedef __uint64_t object_t;

using namespace std;


#define MDS_PORT_MAIN   1
#define MDS_PORT_SERVER 5
#define MDS_PORT_STORE  10

// md ops
#define MDS_OP_STAT    100
#define MDS_OP_READDIR 101

#define MDS_OP_OPEN    111
#define MDS_OP_CLOSE   112

#define MDS_OP_RENAME  121
#define MDS_OP_UNLINK  122
#define MDS_OP_LINK    123


class MDCluster;
class CInode;
class DentryCache;
class MDStore;
class MDLog;
class Messenger;
class Message;
class MClientRequest;

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
  
  bool               opening_root;
  vector<Context*>   waiting_for_root;

  // osd interface
  __uint64_t         osd_last_tid;
  hash_map<__uint64_t,PendingOSDRead_t*>  osd_reads;
  hash_map<__uint64_t,Context*> osd_writes;   

  friend class MDStore;

 public:
  // sub systems
  DentryCache  *mdcache;    // cache
  MDStore      *mdstore;    // storage interface
  Messenger    *messenger;    // message processing
  MDLog        *mdlog;

  //MDBalancer   *balancer;
 


  
 public:
  MDS(MDCluster *mdc, Messenger *m);
  ~MDS();

  int get_nodeid() {
	return whoami;
  }

  int init();
  int shutdown();

  void proc_message(Message *m);
  virtual void dispatch(Message *m);

  bool open_root(Context *c);
  bool open_root_2(int result, Context *c);


  int handle_discover(class MDiscover *m);

  // client fun
  int handle_client_request(MClientRequest *m);

  int do_stat(MClientRequest *m);
  int path_traverse(string& path, vector<CInode*>& trace, vector<string>& trace_dn, Message *req, int onfail);



  // osd fun
  int osd_read(int osd, object_t oid, size_t len, size_t offset, char *bufptr, size_t *bytesread, Context *c);
  int osd_read(int osd, object_t oid, size_t len, size_t offset, char **bufptr, size_t *bytesread, Context *c);
  int osd_read_finish(Message *m);

  int osd_write(int osd, object_t oid, size_t len, size_t offset, char *buf, int flags, Context *c);
  int osd_write_finish(Message *m);



};


//extern MDS *g_mds;


#endif
