
#ifndef __MDS_H
#define __MDS_H

#include <list>
#include <ext/hash_map>

#include "types.h"
#include "Context.h"
#include "Dispatcher.h"

class CInode;
class DentryCache;
class MDStore;
class MDLog;
class Messenger;
class Message;

typedef __uint64_t object_t;

using namespace std;


#define MDS_PORT_MAIN  1
#define MDS_PORT_STORE 10


// 

typedef struct {
  char **bufptr;
  size_t *bytesread;
  char *buf;
  Context *context;
} PendingOSDRead_t;


class MDS : public Dispatcher {
 protected:
  int          nodeid;
  int          num_nodes;

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
  DentryCache  *mdcache;    // cache
  MDStore      *mdstore;    // storage interface
  Messenger    *messenger;    // message processing
  MDLog        *mdlog;

  //MDBalancer   *balancer;
 


  
 public:
  MDS(int id, int num, Messenger *m);
  ~MDS();

  int get_nodeid() {
	return nodeid;
  }

  int init();
  int shutdown();

  void proc_message(Message *m);
  virtual void dispatch(Message *m);

  bool open_root(Context *c);
  bool open_root_2(int result, Context *c);

  // osd fun
  int osd_read(int osd, object_t oid, size_t len, size_t offset, char *bufptr, size_t *bytesread, Context *c);
  int osd_read(int osd, object_t oid, size_t len, size_t offset, char **bufptr, size_t *bytesread, Context *c);
  int osd_read_finish(Message *m);

  int osd_write(int osd, object_t oid, size_t len, size_t offset, char *buf, int flags, Context *c);
  int osd_write_finish(Message *m);



};


//extern MDS *g_mds;


#endif
