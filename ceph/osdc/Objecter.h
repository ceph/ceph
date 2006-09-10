#ifndef __OBJECTER_H
#define __OBJECTER_H

#include "include/types.h"
#include "include/bufferlist.h"

#include "osd/OSDMap.h"
#include "messages/MOSDOp.h"

#include <list>
#include <map>
#include <ext/hash_map>
using namespace std;
using namespace __gnu_cxx;

class Context;
class Messenger;
class OSDMap;
class Message;

class Objecter {
 public:  
  Messenger *messenger;
  OSDMap    *osdmap;
  
 private:
  tid_t last_tid;

  /*** track pending operations ***/
  // read
 public:
  class OSDOp {
  public:
	list<ObjectExtent> extents;
	virtual ~OSDOp() {}
  };

  class OSDRead : public OSDOp {
  public:
	bufferlist *bl;
	Context *onfinish;
	map<tid_t, ObjectExtent> ops;
	map<object_t, bufferlist*> read_data;  // bits of data as they come back

	OSDRead(bufferlist *b) : bl(b), onfinish(0) {
	  bl->clear();
	}
  };

  // generic modify
  class OSDModify : public OSDOp {
  public:
	int op;
	list<ObjectExtent> extents;
	Context *onack;
	Context *oncommit;
	map<tid_t, ObjectExtent> waitfor_ack;
	map<tid_t, eversion_t>   tid_version;
	map<tid_t, ObjectExtent> waitfor_commit;

	OSDModify(int o) : op(o), onack(0), oncommit(0) {}
  };
  
  // write (includes the bufferlist)
  class OSDWrite : public OSDModify {
  public:
	bufferlist bl;
	OSDWrite(bufferlist &b) : OSDModify(OSD_OP_WRITE), bl(b) {}
  };

  /*
  class OSDLock : public OSDModify {
  public:
	map<int,ObjectExtent> by_osd;
	map<int,ObjectExtent>::iterator next;
	OSDLock(int o) : OSDModify(o) {}
  };
  */


 private:
  // pending ops
  hash_map<tid_t,OSDRead*>   op_read;
  hash_map<tid_t,OSDModify*> op_modify;

  /**
   * track pending ops by pg
   *  ...so we can cope with failures, map changes
   */
  class PG {
  public:
	int updater;         // where i write
	int reader;          // where i read, and expect acks from
	set<tid_t>  active_tids; // active ops

	PG() : updater(-1), reader(-1) {}

	void calc(pg_t pgid, OSDMap *osdmap) {  // return true if change
	  updater = osdmap->get_pg_acting_primary(pgid);
	  if (g_conf.osd_rep == OSD_REP_PRIMARY)
		reader = updater;
	  else 
		reader = osdmap->get_pg_acting_tail(pgid);
	}
  };

  hash_map<pg_t,PG> pg_map;

  
  PG &get_pg(pg_t pgid) {
	if (!pg_map.count(pgid)) 
	  pg_map[pgid].calc(pgid, osdmap);
	return pg_map[pgid];
  }
  void close_pg(pg_t pgid) {
	assert(pg_map.count(pgid));
	assert(pg_map[pgid].active_tids.empty());
	pg_map.erase(pgid);
  }
  void scan_pgs(set<pg_t>& chnaged_pgs);//, set<pg_t>& down_pgs);
  void kick_requests(set<pg_t>& changed_pgs);//, set<pg_t>& down_pgs);
	

 public:
  Objecter(Messenger *m, OSDMap *om) : 
	messenger(m), osdmap(om),
	last_tid(0)
	{}
  ~Objecter() {
	// clean up op_*
	// ***
  }

  // messages
 public:
  void dispatch(Message *m);
  void handle_osd_op_reply(class MOSDOpReply *m);
  void handle_osd_read_reply(class MOSDOpReply *m);
  void handle_osd_modify_reply(class MOSDOpReply *m);
  void handle_osd_lock_reply(class MOSDOpReply *m);
  void handle_osd_map(class MOSDMap *m);

 private:

  tid_t readx_submit(OSDRead *rd, ObjectExtent& ex);
  tid_t modifyx_submit(OSDModify *wr, ObjectExtent& ex, tid_t tid=0);



  // public interface
 public:
  bool is_active() {
	return !(op_read.empty() && op_modify.empty());
  }

  // med level
  tid_t readx(OSDRead *read, Context *onfinish);
  tid_t modifyx(OSDModify *wr, Context *onack, Context *oncommit);
  //tid_t lockx(OSDLock *l, Context *onack, Context *oncommit);

  // even lazier
  tid_t read(object_t oid, off_t off, size_t len, bufferlist *bl, 
			 Context *onfinish);
  tid_t write(object_t oid, off_t off, size_t len, bufferlist &bl, 
			  Context *onack, Context *oncommit);
  tid_t zero(object_t oid, off_t off, size_t len,  
			 Context *onack, Context *oncommit);

  tid_t lock(int op, object_t oid, Context *onack, Context *oncommit);
};

#endif
