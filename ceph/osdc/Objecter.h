#ifndef __OBJECTER_H
#define __OBJECTER_H

#include "include/types.h"
#include "include/bufferlist.h"

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
  };

  class OSDRead : public OSDOp {
  public:
	bufferlist *bl;
	Context *onfinish;
	map<tid_t, ObjectExtent> ops;
	map<object_t, bufferlist*> read_data;  // bits of data as they come back

	OSDRead(bufferlist *b) : bl(b), onfinish(0) {}
  };

  // generic modify
  class OSDModify : public OSDOp {
  public:
	int op;
	list<ObjectExtent> extents;
	Context *onack;
	Context *oncommit;
	map<tid_t, ObjectExtent> waitfor_ack;
	map<tid_t, ObjectExtent> waitfor_commit;

	OSDModify(int o) : op(o), onack(0), oncommit(0) {}
  };
  
  // write (includes the bufferlist)
  class OSDWrite : public OSDModify {
  public:
	bufferlist bl;
	OSDWrite(bufferlist &b) : OSDModify(OSD_OP_WRITE), bl(b) {}
  };


 private:
  // pending ops
  hash_map<tid_t,OSDRead*>   op_read;
  hash_map<tid_t,OSDModify*> op_modify;

  // for failures
  //hash_map<int, set<tid_t> > osd_tids;
  //hash_map<tid_t, int>       tid_osd;

 
  /**
   * track pending ops by pg
   */
  class PG {
  public:
	int primary;	         // current osd set
	set<tid_t>  active_tids; // active ops

	PG() : primary(-1) {}

	bool calc_primary(pg_t pgid, OSDMap *osdmap) {  // return true if change
	  int n = osdmap->get_pg_acting_primary(pgid);
	  if (n == primary) 
		return false;
	  primary = n;
	  return true;	  
	}
  };

  hash_map<pg_t,PG> pg_map;

  
  PG &get_pg(pg_t pgid) {
	if (!pg_map.count(pgid)) 
	  pg_map[pgid].calc_primary(pgid, osdmap);
	return pg_map[pgid];
  }
  void close_pg(pg_t pgid) {
	assert(pg_map.count(pgid));
	assert(pg_map[pgid].active_tids.empty());
	pg_map.erase(pgid);
  }
  void scan_pgs(set<pg_t>& chnaged_pgs, set<pg_t>& down_pgs);
  
  void kick_requests(set<pg_t>& changed_pgs, set<pg_t>& down_pgs);
	

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
  void handle_osd_map(class MOSDMap *m);

 private:

  void readx_submit(OSDRead *rd, ObjectExtent& ex);
  void modifyx_submit(OSDModify *wr, ObjectExtent& ex, bool wrnoop=false);



  // public interface
 public:
  bool is_active() {
	return !(op_read.empty() && op_modify.empty());
  }

  // med level
  int readx(OSDRead *read, Context *onfinish);
  int modifyx(OSDModify *wr, Context *onack, Context *oncommit);

  // even lazier
  tid_t read(object_t oid, off_t off, size_t len, bufferlist *bl, 
			 Context *onfinish);
  tid_t write(object_t oid, off_t off, size_t len, bufferlist &bl, 
			  Context *onack, Context *oncommit);
  tid_t zero(object_t oid, off_t off, size_t len,  
			 Context *onack, Context *oncommit);
};

#endif
