#ifndef __EALLOC_H
#define __EALLOC_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "../LogEvent.h"
#include "../IdAllocator.h"

#define EALLOC_EV_ALLOC  1
#define EALLOC_EV_FREE   2

class EAlloc : public LogEvent {
 protected:
  int  idtype;
  idno_t id;
  int  what;  // alloc or dealloc
  version_t table_version;

 public:
  EAlloc() : LogEvent(EVENT_ALLOC) { }
  EAlloc(int idtype, idno_t id, int what, version_t v) :
	LogEvent(EVENT_ALLOC) {
	this->idtype = idtype;
	this->id = id;
	this->what = what;
	this->table_version = v;
  }
  
  void encode_payload(bufferlist& bl) {
	bl.append((char*)&idtype, sizeof(idtype));
	bl.append((char*)&id, sizeof(id));
	bl.append((char*)&what, sizeof(what));
	bl.append((char*)&table_version, sizeof(table_version));
  }
  void decode_payload(bufferlist& bl, int& off) {
	bl.copy(off, sizeof(idtype), (char*)&idtype);
	off += sizeof(idtype);
	bl.copy(off, sizeof(id), (char*)&id);
	off += sizeof(id);
	bl.copy(off, sizeof(what), (char*)&what);
	off += sizeof(what);
	bl.copy(off, sizeof(table_version), (char*)&table_version);
	off += sizeof(table_version);
  }


  void print(ostream& out) {
	if (what == EALLOC_EV_ALLOC) 
	  out << "alloc " << hex << id << dec << " tablev " << table_version;
	else
	  out << "dealloc " << hex << id << dec << " tablev " << table_version;
  }
  

  // live journal
  bool can_expire(MDS *mds) {
	if (mds->idalloc->get_committed_version() <= table_version)
	  return false;   // still dirty
	else
	  return true;    // already flushed
  }

  void retire(MDS *mds, Context *c) {
	mds->idalloc->save(c, table_version);
  }


  // recovery
  bool has_happened(MDS *mds) {
	if (mds->idalloc->get_version() >= table_version) {
	  cout << " event " << table_version << " <= table " << mds->idalloc->get_version() << endl;
	  return true;
	} else 
	  return false;
  }

  void replay(MDS *mds) {
	assert(table_version-1 == mds->idalloc->get_version());
	
	if (what == EALLOC_EV_ALLOC) {
	  idno_t nid = mds->idalloc->alloc_id(true);
	  assert(nid == id);       // this should match.
	} 
	else if (what == EALLOC_EV_FREE) {
	  mds->idalloc->reclaim_id(id, true);
	} 
	else
	  assert(0);
	
	assert(table_version == mds->idalloc->get_version());
  }
  
};

#endif
