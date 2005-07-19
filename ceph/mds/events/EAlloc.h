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
  int  what;  

 public:
  EAlloc(int idtype, idno_t id, int what) :
	LogEvent(EVENT_ALLOC) {
	this->idtype = idtype;
	this->id = id;
	this->what = what;
  }
  EAlloc() :
	LogEvent(EVENT_ALLOC) {
  }
  
  void encode_payload(bufferlist& bl) {
	bl.append((char*)&idtype, sizeof(idtype));
	bl.append((char*)&id, sizeof(id));
	bl.append((char*)&what, sizeof(what));
  }
  void decode_payload(bufferlist& bl, int& off) {
	bl.copy(off, sizeof(idtype), (char*)&idtype);
	off += sizeof(idtype);
	bl.copy(off, sizeof(id), (char*)&id);
	off += sizeof(id);
	bl.copy(off, sizeof(what), (char*)&what);
	off += sizeof(what);
  }

  
  virtual bool obsolete(MDS *mds) {
	if (mds->idalloc->is_dirty(idtype, id))
	  return false;   // still dirty
	else
	  return true;    // already flushed
  }

  virtual void retire(MDS *mds, Context *c) {
	mds->idalloc->save(c);
  }
  
};

#endif
