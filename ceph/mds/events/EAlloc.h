#ifndef __EALLOC_H
#define __EALLOC_H

#include <assert.h>
#include "include/config.h"
#include "include/types.h"

#include "../LogEvent.h"
#include "../IdAllocator.h"

#define EALLOC_EV_ALLOC  1
#define EALLOC_EV_FREE   2

class EAlloc : public LogEvent {
 protected:
  int  type;
  idno_t id;
  int  what;  

 public:
  EAlloc(int type, idno_t id, int what) :
	LogEvent(EVENT_ALLOC) {
	this->type = type;
	this->id = id;
	this->what = what;
  }
  EAlloc() :
	LogEvent(EVENT_ALLOC) {
  }
  
  virtual void encode_payload(bufferlist& bl) {
	bl.append((char*)&type, sizeof(type));
	bl.append((char*)&id, sizeof(id));
	bl.append((char*)&what, sizeof(what));
  }
  void decode_payload(bufferlist& bl, int& off) {
	bl.copy(off, sizeof(type), (char*)&type);
	off += sizeof(type);
	bl.copy(off, sizeof(id), (char*)&id);
	off += sizeof(id);
	bl.copy(off, sizeof(what), (char*)&what);
	off += sizeof(what);
  }

  
  virtual bool obsolete(MDS *mds) {
	if (mds->idalloc->is_dirty(type,id))
	  return false;   // still dirty
	else
	  return true;    // already flushed
  }

  virtual void retire(MDS *mds, Context *c) {
	mds->idalloc->save(c);
  }
  
};

#endif
