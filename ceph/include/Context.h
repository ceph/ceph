
#ifndef __CONTEXT_H
#define __CONTEXT_H

#include <list>
#include <assert.h>

class MDS;

// Context, for retaining context of a message being processed..
// pure abstract!
class Context {
 private:
  int result;
  
 public:
  virtual void finish(int r) = 0;
  //virtual void fail(int r) = 0;

  virtual bool can_redelegate() {
	return false;
  }
  virtual void redelegate(MDS *mds, int newmds) { 
	assert(false);
  }
  
};



#endif
