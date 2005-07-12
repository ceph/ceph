
#ifndef __CONTEXT_H
#define __CONTEXT_H

#include "config.h"

#include <assert.h>
#include <list>
#include <iostream>
using namespace std;

class MDS;

// Context, for retaining context of a message being processed..
// pure abstract!
class Context {
 private:
  int result;
  
 public:
  virtual ~Context() {}       // we want a virtual destructor!!!

  virtual void finish(int r) = 0;
  //virtual void fail(int r) = 0;

  virtual bool can_redelegate() {
	return false;
  }
  virtual void redelegate(MDS *mds, int newmds) { 
	assert(false);
  }
  
};


inline void finish_contexts(list<Context*>& finished, 
							int result = 0)
{
  if (finished.size()) 
	dout(4) << finished.size() << " contexts to finish" << endl;
  for (list<Context*>::iterator it = finished.begin(); 
	   it != finished.end(); 
	   it++) {
	Context *c = *it;
	dout(4) << "---- " << c << endl;
	c->finish(result);
	delete c;
  }
}

#endif
