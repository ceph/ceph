
#ifndef __CONTEXT_H
#define __CONTEXT_H

#include <list>



// Context, for retaining context of a message being processed..
// pure abstract!
class Context {
 private:
  int result;
  
 public:
  virtual void finish(int r) = 0;
  //virtual void fail(int r) = 0;

  void lazy_finish(int r) {
	result = r;
	//context_lazy_finished->push_back(this);

	// FIXME

  }
};



#endif
