#ifndef __LOCK_H
#define __LOCK_H

#include <assert.h>
#include <set>
using namespace std;

// STATES
// basic lock
#define LOCK_SYNC     0
#define LOCK_PRELOCK  1
#define LOCK_LOCK     2
#define LOCK_DELETING 3  // auth only
#define LOCK_DELETED  4

// async lock
#define LOCK_ASYNC    5
#define LOCK_RESYNC   6  // to sync
#define LOCK_RESYNC2  7  // to lock


// -- basic lock

class BasicLock {
 protected:
  // lock state
  char     state;
  set<int> gather_set;  // auth

 public:
  BasicLock() : state(0) {
  }

  char get_state() { return state; }
  char set_state(char s) { state = s; };
  set<int>& get_gather_set() { return gather_set; }

  void init_gather(set<int>& i) {
	gather_set = i;
  }
  
  bool can_read(bool auth) {
	if (auth)
	  return (state == LOCK_SYNC) || (state == LOCK_PRELOCK) || (state == LOCK_LOCK);
	if (!auth)
	  return (state == LOCK_SYNC);
  }
  
  bool can_write(bool auth) {
	return auth && state == LOCK_LOCK;
  }
};

inline ostream& operator<<(ostream& out, BasicLock& l) {
  static char* __lock_states[] = {
	"sync",
	"prelock",
	"lock",
	"deleting",
	"deleted",
	"async",
	"resync",
	"resync2"
  }; 

  out << "Lock(" << __lock_states[l.get_state()];

  if (!l.get_gather_set().empty()) out << " g=" << l.get_gather_set();

  // rw?
  out << " ";
  if (l.can_read(true)) out << "r";
  if (l.can_write(true)) out << "w";
  out << "/";
  if (l.can_read(false)) out << "r";
  if (l.can_write(false)) out << "w";  

  out << ")";
  return out;
}


// -- async lock

class AsyncLock : public BasicLock {
 public:
  AsyncLock() : BasicLock() {
	assert(state == 0);
  }
  bool can_write(bool auth) {
	if (auth) 
	  return (state == LOCK_LOCK) 
		|| (state == LOCK_ASYNC) || (state == LOCK_RESYNC) || (state == LOCK_RESYNC2);
	if (!auth)
	  return (state == LOCK_ASYNC);
  }
};


#endif
