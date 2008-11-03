
#include "Mutex.h"

int g_lockdep = 0;

#ifdef LOCKDEP

#include "include/types.h"
#include "Clock.h"
#include "BackTrace.h"

#include <ext/hash_map>

#include "config.h"

#define  dout(l)    if (l<=g_conf.debug_lockdep) *_dout << g_clock.now() << " " << pthread_self() << " lockdep: "
#define  derr(l)    if (l<=g_conf.debug_lockdep) *_derr << g_clock.now() << " " << pthread_self() << " lockdep: "


pthread_mutex_t lockdep_mutex = PTHREAD_MUTEX_INITIALIZER;

hash_map<pthread_t, map<const char *, BackTrace *> > held;
hash_map<const char *, map<const char *, BackTrace *> > follows;       // <left item> follows <right items>
hash_map<const char *, map<const char *, BackTrace *> > follows_ever;

#define BACKTRACE_SKIP 3

// does a follow b?
bool does_follow(const char *a, const char *b)
{
  if (!follows.count(a))
    return false;
  
  map<const char *, BackTrace *> &s = follows[a];
  if (s.count(b)) {
    dout(0) << "------------------------------------" << std::endl;
    dout(0) << a << " <- " << b << " at: " << std::endl;
    s[b]->print(*_dout);
    *_dout << std::endl;
    return true;
  }

  for (map<const char *, BackTrace *>::iterator p = s.begin(); p != s.end(); p++)
    if (does_follow(p->first, b)) {
      dout(0) << a << " <- " << p->first << " at: " << std::endl;
      p->second->print(*_dout);
      *_dout << std::endl;
      return true;
    }

  return false;
}

void Mutex::_will_lock()
{
  pthread_t p = pthread_self();

  dout(20) << name << " _will_lock" << std::endl;

  pthread_mutex_lock(&lockdep_mutex);

  // check dependency graph
  map<const char *, BackTrace *> &m = held[p];
  for (map<const char *, BackTrace *>::iterator p = m.begin();
       p != m.end();
       p++) {
    if (!follows[name].count(p->first)) {
      // new dependency 
      
      // did we just create a cycle?
      BackTrace *bt = new BackTrace(BACKTRACE_SKIP);
      if (does_follow(p->first, name)) {
	dout(0) << "new dependency " << name << " <- " << p->first
		<< " creates a cycle at"
		<< std::endl;
	bt->print(*_dout);
	*_dout << std::endl;

	dout(0) << "btw, i am holding these locks:" << std::endl;
	for (map<const char *, BackTrace *>::iterator q = m.begin();
	     q != m.end();
	     q++) {
	  dout(0) << q->first << std::endl;
	  if (g_lockdep >= 2) {
	    q->second->print(*_dout);
	    *_dout << std::endl;
	  }
	}

	*_dout << std::endl;
	*_dout << std::endl;

	// don't add this dependency, or we'll get aMutex. cycle in the graph, and
	// does_follow() won't terminate.
      } else {
	follows[name][p->first] = bt;
	dout(10) << name << " <- " << p->first << " at" << std::endl;
	//bt->print(*_dout);
      }
    }
  }

  pthread_mutex_unlock(&lockdep_mutex);
}

void Mutex::_locked()
{
  pthread_t p = pthread_self();

  dout(20) << name << " _locked" << std::endl;

  pthread_mutex_lock(&lockdep_mutex);
  if (g_lockdep >= 2)
    held[p][name] = new BackTrace(BACKTRACE_SKIP);
  else
    held[p][name] = 0;
  pthread_mutex_unlock(&lockdep_mutex);
}

void Mutex::_unlocked()
{
  pthread_t p = pthread_self();
  
  dout(20) << name << " _unlocked" << std::endl;
  pthread_mutex_lock(&lockdep_mutex);
  assert(held.count(p));
  assert(held[p].count(name));
  delete held[p][name];
  held[p].erase(name);
  pthread_mutex_unlock(&lockdep_mutex);
}





#endif
