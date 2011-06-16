#include "BackTrace.h"
#include "Clock.h"
#include "common/config.h"
#include "common/environment.h"
#include "include/types.h"
#include "lockdep.h"

#include <ext/hash_map>

#define DOUT_SUBSYS lockdep

// global
int g_lockdep = get_env_int("CEPH_LOCKDEP");

// disable lockdep when this module destructs.
struct lockdep_stopper_t {
  ~lockdep_stopper_t() {
    g_lockdep = 0;
  }
};

static lockdep_stopper_t lockdep_stopper;


pthread_mutex_t lockdep_mutex = PTHREAD_MUTEX_INITIALIZER;


#define MAX_LOCKS  100   // increase me as needed

hash_map<const char *, int> lock_ids;
map<int, const char *> lock_names;

int last_id = 0;

hash_map<pthread_t, map<int,BackTrace*> > held;
BackTrace *follows[MAX_LOCKS][MAX_LOCKS];       // follows[a][b] means b taken after a


#define BACKTRACE_SKIP 3


int lockdep_dump_locks()
{
  pthread_mutex_lock(&lockdep_mutex);

  for (hash_map<pthread_t, map<int,BackTrace*> >::iterator p = held.begin();
       p != held.end();
       p++) {
    dout(0) << "--- thread " << p->first << " ---" << dendl;
    for (map<int,BackTrace*>::iterator q = p->second.begin();
	 q != p->second.end();
	 q++) {
      dout(0) << "  * " << lock_names[q->first] << "\n";
      if (q->second)
	q->second->print(*_dout);
      *_dout << dendl;
    }
  }

  pthread_mutex_unlock(&lockdep_mutex);
  return 0;
}


int lockdep_register(const char *name)
{
  int id;

  pthread_mutex_lock(&lockdep_mutex);

  if (last_id == 0)
    for (int i=0; i<MAX_LOCKS; i++)
      for (int j=0; j<MAX_LOCKS; j++)
	follows[i][j] = NULL;

  hash_map<const char *, int>::iterator p = lock_ids.find(name);
  if (p == lock_ids.end()) {
    assert(last_id < MAX_LOCKS);
    id = last_id++;
    lock_ids[name] = id;
    lock_names[id] = name;
    dout(10) << "registered '" << name << "' as " << id << dendl;
  } else {
    id = p->second;
    dout(20) << "had '" << name << "' as " << id << dendl;
  }

  pthread_mutex_unlock(&lockdep_mutex);

  return id;
}


// does a follow b?
static bool does_follow(int a, int b)
{
  if (follows[a][b]) {
    dout(0) << "\n";
    *_dout << "------------------------------------" << "\n";
    *_dout << "existing dependency " << lock_names[a] << " (" << a << ") -> "
           << lock_names[b] << " (" << b << ") at:\n";
    follows[a][b]->print(*_dout);
    *_dout << dendl;
    return true;
  }

  for (int i=0; i<MAX_LOCKS; i++) {
    if (follows[a][i] &&
	does_follow(i, b)) {
      dout(0) << "existing intermediate dependency " << lock_names[a]
          << " (" << a << ") -> " << lock_names[i] << " (" << i << ") at:\n";
      follows[a][i]->print(*_dout);
      *_dout << dendl;
      return true;
    }
  }

  return false;
}

int lockdep_will_lock(const char *name, int id)
{
  pthread_t p = pthread_self();
  if (id < 0) id = lockdep_register(name);

  pthread_mutex_lock(&lockdep_mutex);
  dout(20) << "_will_lock " << name << " (" << id << ")" << dendl;

  // check dependency graph
  map<int, BackTrace *> &m = held[p];
  for (map<int, BackTrace *>::iterator p = m.begin();
       p != m.end();
       p++) {
    if (p->first == id) {
      dout(0) << "\n";
      *_dout << "recursive lock of " << name << " (" << id << ")\n";
      BackTrace *bt = new BackTrace(BACKTRACE_SKIP);
      bt->print(*_dout);
      if (p->second) {
	*_dout << "\npreviously locked at\n";
	p->second->print(*_dout);
      }
      *_dout << dendl;
      assert(0);
    }
    else if (!follows[p->first][id]) {
      // new dependency

      // did we just create a cycle?
      BackTrace *bt = new BackTrace(BACKTRACE_SKIP);
      if (does_follow(id, p->first)) {
	dout(0) << "new dependency " << lock_names[p->first]
		<< " (" << p->first << ") -> " << name << " (" << id << ")"
		<< " creates a cycle at\n";
	bt->print(*_dout);
	*_dout << dendl;

	dout(0) << "btw, i am holding these locks:" << dendl;
	for (map<int, BackTrace *>::iterator q = m.begin();
	     q != m.end();
	     q++) {
	  dout(0) << "  " << lock_names[q->first] << " (" << q->first << ")" << dendl;
	  if (q->second) {
	    dout(0) << " ";
	    q->second->print(*_dout);
	    *_dout << dendl;
	  }
	}

	dout(0) << "\n" << dendl;

	// don't add this dependency, or we'll get aMutex. cycle in the graph, and
	// does_follow() won't terminate.

	assert(0);  // actually, we should just die here.
      } else {
	follows[p->first][id] = bt;
	dout(10) << lock_names[p->first] << " -> " << name << " at" << dendl;
	//bt->print(*_dout);
      }
    }
  }

  pthread_mutex_unlock(&lockdep_mutex);
  return id;
}

int lockdep_locked(const char *name, int id, bool force_backtrace)
{
  pthread_t p = pthread_self();

  if (id < 0) id = lockdep_register(name);

  pthread_mutex_lock(&lockdep_mutex);
  dout(20) << "_locked " << name << dendl;
  if (g_lockdep >= 2 || force_backtrace)
    held[p][id] = new BackTrace(BACKTRACE_SKIP);
  else
    held[p][id] = 0;
  pthread_mutex_unlock(&lockdep_mutex);
  return id;
}

int lockdep_will_unlock(const char *name, int id)
{
  pthread_t p = pthread_self();

  if (id < 0) {
    //id = lockdep_register(name);
    assert(id == -1);
    return id;
  }

  pthread_mutex_lock(&lockdep_mutex);
  dout(20) << "_will_unlock " << name << dendl;

  // don't assert.. lockdep may be enabled at any point in time
  //assert(held.count(p));
  //assert(held[p].count(id));

  delete held[p][id];
  held[p].erase(id);
  pthread_mutex_unlock(&lockdep_mutex);
  return id;
}


