#ifndef __TIMER_H
#define __TIMER_H

#include "include/types.h"
#include "include/Context.h"
#include "Clock.h"

#include "Mutex.h"
#include "Cond.h"

#include <map>
#include <set>
using namespace std;


/*** Timer
 * schedule callbacks
 */

class Messenger;

class Timer {
 private:
  map< timepair_t, set<Context*> >  scheduled;    // time -> (context ...)
  map< timepair_t, set<Context*> >  pending;      // time -> (context ...)  
  map< Context*, timepair_t >       event_times;  // event -> time

  // get time of the next event
  Context* get_next_scheduled(timepair_t& when) {
	if (scheduled.empty()) return 0;
	map< timepair_t, set<Context*> >::iterator it = scheduled.begin();
	when = it->first;
	set<Context*>::iterator sit = it->second.begin();
	return *sit;
  }

  // get next pending event
  Context* take_next_pending(timepair_t& when) {
	if (pending.empty()) return 0;
	
	map< timepair_t, set<Context*> >::iterator it = pending.begin();
	when = it->first;

	// take and remove
	set<Context*>::iterator sit = it->second.begin();
	Context *event = *sit;
	it->second.erase(sit);
	if (it->second.empty()) pending.erase(it);

	return event;
  }

  void register_timer();  // make sure i get a callback
  void cancel_timer();    // make sure i get a callback

  pthread_t thread_id;
  bool      thread_stop;
  Mutex     lock;
  Cond      cond;
 public:
  void timer_thread();    // waiter thread (that wakes us up)

 public:
  Timer() { 
	thread_id = 0;
	thread_stop = false;
  }
  ~Timer() { 
	// cancel any wakeup/thread crap
	cancel_timer();

	// clean up pending events
	// ** FIXME **
  }

  void set_messenger(Messenger *m);
  void unset_messenger();

  // schedule events
  void add_event_after(float seconds,
					   Context *callback);
  void add_event_at(timepair_t when,
					Context *callback);
  bool cancel_event(Context *callback);

  // execute pending events
  void execute_pending();

};


// single global instance
extern Timer g_timer;



#endif
