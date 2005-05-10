#ifndef __TIMER_H
#define __TIMER_H

#include "include/Context.h"
#include "Clock.h"

#include <map>
#include <set>
using namespace std;


/*** Timer
 * schedule callbacks
 */

class Messenger;

class Timer {
 private:
  map<double, set<Context*> > event_map;    // time -> (context ...)
  map<Context*, double>        event_times;  // event -> time

  // get time of the next event
  double next_event_time() {
	map< double, set<Context*> >::iterator it = event_map.begin();
	return it->first;
  }
  
  Messenger *messenger;
  void register_timer();  // make sure i get a callback
  void cancel_timer();  // make sure i get a callback

 public:
  Timer() : messenger(0) { }
  ~Timer() { 
	// cancel any wakeup crap
	cancel_timer();

	// 
  }

  void set_messenger(Messenger *m) {
	messenger = m;
  }


  // schedule events
  void add_event_after(double seconds,
					   Context *callback) {
	add_event_at(g_clock.gettime() + seconds, callback);
  }
  
  void add_event_at(double when,
					Context *callback) {
	// insert
	event_map[when].insert(callback);
	event_times[callback] = when;

	// make sure i wake up (soon enough)
	register_timer();
  }

  bool cancel_event(Context *callback) {
	if (!event_times.count(callback)) 
	  return false;     // wasn't scheduled.
	
	double when = event_times[callback];
	event_times.erase(callback);
	event_map[when].erase(callback);
	if (event_map[when].empty()) event_map.erase(when);
	return true;
  }

  // execute pending events
  void execute_pending();

};


// single global instance
extern Timer g_timer;



#endif
