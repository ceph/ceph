#ifndef __TIMER_H
#define __TIMER_H

/*** Timer
 * schedule callbacks
 */

class Timer {

  // event map: time -> context
  map<double, Context*> event_map;

 public:
  
  // schedule events
  void add_event_after(double seconds,
					   Context *callback) {
	add_event_at(clock.gettime(), callback);
  }
  
  void add_event_at(double when,
					Context *callback) {
	
  }

  // execute pending events
  void execute_pending();

};











#endif
