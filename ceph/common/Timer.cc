

#include "Timer.h"

#include "include/config.h"

#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << "Timer: "



void Timer::execute_pending()
{
  double now = clock.gettime();
  dout(12) << "now = " << now << endl;
  
  while (event_map.size()) {
	Context *event = 0;
	
	{ // scope this so my iterator is destroyed quickly
	  // check first event
	  map<double,Context*>::iterator it = event_map.begin();
	  if (it->first > now) {
		dout(12) << "next event at " << it->first << ", stopping" << endl;
		break;  // no events!
	  }
	  
	  // claim and remove from map
	  event = it->second;
	  event_map.erase(it);
	  
	  dout(5) << "executing event " << event << " scheduled for " << it->first << endl;
	}
	
	// exec
	assert(event);
	event->finish(0);
	delete event;
  }
  
  dout(12) << "no more events for now" << endl;
}
