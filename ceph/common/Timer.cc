

#include "Timer.h"

#include "include/config.h"
#include "include/Context.h"

#include "msg/Messenger.h"

#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << "Timer: "


#include <signal.h>
#include <sys/time.h>

Messenger *messenger_to_kick = 0;
Timer     *static_timer = 0;

void timer_signal_handler(int sig)
{
  // kick messenger.
  if (messenger_to_kick && static_timer) {
	messenger_to_kick->trigger_timer(static_timer);
  }
}


void Timer::register_timer()
{
  dout(10) << "register_timer installing signal handler" << endl;

  /* only one of these per process.  
	 should be okay, i think.. in the cases where we have multiple people in the same
	 process (FakeMessenger), we're actually kicking the same event loop anyway.
  */
  // FIXME i shouldn't have to do this every time?
  static_timer = this;
  messenger_to_kick = messenger;
  
  // install handler
  struct sigaction ac;
  memset(&ac, sizeof(ac), 0);
  ac.sa_handler = timer_signal_handler;
  ///FIXMEac.sa_mask = 0;  // hmm.
  
  sigaction(SIGALRM, &ac, NULL);

  // set alarm
  double now = g_clock.gettime();
  double delay = next_event_time() - now;
  double sec;
  double usec = modf(delay, &sec);

  dout(10) << "setting itimer to go off in " << sec << "s " << usec << "us" << endl;
  struct itimerval it;
  it.it_value.tv_sec = (int)sec;
  it.it_value.tv_usec = (int)(usec * 1000000);
  it.it_interval.tv_sec = 0;   // don't repeat!
  it.it_interval.tv_usec = 0;
  setitimer(ITIMER_REAL, &it, 0);
}

void Timer::cancel_timer()
{
  // clear my callback pointers
  messenger_to_kick = 0;
  static_timer = 0;
  
  // remove my handler.  MESSILY FIXME
  sigaction(SIGALRM, 0, 0);
}


void Timer::execute_pending()
{
  double now = g_clock.gettime();
  dout(12) << "now = " << now << endl;
  
  while (event_map.size()) {
	Context *event = 0;
	
	double next = next_event_time();
	if (next > now) break;
	
	{ // scope this so my iterator is destroyed quickly
	  // grab first
	  set<Context*>::iterator it = event_map[next].begin();
	  
	  event = *it;
	  assert(event);
	}

	// claim and remove from map
	event_map[next].erase(event);
	event_times.erase(event);
	
	// exec
	dout(5) << "executing event " << event << " scheduled for " << next << endl;
	
	event->finish(0);
	delete event;
  }
  
  dout(12) << "no more events for now" << endl;
}
