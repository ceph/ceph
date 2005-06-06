

#include "Timer.h"
#include "Cond.h"

#include "include/config.h"
#include "include/Context.h"

#include "msg/Messenger.h"

#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << "Timer: "


#include <signal.h>
#include <sys/time.h>
#include <math.h>

// single global instance
Timer      g_timer;

Messenger *messenger_to_kick = 0;

ostream& operator<<(ostream& out, timepair_t& t)
{
  return out << t.first << "." << t.second;
}


/**** thread solution *****/

void *timer_thread_entrypoint(void *arg) 
{
  Timer *t = (Timer*)arg;
  t->timer_thread();
  return 0;
}

void Timer::timer_thread()
{
  lock.Lock();
  
  while (!thread_stop) {
	
	// now
	struct timeval nowtv;
	g_clock.gettime(&nowtv);
	timepair_t now = timepair_t(nowtv.tv_sec, nowtv.tv_usec);

	// any events due?
	timepair_t next;
	Context *event = get_next_scheduled(next);
	  
	if (event && now > next) {
	  // move to pending list
	  map< timepair_t, set<Context*> >::iterator it = scheduled.begin();
	  while (it != scheduled.end()) {
		if (it->first > now) break;

		timepair_t t = it->first;
		dout(5) << "queuing event(s) scheduled at " << t << endl;

		pending[t] = it->second;
		it++;
		scheduled.erase(t);
	  }

	  dout(5) << "kicking messenger" << endl;
	  messenger_to_kick->trigger_timer(this);
	}

	else {
	  // sleep
	  if (event) {
		dout(5) << "sleeping until " << next << endl;
		struct timeval tv;
		tv.tv_sec = next.first;
		tv.tv_usec = next.second;
		cond.Wait(lock, &tv);  // wait for waker or time
	  } else {
		dout(5) << "sleeping" << endl;
		cond.Wait(lock);         // wait for waker
	  }
	}
  }

  lock.Unlock();
}



/**
 * Timer bits
 */

void Timer::set_messenger(Messenger *m) 
{
  dout(10) << "messenger to kick is " << m << endl;
  messenger_to_kick = m;
}
void Timer::unset_messenger() 
{
  dout(10) << "unset messenger" << endl;
  messenger_to_kick = 0;
  cancel_timer();
}

void Timer::register_timer()
{
  if (thread_id) {
	dout(10) << "register_timer kicking thread" << endl;
	cond.Signal();
  } else {
	dout(10) << "register_timer starting thread" << endl;
	pthread_create(&thread_id, NULL, timer_thread_entrypoint, (void*)this);
  }
}

void Timer::cancel_timer()
{
  // clear my callback pointers
  messenger_to_kick = 0;

  if (thread_id) {
	dout(10) << "setting thread_stop flag" << endl;
	lock.Lock();
	thread_stop = true;
	lock.Unlock();
	cond.Signal();
	
	dout(10) << "waiting for thread to finish" << endl;
	void *ptr;
	pthread_join(thread_id, &ptr);
	
	dout(10) << "thread finished, exit code " << ptr << endl;
  }
}



/***
 * do user callbacks
 *
 * this should be called by the Messenger in the proper thread (usually same as incoming messages)
 */

void Timer::execute_pending()
{
  lock.Lock();

  while (pending.size()) {
	timepair_t when;
	Context *event = take_next_pending(when);

	lock.Unlock();

	dout(5) << "executing event " << event << " scheduled for " << when << endl;
	event->finish(0);
	delete event;
	
	lock.Lock();
  }

  dout(12) << "no more events for now" << endl;

  lock.Unlock();
}
