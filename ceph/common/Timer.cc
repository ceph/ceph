

#include "Timer.h"
#include "Cond.h"

#include "include/config.h"
#include "include/Context.h"

#include "msg/Messenger.h"

#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << "Timer: "

#define DBL 20

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
	timepair_t now = g_clock.gettimepair();

	// any events due?
	timepair_t next;
	Context *event = get_next_scheduled(next);
	  
	if (event && now > next) {
	  // move to pending list
	  map< timepair_t, set<Context*> >::iterator it = scheduled.begin();
	  while (it != scheduled.end()) {
		if (it->first > now) break;

		timepair_t t = it->first;
		dout(DBL) << "queuing event(s) scheduled at " << t << endl;

		pending[t] = it->second;
		it++;
		scheduled.erase(t);
	  }

	  if (messenger_to_kick) {
		dout(DBL) << "kicking messenger" << endl;
		messenger_to_kick->trigger_timer(this);
	  }
	}

	else {
	  // sleep
	  if (event) {
		dout(DBL) << "sleeping until " << next << endl;
		cond.Wait(lock, next);  // wait for waker or time
	  } else {
		dout(DBL) << "sleeping" << endl;
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
	dout(DBL) << "register_timer kicking thread" << endl;
	cond.Signal();
  } else {
	dout(DBL) << "register_timer starting thread" << endl;
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


/*
 * schedule
 */


void Timer::add_event_after(float seconds,
							Context *callback) 
{
  timepair_t when = g_clock.gettimepair();
  when.first += (int)seconds;
  add_event_at(when, callback);
}

void Timer::add_event_at(timepair_t when,
						 Context *callback) 
{
  // insert
  dout(DBL) << "add_event " << callback << " at " << when << endl;

  lock.Lock();
  scheduled[ when ].insert(callback);
  event_times[callback] = when;
  lock.Unlock();
  
  // make sure i wake up
  register_timer();
}

bool Timer::cancel_event(Context *callback) 
{
  lock.Lock();
  
  dout(DBL) << "cancel_event " << callback << endl;

  if (!event_times.count(callback)) {
	dout(DBL) << "cancel_event " << callback << " wasn't scheduled?" << endl;
	lock.Unlock();
	assert(0);
	return false;     // wasn't scheduled.
  }

  timepair_t tp = event_times[callback];

  event_times.erase(callback);
  scheduled.erase(tp);
  pending.erase(tp);
  
  lock.Unlock();
  return true;
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

	dout(DBL) << "executing event " << event << " scheduled for " << when << endl;
	event->finish(0);
	delete event;
	
	lock.Lock();
  }

  dout(DBL) << "no more events for now" << endl;

  lock.Unlock();
}
