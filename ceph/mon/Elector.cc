
#include "Elector.h"
#include "Monitor.h"

#include "common/Timer.h"

#include "messages/MMonElectionRefresh.h"
#include "messages/MMonElectionStatus.h"
#include "messages/MMonElectionAck.h"
#include "messages/MMonElectionCollect.h"

#include "config.h"
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << "mon" << whoami << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << "mon" << whoami << " "



class C_Elect_ReadTimer : public Context {
  Elector *mon;
public:
  C_Elect_ReadTimer(Elector *m) : mon(m){}
  void finish(int r) {
	mon->read_timer();
  }
};

void Elector::read_timer()
{
  lock.Lock();
  {
    read_num++;
    status_msg_count = 0;
    old_views = views;   // TODO deep copy
    for (int i=0; i<processes.size(); i++) {
	  mon->messenger->send_message(new MMonElectionCollect(read_num), 
								   MSG_ADDR_MON(processes[i]));
	}
  }
  lock.Unlock();
};

class C_Elect_TripTimer : public Context {
  Elector *mon;
public:
  C_Elect_TripTimer(Elector *m) : mon(m){}
  void finish(int r) {
	mon->trip_timer();
  }
};

void Elector::trip_timer()
{
  lock.Lock();
  {
    views[whoami].expired = true;
	registry[whoami].epoch.s_num++;
    dout(1) << "Process " << whoami
			<<  " timed out (" << m->ackMsgCount << "/" << (m->f + 1)
			<< ") ... increasing epoch. Now epoch is "
			<< m->registry[whoami]->epoch->s_num
			<< endl;
  }
  lock.Unlock();
};



class C_Elect_RefreshTimer : public Context {
  Elector *mon;
public:
  C_Elect_RefreshTimer(Elector *m) : mon(m) {}
  void finish(int r) {
	mon->refresh_timer();
  }
};

void Elector::refresh_timer()
{
  lock.Lock();
  {
	ack_msg_count = 0;
	refresh_num++;
	MMonElectionRefresh *msg = new MMonElectionRefresh(whoami, registry[whoami], refresh_num);
	for (int i=0; i<processes.size(); i++) {
	  mon->messenger->send_message(msg, MSG_ADDR_MON(processes[i]));
	}
	
	// Start the trip timer
	round_trip_timer = new C_Elect_TripTimer(this);
	g_timer.add_event_after(trip_delta, m->round_trip_timer);        
  }
  lock.Unlock();
};



//////////////////////////


void Elector::dispatch(Message *m)
{
  lock.Lock();
  {
	switch (m->get_type()) {
	case MSG_MON_ELECTION_ACK:
	  handle_ack(m);
	  break;
	
	case MSG_MON_ELECTION_STATUS:
	  handle_status(m);
	  break;
	
	case MSG_MON_ELECTION_COLLECT:
	  handle_collect(m);
	  break;
	
	case MSG_MON_ELECTION_REFRESH:
	  handle_refresh(m);
	  break;
	  
	default:
	  assert(0);
	}
  }
  lock.Unlock();
}

void Elector::handle_ack(MMonElectionAck* msg)
{
  assert(refresh_num >= msg->get_refresh_num());
  
  if (refresh_num > msg->get_refresh_num()) {
	// we got the message too late... discard it
	return;
  }
  ack_msg_count++;
  if (ack_msg_count >= f + 1) {
	dout(5) << "P" << p_id << ": Received _f+1 acks, increase freshness" << endl;
	g_timer.cancel_event(round_trip_task);
	round_trip_timer->cancel();
	registry[p_id]->freshness++;         
  }
  
  delete msg;
}

void Elector::handle_collect(MMonElectionCollect* msg)
{
  messenger->send_message(new MMonElectionStatus(whoami, 
												 msg->getSenderId(), 
												 msg->getReadNum(), 
												 registry),
						  msg->get_source());
  delete msg;
}

void Elector::handle_refresh(MMonElectionRefresh* msg)
{
  if (this->registry[msg->p]->isLesser(msg->state)) {
	// update local data
	registry[msg->p] = msg->state;

	// reply to msg
	messenger->send_message(new MMonElectionAck(whoami, 
												msg->p, 
												msg->refresh_num), 
							msg->get_source());
  }

  delete msg;
}


void Elector::handle_status(MMonElectionStatus* msg)
{
  if (read_num != msg->read_num) {
	dout(1) << _processId << ":HANDLING:" << msg.getType()
			<< ":DISCARDED B/C OF READNUM(" << read_num << ":"
			<< msg->read_num << ")" 
			<< endl;
	return;
  }
  for (int i=0; i<this->processes->size(); i++) {
	int r = processes[i];
	// Put in the view the max value between then new state and the stored one
	if ( msg->registry[r]->isGreater(views[r]->state) ) {
	  views[r]->state = msg->registry[r];
	}
  }
		
  status_msg_count++;
  if (status_msg_count >= processes.size() - _f) { // Responses from quorum collected
	for (int i=0; i<processes.size(); i++) {
	  int r = processes[i];
	  // Check if r has refreshed its epoch number
	  if (! this->views[r]->state->isGreater(this->old_views[r]->state) )
		{
		  dout(5) << this->whoami << ":Other process (" << r << ") has expired" << endl;
		  this->views[r]->expired = true;
		}
	  if (this->views[r]->state->e_num->isGreater(this->old_views[r]->state->e_num))
		{
		  this->views[r]->expired = false;
		}
	}
	Epoch leader_epoch = get_min_epoch();
	this->leader_id = leader_epoch->p_id;
	dout(1) << this->whoami << " thinks leader has ID: " << this->leader_id << endl;
	
	// Restarts the timer for the next iteration
	g_timer.add_event_after(main_delta + trip_delta, new C_Elect_ReadTimer(this));
  }
}




