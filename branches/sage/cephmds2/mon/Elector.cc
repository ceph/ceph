
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
    for (unsigned i=0; i<processes.size(); i++) {
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
            <<  " timed out (" << ack_msg_count << "/" << (f + 1)
            << ") ... increasing epoch. Now epoch is "
            << registry[whoami].epoch.s_num
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
    for (unsigned i=0; i<processes.size(); i++) {
      mon->messenger->send_message(msg, MSG_ADDR_MON(processes[i]));
    }
    
    // Start the trip timer
    //round_trip_timer = new C_Elect_TripTimer(this);
    g_timer.add_event_after(trip_delta, new C_Elect_TripTimer(this));
  }
  lock.Unlock();
};



//////////////////////////


Elector::Epoch Elector::get_min_epoch()
{
  assert(!views.empty());
  Epoch min = views[0].state.epoch;
  for (unsigned i=1; i<views.size(); i++) {
    if (views[i].state.epoch < min && !views[i].expired) {
      min = views[i].state.epoch;
    }
  }
  return min;
}


void Elector::dispatch(Message *m)
{
  lock.Lock();
  {
    switch (m->get_type()) {
    case MSG_MON_ELECTION_ACK:
      handle_ack((MMonElectionAck*)m);
      break;
    
    case MSG_MON_ELECTION_STATUS:
      handle_status((MMonElectionStatus*)m);
      break;
    
    case MSG_MON_ELECTION_COLLECT:
      handle_collect((MMonElectionCollect*)m);
      break;
    
    case MSG_MON_ELECTION_REFRESH:
      handle_refresh((MMonElectionRefresh*)m);
      break;
      
    default:
      assert(0);
    }
  }
  lock.Unlock();
}

void Elector::handle_ack(MMonElectionAck* msg)
{
  assert(refresh_num >= msg->refresh_num);
  
  if (refresh_num > msg->refresh_num) {
    // we got the message too late... discard it
    return;
  }
  ack_msg_count++;
  if (ack_msg_count >= f + 1) {
    dout(5) << "Received _f+1 acks, increase freshness" << endl;
    //g_timer.cancel_event(round_trip_task);
    //round_trip_timer->cancel();
    registry[whoami].freshness++;         
  }
  
  delete msg;
}

void Elector::handle_collect(MMonElectionCollect* msg)
{
  mon->messenger->send_message(new MMonElectionStatus(msg->get_source().num(),
                                                      msg->read_num,
                                                      registry),
                               msg->get_source());
  delete msg;
}

void Elector::handle_refresh(MMonElectionRefresh* msg)
{
  if (registry[msg->p] < msg->state) {
    // update local data
    registry[msg->p] = msg->state;

    // reply to msg
    mon->messenger->send_message(new MMonElectionAck(msg->p, 
                                                     msg->refresh_num), 
                                 msg->get_source());
  }

  delete msg;
}


void Elector::handle_status(MMonElectionStatus* msg)
{
  if (read_num != msg->read_num) {
    dout(1) << "handle_status "
            << ":DISCARDED B/C OF READNUM(" << read_num << ":"
            << msg->read_num << ")" 
            << endl;
    return;
  }
  for (unsigned i=0; i<processes.size(); i++) {
    int r = processes[i];
    // Put in the view the max value between then new state and the stored one
    if ( msg->registry[r] > views[r].state ) {
      views[r].state = msg->registry[r];
    }
  }
        
  status_msg_count++;
  if (status_msg_count >= (int)processes.size() - f) { // Responses from quorum collected
    for (unsigned i=0; i<processes.size(); i++) {
      int r = processes[i];
      // Check if r has refreshed its epoch number
      if (!( views[r].state > old_views[r].state )) {
        dout(5) << ":Other process (" << r << ") has expired" << endl;
        views[r].expired = true;
      }
      if (views[r].state.epoch > old_views[r].state.epoch) {
        views[r].expired = false;
      }
    }
    Epoch leader_epoch = get_min_epoch();
    leader_id = leader_epoch.p_id;
    dout(1) << " thinks leader has ID: " << leader_id << endl;
    
    // Restarts the timer for the next iteration
    g_timer.add_event_after(main_delta + trip_delta, new C_Elect_ReadTimer(this));
  }
}




