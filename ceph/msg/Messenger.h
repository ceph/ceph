
#ifndef __MESSENGER_H
#define __MESSENGER_H

#include <list>
#include <vector>
using namespace std;

#include "Message.h"
#include "Dispatcher.h"

class MDS;
class Timer;

class Messenger {
 protected:
  //vector<Dispatcher*>  dispatch_vec;   // or to specific ports
  Dispatcher          *dispatcher;
  list<Message*>       incoming;        // incoming queue

 public:
  Messenger() : dispatcher(0) { }
  
  // administrative
  void set_dispatcher(Dispatcher *d) {   // for default.. OLD WAY
	//dispatch_vec[0] = d; 
	dispatcher = d;
  }   
  /*
  void add_dispatcher(Dispatcher *d) {                     // NEW WAY
	// allocate a port and add to my vec
	int port = dispatch_vec.size();    
	dispatch_vec.push_back(d);
	assert(dispatch_vec[port] == d);

	// tell dispatcher what port to send from.
	d->set_messenger_port(this,port);
  }
  */

  virtual int shutdown() = 0;

  // -- message interface
  // send message
  virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0) = 0;

  // make a procedure call
  virtual Message* sendrecv(Message *m, msg_addr_t dest, int port=0) = 0;

  // wait (block) for a message, or timeout.  Don't return message yet.
  //virtual int wait_message(time_t seconds) = 0;

  // wait (block) for a request from anyone
  //virtual Message *recv() = 0

  
  // events
  virtual void trigger_timer(Timer *t) = 0;


  // -- incoming queue --
  // (that nothing uses)
  Message *get_message() {
	if (incoming.size() > 0) {
	  Message *m = incoming.front();
	  incoming.pop_front();
	  return m;
	}
	return NULL;
  }
  bool queue_incoming(Message *m) {
	incoming.push_back(m);
	return true;
  }
  int num_incoming() {
	return incoming.size();
  }
  void dispatch(Message *m) {
	dispatcher->dispatch(m);

	/*
	// do we know the port?
	if (m->get_dest_port() >= 1000) {//< dispatch_vec.size()) {
	  // new way
	  dispatch_vec[m->get_dest_port()-1000]->dispatch(m);
	} else {
	  // default (old way)
	  dispatch_vec[0]->dispatch(m);
	}
	*/
  }

};


extern Message *decode_message(crope& rope);



#endif
