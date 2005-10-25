
#ifndef __MESSENGER_H
#define __MESSENGER_H

#include <map>
using namespace std;

#include "Message.h"
#include "Dispatcher.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "include/Context.h"


typedef __uint64_t lamport_t;


class MDS;
class Timer;

class Messenger {
 private:
  Dispatcher          *dispatcher;
  msg_addr_t           _myaddr;
  lamport_t            lamport_clock;

  // callbacks

  // procedure call fun
  long                   _last_pcid;
  Mutex                  _lock;      // protect call_sem, call_reply
  map<long, Cond*>       call_cond;
  map<long, Message*>    call_reply;

 public:
  Messenger(msg_addr_t w) : dispatcher(0), _myaddr(w), lamport_clock(0), _last_pcid(1) { }
  virtual ~Messenger() { }
  
  void       set_myaddr(msg_addr_t m) { _myaddr = m; }
  msg_addr_t get_myaddr() { return _myaddr; }

  lamport_t get_lamport() { return lamport_clock++; }
  lamport_t peek_lamport() { return lamport_clock; }
  void bump_lamport(lamport_t other) { 
	if (other >= lamport_clock)
	  lamport_clock = other+1;
  }

  virtual int shutdown() = 0;
  
  void queue_callback(Context *c);

  // setup
  void set_dispatcher(Dispatcher *d) { dispatcher = d; ready(); }
  Dispatcher *get_dispatcher() { return dispatcher; }
  virtual void ready() { }

  // dispatch incoming messages
  virtual void dispatch(Message *m);

  // send message
  virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0) = 0;

  // make a procedure call
  virtual Message* sendrecv(Message *m, msg_addr_t dest, int port=0);
};

// callbacks
void messenger_do_callbacks();
extern Context *msgr_callback_kicker;


extern Message *decode_message(msg_envelope_t &env, bufferlist& bl);



#endif
