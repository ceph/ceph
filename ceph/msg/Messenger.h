
#ifndef __MESSENGER_H
#define __MESSENGER_H

#include <map>
using namespace std;

#include "Message.h"
#include "Dispatcher.h"
#include "common/Mutex.h"
#include "common/Cond.h"



class MDS;
class Timer;

class Messenger {
 private:
  Dispatcher          *dispatcher;
  msg_addr_t           _myaddr;

  // procedure call fun
  long                   _last_pcid;
  Mutex                  _lock;      // protect call_sem, call_reply
  map<long, Cond*>       call_cond;
  map<long, Message*>    call_reply;

 public:
  Messenger(msg_addr_t w) : dispatcher(0), _myaddr(w), _last_pcid(1) { }
  virtual ~Messenger() { }
  
  msg_addr_t get_myaddr() { return _myaddr; }

  virtual int shutdown() = 0;
  
  // setup
  void set_dispatcher(Dispatcher *d) { dispatcher = d; }
  Dispatcher *get_dispatcher() { return dispatcher; }

  // dispatch incoming messages
  virtual void dispatch(Message *m);

  // send message
  virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0) = 0;

  // make a procedure call
  virtual Message* sendrecv(Message *m, msg_addr_t dest, int port=0);
};


extern Message *decode_message(msg_envelope_t &env, bufferlist& bl);



#endif
