#ifndef __NEWMESSENGER_H
#define __NEWMESSENGER_H


#include <list>
#include <map>
using namespace std;
#include <ext/hash_map>
using namespace __gnu_cxx;


#include "include/types.h"

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"

#include "Messenger.h"
#include "Message.h"
#include "tcp.h"

/* Rank - per-process
 */
class Rank : public Dispatcher {
 
  class EntityMessenger;
  class Sender;
  class Receiver;

  // namer
  class Namer : public Dispatcher {
  public:
	EntityMessenger *messenger;  // namerN

	int nrank;
	int nclient, nmds, nosd, nmon;
	
	map<msg_addr_t, list<Message*> > waiting;

	Namer(EntityMessenger *msgr);
	~Namer();

	void handle_connect(class MNSConnect*);
	void handle_register(class MNSRegister *m);
	//void handle_started(Message *m);
	void handle_lookup(class MNSLookup *m);
	void handle_unregister(Message *m);

	void dispatch(Message *m) {
	  switch (m->get_type()) {
	  case MSG_NS_CONNECT:
		handle_connect((class MNSConnect*)m);
		break;
	  case MSG_NS_REGISTER:
		handle_register((class MNSRegister*)m);
		break;
		//case MSG_NS_STARTED:
		//handle_started(m);
		//break;
	  case MSG_NS_UNREGISTER:
		handle_unregister(m);
		break;
	  case MSG_NS_LOOKUP:
		handle_lookup((class MNSLookup*)m);
		break;
		
	  default:
		assert(0);
	  }
	}
	
  };

  // incoming
  class Accepter : public Thread {
  public:
	bool done;

	tcpaddr_t listen_addr;
	int       listen_sd;
	
	Accepter() : done(false) {}
	
	void *entry();
	void stop() {
	  done = true;
	  ::close(listen_sd);
	  join();
	}
	int start();
  } accepter;
  

  class Receiver : public Thread {
  public:
	int sd;
	bool done;

	Receiver(int _sd) : sd(_sd), done(false) {}
	
	void *entry();
	void stop() {
	  done = true;
	  ::close(sd);
	  join();
	}
	Message *read_message();
  };



  // outgoing
  class Sender : public Thread {
  public:
	tcpaddr_t tcpaddr;
	bool done;
	int sd;

	set<msg_addr_t> entities;
	list<Message*> q;

	Mutex lock;
	Cond cond;
	
	Sender(tcpaddr_t a) : tcpaddr(a), done(false), sd(0) {}
	virtual ~Sender() {}
	
	void *entry();
	void stop() {
	  lock.Lock();
	  done = true;
	  cond.Signal();
	  lock.Unlock();
	  join();
	}
	
	int connect();	
	void send(Message *m) {
	  lock.Lock();
	  q.push_back(m);
	  cond.Signal();
	  lock.Unlock();
	}	
	void send(list<Message*>& ls) {
	  lock.Lock();
	  q.splice(q.end(), ls);
	  cond.Signal();
	  lock.Unlock();
	}

	void write_message(Message *m);
  };


  // messenger interface
  class EntityMessenger : public Messenger {
	Mutex lock;
	Cond cond;
	list<Message*> dispatch_queue;
	bool stop;

	class DispatchThread : public Thread {
	  EntityMessenger *m;
	public:
	  DispatchThread(EntityMessenger *_m) : m(_m) {}
	  void *entry() {
		m->dispatch_entry();
		return 0;
	  }
	} dispatch_thread;
	void dispatch_entry();

  public:
	void queue_message(Message *m) {
	  lock.Lock();
	  dispatch_queue.push_back(m);
	  cond.Signal();
	  lock.Unlock();
	}
	void queue_messages(list<Message*> ls) {
	  lock.Lock();
	  dispatch_queue.splice(dispatch_queue.end(), ls);
	  cond.Signal();
	  lock.Unlock();
	}

  public:
	EntityMessenger(msg_addr_t myaddr);
	~EntityMessenger();

	void ready();

	virtual void callback_kick() {} 
	virtual int shutdown();
	virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0);
  };


  // Rank stuff
 public:
  Mutex lock;
  
  // rank
  int   my_rank;
  Cond  waiting_for_rank;
  
  // lookup
  map<msg_addr_t, int>    entity_rank;
  map<int, tcpaddr_t>     rank_addr;
  
  map<msg_addr_t, list<Message*> > waiting_for_lookup;
  set<msg_addr_t>         looking_up;
  
  // register
  map<int, Cond* >        waiting_for_register_cond;
  map<int, msg_addr_t >   waiting_for_register_result;
  
  // local
  map<msg_addr_t, EntityMessenger*> local;
  
  // remote
  map<int, Sender*> rank_sender;
  
  set<Receiver*>    receivers;   
	
  EntityMessenger *messenger;   // rankN
  Namer           *namer;

  // constructor
  void lookup(msg_addr_t addr);
  
  void dispatch(Message *m);
  void handle_connect_ack(class MNSConnectAck *m);
  void handle_register_ack(class MNSRegisterAck *m);
  void handle_lookup_reply(class MNSLookupReply *m);
  
  Sender *connect_rank(int r);

public:
  Rank(int r=-1);
  ~Rank();

  int start_rank(tcpaddr_t& ns);
  void stop_rank();

  EntityMessenger *register_entity(msg_addr_t addr);
  void unregister_entity(EntityMessenger *ms);

  void submit_message(Message *m);  

  // create a new messenger
  EntityMessenger *new_entity(msg_addr_t addr);

} rank;



#endif
