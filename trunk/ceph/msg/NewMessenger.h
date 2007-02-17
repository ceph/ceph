#ifndef __NEWMESSENGER_H
#define __NEWMESSENGER_H


#include <list>
#include <map>
using namespace std;
#include <ext/hash_map>
#include <ext/hash_set>
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
    
    map<entity_name_t, list<Message*> > waiting;

    Namer(EntityMessenger *msgr);
    ~Namer();

    void handle_connect(class MNSConnect*);
    void handle_register(class MNSRegister *m);
    void handle_started(Message *m);
    void handle_lookup(class MNSLookup *m);
    void handle_unregister(Message *m);
    void handle_failure(class MNSFailure *m);

    void dispatch(Message *m); 

    void manual_insert_inst(const entity_inst_t &inst);

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
      //join();
    }
    Message *read_message();
  };


  // outgoing
  class Sender : public Thread {
  public:
    entity_inst_t inst;
    bool done;
    int sd;

    set<entity_name_t> entities;
    list<Message*> q;

    Mutex lock;
    Cond cond;
    
    Sender(const entity_inst_t& i, int s=0) : inst(i), done(false), sd(s) {}
    virtual ~Sender() {}
    
    void *entry();

    int connect();
    void fail_and_requeue(list<Message*>& ls);
    void finish();

    void stop() {
      lock.Lock();
      done = true;
      cond.Signal();
      lock.Unlock();
    }
    
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

    int write_message(Message *m);
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
    EntityMessenger(entity_name_t myaddr);
    ~EntityMessenger();

    void ready();
    bool is_stopped() { return stop; }

    void wait() {
      dispatch_thread.join();
    }
    
    virtual void callback_kick() {} 
    virtual int shutdown();
    virtual void prepare_send_message(entity_name_t dest);
    virtual int send_message(Message *m, entity_name_t dest, int port=0, int fromport=0);
    virtual int send_message(Message *m, entity_name_t dest, const entity_inst_t& inst);

    virtual void mark_down(entity_name_t a, entity_inst_t& i);
    virtual void mark_up(entity_name_t a, entity_inst_t& i);
    //virtual void reset(msg_addr_t a);
  };


  class SingleDispatcher : public Thread {
    Rank *rank;
  public:
    SingleDispatcher(Rank *r) : rank(r) {}
    void *entry() {
      rank->single_dispatcher_entry();
      return 0;
    }
  } single_dispatcher;

  Cond            single_dispatch_cond;
  bool            single_dispatch_stop;
  list<Message*>  single_dispatch_queue;

  map<entity_name_t, list<Message*> > waiting_for_ready;

  void single_dispatcher_entry();
  void _submit_single_dispatch(Message *m);


  // Rank stuff
 public:
  Mutex lock;
  Cond  wait_cond;  // for wait()
  
  // my rank
  int   my_rank;
  Cond  waiting_for_rank;

  // my instance
  entity_inst_t my_inst;
  
  // lookup
  hash_map<entity_name_t, entity_inst_t> entity_map;
  hash_set<entity_name_t>                entity_unstarted;
  
  map<entity_name_t, list<Message*> > waiting_for_lookup;
  set<entity_name_t>                  looking_up;

  hash_set<entity_name_t>            down;
  
  // register
  map<int, Cond* >        waiting_for_register_cond;
  map<int, entity_name_t >   waiting_for_register_result;
  
  // local
  map<entity_name_t, EntityMessenger*> local;
  
  // remote
  hash_map<int, Sender*> rank_sender;
  
  set<Receiver*>    receivers;   

  list<Sender*>     sender_reap_queue;
  list<Receiver*>   receiver_reap_queue;
    
  EntityMessenger *messenger;   // rankN
  Namer           *namer;


  void show_dir();

  void lookup(entity_name_t addr);
  
  void dispatch(Message *m);
  void handle_connect_ack(class MNSConnectAck *m);
  void handle_register_ack(class MNSRegisterAck *m);
  void handle_lookup_reply(class MNSLookupReply *m);
  
  Sender *connect_rank(const entity_inst_t& inst);

  void mark_down(entity_name_t addr, entity_inst_t& i);
  void mark_up(entity_name_t addr, entity_inst_t& i);

  tcpaddr_t get_listen_addr() { return accepter.listen_addr; }

  void reaper();


public:
  Rank(int r=-1);
  ~Rank();

  int find_ns_addr(tcpaddr_t &tcpaddr);

  void set_namer(const tcpaddr_t& ns);
  void start_namer();

  int start_rank();
  void wait();

  EntityMessenger *register_entity(entity_name_t addr);
  void unregister_entity(EntityMessenger *ms);

  void submit_message(Message *m, const entity_inst_t& inst);  
  void prepare_dest(entity_name_t dest);
  void submit_message(Message *m);  
  void submit_messages(list<Message*>& ls);  

  // create a new messenger
  EntityMessenger *new_entity(entity_name_t addr);

} ;

extern Rank rank;

#endif
