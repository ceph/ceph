// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#include "config.h"
#include "include/error.h"

#include "common/Timer.h"
#include "common/Mutex.h"

#include "TCPMessenger.h"
#include "Message.h"

#include <iostream>
#include <cassert>
using namespace std;
#include <ext/hash_map>
using namespace __gnu_cxx;

#include <errno.h>
# include <netdb.h>
# include <sys/socket.h>
# include <netinet/in.h>
# include <arpa/inet.h>
#include <sys/select.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>

#include <unistd.h>

#include "messages/MGenericMessage.h"
#include "messages/MNSConnect.h"
#include "messages/MNSConnectAck.h"
#include "messages/MNSRegister.h"
#include "messages/MNSRegisterAck.h"
#include "messages/MNSLookup.h"
#include "messages/MNSLookupReply.h"

#include "TCPDirectory.h"

#include "common/Logger.h"

#define DBL 18

//#define TCP_SERIALMARSHALL  // do NOT turn this off until you check messages/* encode_payload methods
//#define TCP_SERIALOUT       // be paranoid/annoying and send messages in same thread


TCPMessenger *rankmessenger = 0; // 

TCPDirectory *nameserver = 0;    // only defined on rank 0
TCPMessenger *nsmessenger = 0;


/***************************/
LogType rank_logtype;
Logger *logger;

int stat_num = 0;
off_t stat_inq = 0, stat_inqb = 0;
off_t stat_disq = 0, stat_disqb = 0;
off_t stat_outq = 0, stat_outqb = 0;
/***************************/


// local directory
hash_map<entity_name_t, TCPMessenger*>  directory;  // local
hash_set<entity_name_t>                 directory_ready;
Mutex                         directory_lock;

// connecting
struct sockaddr_in listen_addr;     // my listen addr
int                listen_sd = 0;
int                my_rank = -1;
Cond               waiting_for_rank;

// register
long regid = 0;
map<int, Cond* >        waiting_for_register_cond;
map<int, entity_name_t >   waiting_for_register_result;

// incoming messages
list<Message*>                incoming;
Mutex                         incoming_lock;
Cond                          incoming_cond;

// outgoing messages
/*
list<Message*>                outgoing;
Mutex                         outgoing_lock;
Cond                          outgoing_cond;
*/

class OutThread : public Thread {
public:
  Mutex lock;
  Cond cond;
  list<Message*> q;
  bool done;

  OutThread() : done(false) {}
  virtual ~OutThread() {}

  void *entry();
  
  void stop() {
    lock.Lock();
    done = true;
    cond.Signal();
    lock.Unlock();
    join();
  }

  void send(Message *m) {
    lock.Lock();
    q.push_back(m);
    cond.Signal();
    lock.Unlock();
  }
} single_out_thread;

Mutex lookup_lock;  // 
hash_map<entity_name_t, int> entity_rank;      // entity -> rank
hash_map<int, int>        rank_sd;   // outgoing sockets, rank -> sd
hash_map<int, OutThread*> rank_out;
hash_map<int, tcpaddr_t>  rank_addr; // rank -> tcpaddr
map<entity_name_t, list<Message*> > waiting_for_lookup;


/* this process */
bool tcp_done = false;     // set this flag to stop the event loop


// threads
pthread_t dispatch_thread_id = 0;   // thread id of the event loop.  init value == nobody
pthread_t out_thread_id = 0;        // thread id of the event loop.  init value == nobody
pthread_t listen_thread_id = 0;
map<int, pthread_t>      in_threads;    // sd -> threadid

//bool pending_timer = false;

// per-rank fun


// debug
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "[TCP " << my_rank /*(<< " " << getpid() << "." << pthread_self() */  << "] "


#include "tcp.cc"

// some declarations
void tcp_open(int rank);
int tcp_send(Message *m);
void tcpmessenger_kick_dispatch_loop();
OutThread *tcp_lookup(Message *m);

int tcpmessenger_get_rank()
{
  return my_rank;
}


int tcpmessenger_findns(tcpaddr_t &nsa)
{
  char *nsaddr = 0;
  bool have_nsa = false;

  // env var?
  /*int e_len = 0;
  for (int i=0; envp[i]; i++)
    e_len += strlen(envp[i]) + 1;
  */
  nsaddr = getenv("CEPH_NAMESERVER");////envz_entry(*envp, e_len, "CEPH_NAMESERVER");    
  if (nsaddr) {
    while (nsaddr[0] != '=') nsaddr++;
    nsaddr++;
  }

  else {
    // file?
    int fd = ::open(".ceph_ns",O_RDONLY);
    if (fd > 0) {
      ::read(fd, (void*)&nsa, sizeof(nsa));
      ::close(fd);
      have_nsa = true;
      nsaddr = "from .ceph_ns";
    }
  }

  if (!nsaddr && !have_nsa) {
    cerr << "i need ceph ns addr.. either CEPH_NAMESERVER env var or --ns blah" << endl;
    return -1;
    //exit(-1);
  }
  
  // look up nsaddr?
  if (!have_nsa && tcpmessenger_lookup(nsaddr, nsa) < 0) {
    return -1;
  }

  dout(2) << "ceph ns is " << nsaddr << " or " << nsa << endl;
  return 0;
}



/** rankserver
 *
 * one per rank.  handles entity->rank lookup replies.
 */

class RankServer : public Dispatcher {
public:
  void dispatch(Message *m) {
    lookup_lock.Lock();

    dout(DBL) << "rankserver dispatching " << *m << endl;

    switch (m->get_type()) {
    case MSG_NS_CONNECTACK:
      handle_connect_ack((MNSConnectAck*)m);
      break;

    case MSG_NS_REGISTERACK:
      handle_register_ack((MNSRegisterAck*)m);
      break;

    case MSG_NS_LOOKUPREPLY:
      handle_lookup_reply((MNSLookupReply*)m);
      break;

    default:
      assert(0);
    }

    lookup_lock.Unlock();
  }

  void handle_connect_ack(MNSConnectAck *m) {
    dout(DBL) << "my rank is " << m->get_rank();
    my_rank = m->get_rank();

    // now that i know my rank,
    entity_rank[MSG_ADDR_RANK(my_rank)] = my_rank; 
    rank_addr[my_rank] = listen_addr;
    
    waiting_for_rank.SignalAll();

    delete m;

    // logger!
    dout(DBL) << "logger" << endl;
    char names[100];
    sprintf(names, "rank%d", my_rank);
    string name = names;

    if (g_conf.tcp_log) {
      logger = new Logger(name, (LogType*)&rank_logtype);
      rank_logtype.add_set("num");
      rank_logtype.add_inc("in");
      rank_logtype.add_inc("inb");
      rank_logtype.add_inc("dis");
      rank_logtype.add_set("inq");
      rank_logtype.add_set("inqb");
      rank_logtype.add_set("outq");
      rank_logtype.add_set("outqb");
    }

  }

  void handle_register_ack(MNSRegisterAck *m) {
    long tid = m->get_tid();
    waiting_for_register_result[tid] = m->get_entity();
    waiting_for_register_cond[tid]->Signal();
    delete m;
  }
  
  void handle_lookup_reply(MNSLookupReply *m) {
    list<Message*> waiting;
    dout(DBL) << "got lookup reply" << endl;

    for (map<entity_name_t, int>::iterator it = m->entity_rank.begin();
         it != m->entity_rank.end();
         it++) {
      dout(DBL) << "lookup got " << MSG_ADDR_NICE(it->first) << " on rank " << it->second << endl;
      entity_rank[it->first] = it->second;
      
      if (it->second == my_rank) {
        // deliver locally
        dout(-DBL) << "delivering lookup results locally" << endl;
        incoming_lock.Lock();
        
        for (list<Message*>::iterator i = waiting_for_lookup[it->first].begin();
             i != waiting_for_lookup[it->first].end();
             i++) {
          stat_inq++;
          stat_inqb += (*i)->get_payload().length();
          (*i)->decode_payload();
          incoming.push_back(*i);
        }
        incoming_cond.Signal();
        incoming_lock.Unlock();
      } else {
        // take waiters
        waiting.splice(waiting.begin(), waiting_for_lookup[it->first]);
      }
      waiting_for_lookup.erase(it->first);
      
    }

    for (map<int,tcpaddr_t>::iterator it = m->rank_addr.begin();
         it != m->rank_addr.end();
         it++) {
      dout(DBL) << "lookup got rank " << it->first << " addr " << it->second << endl;
      rank_addr[it->first] = it->second;

      // open it now
      if (rank_sd.count(it->first) == 0)
            tcp_open(it->first);
    }

    // send waiting messages
#ifdef TCP_SERIALOUT
    for (list<Message*>::iterator it = waiting.begin();
         it != waiting.end();
         it++) {
      OutThread *outt = tcp_lookup(*it);
      assert(outt);
      tcp_send(*it);
    }
#else
    for (list<Message*>::iterator it = waiting.begin();
         it != waiting.end();
         it++) {
      OutThread *outt = tcp_lookup(*it);
      assert(outt);
      outt->send(*it);
//      dout(0) << "lookup done, splicing in " << *it << endl;
    }
#endif

    delete m;
  }
  
} rankserver;


class C_TCPKicker : public Context {
  void finish(int r) {
    dout(DBL) << "timer kick" << endl;
    tcpmessenger_kick_dispatch_loop();
  }
};

void TCPMessenger::callback_kick()
{
  tcpmessenger_kick_dispatch_loop();
}


extern int tcpmessenger_lookup(char *str, tcpaddr_t& ta)
{
  char *host = str;
  char *port = 0;
  
  for (int i=0; str[i]; i++) {
    if (str[i] == ':') {
      port = str+i+1;
      str[i] = 0;
      break;
    }
  }
  if (!port) {
    cerr << "addr '" << str << "' doesn't look like 'host:port'" << endl;
    return -1;
  } 
  //cout << "host '" << host << "' port '" << port << "'" << endl;

  int iport = atoi(port);
  
  struct hostent *myhostname = gethostbyname( host ); 
  if (!myhostname) {
    cerr << "host " << host << " not found" << endl;
    return -1;
  }

  memset(&ta, 0, sizeof(ta));

  //cout << "addrtype " << myhostname->h_addrtype << " len " << myhostname->h_length << endl;

  ta.sin_family = myhostname->h_addrtype;
  memcpy((char *)&ta.sin_addr,
         myhostname->h_addr, 
         myhostname->h_length);
  ta.sin_port = iport;
    
  cout << "lookup '" << host << ":" << port << "' -> " << ta << endl;

  return 0;
}



/*****
 * global methods for process-wide startup, shutdown.
 */

int tcpmessenger_init()
{
  // LISTEN
  dout(DBL) << "binding to listen " << endl;
  
  /* socket creation */
  listen_sd = socket(AF_INET,SOCK_STREAM,0);
  assert(listen_sd > 0);
  
  /* bind to port */
  memset((char*)&listen_addr, 0, sizeof(listen_addr));
  listen_addr.sin_family = AF_INET;
  listen_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  listen_addr.sin_port = 0;
  
  int rc = bind(listen_sd, (struct sockaddr *) &listen_addr, sizeof(listen_addr));
  assert(rc >= 0);

  socklen_t llen = sizeof(listen_addr);
  getsockname(listen_sd, (sockaddr*)&listen_addr, &llen);

  int myport = listen_addr.sin_port;

  // listen!
  rc = ::listen(listen_sd, 1000);
  assert(rc >= 0);

  dout(DBL) << "listening on " << myport << endl;
  
  // my address is...
  char host[100];
  gethostname(host, 100);
  dout(DBL) << "my hostname is " << host << endl;

  struct hostent *myhostname = gethostbyname( host ); 

  struct sockaddr_in my_addr;  
  memset(&my_addr, 0, sizeof(my_addr));

  my_addr.sin_family = myhostname->h_addrtype;
  memcpy((char *) &my_addr.sin_addr.s_addr, 
         myhostname->h_addr_list[0], 
         myhostname->h_length);
  my_addr.sin_port = myport;

  listen_addr = my_addr;

  dout(DBL) << "listen addr is " << listen_addr << endl;

  // register to execute timer events
  //g_timer.set_messenger_kicker(new C_TCPKicker());


  dout(DBL) << "init done" << endl;
  return 0;
}


// on first rank only
void tcpmessenger_start_nameserver(tcpaddr_t& diraddr)
{
  dout(DBL) << "starting nameserver on " << MSG_ADDR_NICE(MSG_ADDR_DIRECTORY) << endl;

  // i am rank 0.
  nsmessenger = new TCPMessenger(MSG_ADDR_DIRECTORY);

  // start name server
  nameserver = new TCPDirectory(nsmessenger);

  // diraddr is my addr!
  diraddr = rank_addr[0] = listen_addr;
  my_rank = 0;
  entity_rank[MSG_ADDR_DIRECTORY] = 0;
}
void tcpmessenger_stop_nameserver()
{
  if (nsmessenger) {
    dout(DBL) << "shutting down nsmessenger" << endl;
    TCPMessenger *m = nsmessenger;
    nsmessenger = 0;
    m->shutdown();
    delete m;
  }
}

// on all ranks
void tcpmessenger_start_rankserver(tcpaddr_t& ns)
{
  // connect to nameserver
  entity_rank[MSG_ADDR_DIRECTORY] = 0;
  rank_addr[0] = ns;
  tcp_open(0);

  if (my_rank >= 0) {
    // i know my rank
    rankmessenger = new TCPMessenger(MSG_ADDR_RANK(my_rank));
  } else {
    // start rank messenger, and discover my rank.
    rankmessenger = new TCPMessenger(MSG_ADDR_RANK_NEW);
  }
}
void tcpmessenger_stop_rankserver()
{
  if (rankmessenger) {
    dout(DBL) << "shutting down rankmessenger" << endl;
    rankmessenger->shutdown();
    delete rankmessenger;
    rankmessenger = 0;
  }
}






int tcpmessenger_shutdown() 
{
  dout(DBL) << "tcpmessenger_shutdown barrier" << endl;


  dout(2) << "tcpmessenger_shutdown closing all sockets etc" << endl;

  // bleh
  for (hash_map<int,int>::iterator it = rank_sd.begin();
       it != rank_sd.end();
       it++) {
    ::close(it->second);
  }

  return 0;
}




/***
 * internal send/recv
 */




/*
 * recv a Message*
 */



Message *tcp_recv(int sd)
{
  // envelope
  dout(DBL) << "tcp_recv receiving message from sd " << sd  << endl;
  
  msg_envelope_t env;
  if (!tcp_read( sd, (char*)&env, sizeof(env) ))
    return 0;

  if (env.type == 0) {
    dout(DBL) << "got dummy env, bailing" << endl;
    return 0;
  }

  dout(DBL) << "tcp_recv got envelope type=" << env.type << " src " << MSG_ADDR_NICE(env.source) << " dst " << MSG_ADDR_NICE(env.dest) << " nchunks=" << env.nchunks << endl;
  
  // payload
  bufferlist blist;
  for (int i=0; i<env.nchunks; i++) {
    int size;
    tcp_read( sd, (char*)&size, sizeof(size) );

    bufferptr bp = new buffer(size);
    
    if (!tcp_read( sd, bp.c_str(), size )) return 0;

    blist.push_back(bp);

    dout(DBL) << "tcp_recv got frag " << i << " of " << env.nchunks << " len " << bp.length() << endl;
  }
  
  // unmarshall message
  size_t s = blist.length();
  Message *m = decode_message(env, blist);

  if (logger) {
    logger->inc("in");
    logger->inc("inb", s+sizeof(env));
  }

  dout(DBL) << "tcp_recv got " << s << " byte message from " << MSG_ADDR_NICE(m->get_source()) << endl;

  return m;
}




void tcp_open(int rank)
{
  dout(DBL) << "tcp_open to rank " << rank << " at " << rank_addr[rank] << endl;

  // create socket?
  int sd = socket(AF_INET,SOCK_STREAM,0);
  assert(sd > 0);
  
  // bind any port
  struct sockaddr_in myAddr;
  myAddr.sin_family = AF_INET;
  myAddr.sin_addr.s_addr = htonl(INADDR_ANY);
  myAddr.sin_port = htons( 0 );    
  
  int rc = bind(sd, (struct sockaddr *) &myAddr, sizeof(myAddr));
  assert(rc>=0);

  // connect!
  int r = connect(sd, (sockaddr*)&rank_addr[rank], sizeof(myAddr));
  assert(r >= 0);

  //dout(DBL) << "tcp_open connected to " << who << endl;
  assert(rank_sd.count(rank) == 0);
  rank_sd[rank] = sd;

  if (g_conf.tcp_multi_out) {
    rank_out[rank] = new OutThread();
    rank_out[rank]->create();
  } else {
    rank_out[rank] = &single_out_thread;
    if (!single_out_thread.is_started())
      single_out_thread.create();
  }
}


void tcp_marshall(Message *m)
{
  // marshall
  if (m->empty_payload())
    m->encode_payload();
}

OutThread *tcp_lookup(Message *m)
{
  entity_name_t addr = m->get_dest();

  if (!entity_rank.count(m->get_dest())) {
    // lookup and wait.
    if (waiting_for_lookup.count(addr)) {
      dout(DBL) << "already looking up " << MSG_ADDR_NICE(addr) << endl;
    } else {
      dout(DBL) << "lookup on " << MSG_ADDR_NICE(addr) << " for " << m << endl;
      MNSLookup *r = new MNSLookup(addr);
      rankmessenger->send_message(r, MSG_ADDR_DIRECTORY);
    }
    
    // add waiter
    waiting_for_lookup[addr].push_back(m);
    return 0;
  }

  int rank = entity_rank[m->get_dest()];

  if (rank_sd.count(rank) == 0) { // should only happen on rank0?
    tcp_open(rank);
  } 
  assert(rank_sd.count(rank));
  m->set_tcp_sd( rank_sd[rank] );
  return rank_out[rank];
}


/*
 * send a Message* over the wire.  ** do not block **.
 */
int tcp_send(Message *m)
{
  /*int rank = entity_rank[m->get_dest()];
  //if (rank_sd.count(rank) == 0) tcp_open(rank);
  assert(rank_sd.count(rank));

  int sd = rank_sd[rank];
  assert(sd);
  */
  int sd = m->get_tcp_sd();
  assert(sd);

  // get envelope, buffers
  msg_envelope_t *env = &m->get_envelope();
  bufferlist blist;
  blist.claim( m->get_payload() );

#ifdef TCP_KEEP_CHUNKS
  env->nchunks = blist.buffers().size();
#else
  env->nchunks = 1;
#endif

  // HACK osd -> client only
  //if (m->get_source() >= MSG_ADDR_OSD(0) && m->get_source() < MSG_ADDR_CLIENT(0) &&
 // m->get_dest() >= MSG_ADDR_CLIENT(0))
  dout(DBL) << g_clock.now() << " sending " << m << " " << *m << " to " << MSG_ADDR_NICE(m->get_dest()) 
      //<< " rank " << rank 
            << " sd " << sd << endl;
  
  // send envelope
  int r = tcp_write( sd, (char*)env, sizeof(*env) );
  if (r < 0) { cerr << "error sending envelope for " << *m << " to " << MSG_ADDR_NICE(m->get_dest()) << endl; assert(0); }

  // payload
#ifdef TCP_KEEP_CHUNKS
  // send chunk-wise
  int i = 0;
  for (list<bufferptr>::iterator it = blist.buffers().begin();
       it != blist.buffers().end();
       it++) {
    dout(DBL) << "tcp_sending frag " << i << " len " << (*it).length() << endl;
    int size = (*it).length();
    r = tcp_write( sd, (char*)&size, sizeof(size) );
    if (r < 0) { cerr << "error sending chunk len for " << *m << " to " << MSG_ADDR_NICE(m->get_dest()) << endl; assert(0); }
    r = tcp_write( sd, (*it).c_str(), size );
    if (r < 0) { cerr << "error sending data chunk for " << *m << " to " << MSG_ADDR_NICE(m->get_dest()) << endl; assert(0); }
    i++;
  }
#else
  // one big chunk
  int size = blist.length();
  r = tcp_write( sd, (char*)&size, sizeof(size) );
  if (r < 0) { cerr << "error sending data len for " << *m << " to " << MSG_ADDR_NICE(m->get_dest()) << endl; assert(0); }
  for (list<bufferptr>::iterator it = blist.buffers().begin();
       it != blist.buffers().end();
       it++) {
    r = tcp_write( sd, (*it).c_str(), (*it).length() );
    if (r < 0) { cerr << "error sending data megachunk for " << *m << " to " << MSG_ADDR_NICE(m->get_dest()) << " : len " << (*it).length() << endl; assert(0); }
  }
#endif

  // hose message
  delete m;
  return 0;
}





/** tcp_outthread
 * this thread watching the outgoing queue, and encodes+sends any queued messages
 */

void* OutThread::entry() 
{
  lock.Lock();
  while (!q.empty() || !done) {
    
    if (!q.empty()) {
      dout(DBL) << "outthread grabbing message(s)" << endl;
      
      // grab outgoing list
      list<Message*> out;
      out.splice(out.begin(), q);
      
      // drop lock while i send these
      lock.Unlock();
      
      while (!out.empty()) {
        Message *m = out.front();
        out.pop_front();
        
        dout(DBL) << "outthread sending " << m << endl;
        
        if (!g_conf.tcp_serial_marshall) 
          tcp_marshall(m);
        
        tcp_send(m);
      }
      
      lock.Lock();
      continue;
    }
    
    // wait
    dout(DBL) << "outthread sleeping" << endl;
    cond.Wait(lock);
  }
  dout(DBL) << "outthread done" << endl;
  
  lock.Unlock();  
  return 0;
}



/** tcp_inthread
 * read incoming messages from a given peer.
 * give received and decoded messages to dispatch loop.
 */
void *tcp_inthread(void *r)
{
  int sd = (int)r;

  dout(DBL) << "tcp_inthread reading on sd " << sd << endl;

  while (!tcp_done) {
    Message *m = tcp_recv(sd);
    if (!m) break;
    entity_name_t who = m->get_source();

    dout(20) << g_clock.now() <<  " inthread got " << m << " from sd " << sd << " who is " << who << endl;

    // give to dispatch loop
    size_t sz = m->get_payload().length();

    if (g_conf.tcp_multi_dispatch) {
      const entity_name_t dest = m->get_dest();
      directory_lock.Lock();
      TCPMessenger *messenger = directory[ dest ];
      directory_lock.Unlock();

      if (messenger) 
        messenger->dispatch_queue(m);
      else
        dout(0) << "dest " << dest << " dne" << endl;

    } else {
      // single dispatch queue
      incoming_lock.Lock();
      {
        //dout(-20) << "in1 stat_inq " << stat_inq << ", incoming " << incoming.size() << endl;
        //assert(stat_inq == incoming.size());
        incoming.push_back(m);
        incoming_cond.Signal();
        
        stat_inq++;
        //assert(stat_inq == incoming.size());
        //dout(-20) << "in2 stat_inq " << stat_inq << ", incoming " << incoming.size() << endl;
        stat_inqb += sz;
      }
      incoming_lock.Unlock();
    }

    if (logger) {
      //logger->inc("in");
      //logger->inc("inb", sz);
    }
  }

  dout(DBL) << "tcp_inthread closing " << sd << endl;

  //::close(sd);
  return 0;  
}

/** tcp_accepthread
 * accept incoming connections from peers.
 * start a tcp_inthread for each.
 */
void *tcp_acceptthread(void *)
{
  dout(DBL) << "tcp_acceptthread starting" << endl;

  while (!tcp_done) {
    //dout(DBL) << "accepting, left = " << left << endl;

    struct sockaddr_in addr;
    socklen_t slen = sizeof(addr);
    int sd = ::accept(listen_sd, (sockaddr*)&addr, &slen);
    if (sd > 0) {
      dout(DBL) << "accepted incoming on sd " << sd << endl;

      pthread_t th;
      pthread_create(&th,
                     NULL,
                     tcp_inthread,
                     (void*)sd);
      in_threads[sd] = th;
    } else {
      dout(DBL) << "no incoming connection?" << endl;
      break;
    }
  }
  return 0;
}




/** tcp_dispatchthread
 * wait for pending timers, incoming messages.  dispatch them.
 */
void TCPMessenger::dispatch_entry()
{
  incoming_lock.Lock();
  while (!incoming.empty() || !incoming_stop) {
    if (!incoming.empty()) {
      // grab incoming messages  
      list<Message*> in;
      in.splice(in.begin(), incoming);

      assert(stat_disq == 0);
      stat_disq = stat_inq;
      stat_disqb = stat_inqb;
      stat_inq = 0;
      stat_inqb = 0;
    
      // drop lock while we deliver
      //assert(stat_inq == incoming.size());
      incoming_lock.Unlock();

      // dispatch!
      while (!in.empty()) {
        Message *m = in.front();
        in.pop_front();
      
        stat_disq--;
        stat_disqb -= m->get_payload().length();
        if (logger) {
          logger->set("inq", stat_inq+stat_disq);
          logger->set("inqb", stat_inqb+stat_disq);
          logger->inc("dis");
        }
          
        dout(4) << g_clock.now() << " ---- '" << m->get_type_name() << 
          "' from " << MSG_ADDR_NICE(m->get_source()) << ':' << m->get_source_port() <<
          " to " << MSG_ADDR_NICE(m->get_dest()) << ':' << m->get_dest_port() << " ---- " 
                << m 
                << endl;
        
        dispatch(m);
      }

      continue;
    }

    // sleep
    dout(DBL) << "dispatch: waiting for incoming messages" << endl;
    incoming_cond.Wait(incoming_lock);
    dout(DBL) << "dispatch: woke up" << endl;
  }
  incoming_lock.Unlock();
}


void* tcp_dispatchthread(void*)
{
  dout(5) << "tcp_dispatchthread start pid " << getpid() << endl;

  while (1) {
    // inq?
    incoming_lock.Lock();

    // done?
    if (tcp_done && incoming.empty()) {
      incoming_lock.Unlock();
      break;
    }

    // wait?
    if (incoming.empty()) {
      // wait
      dout(DBL) << "dispatch: incoming empty, waiting for incoming messages" << endl;
      incoming_cond.Wait(incoming_lock);
      dout(DBL) << "dispatch: woke up" << endl;
    }

    // grab incoming messages  
    //dout(-20) << "dis1 stat_inq " << stat_inq << ", incoming " << incoming.size() << endl;
    //assert(stat_inq == incoming.size());

    list<Message*> in;
    in.splice(in.begin(), incoming);

    assert(stat_disq == 0);
    stat_disq = stat_inq;
    stat_disqb = stat_inqb;
    stat_inq = 0;
    stat_inqb = 0;
    //assert(stat_inq == incoming.size());
    //dout(-20) << "dis2 stat_inq " << stat_inq << ", incoming " << incoming.size() << endl;

    // drop lock while we deliver
    incoming_lock.Unlock();

    // dispatch!
    while (!in.empty()) {
      Message *m = in.front();
      in.pop_front();
      
      stat_disq--;
      stat_disqb -= m->get_payload().length();
      if (logger) {
        logger->set("inq", stat_inq+stat_disq);
        logger->set("inqb", stat_inqb+stat_disq);
        logger->inc("dis");
      }
      
      dout(DBL) << "dispatch doing " << *m << endl;
      
      // for rankserver?
      if (m->get_type() == MSG_NS_CONNECTACK ||        // i just connected
          m->get_dest() == MSG_ADDR_RANK(my_rank)) {
        dout(DBL) <<  " giving to rankserver" << endl;
        rankserver.dispatch(m);
        continue;
      }
      
      // ok
      entity_name_t dest = m->get_dest();
      directory_lock.Lock();
      if (directory.count(dest)) {
        Messenger *who = directory[ dest ];
        directory_lock.Unlock();          
        
        dout(4) << g_clock.now() << " ---- '" << m->get_type_name() << 
          "' from " << MSG_ADDR_NICE(m->get_source()) << ':' << m->get_source_port() <<
          " to " << MSG_ADDR_NICE(m->get_dest()) << ':' << m->get_dest_port() << " ---- " 
                << *m 
                << endl;
        
        who->dispatch(m);
      } else {
        directory_lock.Unlock();
        dout (1) << "---- i don't know who " << MSG_ADDR_NICE(dest) << " " << dest << " is." << endl;
        assert(0);
      }
    }
    assert(stat_disq == 0);

  }


  g_timer.shutdown();

  dout(5) << "tcp_dispatchthread exiting loop" << endl;
  return 0;
}


// start/stop mpi receiver thread (for unsolicited messages)
int tcpmessenger_start()
{
  dout(5) << "starting accept thread" << endl;
  pthread_create(&listen_thread_id,
                 NULL,
                 tcp_acceptthread,
                 0);                

  dout(5) << "starting dispatch thread" << endl;
  
  // start a thread
  pthread_create(&dispatch_thread_id, 
                 NULL, 
                 tcp_dispatchthread,
                 0);


  /*
  dout(5) << "starting outgoing thread" << endl;
  pthread_create(&out_thread_id, 
                 NULL, 
                 tcp_outthread,
                 0);
  */
  if (!g_conf.tcp_multi_out)
    single_out_thread.create();
  return 0;
}


/*
 * kick and wake up _loop (to pick up new outgoing message, or quit)
 */

void tcpmessenger_kick_dispatch_loop()
{
  if (g_conf.tcp_multi_dispatch) {
    assert(0);
    // all of them
    /*for (hash_map<msg_addr_t, TCPMessenger*>::iterator i = directory.begin();
         i != directory.end();
         i++)
      i->second->dispatch_kick();
    */
  } else {
    // just one
    dout(DBL) << "kicking" << endl;
    incoming_lock.Lock();
    dout(DBL) << "prekick" << endl;
    incoming_cond.Signal();
    incoming_lock.Unlock();
    dout(DBL) << "kicked" << endl;
  }
}

/*
void tcpmessenger_kick_outgoing_loop()
{
  outgoing_lock.Lock();
  outgoing_cond.Signal();
  outgoing_lock.Unlock();
}
*/


// wait for thread to finish

void tcpmessenger_wait()
{
  if (g_conf.tcp_multi_dispatch) {
    // new way
    incoming_lock.Lock();
    while (!tcp_done)
      incoming_cond.Wait(incoming_lock);
    incoming_lock.Unlock();
  } else {
    // old way
    dout(10) << "tcpmessenger_wait waking up dispatch loop" << endl;
    tcpmessenger_kick_dispatch_loop();
    
    void *returnval;
    dout(10) << "tcpmessenger_wait waiting for thread to finished." << endl;
    pthread_join(dispatch_thread_id, &returnval);
    dout(10) << "tcpmessenger_wait thread finished." << endl;
  }
}




entity_name_t register_entity(entity_name_t addr) 
{
  lookup_lock.Lock();
  
  // prepare to wait
  long id = ++regid;
  Cond cond;
  waiting_for_register_cond[id] = &cond;

  if (my_rank < 0) {
    dout(DBL) << "register_entity don't know my rank, connecting" << endl;
    
    // connect to nameserver; discover my rank.
    Message *m = new MNSConnect(listen_addr);
    m->set_dest(MSG_ADDR_DIRECTORY, 0);
    tcp_marshall(m);
    OutThread *outt = tcp_lookup(m);
    assert(outt);
    tcp_send(m);
    
    // wait for reply
    while (my_rank < 0) 
      waiting_for_rank.Wait(lookup_lock);
    assert(my_rank > 0);
  }
  
  // send req
  dout(DBL) << "register_entity " << MSG_ADDR_NICE(addr) << endl;
  Message *m = new MNSRegister(addr, my_rank, id);
  m->set_dest(MSG_ADDR_DIRECTORY, 0);
  tcp_marshall(m);
  OutThread *outt = tcp_lookup(m);
  assert(outt);
  tcp_send(m);
  
  // wait?
  while (!waiting_for_register_result.count(id)) 
    cond.Wait(lookup_lock);

  // get result, clean up
  entity_name_t entity = waiting_for_register_result[id];
  waiting_for_register_result.erase(id);
  waiting_for_register_cond.erase(id);
  
  dout(DBL) << "register_entity got " << MSG_ADDR_NICE(entity) << endl;

  lookup_lock.Unlock();

  // ok!
  return entity;
}



/***********
 * Tcpmessenger class implementation
 */


TCPMessenger::TCPMessenger(entity_name_t myaddr) : 
  Messenger(myaddr), 
  dispatch_thread(this)
{
  if (myaddr != MSG_ADDR_DIRECTORY) {
    // register!
    myaddr = register_entity(myaddr);
  }


  // my address
  set_myaddr( myaddr );

  // register myself in the messenger directory
  directory_lock.Lock();
  {
    directory[myaddr] = this;
    
    stat_num++;
    if (logger) logger->set("num", stat_num);
  }
  directory_lock.Unlock();

  // register to execute timer events
  //g_timer.set_messenger_kicker(new C_TCPKicker());
  //  g_timer.set_messenger(this);
}


void TCPMessenger::ready()
{
  directory_lock.Lock();
  directory_ready.insert(get_myaddr());
  directory_lock.Unlock();

  if (get_myaddr() != MSG_ADDR_DIRECTORY) {
    // started!  tell namer we are up and running.
    lookup_lock.Lock();
    {
      Message *m = new MGenericMessage(MSG_NS_STARTED);
      m->set_source(get_myaddr(), 0);
      m->set_dest(MSG_ADDR_DIRECTORY, 0);
      tcp_marshall(m);
      OutThread *outt = tcp_lookup(m);
      assert(outt);
      tcp_send(m);
    }
    lookup_lock.Unlock();
  }
}


TCPMessenger::~TCPMessenger()
{
  //delete logger;
}

tcpaddr_t& TCPMessenger::get_tcpaddr() 
{
  return listen_addr;
}

void TCPMessenger::map_entity_rank(entity_name_t e, int r)
{
  lookup_lock.Lock();
  entity_rank[e] = r;
  lookup_lock.Unlock();
}

void TCPMessenger::map_rank_addr(int r, tcpaddr_t a)
{
  lookup_lock.Lock();
  rank_addr[r] = a;
  lookup_lock.Unlock();
}


int TCPMessenger::get_dispatch_queue_len() 
{
  return stat_inq+stat_disq;
}


int TCPMessenger::shutdown()
{
  dout(DBL) << "shutdown by " << MSG_ADDR_NICE(get_myaddr()) << endl;

  // dont' send unregistery from nsmessenger shutdown!
  if (this != nsmessenger && 
      (my_rank > 0 || nsmessenger)) {
    dout(DBL) << "sending unregister from " << MSG_ADDR_NICE(get_myaddr()) << " to ns" << endl;
    send_message(new MGenericMessage(MSG_NS_UNREGISTER),
                 MSG_ADDR_DIRECTORY);
  }

  // remove me from the directory
  directory_lock.Lock();
  directory.erase(get_myaddr());
  
  // last one?
  bool lastone = directory.empty();  
  //dout(1) << "lastone = " << lastone << " .. " << directory.size() << endl;
  

  // or almost last one?
  if (rankmessenger && directory.size() == 1) {
    directory_lock.Unlock();
    tcpmessenger_stop_rankserver();
    directory_lock.Lock();
  }

  stat_num--;
  if (logger) logger->set("num", stat_num);

  directory_lock.Unlock();

  // last one?
  if (lastone) {
    dout(2) << "shutdown last tcpmessenger on rank " << my_rank << " shut down" << endl;
    //pthread_t whoami = pthread_self();
    
    // no more timer events
    //g_timer.unset_messenger();
    
    // close incoming sockets
    //void *r;
    for (map<int,pthread_t>::iterator it = in_threads.begin();
         it != in_threads.end();
         it++) {
      dout(DBL) << "closing reader on sd " << it->first << endl;      
      ::close(it->first);
      //pthread_join(it->second, &r);
    }

    if (g_conf.tcp_multi_dispatch) {
      // kill off dispatch threads
      dout(DBL) << "killing dispatch threads" << endl;
      for (hash_map<entity_name_t,TCPMessenger*>::iterator it = directory.begin();
           it != directory.end();
           it++) 
        it->second->dispatch_stop();
    }

    dout(DBL) << "setting tcp_done" << endl;

    // kick/kill incoming thread
    incoming_lock.Lock();
    tcp_done = true;
    incoming_cond.Signal();
    incoming_lock.Unlock();

    // finish off outgoing thread
    dout(10) << "waiting for outgoing to finish" << endl;
    if (g_conf.tcp_multi_out) {
      for (hash_map<int,OutThread*>::iterator it = rank_out.begin();
           it != rank_out.end();
           it++) {
        it->second->stop();
        delete it->second;
      }
    } else {
      single_out_thread.stop();
    }

    
    /*

    dout(15) << "whoami = " << whoami << ", thread = " << dispatch_thread_id << endl;
    if (whoami == thread_id) {
      // i am the event loop thread, just set flag!
      dout(15) << "  set tcp_done=true" << endl;
      tcp_done = true;
    }
    */
  } 
  return 0;
}




/***
 * public messaging interface
 */


/* note: send_message _MUST_ be non-blocking */
int TCPMessenger::send_message(Message *m, entity_name_t dest, int port, int fromport)
{
  // set envelope
  m->set_source(get_myaddr(), fromport);
  m->set_dest(dest, port);
  m->set_lamport_send_stamp( get_lamport() );

  dout(4) << "--> " << m->get_type_name() 
          << " from " << MSG_ADDR_NICE(m->get_source()) << ':' << m->get_source_port() 
          << " to " << MSG_ADDR_NICE(m->get_dest()) << ':' << m->get_dest_port() 
          << " ---- " << m 
          << endl;

  // local?
  TCPMessenger *entity = 0;
  directory_lock.Lock();
  if (directory.count(dest) &&
      directory_ready.count(dest)) entity = directory[dest];
  directory_lock.Unlock();
  
  if (entity) {  
    // local!
    ::incoming_lock.Lock();
    {
      dout(20) << " queueing locally for " << dest << " " << m << endl;  //", stat_inq " << stat_inq << ", incomign " << ::incoming.size() << endl;
      //assert(stat_inq == ::incoming.size());
      ::incoming.push_back(m);
      ::incoming_cond.Signal();
      stat_inq++;
      //assert(stat_inq == ::incoming.size());
      //dout(-20) << " stat_inq " << stat_inq << ", incoming " << ::incoming.size() << endl;
      stat_inqb += m->get_payload().length();
    }
    ::incoming_lock.Unlock();
  } else {
    // remote!

    if (g_conf.tcp_serial_marshall)
      tcp_marshall(m);
    
    if (g_conf.tcp_serial_out) {
      lookup_lock.Lock();
      // send in this thread
      if (tcp_lookup(m))
        tcp_send(m);
      lookup_lock.Unlock();
    } else {
      lookup_lock.Lock();
      OutThread *outt = tcp_lookup(m);
      lookup_lock.Unlock();
      
      if (outt) outt->send(m);
    }
  }

  return 0;
}




