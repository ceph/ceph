
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

#define DBL 18


TCPMessenger *rankmessenger = 0; // 

TCPDirectory *nameserver = 0;    // only defined on rank 0
TCPMessenger *nsmessenger = 0;



// local directory
hash_map<int, TCPMessenger*>  directory;  // local
Mutex                         directory_lock;

// connecting
struct sockaddr_in listen_addr;     // my listen addr
int                listen_sd = 0;
int                my_rank = -1;
Cond               waiting_for_rank;

// register
long regid = 0;
map<int, Cond* >        waiting_for_register_cond;
map<int, msg_addr_t >   waiting_for_register_result;

// incoming messages
list<Message*>                incoming;
Mutex                         incoming_lock;
Cond                          incoming_cond;

// outgoing messages
list<Message*>                outgoing;
Mutex                         outgoing_lock;
Cond                          outgoing_cond;

Mutex lookup_lock;  // 
hash_map<msg_addr_t, int> entity_rank;      // entity -> rank
hash_map<int, int>        rank_sd;   // outgoing sockets, rank -> sd
hash_map<int, tcpaddr_t>  rank_addr; // rank -> tcpaddr
map<msg_addr_t, list<Message*> > waiting_for_lookup;


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
#define  dout(l)    if (l<=g_conf.debug) cout << "[TCP " << my_rank << " " << getpid() << "." << pthread_self() << "] "




// some declarations
void tcp_open(int rank);
int tcp_send(Message *m);
void tcpmessenger_kick_dispatch_loop();




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
	
	waiting_for_rank.Signal();

	delete m;
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

	for (map<msg_addr_t, int>::iterator it = m->entity_map.begin();
		 it != m->entity_map.end();
		 it++) {
	  dout(DBL) << "lookup got " << MSG_ADDR_NICE(it->first) << " on rank " << it->second << endl;
	  entity_rank[it->first] = it->second;
	  
	  // take waiters
	  waiting.splice(waiting.begin(), waiting_for_lookup[it->first]);
	  waiting_for_lookup.erase(it->first);
	}

	for (map<int,tcpaddr_t>::iterator it = m->rank_addr.begin();
		 it != m->rank_addr.end();
		 it++) {
	  dout(DBL) << "lookup got rank " << it->first << " addr " << it->second << endl;
	  rank_addr[it->first] = it->second;

	  // open it now
	  tcp_open(it->first);
	}

	// send waiting messages
	for (list<Message*>::iterator it = waiting.begin();
		 it != waiting.end();
		 it++) {
	  tcp_send(*it);
	}

	delete m;
  }
  
} rankserver;


class C_TCPKicker : public Context {
  void finish(int r) {
	dout(DBL) << "timer kick" << endl;
	tcpmessenger_kick_dispatch_loop();
  }
};


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
  msgr_callback_kicker = new C_TCPKicker();

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



bool tcp_read(int sd, char *buf, int len)
{
  while (len > 0) {
	int got = ::recv( sd, buf, len, 0 );
	if (got < 0) return false;
	assert(got >= 0);
	len -= got;
	buf += got;
	//dout(DBL) << "tcp_read got " << got << ", " << len << " left" << endl;
  }
  return true;
}

int tcp_write(int sd, char *buf, int len)
{
  //dout(DBL) << "tcp_write writing " << len << endl;
  assert(len > 0);
  while (len > 0) {
	int did = ::send( sd, buf, len, 0 );
	if (did < 0) {
	  dout(1) << "tcp_write error did = " << did << "  errno " << errno << " " << strerror(errno) << endl;
	  cerr << "tcp_write error did = " << did << "  errno " << errno << " " << strerror(errno) << endl;
	}
	//assert(did >= 0);
	if (did < 0) return did;
	len -= did;
	buf += did;
	dout(DBL) << "tcp_write did " << did << ", " << len << " left" << endl;
  }
  return 0;
}


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

  dout(DBL) << "tcp_recv got envelope type=" << env.type << " src " << env.source << " dst " << env.dest << " nchunks=" << env.nchunks << endl;
  
  // payload
  bufferlist blist;
  for (int i=0; i<env.nchunks; i++) {
	int size;
	tcp_read( sd, (char*)&size, sizeof(size) );

	bufferptr bp = new buffer(size);
	
	tcp_read( sd, bp.c_str(), size );

	blist.push_back(bp);

	dout(DBL) << "tcp_recv got frag " << i << " of " << env.nchunks << " len " << bp.length() << endl;
  }
  
  // unmarshall message
  size_t s = blist.length();
  Message *m = decode_message(env, blist);

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
  rank_sd[rank] = sd;
}


void tcp_marshall(Message *m)
{
  // marshall
  if (m->empty_payload())
	m->encode_payload();
}

bool tcp_lookup(Message *m)
{
  msg_addr_t addr = m->get_dest();

  if (!entity_rank.count(m->get_dest())) {
	// lookup and wait.
	if (waiting_for_lookup.count(addr)) {
	  dout(DBL) << "already looking up " << MSG_ADDR_NICE(addr) << endl;
	} else {
	  dout(DBL) << "lookup on " << MSG_ADDR_NICE(addr) << endl;
	  MNSLookup *r = new MNSLookup(addr);
	  rankmessenger->send_message(r, MSG_ADDR_DIRECTORY);
	}
	
	// add waiter
	waiting_for_lookup[addr].push_back(m);
	return false;
  }

  return true;
}


/*
 * send a Message* over the wire.  ** do not block **.
 */
int tcp_send(Message *m)
{
  int rank = entity_rank[m->get_dest()];
  if (rank_sd.count(rank) == 0) tcp_open(rank);

  int sd = rank_sd[rank];
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

  dout(7) << "sending " << *m << " to " << MSG_ADDR_NICE(m->get_dest()) << " rank " << rank << endl;//" sd " << sd << ")" << endl;
  
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
void* tcp_outthread(void*)
{
  outgoing_lock.Lock();
  while (!outgoing.empty() || !tcp_done) {
	
	if (!outgoing.empty()) {
	  // grab outgoing list
	  list<Message*> out;
	  out.splice(out.begin(), outgoing);

	  // drop lock while i send these
	  outgoing_lock.Unlock();

	  while (!out.empty()) {
		Message *m = out.front();
		out.pop_front();

		tcp_marshall(m);
		tcp_send(m);
	  }

	  outgoing_lock.Lock();
	}

	// wait
	if (outgoing.empty())
	  outgoing_cond.Wait(outgoing_lock);
	
  }
  outgoing_lock.Unlock();
  return 0;
}

/** tcp_inthread
 * read incoming messages from a given peer.
 * give received and decoded messages to dispatch loop.
 */
void *tcp_inthread(void *r)
{
  int sd = (int)r;
  int who = -1;

  dout(DBL) << "tcp_inthread reading on sd " << sd << " who is " << who << endl;

  while (!tcp_done) {
	Message *m = tcp_recv(sd);
	if (!m) break;
	who = m->get_source();

	// give to dispatch loop
	incoming_lock.Lock();
	incoming.push_back(m);
	incoming_cond.Signal();
	incoming_lock.Unlock();
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
void* tcp_dispatchthread(void*)
{
  dout(5) << "tcp_dispatchthread start pid " << getpid() << endl;

  incoming_lock.Lock();

  while (1) {

	// callbacks?
	messenger_do_callbacks();

	// timer events?
	/*if (pending_timer) {
	  pending_timer = false;
	  dout(DBL) << "dispatch: pending timer" << endl;
	  g_timer.execute_pending();
	}
	*/

	// done?
	if (tcp_done &&
		incoming.empty()) break;
		//&&
		//!pending_timer) break;

	// wait?
	if (incoming.empty()) {
	  // wait
	  dout(12) << "dispatch: waiting for incoming messages" << endl;
	  incoming_cond.Wait(incoming_lock);
	}

	// incoming?
	while (!incoming.empty()) {
	  // grab incoming messages
	  list<Message*> in;
	  in.splice(in.begin(), incoming);

	  // drop lock while we deliver
	  incoming_lock.Unlock();

	  while (!in.empty()) {
		Message *m = in.front();
		in.pop_front();

		dout(DBL) << "dispatch doing " << *m << endl;
	  
		// for rankserver?
		if (m->get_type() == MSG_NS_CONNECTACK ||        // i just connected
			m->get_dest() == MSG_ADDR_RANK(my_rank)) {
		  dout(DBL) <<  " giving to rankserver" << endl;
		  rankserver.dispatch(m);
		  continue;
		}

		// ok
		int dest = m->get_dest();
		directory_lock.Lock();
		if (directory.count(dest)) {
		  Messenger *who = directory[ dest ];
		  directory_lock.Unlock();		  

		  dout(4) << "---- '" << m->get_type_name() << 
			"' from " << MSG_ADDR_NICE(m->get_source()) << ':' << m->get_source_port() <<
			" to " << MSG_ADDR_NICE(m->get_dest()) << ':' << m->get_dest_port() << " ---- " 
				  << m 
				  << endl;
		  
		  who->dispatch(m);
		} else {
		  directory_lock.Unlock();
		  dout (1) << "---- i don't know who " << MSG_ADDR_NICE(dest) << " " << dest << " is." << endl;
		  assert(0);
		}
	  }

	  incoming_lock.Lock();
	}
  }

  incoming_lock.Unlock();

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


  dout(5) << "starting outgoing thread" << endl;
  pthread_create(&out_thread_id, 
				 NULL, 
				 tcp_outthread,
				 0);
  return 0;
}


/*
 * kick and wake up _loop (to pick up new outgoing message, or quit)
 */

void tcpmessenger_kick_dispatch_loop()
{
  incoming_lock.Lock();
  incoming_cond.Signal();
  incoming_lock.Unlock();
}

void tcpmessenger_kick_outgoing_loop()
{
  outgoing_lock.Lock();
  outgoing_cond.Signal();
  outgoing_lock.Unlock();
}


// wait for thread to finish

void tcpmessenger_wait()
{
  tcpmessenger_kick_dispatch_loop();

  void *returnval;
  dout(10) << "tcpmessenger_wait waiting for thread to finished." << endl;
  pthread_join(dispatch_thread_id, &returnval);
  dout(10) << "tcpmessenger_wait thread finished." << endl;

}




msg_addr_t register_entity(msg_addr_t addr) 
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
	tcp_send(m);
	
	// wait for reply
	waiting_for_rank.Wait(lookup_lock);
	assert(my_rank > 0);
  }
  
  // send req
  dout(DBL) << "register_entity " << MSG_ADDR_NICE(addr) << endl;
  Message *m = new MNSRegister(addr, my_rank, id);
  m->set_dest(MSG_ADDR_DIRECTORY, 0);
  tcp_marshall(m);
  tcp_send(m);
  
  // wait?
  if (waiting_for_register_result.count(id)) {
	// already here?
  } else
	cond.Wait(lookup_lock);

  // get result, clean up
  int entity = waiting_for_register_result[id];
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


TCPMessenger::TCPMessenger(msg_addr_t myaddr) : Messenger(myaddr)
{
  if (myaddr != MSG_ADDR_DIRECTORY) {
	// register!
	myaddr = register_entity(myaddr);
  }

  // my address
  set_myaddr( myaddr );

  // register myself in the messenger directory
  directory_lock.Lock();
  directory[myaddr] = this;
  directory_lock.Unlock();

  // register to execute timer events
  //g_timer.set_messenger_kicker(new C_TCPKicker());
  g_timer.set_messenger(this);


  // logger
  /*
  string name;
  name = "m.";
  name += MSG_ADDR_TYPE(whoami);
  int w = MSG_ADDR_NUM(whoami);
  if (w >= 1000) name += ('0' + ((w/1000)%10));
  if (w >= 100) name += ('0' + ((w/100)%10));
  if (w >= 10) name += ('0' + ((w/10)%10));
  name += ('0' + ((w/1)%10));

  logger = new Logger(name, (LogType*)&mpimsg_logtype);
  loggers[ whoami ] = logger;
  */

}


void TCPMessenger::ready()
{
  if (get_myaddr() != MSG_ADDR_DIRECTORY) {
	// started!  tell namer we are up and running.
	lookup_lock.Lock();
	Message *m = new MGenericMessage(MSG_NS_STARTED);
	m->set_source(get_myaddr(), 0);
	m->set_dest(MSG_ADDR_DIRECTORY, 0);
	tcp_marshall(m);
	tcp_send(m);
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

void TCPMessenger::map_entity_rank(msg_addr_t e, int r)
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



int TCPMessenger::shutdown()
{
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

  // or almost last one?
  if (rankmessenger && directory.size() == 1) {
	directory_lock.Unlock();
	tcpmessenger_stop_rankserver();
	directory_lock.Lock();
  }

  directory_lock.Unlock();

  // last one?
	  if (lastone) {
	dout(2) << "shutdown last tcpmessenger on rank " << my_rank << " shut down" << endl;
	//pthread_t whoami = pthread_self();

	// no more timer events
	g_timer.unset_messenger();
	msgr_callback_kicker = 0;

  
	// close incoming sockets
	//void *r;
	for (map<int,pthread_t>::iterator it = in_threads.begin();
		 it != in_threads.end();
		 it++) {
	  dout(DBL) << "closing reader on sd " << it->first << endl;	  
	  ::close(it->first);
	  //pthread_join(it->second, &r);
	}

	dout(DBL) << "setting tcp_done" << endl;

	incoming_lock.Lock();
	tcp_done = true;
	incoming_cond.Signal();
	incoming_lock.Unlock();

	tcpmessenger_kick_outgoing_loop();


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
int TCPMessenger::send_message(Message *m, msg_addr_t dest, int port, int fromport)
{
  // set envelope
  m->set_source(get_myaddr(), fromport);
  m->set_dest(dest, port);
  m->set_lamport_stamp( get_lamport() );

  if (1) {
	// serialize all output
	tcp_marshall(m);

	lookup_lock.Lock();
	if (tcp_lookup(m))
	  tcp_send(m);
	lookup_lock.Unlock();
  } else {
	// good way (that's actually similarly lame?)
	outgoing_lock.Lock();
	outgoing.push_back(m);
	outgoing_cond.Signal();
	outgoing_lock.Unlock();
  }
  return 0;
}




