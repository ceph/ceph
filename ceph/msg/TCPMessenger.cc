
#include "include/config.h"
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


# include <netdb.h>
# include <sys/socket.h>
# include <netinet/in.h>
# include <arpa/inet.h>
#include <sys/select.h>
#include <fcntl.h>
#include <sys/types.h>

#include <unistd.h>
#include <mpi.h>


#define DBL 18

int tcp_port = 9876;

/*
 * We make a directory, so that we can have multiple Messengers in the
 * same process (rank).  This is useful for benchmarking and creating lots of 
 * simulated clients, e.g.
 */

hash_map<int, TCPMessenger*>  directory;  // local
list<Message*>                incoming;
Mutex                         incoming_lock;
Cond                          incoming_cond;
list<Message*>                outgoing;
Mutex                         outgoing_lock;
Cond                          outgoing_cond;

struct sockaddr_in *remote_addr;
int                *in_sd;     // incoming sockets
pthread_t          *in_threads;
int                *out_sd;    // outgoing sockets

struct sockaddr_in listen_addr;
int                listen_sd = 0;



/* this process */
int mpi_world;
int mpi_rank;
bool tcp_done = false;     // set this flag to stop the event loop

pthread_t dispatch_thread_id = 0;   // thread id of the event loop.  init value == nobody
pthread_t out_thread_id = 0;   // thread id of the event loop.  init value == nobody
pthread_t listen_thread_id = 0;
Mutex sender_lock;

bool pending_timer = false;



// debug
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "[TCP " << mpi_rank << "/" << mpi_world << " " << getpid() << "." << pthread_self() << "] "

ostream& operator<<(ostream& out, struct sockaddr_in &a)
{
  char addr[4];
  memcpy((char*)addr, (char*)&a.sin_addr.s_addr, 4);
  out << (unsigned)addr[0] << "."
	  << (unsigned)addr[1] << "."
	  << (unsigned)addr[2] << "."
	  << (unsigned)addr[3] << ":"
	  << (int)a.sin_port;
  return out;
}


/*****
 * MPI global methods for process-wide startup, shutdown.
 */

int tcpmessenger_init(int& argc, char**& argv)
{
  // exhcnage addresses with other nodes
  MPI_Init(&argc, &argv);
  
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_world);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

  dout(DBL) << "rank is " << mpi_rank << " / " << mpi_world << endl;

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
  rc = ::listen(listen_sd, 2*mpi_world);
  assert(rc >= 0);

  dout(DBL) << "listening on " << myport << endl;
  
  remote_addr = new sockaddr_in[mpi_world];

  // my address is...
  char host[100];
  gethostname(host, 100);
  dout(DBL) << "my hostname is " << host << endl;

  struct hostent *myhostname = gethostbyname( host ); 
  
  struct sockaddr_in myaddr;
  myaddr.sin_family = myhostname->h_addrtype;
  memcpy((char *) &myaddr.sin_addr.s_addr, 
		 myhostname->h_addr_list[0], 
		 myhostname->h_length);
  myaddr.sin_port = myport;

  dout(DBL) << "my ip is " << myaddr << endl;

  remote_addr[mpi_rank] = myaddr;

  dout(DBL) << "MPI_Allgathering addrs" << endl;
  MPI_Allgather( &myaddr, sizeof(struct sockaddr_in), MPI_CHAR,
				 remote_addr, sizeof(struct sockaddr_in), MPI_CHAR,
				 MPI_COMM_WORLD);

  //  for (int i=0; i<mpi_world; i++) 
  //dout(DBL) << "  addr of " << i << " is " << remote_addr[i] << endl;

  dout(DBL) << "tcpmessenger_shutdown barrier" << endl;
  MPI_Barrier (MPI_COMM_WORLD);
  MPI_Finalize();


  // init socket arrays
  in_sd = new int[mpi_world];
  memset(in_sd, 0, sizeof(int)*mpi_world);
  out_sd = new int[mpi_world];
  memset(out_sd, 0, sizeof(int)*mpi_world);
  in_threads = new pthread_t[mpi_world];
  memset(in_threads, 0, sizeof(pthread_t)*mpi_world);

  dout(DBL) << "init done" << endl;
  return mpi_rank;
}



int tcpmessenger_shutdown() 
{
  dout(1) << "tcpmessenger_shutdown closing all sockets etc" << endl;

  // bleh
  for (int i=0; i<mpi_world; i++) {
	if (out_sd[i]) ::close(out_sd[i]);
  }

  delete[] remote_addr;
  delete[] in_sd;
  delete[] out_sd;
}

int tcpmessenger_world()
{
  return mpi_world;
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

void tcp_write(int sd, char *buf, int len)
{
  //dout(DBL) << "tcp_write writing " << len << endl;
  while (len > 0) {
	int did = ::send( sd, buf, len, 0 );
	assert(did >= 0);
	len -= did;
	buf += did;
	dout(DBL) << "tcp_write did " << did << ", " << len << " left" << endl;
  }

}


/*
 * recv a Message*
 */


/*
void tcp_wait()
{
  fd_set fds;
  FD_ZERO(&fds);

  int n = 0;

  for (int i=0; i<mpi_world; i++) {
	if (in_sd[i] == 0) continue;
	FD_SET(in_sd[i], &fds);
	n++;
  }
  assert(n == mpi_world);

  struct timeval tv;
  tv.tv_sec = 10;        // time out every few seconds
  tv.tv_usec = 0;
  
  dout(DBL) << "tcp_wait on " << n << endl;
  int r = ::select(n, &fds, 0, &fds, 0);//&tv);
  dout(DBL) << "select returned " << r << endl;
}
*/


Message *tcp_recv(int from)
{
  // envelope
  dout(DBL) << "tcp_recv receiving message from " << from  << endl;
  
  msg_envelope_t env;
  if (!tcp_read( in_sd[from], (char*)&env, sizeof(env) ))
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
	tcp_read( in_sd[from], (char*)&size, sizeof(size) );

	bufferptr bp = new buffer(size);
	
	tcp_read( in_sd[from], bp.c_str(), size );

	bp.set_length(size);
	blist.push_back(bp);

	dout(DBL) << "tcp_recv got frag " << i << " of " << env.nchunks << " len " << bp.length() << endl;
  }
  
  dout(DBL) << "tcp_recv got " << blist.length() << " byte message" << endl;

  // unmarshall message
  Message *m = decode_message(env, blist);
  return m;
}


void *tcp_inthread(void *r)
{
  int who = (int)r;

  dout(DBL) << "tcp_inthread reading for " << who << endl;

  while (!tcp_done) {
	/*
	fd_set fds;
	FD_ZERO(&fds);
	FD_SET(in_sd[who], &fds);
	
	dout(DBL) << "tcp_inthread waiting on socket" << endl;
	::select(1, &fds, 0, &fds, 0);
	*/

	Message *m = tcp_recv(who);
	if (!m) break;

	incoming_lock.Lock();
	incoming.push_back(m);
	incoming_cond.Signal();
	incoming_lock.Unlock();
  }

  dout(DBL) << "tcp_inthrad closing " << who << endl;

  ::close(in_sd[who]);
  in_sd[who] = 0;

  return 0;  
}


void *tcp_accepter(void *)
{
  dout(DBL) << "tcp_accepter starting" << endl;

  int left = mpi_world;
  while (left > 0) {
	//dout(DBL) << "accepting, left = " << left << endl;

	struct sockaddr_in addr;
	socklen_t slen = sizeof(addr);
	int sd = ::accept(listen_sd, (sockaddr*)&addr, &slen);
	if (sd > 0) {
	  
	  //dout(DBL) << "accepted incoming, reading who it is " << endl;
	  
	  int who;
	  tcp_read(sd, (char*)&who, sizeof(who));
	  
	  in_sd[who] = sd;
	  left--;

	  dout(DBL) << "accepted incoming from " << who << ", left = " << left << endl;

	  pthread_create(&in_threads[who],
					 NULL,
					 tcp_inthread,
					 (void*)who);
	} else {
	  dout(DBL) << "no incoming connection?" << endl;
	}
  }
  dout(DBL) << "got incoming from everyone!" << endl;
}



void tcp_open(int who)
{
  //dout(DBL) << "tcp_open " << who << " to " << remote_addr[who] << endl;

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
  int r = connect(sd, (sockaddr*)&remote_addr[who], sizeof(myAddr));
  assert(r >= 0);

  //dout(DBL) << "tcp_open connected to " << who << endl;

  int me = mpi_rank;
  tcp_write(sd, (char*)&me, sizeof(me));

  out_sd[who] = sd;
}



/*
 * send a Message* over the wire.  ** do not block **.
 */
int tcp_send(Message *m)
{
  int rank = MPI_DEST_TO_RANK(m->get_dest(), mpi_world);

  // marshall
  m->encode_payload();
  msg_envelope_t *env = &m->get_envelope();
  bufferlist blist = m->get_payload();
  env->nchunks = blist.buffers().size();

  dout(7) << "sending " << *m << " to " << MSG_ADDR_NICE(env->dest) << " (rank " << rank << ")" << endl;
  
  sender_lock.Lock();
  
  // open first?
  if (out_sd[rank] == 0) tcp_open(rank);

  // send envelope
  tcp_write( out_sd[rank], (char*)env, sizeof(*env) );

  // payload
  int i = 0;
  for (list<bufferptr>::iterator it = blist.buffers().begin();
	   it != blist.buffers().end();
	   it++) {
	dout(DBL) << "tcp_sending frag " << i << " len " << (*it).length() << endl;
	int size = (*it).length();
	tcp_write( out_sd[rank], (char*)&size, sizeof(size) );
	tcp_write( out_sd[rank], (*it).c_str(), size );
	i++;
  }

  sender_lock.Unlock();
}



// recv event loop, for unsolicited messages.

void* tcp_sendthread(void*)
{
  outgoing_lock.Lock();
  while (!outgoing.empty() || !tcp_done) {
	
	while (outgoing.size()) {
	  Message *m = outgoing.front();
	  outgoing.pop_front();
	  tcp_send(m);
	}

	outgoing_cond.Wait(outgoing_lock);
	
  }
  outgoing_lock.Unlock();
}

void* tcpmessenger_loop(void*)
{
  dout(5) << "tcpmessenger_loop start pid " << getpid() << endl;

  incoming_lock.Lock();

  while (1) {

	// timer events?
	if (pending_timer) {
	  dout(DBL) << "pending timer" << endl;
	  g_timer.execute_pending();
	}

	// done?
	if (tcp_done &&
		incoming.empty() &&
		!pending_timer) break;
	
	// incoming
	dout(12) << "loop waiting for incoming messages" << endl;
	
	incoming_cond.Wait(incoming_lock);
	
	while (incoming.size()) {
	  list<Message*> in;
	  in.splice(in.begin(), incoming);

	  incoming_lock.Unlock();

	  while (in.size()) {
		Message *m = in.front();
		in.pop_front();
	  
		int dest = m->get_dest();
		if (directory.count(dest)) {
		  Messenger *who = directory[ dest ];
		  
		  dout(4) << "---- '" << m->get_type_name() << 
			"' from " << MSG_ADDR_NICE(m->get_source()) << ':' << m->get_source_port() <<
			" to " << MSG_ADDR_NICE(m->get_dest()) << ':' << m->get_dest_port() << " ---- " 
				  << m 
				  << endl;
		  
		  who->dispatch(m);
		} else {
		  dout (1) << "---- i don't know who " << dest << " is." << endl;
		  assert(0);
		}
	  }

	  incoming_lock.Lock();
	}
  }

  incoming_lock.Unlock();

  g_timer.shutdown();


  dout(5) << "tcpmessenger_loop exiting loop" << endl;
}


// start/stop mpi receiver thread (for unsolicited messages)
int tcpmessenger_start()
{
  dout(5) << "starting accept thread" << endl;
  pthread_create(&listen_thread_id,
				 NULL,
				 tcp_accepter,
				 0);				

  dout(5) << "starting dispatch thread" << endl;
  
  // start a thread
  pthread_create(&dispatch_thread_id, 
				 NULL, 
				 tcpmessenger_loop, 
				 0);


  dout(5) << "starting outgoing thread" << endl;
  pthread_create(&out_thread_id, 
				 NULL, 
				 tcp_sendthread,
				 0);

}


/*
 * kick and wake up _loop (to pick up new outgoing message, or quit)
 */

void tcpmessenger_kick_incoming_loop()
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
  tcpmessenger_kick_incoming_loop();

  void *returnval;
  dout(10) << "tcpmessenger_wait waiting for thread to finished." << endl;
  pthread_join(dispatch_thread_id, &returnval);
  dout(10) << "tcpmessenger_wait thread finished." << endl;

}




/***********
 * Tcpmessenger class implementation
 */

class C_TCPKicker : public Context {
  void finish(int r) {
	dout(DBL) << "timer kick" << endl;
	tcpmessenger_kick_incoming_loop();
  }
};

TCPMessenger::TCPMessenger(msg_addr_t myaddr) : Messenger(myaddr)
{
  // my address
  this->myaddr = myaddr;

  // register myself in the messenger directory
  directory[myaddr] = this;

  // register to execute timer events
  g_timer.set_messenger_kicker(new C_TCPKicker());

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

TCPMessenger::~TCPMessenger()
{
  //delete logger;
}


int TCPMessenger::shutdown()
{
  // remove me from the directory
  directory.erase(myaddr);

  // no more timer events
  g_timer.unset_messenger_kicker();

  // last one?
  if (directory.empty()) {
	dout(1) << "shutdown last tcpmessenger on rank " << mpi_rank << " shut down" << endl;
	pthread_t whoami = pthread_self();


  
	// close incoming sockets
	void *r;
	for (int i=0; i<mpi_world; i++) {
	  if (in_sd[i] == 0) continue;
	  dout(DBL) << "closing reader on " << i << " sd " << in_sd[i] << endl;
	  ::close(in_sd[i]);
	  //dout(DBL) << "waiting for reader thread to close on " << i << endl;
	  //pthread_join(in_threads[i], &r);
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
  } else {
	dout(10) << "shutdown still " << directory.size() << " other messengers on rank " << mpi_rank << endl;
  }
}




/***
 * public messaging interface
 */


/* note: send_message _MUST_ be non-blocking */
int TCPMessenger::send_message(Message *m, msg_addr_t dest, int port, int fromport)
{
  // set envelope
  m->set_source(myaddr, fromport);
  m->set_dest(dest, port);

  if (0) {
	// der
	tcp_send(m);
  } else {
	// good way
	outgoing_lock.Lock();
	outgoing.push_back(m);
	outgoing_cond.Signal();
	outgoing_lock.Unlock();
  }
}




