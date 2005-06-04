
#include "include/config.h"
#include "include/error.h"

#include "common/Timer.h"
#include "common/Mutex.h"

#include "MPIMessenger.h"
#include "CheesySerializer.h"
#include "Message.h"

#include <iostream>
#include <cassert>
using namespace std;
#include <ext/hash_map>
using namespace __gnu_cxx;

#include <unistd.h>
#include <mpi.h>

/*
 * We make a directory, so that we can have multiple Messengers in the
 * same process (rank).  This is useful for benchmarking and creating lots of 
 * simulated clients, e.g.
 */

hash_map<int, MPIMessenger*>  directory;
list<Message*>                outgoing;

/* this process */
int mpi_world;
int mpi_rank;
bool mpi_done = false;     // set this flag to stop the event loop

#define TAG_ENV         1
#define TAG_PAYLOAD     2
#define TAG_ACK         3

// the key used to fetch the tag for the current thread.
pthread_key_t tag_key;
pthread_t thread_id = 0;   // thread id of the event loop.  init value == nobody

Mutex sender_lock;

// our lock for any common data; it's okay to have only the one global mutex
// because our common data isn't a whole lot.
static pthread_mutex_t mutex;

// the number of distinct threads we've seen so far; used to generate
// a unique tag for each thread.
static int nthreads;

//#define TAG_UNSOLICITED 0

// debug
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "[MPI " << mpi_rank << "/" << mpi_world << " " << getpid() << "." << pthread_self() << "] "



/*****
 * MPI global methods for process-wide setup, shutdown.
 */

int mpimessenger_init(int& argc, char**& argv)
{
  MPI_Init(&argc, &argv);
  
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_world);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

  char hostname[100];
  gethostname(hostname,100);
  int pid = getpid();

  dout(12) << "init: i am " << hostname << " pid " << pid << endl;
  
  assert(mpi_world > g_conf.num_osd+g_conf.num_mds);

  return mpi_rank;
}

int mpimessenger_shutdown() 
{
  MPI_Finalize();
}

int mpimessenger_world()
{
  return mpi_world;
}

/***
 * internal send/recv
 */


MPI_Request recv_env_req;

Message *mpi_recv()
{
  MPI_Status status;
  
  //dout(10) << "mpi_recv waiting for message" << endl;

  /*
  // get message size
  ASSERT(MPI_Probe(MPI_ANY_SOURCE, 
				   TAG_ENV,
				   MPI_COMM_WORLD,
				   &status) == MPI_SUCCESS);
  
  dout(10) << "mpi_recv probe sees " << status.count << " bytes incoming" << endl;

  // make sure it's the size of an envelope!
  assert(status.count <= MSG_ENVELOPE_LEN);
  */

  msg_envelope_t env;

  dout(10) << "mpi_recv Irecv envelope" << endl;
  ASSERT(MPI_Irecv((void*)&env,
				  sizeof(env),
				  MPI_CHAR, 
				  MPI_ANY_SOURCE,
				  TAG_ENV,
				  MPI_COMM_WORLD,
				  &recv_env_req) == MPI_SUCCESS);
  
  dout(10) << "mpi_recv wait" << endl;

  if (MPI_Wait(&recv_env_req, &status) != MPI_SUCCESS) {
	dout(10) << "mpi_recv MPI_Wait not success" << endl;
	return 0;
  }
  
  if (status.count < MSG_ENVELOPE_LEN) {
	dout(10) << "mpi_recv got short recv " << status.count << " bytes" << endl;
	return 0;
  }
  if (env.type == 0) {
	dout(10) << "mpi_recv got type 0 message, kicked!" << endl;
	return 0;
  }

  dout(10) << "mpi_recv got envelope " << status.count << ", type=" << env.type << " src " << env.source << " dst " << env.dest << " nchunks=" << env.nchunks << " from " << status.MPI_SOURCE << endl;

  // get the rest of the message
  bufferlist blist;
  for (int i=0; i<env.nchunks; i++) {
	MPI_Status fragstatus;
	ASSERT(MPI_Probe(status.MPI_SOURCE,
					 TAG_PAYLOAD,
					 MPI_COMM_WORLD,
					 &fragstatus) == MPI_SUCCESS);

	dout(10) << "mpi_recv frag " << i << " of " << env.nchunks << " is len " << fragstatus.count << endl;

	bufferptr bp = new buffer(fragstatus.count);
	
	ASSERT(MPI_Recv(bp.c_str(),
					fragstatus.count,
					MPI_CHAR, 
					status.MPI_SOURCE,
					TAG_PAYLOAD,
					MPI_COMM_WORLD,
					&fragstatus) == MPI_SUCCESS);
	bp.set_length(fragstatus.count);
	blist.push_back(bp);

	dout(10) << "mpi_recv got frag " << i << " of " << env.nchunks << " len " << fragstatus.count << endl;
  }
  
  /*
  dout(10) << "mpi_recv sending (*&#($#$ ack to " << status.MPI_SOURCE << endl;
  char ack = 1;
  MPI_Send(&ack,
		   1,
		   MPI_CHAR,
		   status.MPI_SOURCE,
		   TAG_ACK,
		   MPI_COMM_WORLD);
  */
  dout(10) << "mpi_recv got " << blist.length() << " byte message tag " << status.MPI_TAG << endl;
  

  // unmarshall message
  Message *m = decode_message(env, blist);
  return m;
}

int mpi_send(Message *m)
{
  int rank = MPI_DEST_TO_RANK(m->get_dest(), mpi_world);
  if (rank == mpi_rank) {      
	dout(1) << "local delivery not implemented" << endl;
	assert(0);
  } 

  // marshall
  m->encode_payload();
  msg_envelope_t *env = &m->get_envelope();
  bufferlist blist = m->get_payload();
  env->nchunks = blist.buffers().size();

  dout(10) << "mpi_sending " << blist.length() << " size message type " << env->type << " src " << env->source << " dst " << env->dest << " to rank " << rank << " nchunks=" << env->nchunks << endl;

  //sender_mutex.Lock();

  // send envelope
  MPI_Request req; 
  ASSERT(MPI_Isend((void*)env,
				  sizeof(*env),
				  MPI_CHAR,
				  rank,
				  TAG_ENV,
				  MPI_COMM_WORLD,
				   &req) == MPI_SUCCESS);
  
  int i = 0;
  vector<MPI_Request> chunk_reqs(env->nchunks);
  for (list<bufferptr>::iterator it = blist.buffers().begin();
	   it != blist.buffers().end();
	   it++) {
	dout(10) << "mpi_sending frag " << i << " len " << (*it).length() << endl;
	ASSERT(MPI_Isend((void*)(*it).c_str(),
					 (*it).length(),
					 MPI_CHAR,
					 rank,
					 TAG_PAYLOAD,
					 MPI_COMM_WORLD,
					 &chunk_reqs[i]) == MPI_SUCCESS);
	i++;
  }

  /*
  dout(10) << "mpi_send aaaaahhhhhh waiting for ack" << endl;
  char ack;
  MPI_Status ackstatus;
  ASSERT(MPI_Recv(&ack,
				  1,
				  MPI_CHAR, 
				  rank,
				  TAG_ACK,
				  MPI_COMM_WORLD,
				  &ackstatus) == MPI_SUCCESS);
  */

  dout(10) << "mpi_send done" << endl;

  //sender_mutex.Unlock();
}



// get the tag for this thread
/*
static int get_thread_tag()
{
  int tag = (int)pthread_getspecific(tag_key);
  
  if (tag == 0) {
	// first time this thread has performed MPI messaging
	
	if (pthread_mutex_lock(&mutex) < 0)
	  SYSERROR();
	
	tag = ++nthreads;
	
	if (pthread_mutex_unlock(&mutex) < 0)
	  SYSERROR();
	
	if (pthread_setspecific(tag_key, (void*)tag) < 0)
	  SYSERROR();
  }
  
  return tag;
}
*/



// recv event loop, for unsolicited messages.

void* mpimessenger_loop(void*)
{
  dout(1) << "mpimessenger_loop start pid " << getpid() << endl;

  while (!mpi_done) {
	// check outgoing queue
	sender_lock.Lock();
	for (list<Message*>::iterator it = outgoing.begin();
		 it != outgoing.end();
		 it++) {
	  mpi_send(*it);
	}
	outgoing.clear();
	sender_lock.Unlock();
	
	// check mpi
	dout(12) << "mpimessenger_loop waiting for incoming messages" << endl;

	// get message
	Message *m = mpi_recv();
	if (!m) continue;  // no message?
	
	int dest = m->get_dest();
	if (directory.count(dest)) {
	  Messenger *who = directory[ dest ];
	  
	  dout(3) << "---- mpimessenger_loop dispatching '" << m->get_type_name() << 
		"' from " << MSG_ADDR_NICE(m->get_source()) << ':' << m->get_source_port() <<
		" to " << MSG_ADDR_NICE(m->get_dest()) << ':' << m->get_dest_port() << " ---- " << m 
			  << endl;
	  
	  who->dispatch(m);
	} else {
	  dout (1) << "---- i don't know who " << dest << " is." << endl;
	  assert(0);
	  break;
	}
  }

  dout(5) << "mpimessenger_loop finish, waiting for all to finish" << endl;
  MPI_Barrier (MPI_COMM_WORLD);
  dout(5) << "mpimessenger_loop everybody done, exiting loop" << endl;
}


// start/stop mpi receiver thread (for unsolicited messages)

int mpimessenger_start()
{
  dout(5) << "mpimessenger_start starting thread" << endl;
  
  // start a thread
  pthread_create(&thread_id, 
				 NULL, 
				 mpimessenger_loop, 
				 0);
}

MPI_Request kick_req;

void mpimessenger_kick_loop()
{
  /*
  dout(10) << "kicking" << endl;
  MPI_Cancel(&recv_env_req);
  dout(10) << "kicked" << endl;
  return;
  */

  // wake up the event loop with a bad "message"
  //char stop = 0;               // a byte will do
  msg_envelope_t env;
  env.type = 0;
  dout(10) << "kicking" << endl;
  ASSERT(MPI_Isend(&env,
				   sizeof(env),
				   MPI_CHAR,
				   mpi_rank,
				   TAG_ENV,
				   MPI_COMM_WORLD,
				   &kick_req) == MPI_SUCCESS);
  dout(10) << "kicked" << endl;
}

void mpimessenger_stop()
{
  dout(5) << "mpimessenger_stop stopping thread" << endl;

  if (mpi_done) {
	dout(1) << "mpimessenger_stop called, but already done!" << endl;
	assert(!mpi_done);
  }

  // set finish flag
  mpi_done = true;

  mpimessenger_kick_loop();
  
  // wait for thread to stop
  mpimessenger_wait();
}

void mpimessenger_wait()
{
  void *returnval;
  pthread_join(thread_id, &returnval);
  dout(10) << "mpimessenger_wait thread finished." << endl;
}



/***********
 * MPIMessenger implementation
 */

MPIMessenger::MPIMessenger(msg_addr_t myaddr) : Messenger()
{
  // my address
  this->myaddr = myaddr;

  // register myself in the messenger directory
  directory[myaddr] = this;

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

MPIMessenger::~MPIMessenger()
{
  //delete logger;
}


int MPIMessenger::shutdown()
{
  // remove me from the directory
  directory.erase(myaddr);

  // last one?
  if (directory.empty()) {
	dout(10) << "last mpimessenger on rank " << mpi_rank << " shut down" << endl;
	pthread_t whoami = pthread_self();

	dout(15) << "whoami = " << whoami << ", thread = " << thread_id << endl;
	if (whoami == thread_id) {
	  // i am the event loop thread, just set flag!
	  dout(15) << "  set mpi_done=true" << endl;
	  mpi_done = true;
	} else {
	  // i am a different thread, tell the event loop to stop.
	  dout(15) << "  calling mpimessenger_stop()" << endl;
	  mpimessenger_stop();
	}
  }
}



/*** events
 */

void MPIMessenger::trigger_timer(Timer *t)
{
  assert(0); //implement me
}

/***
 * public messaging interface
 */

int MPIMessenger::send_message(Message *m, msg_addr_t dest, int port, int fromport)
{
  // set envelope
  m->set_source(myaddr, fromport);
  m->set_dest(dest, port);

  if (1) {
	// send in this thread
	mpi_send(m);
  } else {
	// queue up
	sender_lock.Lock();
	dout(10) << "send_message queueing up outgoing message " << *m << endl;
	outgoing.push_back(m);
	mpimessenger_kick_loop();
	sender_lock.Unlock();
  }
}



/*
Message *MPIMessenger::sendrecv(Message *m, msg_addr_t dest, int port)
{

  // no worky no more, use CheesySerializer
  assert(0);


  int fromport = 0;

  // set envelope
  m->set_source(myaddr, fromport);
  m->set_dest(dest, port);
  
  int rank = MPI_DEST_TO_RANK(dest, mpi_world);
  
  // get a tag to uniquely identify this procedure call
  int my_tag = get_thread_tag();
  m->set_pcid(my_tag);

  mpi_send(m, dest, TAG_UNSOLICITED);

  return mpi_recv();
}

*/

