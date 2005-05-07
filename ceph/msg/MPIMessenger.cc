
#include "include/config.h"
#include "include/error.h"

#include "common/Timer.h"

#include "MPIMessenger.h"
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

/* this process */
int mpi_world;
int mpi_rank;
bool mpi_done = false;     // set this flag to stop the event loop


// the key used to fetch the tag for the current thread.
pthread_key_t tag_key;
pthread_t thread_id = 0;   // thread id of the event loop.  init value == nobody

// our lock for any common data; it's okay to have only the one global mutex
// because our common data isn't a whole lot.
static pthread_mutex_t mutex;

// the number of distinct threads we've seen so far; used to generate
// a unique tag for each thread.
static int nthreads;

#define TAG_UNSOLICITED 0

// debug
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "[MPI " << mpi_rank << "/" << mpi_world << " " << getpid() << "] "



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

Message *mpi_recv(int tag)
{
  MPI_Status status;
  
  dout(10) << "mpi_recv waiting for message on tag " << tag << endl;

  // get message size
  ASSERT(MPI_Probe(MPI_ANY_SOURCE, 
				   tag,
				   MPI_COMM_WORLD,
				   &status) == MPI_SUCCESS);
  
  // get message; there may be multiple messages on the queue, we
  // need to be sure to read the one which corresponds to size
  // obtained above.
  char *buf = new char[status.count];
  ASSERT(MPI_Recv(buf,
				  status.count,
				  MPI_CHAR, 
				  status.MPI_SOURCE,
				  status.MPI_TAG,
				  MPI_COMM_WORLD,
				  &status) == MPI_SUCCESS);

  if (status.count < 4) {
	dout(10) << "mpi_recv got short recv " << status.count << " bytes" << endl;
	return 0;
  }

  dout(10) << "mpi_recv got " << status.count << " byte message tag " << status.MPI_TAG << endl;
  
  // unmarshall message
  crope r(buf, status.count);
  delete[] buf;
  Message *m = decode_message(r);
  
  return m;
}

int mpi_send(Message *m, int rank, int tag)
{
  if (rank == mpi_rank) {      
	dout(1) << "local delivery not implemented" << endl;
	assert(0);
  } 

  // marshall
  crope r;
  m->encode(r);
  int size = r.length();
  const char *buf = r.c_str();
  
  dout(10) << "mpi_sending " << size << " byte message to rank " << rank << " tag " << tag << endl;

  // sending
  ASSERT(MPI_Send((void*)buf,
				  size,
				  MPI_CHAR,
				  rank,
				  tag,
				  MPI_COMM_WORLD) == MPI_SUCCESS);
}



// get the tag for this thread
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




// recv event loop, for unsolicited messages.

void* mpimessenger_loop(void*)
{
  dout(1) << "mpimessenger_loop start pid " << getpid() << endl;

  while (!mpi_done) {
	// check mpi
	dout(12) << "mpimessenger_loop waiting for (unsolicited) messages" << endl;

	// get message
	Message *m = mpi_recv(TAG_UNSOLICITED);
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

void mpimessenger_stop()
{
  dout(5) << "mpimessenger_stop stopping thread" << endl;

  if (mpi_done) {
	dout(1) << "mpimessenger_stop called, but already done!" << endl;
	assert(!mpi_done);
  }

  // set finish flag
  mpi_done = true;

  // wake up the event loop with a bad "message"
  char stop = 0;               // a byte will do
  ASSERT(MPI_Send(&stop,
				  1,
				  MPI_CHAR,
				  mpi_rank,
				  TAG_UNSOLICITED,
				  MPI_COMM_WORLD) == MPI_SUCCESS);
  
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

  int rank = MPI_DEST_TO_RANK(dest, mpi_world);

  mpi_send(m, rank, m->get_pcid());
}

Message *MPIMessenger::sendrecv(Message *m, msg_addr_t dest, int port)
{
  int fromport = 0;

  // set envelope
  m->set_source(myaddr, fromport);
  m->set_dest(dest, port);
  
  int rank = MPI_DEST_TO_RANK(dest, mpi_world);
  
  // get a tag to uniquely identify this procedure call
  int my_tag = get_thread_tag();
  m->set_pcid(my_tag);

  mpi_send(m, dest, TAG_UNSOLICITED);

  return mpi_recv(my_tag);
}



