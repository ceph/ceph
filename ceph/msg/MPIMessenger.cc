
#include "include/config.h"

#include "include/MPIMessenger.h"
#include "include/Message.h"

#include <iostream>
#include <cassert>
#include <ext/hash_map>
using namespace std;
#include <unistd.h>

#include "mpi.h"

#include "include/LogType.h"
#include "include/Logger.h"

LogType mpimsg_logtype;
hash_map<int, Logger*>        loggers;
hash_map<int, MPIMessenger*>  directory;

hash_map<int, Message*>       incoming;

int mpi_world_size;
int mpi_rank;

bool mpi_done = false;


#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "[MPI " << mpi_rank << "/" << mpi_world_size << "] "


int mpimessenger_init(int& argc, char**& argv)
{
  //MPI::Init(argc, argv);
  MPI_Init(&argc, &argv);

  MPI_Comm_size(MPI_COMM_WORLD, &mpi_world_size);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

  char hostname[100];
  gethostname(hostname,100);
  int pid = getpid();

  dout(1) << "init: i am " << hostname << " pid " << pid << endl;
  
  assert(mpi_world_size > g_conf.num_osd+g_conf.num_mds);

  return mpi_rank;
}

int mpimessenger_world()
{
  return mpi_world_size;
}


int mpimessenger_loop()
{
  while (!mpi_done) {
	// check mpi
	dout(12) << "waiting for message" << endl;

	// get size
	//MPI::Status status;
	MPI_Status status;
	int msize = 0;
	//MPI::COMM_WORLD.Recv(&msize, 
	MPI_Recv(&msize, 
			 1,
			 MPI_INT, 
			 MPI_ANY_SOURCE, 
			 MPI_ANY_TAG, 
			 MPI_COMM_WORLD,
			 &status); // receives greeting from each process

	if (msize == -1) {
	  dout(1) << "got -1 terminate signal" << endl;
	  mpi_done = true;
	  break;
	}

	int tag = status.MPI_TAG;
	int source = status.MPI_SOURCE;
	dout(12) << "incoming size " << msize << " tag " << tag << " from rank " << source << endl;

	// get message
	char *buf = new char[msize];
	//MPI::COMM_WORLD.Recv(buf, 
	MPI_Recv(buf,
			 msize,
			 MPI_CHAR, 
			 status.MPI_SOURCE,
			 tag,
			 MPI_COMM_WORLD,
			 &status); // receives greeting from each process

	crope r(buf, msize);
	delete[] buf;
	
	// decode message
	Message *m = decode_message(r);

	if (directory.count(tag)) {
	  Messenger *who = directory[ tag ];

	  dout(3) << "---- do_loop dispatching '" << m->get_type_name() << 
		"' from " << MSG_ADDR_NICE(m->get_source()) << ':' << m->get_source_port() <<
		" to " << MSG_ADDR_NICE(m->get_dest()) << ':' << m->get_dest_port() << " ---- " << m 
			  << endl;
	  
	  who->dispatch(m);
	} else {
	  dout (1) << "---- i don't know who " << tag << " is." << endl;
	  break;
	}
  }

  dout(1) << "waiting for all to finish" << endl;
  MPI_Barrier (MPI_COMM_WORLD);
}

int mpimessenger_shutdown()
{
  dout(1) << "MPI_Finalize" << endl;
  MPI_Finalize();
}


void MPIMessenger::done() 
{
  dout(1) << "done()" << endl;
  mpi_done = true;
  
  // tell everyone
  for (int i=0; i<mpi_world_size; i++) {
	if (i == mpi_rank) continue;
	int m = -1;
	
	dout(12) << "done() telling rank " << i << endl;
	MPI_Send(&m,
			 1,
			 MPI_INT,
			 i,
			 0, 
			 MPI_COMM_WORLD);
  }
}

MPIMessenger::MPIMessenger(long me)
{
  whoami = me;
  directory[ me ] = this;

  // logger
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
}


int MPIMessenger::init(Dispatcher *d)
{
  set_dispatcher(d);
}

int MPIMessenger::shutdown()
{
  directory.erase(whoami);
  remove_dispatcher();
}

int MPIMessenger::send_message(Message *m, msg_addr_t dest, int port, int fromport)
{
  // set envelope
  m->set_source(whoami, fromport);
  m->set_dest(dest, port);

  // send!
  int trank = MPI_DEST_TO_RANK(dest,mpi_world_size);
  crope r;
  m->encode(r);
  int size = r.length();

  if (trank == mpi_rank) {	

	dout(3) << "queueing message locally for (tag) " << dest << " at my rank " << trank << " size " << size << endl;
	//incoming.insert(dest, m);
	// no implemented
	assert(0);

  } 

  dout(10) << "sending message via MPI for (tag) " << dest << " to rank " << trank << " size " << size << endl;
  
  //MPI::COMM_WORLD.Send(&r,
  MPI_Send(&size,
		   1,
		   MPI_INT,
		   trank,
		   dest, 
		   MPI_COMM_WORLD);
  
  char *buf = (char*)r.c_str();
  //MPI::COMM_WORLD.Send(buf,
  MPI_Send(buf,
		   size,
		   MPI_CHAR,
		   trank,
		   dest,
		   MPI_COMM_WORLD);
}

int MPIMessenger::wait_message(time_t seconds)
{
}

int MPIMessenger::loop() 
{
  // this only better be called once or we'll overflow the stack or something dumb.
  mpimessenger_loop();
}

