
#include "include/config.h"

#include "include/MPIMessenger.h"
#include "include/Message.h"

#include <iostream>
#include <cassert>
#include <ext/hash_map>
using namespace std;

#include "mpi++.h"

#include "include/LogType.h"
#include "include/Logger.h"

LogType mpimsg_logtype;
hash_map<int, Logger*>        loggers;
hash_map<int, MPIMessenger*>  directory;

hash_map<int, Message*>       incoming;

#define  dout(l)    if (l<=DEBUG_LEVEL) cout << "mpi "
#define  dout2(l)    if (1<=DEBUG_LEVEL) cout


int mpi_world_size;
int mpi_rank;

int mpimessenger_init(int& argc, char**& argv)
{
  dout(1) << "MPI_Init" << endl;
  MPI::Init(argc, argv);
  mpi_world_size = MPI::COMM_WORLD.Get_size();
  mpi_rank = MPI::COMM_WORLD.Get_rank();

  dout(1) << "i am " << mpi_rank << " of " << mpi_world_size << endl;
  
  assert(mpi_world_size > NUMOSD+NUMMDS);

  return mpi_rank;
}

int mpimessenger_world()
{
  return mpi_world_size;
}


int mpimessenger_loop()
{
  while (1) {
	// check mpi
	dout(1) << "waiting for message" << endl;

	// get size
	MPI::Status status;
	int msize;
	MPI::COMM_WORLD.Recv(&msize, 
						 1,
						 MPI_INT, 
						 MPI_ANY_SOURCE, 
						 MPI_ANY_TAG, 
						 status); // receives greeting from each process

	int tag = status.Get_tag();
	int source = status.Get_source();
	dout(1) << "incoming size " << msize << " tag " << tag << " from rank " << source << endl;

	// get message
	char *buf = new char[msize];
	MPI::COMM_WORLD.Recv(buf, 
						 msize,
						 MPI_CHAR, 
						 status.Get_source(),
						 tag,
						 status); // receives greeting from each process

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
}

int mpimessenger_shutdown()
{
  dout(1) << "MPI_Finalize" << endl;
  MPI::Finalize();
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

int MPIMessenger::send_message(Message *m, long dest, int port, int fromport)
{
  int trank = MPI_DEST_TO_RANK(dest,mpi_world_size);
  crope r = m->get_serialized();
  int size = r.length();

  if (trank == mpi_rank) {	

	dout(3) << "queueing message locally for (tag) " << dest << " at my rank " << trank << " size " << size << endl;
	//incoming.insert(dest, m);
	// no implemented
	assert(0);

  } else {
	dout(3) << "sending message via MPI for (tag) " << dest << " to rank " << trank << " size " << size << endl;

	MPI::COMM_WORLD.Send(&r,
						 1,
						 MPI_INT,
						 trank,
						 dest);
	
	const char *buf = r.c_str();
	MPI::COMM_WORLD.Send(buf,
						 size,
						 MPI_CHAR,
						 trank,
						 dest);
  }
}
int MPIMessenger::wait_message(time_t seconds)
{
}

int MPIMessenger::loop() 
{
  // this only better be called once or we'll overflow the stack or something dumb.
  mpimessenger_loop();
}

