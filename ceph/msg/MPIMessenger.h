#ifndef __MPIMESSENGER_H
#define __MPIMESSENGER_H

#include "Messenger.h"
#include "Dispatcher.h"

#define NUMMDS g_conf.num_mds
#define NUMOSD g_conf.num_osd
#define MPI_DEST_TO_RANK(dest,world)    ((dest)<(NUMMDS+NUMOSD) ? \
										 (dest) : \
										 ((NUMMDS+NUMOSD)+(((dest)-NUMMDS-NUMOSD) % ((world)-NUMMDS-NUMOSD))))


class MPIMessenger : public Messenger {
 protected:
  msg_addr_t myaddr;     // my address
  //class Logger *logger;  // for logging
  
 public:
  MPIMessenger(msg_addr_t myaddr);
  ~MPIMessenger();

  // init, shutdown MPI and associated event loop thread.
  virtual int shutdown();

  // message interface
  virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0);
  virtual Message* sendrecv(Message *m, msg_addr_t dest, int port=0);
};

/**
 * these are all ONE per process.
 */
extern int mpimessenger_world();   // get world size
extern int mpimessenger_init(int& argc, char**& argv);   // init mpi
extern int mpimessenger_start();   // start thread
extern void mpimessenger_stop();    // stop thread.
extern void mpimessenger_wait();    // wait for thread to finish.
extern int mpimessenger_shutdown();   // finalize MPI


#endif
