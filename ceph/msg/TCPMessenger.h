#ifndef __TCPMESSENGER_H
#define __TCPMESSENGER_H

#include "Messenger.h"
#include "Dispatcher.h"

#define NUMMDS g_conf.num_mds
#define NUMOSD g_conf.num_osd
#define MPI_DEST_TO_RANK(dest,world)    ((dest)<(NUMMDS+NUMOSD) ? \
										 (dest) : \
										 ((NUMMDS+NUMOSD)+(((dest)-NUMMDS-NUMOSD) % ((world)-NUMMDS-NUMOSD))))

class Timer;

class TCPMessenger : public Messenger {
 protected:
  msg_addr_t myaddr;     // my address
  
  //class Logger *logger;  // for logging
  
 public:
  TCPMessenger(msg_addr_t myaddr);
  ~TCPMessenger();

  // init, shutdown MPI and associated event loop thread.
  virtual int shutdown();

  // message interface
  virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0);
  virtual Message* sendrecv(Message *m, msg_addr_t dest, int port=0) { assert(0); }

  // events
  virtual void trigger_timer(Timer *t);
};

/**
 * these are all ONE per process.
 */

extern int tcpmessenger_world();
extern int tcpmessenger_init(int& argc, char**& argv);   // init mpi
extern int tcpmessenger_start();   // start thread
extern void tcpmessenger_wait();    // wait for thread to finish.
extern int tcpmessenger_shutdown();   // finalize MPI


#endif
