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


#ifndef __MPIMESSENGER_H
#define __MPIMESSENGER_H

#include "Messenger.h"
#include "Dispatcher.h"

#define NUMMDS g_conf.num_mds
#define NUMOSD g_conf.num_osd
#define MPI_DEST_TO_RANK(dest,world)    ((dest)<(NUMMDS+NUMOSD) ? \
                                         (dest) : \
                                         ((NUMMDS+NUMOSD)+(((dest)-NUMMDS-NUMOSD) % ((world)-NUMMDS-NUMOSD))))

class Timer;

class MPIMessenger : public Messenger {
 protected:
  entity_name_t myaddr;     // my address
  //class Logger *logger;  // for logging
  
 public:
  MPIMessenger(entity_name_t myaddr);
  ~MPIMessenger();

  // init, shutdown MPI and associated event loop thread.
  virtual int shutdown();

  // message interface
  virtual int send_message(Message *m, entity_name_t dest, int port=0, int fromport=0);
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
