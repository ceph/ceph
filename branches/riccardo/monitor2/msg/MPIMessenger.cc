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
list<Message*>                outgoing, incoming;
list<MPI_Request*>            unfinished_sends;
map<MPI_Request*, Message*>   unfinished_send_message;

/* this process */
int mpi_world;
int mpi_rank;
bool mpi_done = false;     // set this flag to stop the event loop


#define FUNNEL_MPI         // if we want to funnel mpi through a single thread
#define TAG_UNSOLICITED 0
#define DBLVL 18

// the key used to fetch the tag for the current thread.
pthread_key_t tag_key;
pthread_t thread_id = 0;   // thread id of the event loop.  init value == nobody

Mutex sender_lock;
Mutex out_queue_lock;

bool pending_timer;


// our lock for any common data; it's okay to have only the one global mutex
// because our common data isn't a whole lot.
//static pthread_mutex_t mutex;

// the number of distinct threads we've seen so far; used to generate
// a unique tag for each thread.
//static int nthreads = 10;

//#define TAG_UNSOLICITED 0

// debug
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "[MPI " << mpi_rank << "/" << mpi_world << " " << getpid() << "." << pthread_self() << "] "



/*****
 * MPI global methods for process-wide startup, shutdown.
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
  dout(5) << "mpimessenger_shutdown barrier waiting for all to finish" << endl;
  MPI_Barrier (MPI_COMM_WORLD);
  dout(1) << "mpimessenger_shutdown all done, MPI_Finalize()" << endl;
  MPI_Finalize();
  return 0;
}

int mpimessenger_world()
{
  return mpi_world;
}



/***
 * internal send/recv
 */


/*
 * get fresh MPI_Request* (on heap) for a new async MPI_Isend
 */

MPI_Request *mpi_prep_send_req() {
  MPI_Request *req = new MPI_Request;
  unfinished_sends.push_back(req);
  dout(DBLVL) << "prep_send_req " << req << endl;
  return req;
}


/*
 * clean up MPI_Request*'s for Isends that have completed.
 * also, hose any associated Message*'s for Messages that are completely sent.
 *
 * if wait=true, block and wait for sends to finish.
 */

void mpi_reap_sends(bool wait=false) {
  sender_lock.Lock();

  list<MPI_Request*>::iterator it = unfinished_sends.begin();
  while (it != unfinished_sends.end()) {
    MPI_Status status;
    int flag;
    
    if (wait) {
      MPI_Wait(*it, &status);
    } else {
      MPI_Test(*it, &flag, &status);
      if (!flag) break;   // not finished yet
    }

    dout(DBLVL) << "send " << *it << " completed" << endl;

    if (unfinished_send_message.count(*it)) {
      dout(DBLVL) << "send message " << unfinished_send_message[*it] << " completed" << endl;
      delete unfinished_send_message[*it];
      unfinished_send_message.erase(*it);
    }

    delete *it;
    it++;
    unfinished_sends.pop_front();
  }

  dout(DBLVL) << "reap has " << unfinished_sends.size() << " Isends outstanding, " << unfinished_send_message.size() << " messages" << endl;

  sender_lock.Unlock();
}


void mpi_finish_sends() {
  mpi_reap_sends(true);
}


/*
 * recv a Message*
 */
Message *mpi_recv(int tag)
{
  // envelope
  dout(DBLVL) << "mpi_recv waiting for message tag " << tag  << endl;

  MPI_Status status;
  msg_envelope_t env;
  
  ASSERT(MPI_Recv((void*)&env,
                  sizeof(env),
                  MPI_CHAR, 
                  MPI_ANY_SOURCE,// status.MPI_SOURCE,//MPI_ANY_SOURCE,
                  tag,
                  MPI_COMM_WORLD,
                  &status/*,
                           &recv_env_req*/) == MPI_SUCCESS);
  assert(status.count == MSG_ENVELOPE_LEN);

  if (env.type == 0) {
    dout(DBLVL) << "mpi_recv got type 0 message, kicked!" << endl;
    return 0;
  }

  dout(DBLVL) << "mpi_recv got envelope " << status.count << ", type=" << env.type << " src " << env.source << " dst " << env.dest << " nchunks=" << env.nchunks << " from " << status.MPI_SOURCE << endl;

  // payload
  bufferlist blist;
  for (int i=0; i<env.nchunks; i++) {
    MPI_Status fragstatus;
    ASSERT(MPI_Probe(status.MPI_SOURCE,
                     tag,
                     MPI_COMM_WORLD,
                     &fragstatus) == MPI_SUCCESS);

    bufferptr bp = new buffer(fragstatus.count);
    
    ASSERT(MPI_Recv(bp.c_str(),
                    fragstatus.count,
                    MPI_CHAR, 
                    status.MPI_SOURCE,
                    tag,
                    MPI_COMM_WORLD,
                    &fragstatus) == MPI_SUCCESS);

    blist.push_back(bp);

    dout(DBLVL) << "mpi_recv got frag " << i << " of " << env.nchunks << " len " << fragstatus.count << endl;
  }
  
  dout(DBLVL) << "mpi_recv got " << blist.length() << " byte message tag " << status.MPI_TAG << endl;

  // unmarshall message
  Message *m = decode_message(env, blist);
  return m;
}


/*
 * send a Message* over the wire.  ** do not block **.
 */
int mpi_send(Message *m, int tag)
{
  int rank = MPI_DEST_TO_RANK(m->get_dest(), mpi_world);

  // local?
  if (rank == mpi_rank) {      
    dout(DBLVL) << "queuing local delivery" << endl;
    incoming.push_back(m);
    return 0;
  } 

  // marshall
  if (m->empty_payload())
    m->encode_payload();
  msg_envelope_t *env = &m->get_envelope();
  env->nchunks = m->get_payload().buffers().size();

  dout(7) << "sending " << *m << " to " << MSG_ADDR_NICE(env->dest) << " (rank " << rank << ")" << endl;

#ifndef FUNNEL_MPI
  sender_lock.Lock();
#endif

  // send envelope
  ASSERT(MPI_Isend((void*)env,
                   sizeof(*env),
                   MPI_CHAR,
                   rank,
                   tag,
                   MPI_COMM_WORLD,
                   mpi_prep_send_req()) == MPI_SUCCESS);

  // payload
  int i = 0;
  for (list<bufferptr>::iterator it = m->get_payload().buffers().begin();
       it != m->get_payload().buffers().end();
       it++) {
    dout(DBLVL) << "mpi_sending frag " << i << " len " << (*it).length() << endl;
    //MPI_Request *req = new MPI_Request;
    ASSERT(MPI_Isend((void*)(*it).c_str(),
                     (*it).length(),
                     MPI_CHAR,
                     rank,
                     tag,
                     MPI_COMM_WORLD,
                     mpi_prep_send_req()) == MPI_SUCCESS);
    i++;
  }

  // attach message to last send, so we can free it later
  MPI_Request *req = unfinished_sends.back();
  unfinished_send_message[req] = m;
  
  dout(DBLVL) << "mpi_send done, attached message to Isend " << req << endl;

#ifndef FUNNEL_MPI
  sender_lock.Unlock();
#endif
  return 0;
}



// get the tag for this thread

#ifndef FUNNEL_MPI
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
#endif



// recv event loop, for unsolicited messages.

void* mpimessenger_loop(void*)
{
  dout(5) << "mpimessenger_loop start pid " << getpid() << endl;

  while (1) {

    // outgoing
    mpi_reap_sends();
    
#ifdef FUNNEL_MPI
    // check outgoing queue
    out_queue_lock.Lock();
    if (outgoing.size()) {
      dout(10) << outgoing.size() << " outgoing messages" << endl;
      for (list<Message*>::iterator it = outgoing.begin();
           it != outgoing.end();
           it++) {
        mpi_send(*it, TAG_UNSOLICITED);
      }
    }
    outgoing.clear();
    out_queue_lock.Unlock();
#endif


    // timer events?
    if (pending_timer) {
      dout(DBLVL) << "pending timer" << endl;
      g_timer.execute_pending();
    }

    // done?
    if (mpi_done &&
        incoming.empty() &&
        outgoing.empty() &&
        !pending_timer) break;


    // incoming
    Message *m = 0;

    if (incoming.size()) {
      dout(12) << "loop pulling message off incoming" << endl;
      m = incoming.front();
      incoming.pop_front();
    } 
    else {
      // check mpi
      dout(12) << "loop waiting for incoming messages" << endl;

      // get message
      m = mpi_recv(TAG_UNSOLICITED);
    }

    // dispatch?
    if (m) {
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
        break;
      }
    }

  }

  dout(5) << "finishing async sends" << endl;
  mpi_finish_sends();

  g_timer.shutdown();

  dout(5) << "mpimessenger_loop exiting loop" << endl;
  return 0;
}


// start/stop mpi receiver thread (for unsolicited messages)
int mpimessenger_start()
{
  dout(5) << "starting thread" << endl;
  
  // start a thread
  pthread_create(&thread_id, 
                 NULL, 
                 mpimessenger_loop, 
                 0);
  return 0;
}


/*
 * kick and wake up _loop (to pick up new outgoing message, or quit)
 */

MPI_Request    kick_req;
msg_envelope_t kick_env;

void mpimessenger_kick_loop()
{
  // if we're same thread as the loop, no kicking necessary
  if (pthread_self() == thread_id) return;   

  kick_env.type = 0;

  sender_lock.Lock();
  ASSERT(MPI_Isend(&kick_env,               // kick sync for now, but ONLY because it makes me feel safer.
                   sizeof(kick_env),
                   MPI_CHAR,
                   mpi_rank,
                   TAG_UNSOLICITED,
                   MPI_COMM_WORLD,
                   mpi_prep_send_req()) == MPI_SUCCESS);
  sender_lock.Unlock();
}


// stop thread

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


// wait for thread to finish

void mpimessenger_wait()
{
  void *returnval;
  dout(10) << "mpimessenger_wait waiting for thread to finished." << endl;
  pthread_join(thread_id, &returnval);
  dout(10) << "mpimessenger_wait thread finished." << endl;
}




/***********
 * MPIMessenger class implementation
 */

class C_MPIKicker : public Context {
  void finish(int r) {
    dout(DBLVL) << "timer kick" << endl;
    mpimessenger_kick_loop();
  }
};

MPIMessenger::MPIMessenger(entity_name_t myaddr) : Messenger(myaddr)
{
  // my address
  this->myaddr = myaddr;

  // register myself in the messenger directory
  directory[myaddr] = this;

  // register to execute timer events
  g_timer.set_messenger_kicker(new C_MPIKicker());

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

  // no more timer events
  g_timer.unset_messenger_kicker();

  // last one?
  if (directory.empty()) {
    dout(10) << "shutdown last mpimessenger on rank " << mpi_rank << " shut down" << endl;
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
  } else {
    dout(10) << "shutdown still " << directory.size() << " other messengers on rank " << mpi_rank << endl;
  }
  return 0;
}




/***
 * public messaging interface
 */


/* note: send_message _MUST_ be non-blocking */
int MPIMessenger::send_message(Message *m, entity_name_t dest, int port, int fromport)
{
  // set envelope
  m->set_source(myaddr, fromport);
  m->set_dest(dest, port);

#ifdef FUNNEL_MPI

  // queue up
  out_queue_lock.Lock();
  dout(DBLVL) << "queuing outgoing message " << *m << endl;
  outgoing.push_back(m);
  out_queue_lock.Unlock();

  mpimessenger_kick_loop();
  
#else

  // send in this thread
  mpi_send(m, m->get_pcid());

#endif
  return 0;
}






