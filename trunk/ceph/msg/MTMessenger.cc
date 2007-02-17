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


#include <pthread.h>
#include "mpi.h"

#include "include/config.h"
#include "include/error.h"
#include "Messenger.h"
#include "MTMessenger.h"

// This module uses MPI to implement a blocking sendrecv function that
// feels more like a procedure call and less like event processesing.
//
// Threads are not independently addressable in MPI, only processes
// are.  However, MPI does include a user defined tag in the message
// envelope, and a reader may selectively read only messages with a
// matching tag.  The modules assign an integer to each thread to use
// as the tag.
//

// our lock for any common data; it's okay to have only the one global mutex
// because our common data isn't a whole lot.
static pthread_mutex_t mutex;

// the key used to fetch the tag for the current thread.
pthread_key_t tag_key;

// the number of distinct threads we've seen so far; used to generate
// a unique tag for each thread.
static int nthreads;

// the MPI identity of this process
static int mpi_rank;


// get the tag for this thread
static int get_tag()
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


// marshall a message and send it over MPI
static void send(Message *m, int rank, int tag)
{
    // marshall the message
    crope r;
    m->encode(r);
    int size = r.length();

    char *buf = (char*)r.c_str();
    ASSERT(MPI_Send(buf,
            size,
            MPI_CHAR,
            rank,
            tag,
            MPI_COMM_WORLD) == MPI_SUCCESS);
}

// read a message from MPI and unmarshall it
static Message *receive(int tag)
{
    MPI_Status status;

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

    // unmarshall message
    crope r(buf, status.count);
    delete[] buf;
    Message *m = decode_message(r);

    return m;
}

MTMessenger::MTMessenger(int& argc, char**& argv)
{
    // setup MPI; MPI errors will probably invoke the default MPI error
    // handler, which aborts the program with a friendly message rather
    // than returning from a function; just in case, we abort the
    // program if we get an MPI error.

    int provided;
    ASSERT(MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided)
       == MPI_SUCCESS);

    ASSERT(MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank) == MPI_SUCCESS);

    if (pthread_mutex_init(&mutex, NULL) < 0)
    SYSERROR();

    if (pthread_key_create(&tag_key, NULL) < 0)
    SYSERROR();

    nthreads = 0;
}

MTMessenger::~MTMessenger()
{
    // ignore shutdown errors

    pthread_key_delete(tag_key);

    pthread_mutex_destroy(&mutex);

    MPI_Finalize();
}

// send a request and wait for the response
Message *MTMessenger::sendrecv(Message *m, entity_name_t dest)
{
    int dest_tag = 0;        // servers listen for any tag
    int my_tag = get_tag();

    // set our envelope (not to be confused with the MPI envelope)
    m->set_source(mpi_rank, my_tag);
    m->set_dest(dest, dest_tag);

    send(m, dest, dest_tag);

    return receive(my_tag);
}

// receive a request from anyone
Message *MTMessenger::recvreq()
{
    return receive(MPI_ANY_TAG);
}

// forward request, masquerading as original source
void MTMessenger::fwdreq(Message *req, int dest)
{
    int dest_tag = 0;        // servers listen for any tag

    // set our envelope (not to be confused with the MPI envelope)
    req->set_dest(dest, dest_tag);

    send(req, dest, dest_tag);
}

// send a response to the originator of the request
void MTMessenger::sendresp(Message *req, Message *resp)
{
    int req_rank = req->get_source();
    int req_tag = req->get_source_port();
    int my_tag = get_tag();

    // set our envelope (not to be confused with the MPI envelope)
    resp->set_source(mpi_rank, my_tag);
    resp->set_dest(req_rank, req_tag);

    send(resp, req_rank, req_tag);
}
