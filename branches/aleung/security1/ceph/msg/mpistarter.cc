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



#include <mpi.h>

#include "TCPMessenger.h"

/*
 * start up TCPMessenger via MPI.
 */ 

pair<int,int> mpi_bootstrap_tcp(int& argc, char**& argv)
{
  tcpmessenger_init();
  tcpmessenger_start();

  // exchnage addresses with other nodes
  MPI_Init(&argc, &argv);
  
  int mpi_world;
  int mpi_rank;
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_world);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

  //dout(1) << "i am " << mpi_rank << " of " << mpi_world << endl;
  
  // start up directory?
  tcpaddr_t ta;
  if (mpi_rank == 0) {
    dout(30) << "i am rank 0, starting ns directory" << endl;
    tcpmessenger_start_nameserver(ta);
  } else {
    memset(&ta, 0, sizeof(ta));
  }

  // distribute tcpaddr
  int r = MPI_Bcast(&ta, sizeof(ta), MPI_CHAR,
                    0, MPI_COMM_WORLD);

  dout(30) << "r = " << r << " ns tcpaddr is " << ta << endl;
  tcpmessenger_start_rankserver(ta);
  
  MPI_Barrier(MPI_COMM_WORLD);
  //g_clock.tare();
  MPI_Finalize();

  return pair<int,int>(mpi_rank, mpi_world);
}


