
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
  MPI_Finalize();

  return pair<int,int>(mpi_rank, mpi_world);
}
