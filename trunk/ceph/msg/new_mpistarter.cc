#include <mpi.h>
#include "NewMessenger.h"

/*
 * start up NewMessenger via MPI.
 */ 

pair<int,int> mpi_bootstrap_new(int& argc, char**& argv)
{
  MPI_Init(&argc, &argv);
  
  int mpi_world;
  int mpi_rank;
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_world);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

  tcpaddr_t nsaddr;
  memset(&nsaddr, 0, sizeof(nsaddr));
  
  if (mpi_rank == 0) {
    // i am root.
    rank.my_rank = 0;  
    rank.start_rank(nsaddr);
    nsaddr = rank.get_listen_addr();
  }

  int r = MPI_Bcast(&nsaddr, sizeof(nsaddr), MPI_CHAR,
                    0, MPI_COMM_WORLD);

  dout(30) << "r = " << r << " ns tcpaddr is " << nsaddr << endl;

  if (mpi_rank != 0) {
    rank.start_rank(nsaddr);
  }

  MPI_Barrier(MPI_COMM_WORLD);

  //g_clock.tare();

  MPI_Finalize();

  return pair<int,int>(mpi_rank, mpi_world);
}
