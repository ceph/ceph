

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "mds/MDCluster.h"
#include "mds/MDS.h"
#include "osd/OSD.h"
#include "fakeclient/FakeClient.h"

#include "mds/MDCache.h"
#include "mds/MDStore.h"

#include "msg/MPIMessenger.h"
//#include "msg/CheesySerializer.h"

#include "messages/MPing.h"


__uint64_t ino = 1;



#include "config.h"
#define NUMMDS g_conf.num_mds
#define NUMOSD g_conf.num_osd
#define NUMCLIENT g_conf.num_client

// this parses find output
int play();

int main(int argc, char **argv) {
  cout << "mpitest starting" << endl;

  int myrank = mpimessenger_init(argc, argv);
  int world = mpimessenger_world();



  MDCluster *mdc = new MDCluster(NUMMDS, NUMOSD);
  
  // create osds
  OSD *osd[NUMOSD];
  for (int i=0; i<NUMOSD; i++) {
    if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_OSD(i),world)) continue;
    osd[i] = new OSD(i, new MPIMessenger(MSG_ADDR_OSD(i)));
    osd[i]->init();
  }
  
  // create mds
  MDS *mds[NUMMDS];
  for (int i=0; i<NUMMDS; i++) {
    if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_MDS(i),world)) continue;
    mds[i] = new MDS(mdc, i, new MPIMessenger(MSG_ADDR_MDS(i)));
    mds[i]->init();
  }
  
  // create clients
  FakeClient *client[NUMCLIENT];
  for (int i=0; i<NUMCLIENT; i++) {
    if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_CLIENT(i),world)) continue;

    MPIMessenger *real = new MPIMessenger(MSG_ADDR_CLIENT(i));
    CheesySerializer *serializer = new CheesySerializer(real);
    real->set_dispatcher(serializer);   

    client[i] = new FakeClient(mdc, i, real, g_conf.fakeclient_requests);
    client[i]->init();
  }
  
  // seed initial requests
  for (int i=0; i<NUMCLIENT; i++) {
    if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_CLIENT(i),world)) continue;
    client[i]->issue_request();
  }

  mpimessenger_start();     // start message loop
  mpimessenger_wait();      // wait for thread to finish
  mpimessenger_shutdown();  // shutdown MPI

  // 
  /*
  cout << "---- check ----" << endl;
  for (int i=0; i<NUMMDS; i++) {
    if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_MDS(i),world)) continue;
    mds[i]->mdcache->shutdown_pass();
  }
  */

  // cleanup
  //cout << "cleanup" << endl;
  for (int i=0; i<NUMMDS; i++) {
    if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_MDS(i),world)) continue;
    delete mds[i];
  }
  for (int i=0; i<NUMOSD; i++) {
    if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_OSD(i),world)) continue;
    delete osd[i];
  }
  for (int i=0; i<NUMCLIENT; i++) {
    if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_CLIENT(i),world)) continue;
    delete client[i];
  }
  delete mdc;
  
  //cout << "done." << endl;
  return 0;
}

