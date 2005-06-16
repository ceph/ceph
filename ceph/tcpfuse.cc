

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "include/config.h"

#include "mds/MDCluster.h"
#include "mds/MDS.h"
#include "osd/OSD.h"
#include "client/Client.h"
#include "client/fuse.h"

#include "msg/TCPMessenger.h"
#include "msg/CheesySerializer.h"

#include "common/Timer.h"

#define NUMMDS g_conf.num_mds
#define NUMOSD g_conf.num_osd
#define NUMCLIENT g_conf.num_client

class C_Test : public Context {
public:
  void finish(int r) {
	cout << "C_Test->finish(" << r << ")" << endl;
  }
};


int main(int oargc, char **oargv) {

  //cerr << "tcpfuse starting " << myrank << "/" << world << endl;
  int argc;
  char **argv;
  parse_config_options(oargc, oargv,
					   argc, argv);

  int start = 0;

  // build new argc+argv for fuse
  typedef char* pchar;
  int nargc = 0;
  char **nargv = new pchar[argc];
  nargv[nargc++] = argv[0];

  int mkfs = 0;
  for (int i=1; i<argc; i++) {
	if (strcmp(argv[i], "--fastmkfs") == 0) {
	  mkfs = MDS_MKFS_FAST;
	}
	else if (strcmp(argv[i], "--fullmkfs") == 0) {
	  mkfs = MDS_MKFS_FULL;
	}
	else {
	  // unknown arg, pass it on.
	  nargv[nargc++] = argv[i];
	}
  }

  int myrank = tcpmessenger_init(argc, argv);
  int world = tcpmessenger_world();

  assert(NUMMDS + NUMOSD + NUMCLIENT <= world);

  MDCluster *mdc = new MDCluster(NUMMDS, NUMOSD);


  char hostname[100];
  gethostname(hostname,100);
  int pid = getpid();

  // create mds
  MDS *mds[NUMMDS];
  for (int i=0; i<NUMMDS; i++) {
	if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_MDS(i),world)) continue;
	cerr << "mds" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
	mds[i] = new MDS(mdc, i, new TCPMessenger(MSG_ADDR_MDS(i)));
	start++;
  }
  
  // create osd
  OSD *osd[NUMOSD];
  for (int i=0; i<NUMOSD; i++) {
	if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_OSD(i),world)) continue;
	cerr << "osd" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
	osd[i] = new OSD(i, new TCPMessenger(MSG_ADDR_OSD(i)));
	start++;
  }
  
  // create client
  Client *client[NUMCLIENT];
  for (int i=0; i<NUMCLIENT; i++) {
	if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_CLIENT(i),world)) continue;
	cerr << "client" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
	CheesySerializer *serializer = new CheesySerializer( new TCPMessenger(MSG_ADDR_CLIENT(i)) );
	client[i] = new Client(mdc, i, serializer);
	start++;
  }


  // start message loop
  if (start) {
	tcpmessenger_start();
    
	// init
	for (int i=0; i<NUMMDS; i++) {
	  if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_MDS(i),world)) continue;
	  mds[i]->init();
	}
	
	for (int i=0; i<NUMOSD; i++) {
	  if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_OSD(i),world)) continue;
	  osd[i]->init();
	}
	
	// create client
	for (int i=0; i<NUMCLIENT; i++) {
	  if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_CLIENT(i),world)) continue;
	  
	  client[i]->init();
	  
	  // start up fuse
	  // use my argc, argv (make sure you pass a mount point!)
	  cout << "mounting" << endl;
	  client[i]->mount(mkfs);

	  cout << "starting fuse on rank " << myrank << " pid " << getpid() << endl;
	  ceph_fuse_main(client[i], nargc, nargv);
	  cout << "fuse finished on rank " << myrank << " pid " << getpid() << endl;
	}
	for (int i=0; i<NUMCLIENT; i++) {
	  if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_CLIENT(i),world)) continue;
	  
	  client[i]->unmount();
	  cout << "unmounted" << endl;
	  client[i]->shutdown();
	}
	
		
	// wait for it to finish
	tcpmessenger_wait();

  } else {
	cerr << "IDLE rank " << myrank << endl;
  }

  tcpmessenger_shutdown();  // shutdown MPI
  
  // cleanup
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
  
  return 0;
}

