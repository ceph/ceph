

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "include/config.h"

#include "mds/MDCluster.h"
#include "mds/MDS.h"
#include "osd/OSD.h"
#include "client/Client.h"
#include "client/SyntheticClient.h"

#include "msg/FakeMessenger.h"
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

  //cerr << "mpisyn starting " << myrank << "/" << world << endl;
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

  int synthetic = 100;
  int mkfs = 0;
  for (int i=1; i<argc; i++) {
	if (strcmp(argv[i], "--fastmkfs") == 0) {
	  mkfs = MDS_MKFS_FAST;
	}
	else if (strcmp(argv[i], "--fullmkfs") == 0) {
	  mkfs = MDS_MKFS_FULL;
	}
	else if (strcmp(argv[i],"--synthetic") == 0) {
	  synthetic = atoi(argv[++i]);
	}
	else {
	  // unknown arg, pass it on.
	  nargv[nargc++] = argv[i];
	}
  }

  fakemessenger_startthread();

  MDCluster *mdc = new MDCluster(NUMMDS, NUMOSD);


  char hostname[100];
  gethostname(hostname,100);
  int pid = getpid();

  // create mds
  MDS *mds[NUMMDS];
  for (int i=0; i<NUMMDS; i++) {
	//cerr << "mds" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
	mds[i] = new MDS(mdc, i, new FakeMessenger(MSG_ADDR_MDS(i)));
	start++;
  }
  
  // create osd
  OSD *osd[NUMOSD];
  for (int i=0; i<NUMOSD; i++) {
	//cerr << "osd" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
	osd[i] = new OSD(i, new FakeMessenger(MSG_ADDR_OSD(i)));
	start++;
  }
  
  // create client
  Client *client[NUMCLIENT];
  SyntheticClient *syn[NUMCLIENT];
  for (int i=0; i<NUMCLIENT; i++) {
	//cerr << "client" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
	CheesySerializer *serializer = new CheesySerializer( new FakeMessenger(MSG_ADDR_CLIENT(i)) );
	client[i] = new Client(mdc, i, serializer);
	start++;
  }


  // start message loop
  fakemessenger_startthread();
  
  // init
  for (int i=0; i<NUMMDS; i++) {
	mds[i]->init();
  }
  
  for (int i=0; i<NUMOSD; i++) {
	osd[i]->init();
  }
  
  // create client
  for (int i=0; i<NUMCLIENT; i++) {
	client[i]->init();
	
	// use my argc, argv (make sure you pass a mount point!)
	cout << "mounting" << endl;
	client[i]->mount(mkfs);
	
	cout << "starting synthatic client  " << endl;
	syn[i] = new SyntheticClient(client[i]);

	syn[i]->mode = SYNCLIENT_MODE_MAKEDIRS;
	char s[20];
	sprintf(s,"syn.%d", i);
	syn[i]->sarg1 = s;
	syn[i]->iarg1 = 5;
	syn[i]->iarg2 = 5;
	syn[i]->iarg3 = 2;
	  
	syn[i]->start_thread();
  }
  for (int i=0; i<NUMCLIENT; i++) {
	
	cout << "waiting for synthetic client to finish" << endl;
	syn[i]->join_thread();
	delete syn[i];
	
	client[i]->unmount();
	cout << "unmounted" << endl;
	client[i]->shutdown();
  }
  
		
  // wait for it to finish
  fakemessenger_wait();
  
  // cleanup
  for (int i=0; i<NUMMDS; i++) {
	delete mds[i];
  }
  for (int i=0; i<NUMOSD; i++) {
	delete osd[i];
  }
  for (int i=0; i<NUMCLIENT; i++) {
	delete client[i];
  }
  delete mdc;
  
  return 0;
}

