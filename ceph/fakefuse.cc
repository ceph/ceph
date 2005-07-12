

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

#include "common/Timer.h"

#include "msg/FakeMessenger.h"




#define NUMMDS g_conf.num_mds
#define NUMOSD g_conf.num_osd
#define NUMCLIENT g_conf.num_client


class C_Test : public Context {
public:
  void finish(int r) {
	cout << "C_Test->finish(" << r << ")" << endl;
  }
};
class C_Test2 : public Context {
public:
  void finish(int r) {
	cout << "C_Test2->finish(" << r << ")" << endl;
	g_timer.add_event_after(2, new C_Test);
  }
};



int main(int oargc, char **oargv) {
  cerr << "fakefuse starting" << endl;

  int argc;
  char **argv;
  parse_config_options(oargc, oargv,
					   argc, argv,
					   true);

  MDCluster *mdc = new MDCluster(NUMMDS, NUMOSD);

  // start messenger thread
  fakemessenger_startthread();

  //g_timer.add_event_after(5.0, new C_Test2);
  //g_timer.add_event_after(10.0, new C_Test);

  int mkfs = 0;
  for (int i=1; i<argc; i++) {
	if (strcmp(argv[i], "--fastmkfs") == 0) {
	  mkfs = MDS_MKFS_FAST;
	  argv[i] = 0;
	  argc--;
	  break;
	}
	if (strcmp(argv[i], "--fullmkfs") == 0) {
	  mkfs = MDS_MKFS_FULL;
	  argv[i] = 0;
	  argc--;
	  break;
	}
  }

  // create osd
  OSD *osd[NUMOSD];
  for (int i=0; i<NUMOSD; i++) {
	osd[i] = new OSD(i, new FakeMessenger(MSG_ADDR_OSD(i)));
	osd[i]->init();
  }

  // create mds
  MDS *mds[NUMMDS];
  for (int i=0; i<NUMMDS; i++) {
	mds[i] = new MDS(mdc, i, new FakeMessenger(MSG_ADDR_MDS(i)));
	mds[i]->init();
  }
 
  
  // create client
  Client *client[NUMCLIENT];
  for (int i=0; i<NUMCLIENT; i++) {
	client[i] = new Client(mdc, i, new FakeMessenger(MSG_ADDR_CLIENT(0)));
	client[i]->init();


	// start up fuse
	// use my argc, argv (make sure you pass a mount point!)
	cout << "starting fuse on pid " << getpid() << endl;
	client[i]->mount(mkfs);
	ceph_fuse_main(client[i], argc, argv);
	client[i]->unmount();
	cout << "fuse finished on pid " << getpid() << endl;
	client[i]->shutdown();
  }
  


  // wait for it to finish
  cout << "DONE -----" << endl;
  fakemessenger_stopthread();  // blocks until messenger stops
  

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

