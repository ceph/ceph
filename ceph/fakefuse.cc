

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
#include "msg/CheesySerializer.h"



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



int main(int argc, char **argv) {
  cout << "fakefuse starting" << endl;

  MDCluster *mdc = new MDCluster(NUMMDS, NUMOSD);

  // start messenger thread
  fakemessenger_startthread();

  g_timer.add_event_after(5.0, new C_Test2);
  g_timer.add_event_after(10.0, new C_Test);


  // create mds
  MDS *mds[NUMMDS];
  for (int i=0; i<NUMMDS; i++) {
	mds[i] = new MDS(mdc, i, new FakeMessenger(MSG_ADDR_MDS(i)));
	mds[i]->init();
  }
  
  // create osd
  OSD *osd[NUMOSD];
  for (int i=0; i<NUMOSD; i++) {
	osd[i] = new OSD(i, new FakeMessenger(MSG_ADDR_OSD(i)));
	osd[i]->init();
  }
  
  // create client
  Client *client[NUMCLIENT];
  for (int i=0; i<NUMCLIENT; i++) {
	// build a serialized fakemessenger...
	FakeMessenger *fake = new FakeMessenger(MSG_ADDR_CLIENT(0));
	CheesySerializer *serializer = new CheesySerializer(fake);
	fake->set_dispatcher(serializer);   

	client[i] = new Client(mdc, i, serializer);
	client[i]->init();


	// start up fuse
	// use my argc, argv (make sure you pass a mount point!)
	cout << "starting fuse on pid " << getpid() << endl;
	client[i]->mount();
	ceph_fuse_main(client[i], argc, argv);
	client[i]->unmount();
	cout << "fuse finished on pid " << getpid() << endl;
	client[i]->shutdown();
  }
  


  // wait for it to finish
  cout << "DONE -----" << endl;
  fakemessenger_stopthread();  // blocks until messenger stops
  
  // shutdown
  /*
  cout << "---- check ----" << endl;
  for (int i=0; i<NUMMDS; i++) {
	if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_MDS(i),world)) continue;
	mds[i]->mdcache->shutdown_pass();
  }
  */

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

