

#include <sys/stat.h>
#include <iostream>
#include <string>

#include "mds/MDCluster.h"
#include "mds/MDS.h"
#include "osd/OSD.h"
#include "client/Client.h"

#include "mds/MDCache.h"
#include "mds/MDStore.h"

#include "include/FakeMessenger.h"

#include "messages/MPing.h"

using namespace std;

__uint64_t ino = 1;


#define NUMMDSS 10
#define NUMOSDS 10
#define NUMCLIENTS 10


// this parses find output
int play();

int main(char **argv, int argc) {
  cout << "hi there" << endl;
  
  try {
	play();
  }
  catch (char *s) {
	cout << "exception: " << s << endl;
  }
}

int play() {
  cout << "creating stuff" << endl;

  // init
  // create mds
  MDCluster *mdc = new MDCluster();
  MDS *mds[10];
  for (int i=0; i<NUMMDSS; i++) {
	mds[i] = new MDS(mdc, new FakeMessenger(MSG_ADDR_MDS(i)));
	mds[i]->init();
  }

  // create osds
  OSD *osd[10];
  for (int i=0; i<NUMOSDS; i++) {
	osd[i] = new OSD(i, new FakeMessenger(MSG_ADDR_OSD(i)));
	osd[i]->init();
  }
  OSD *logosd = new OSD(666, new FakeMessenger(MSG_ADDR_OSD(666)));
  logosd->init();
  
  // create clients
  Client *client[NUMCLIENTS];
  for (int i=0; i<NUMCLIENTS; i++) {
	client[i] = new Client(i, new FakeMessenger(MSG_ADDR_CLIENT(i)));
	client[i]->init();
  }

  cout << "sending test ping, load" << endl;


  // send an initial message...?
  mds[0]->messenger->send_message(new MPing(10), 1, MDS_PORT_MAIN, MDS_PORT_MAIN);

  //for (int i=0; i<NUMCLIENTS; i++) 
	for (int i=0; i<1; i++) 
	client[i]->issue_request();

  // loop
  fakemessenger_do_loop();

  // cleanup
  cout << "cleanup" << endl;
  for (int i=0; i<NUMMDSS; i++) {
	if (mds[i]->shutdown() == 0) {
	  //cout << "clean shutdown of mds " << i << endl;
	  delete mds[i];
	} else {
	  cout << "problems shutting down mds " << i << endl;
	}
  }
  for (int i=0; i<NUMOSDS; i++) {
	if (osd[i]->shutdown() == 0) { 
	  //cout << "clean shutdown of osd " << i << endl;
	  delete osd[i];
	} else {
	  cout << "problems shutting down osd " << i << endl;
	}
  }
  for (int i=0; i<NUMCLIENTS; i++) {
	if (client[i]->shutdown() == 0) { 
	  //cout << "clean shutdown of client " << i << endl;
	  delete client[i];
	} else {
	  cout << "problems shutting down client " << i << endl;
	}
  }
  delete mdc;
  cout << "done." << endl;
  return 0;
}

