

#include <sys/stat.h>
#include <iostream>
#include <string>

#include "include/MDS.h"
#include "include/OSD.h"
#include "include/MDCache.h"
#include "include/MDStore.h"
#include "include/FakeMessenger.h"

#include "messages/MPing.h"

using namespace std;

__uint64_t ino = 1;


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
  cout << "hello" << endl;

  // init
  // create mds
  MDS *mds[10];
  for (int i=0; i<10; i++) {
	mds[i] = new MDS(0, 1, new FakeMessenger(MSG_ADDR_MDS(i)));
	mds[i]->open_root(NULL);
	mds[i]->init();
  }

  // create osds
  OSD *osd[10];
  for (int i=0; i<10; i++) {
	osd[i] = new OSD(i, new FakeMessenger(MSG_ADDR_OSD(i)));
	osd[i]->init();
  }

  // fetch root on mds0
  mds[0]->mdstore->fetch_dir( mds[0]->mdcache->get_root(), NULL );

  // send an initial message...?
  mds[0]->messenger->send_message(new MPing(10), 1, MDS_PORT_MAIN, MDS_PORT_MAIN);

  // loop
  fakemessenger_do_loop();

  // cleanup
  for (int i=0; i<10; i++) {
	if (mds[i]->shutdown() == 0) {
	  //cout << "clean shutdown of mds " << i << endl;
	  delete mds[i];
	} else {
	  cout << "problems shutting down mds " << i << endl;
	}

	if (osd[i]->shutdown() == 0) { 
	  //cout << "clean shutdown of osd " << i << endl;
	  delete osd[i];
	} else {
	  cout << "problems shutting down osd " << i << endl;
	}
  }
  cout << "done." << endl;
  return 0;
}

