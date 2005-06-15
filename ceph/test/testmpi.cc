#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "include/config.h"
#include "messages/MPing.h"

#include "msg/MPIMessenger.h"

class Pinger : public Dispatcher {
public:
  Messenger *messenger;
  Pinger(Messenger *m) : messenger(m) {
	m->set_dispatcher(this);
  }
  void dispatch(Message *m) {
	cout << "got incoming " << m << endl;
	delete m;
  }
};

int main(int argc, char **argv) {


  int myrank = mpimessenger_init(argc, argv);
  int world = mpimessenger_world();
  
  Pinger *p = new Pinger( new MPIMessenger(myrank) );

  mpimessenger_start();

  while (1) {
	
	// ping random nodes
	int d = rand() % world;
	p->messenger->send_message(new MPing(), d);

  }

  
  mpimessenger_wait();
  mpimessenger_shutdown();  // shutdown MPI
}
