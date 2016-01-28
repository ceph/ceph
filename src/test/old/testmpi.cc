#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "common/config.h"
#include "messages/MPing.h"
#include "common/Mutex.h"

#include "msg/MPIMessenger.h"

class Pinger : public Dispatcher {
public:
  Messenger *messenger;
  explicit Pinger(Messenger *m) : messenger(m) {
    m->set_dispatcher(this);
  }
  void dispatch(Message *m) {
    //dout(1) << "got incoming " << m << endl;
    delete m;

  }
};

int main(int argc, char **argv) {
  int num = 1000;

  int myrank = mpimessenger_init(argc, argv);
  int world = mpimessenger_world();
  
  Pinger *p = new Pinger( new MPIMessenger(myrank) );

  mpimessenger_start();

  //while (1) {
  for (int i=0; i<10000; i++) {
    
    // ping random nodes
    int d = rand() % world;
    if (d != myrank) {
      //cout << "sending " << i << " to " << d << endl;
      p->messenger->send_message(new MPing(), d);
     }
    
  }


  //cout << "shutting down" << endl;
  //p->messenger->shutdown();
  
  mpimessenger_wait();
  mpimessenger_shutdown();  // shutdown MPI
}
