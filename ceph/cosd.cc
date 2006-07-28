
#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "config.h"

#include "osd/OSD.h"
#include "ebofs/Ebofs.h"

#include "msg/NewMessenger.h"

#include "common/Timer.h"


class C_Die : public Context {
public:
  void finish(int) {
	cerr << "die" << endl;
	exit(1);
  }
};

class C_Debug : public Context {
  public:
  void finish(int) {
	int size = &g_conf.debug_after - &g_conf.debug;
	memcpy((char*)&g_conf.debug, (char*)&g_debug_after_conf.debug, size);
	dout(0) << "debug_after flipping debug settings" << endl;
  }
};


int main(int argc, char **argv) 
{
  vector<char*> args;
  argv_to_vec(argc, argv, args);

  parse_config_options(args);

  if (g_conf.kill_after) 
	g_timer.add_event_after(g_conf.kill_after, new C_Die);
  if (g_conf.debug_after) 
	g_timer.add_event_after(g_conf.debug_after, new C_Debug);


  assert(args.size() == 1);
  char *dev = args[0];
  cerr << "dev " << dev << endl;

  // who am i?   peek at superblock!
  OSDSuperblock sb;
  ObjectStore *store = new Ebofs(dev);
  bufferlist bl;
  store->mount();
  int r = store->read(0, 0, sizeof(sb), bl);
  if (r < 0) {
	cerr << "couldn't read superblock object on " << dev << endl;
	exit(0);
  }
  bl.copy(0, sizeof(sb), (char*)&sb);
  store->umount();
  delete store;

  cout << "osd fs says i am osd" << sb.whoami << endl;

  // start up messenger
  tcpaddr_t nsa;
  if (rank.find_ns_addr(nsa) < 0) exit(1);
  cout << "ns is at " << nsa << endl;

  rank.start_rank(nsa);

  // start osd
  Messenger *m = rank.register_entity(MSG_ADDR_OSD(sb.whoami));
  assert(m);
  OSD *osd = new OSD(sb.whoami, m, dev);
  osd->init();

  // wait
  rank.wait();

  // done
  delete osd;

  return 0;
}

