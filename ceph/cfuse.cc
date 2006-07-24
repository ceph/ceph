

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "config.h"

#include "mds/MDCluster.h"
#include "mds/MDS.h"
#include "osd/OSD.h"
#include "client/Client.h"
#include "client/fuse.h"

#include "msg/NewMessenger.h"

#include "common/Timer.h"
       
#include <envz.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int main(int argc, char **argv, char *envp[]) {

  //cerr << "cfuse starting " << myrank << "/" << world << endl;
  vector<char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  // args for fuse
  vec_to_argv(args, argc, argv);

  // start up messenger
  tcpaddr_t nsa;
  if (rank.find_ns_addr(nsa) < 0) exit(1);
  cout << "ns is at " << nsa << endl;
  rank.start_rank(nsa);

  // start client
  Client *client = new Client(rank.register_entity(MSG_ADDR_CLIENT_NEW));
  client->init();
	
  // start up fuse
  // use my argc, argv (make sure you pass a mount point!)
  cout << "mounting" << endl;
  client->mount();
  
  cerr << "starting fuse on pid " << getpid() << endl;
  ceph_fuse_main(client, argc, argv);
  cerr << "fuse finished on pid " << getpid() << endl;
  
  client->unmount();
  cout << "unmounted" << endl;
  client->shutdown();
  
  delete client;
  
  // wait for messenger to finish
  rank.wait();
  

  return 0;
}

