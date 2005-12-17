

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

#include "msg/TCPMessenger.h"

#include "common/Timer.h"
       
#include <envz.h>


int main(int argc, char **argv, char *envp[]) {

  //cerr << "tcpfuse starting " << myrank << "/" << world << endl;
  vector<char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  vector<char*> nargs;
  
  char *nsaddr = 0;

  for (unsigned i=0; i<args.size(); i++) {
	if (strcmp(args[i], "--ns") == 0) {
	  nsaddr = args[++i];
	}
	else {
	  // unknown arg, pass it on.
	  nargs.push_back(args[i]);
	  cout << "fuse arg: " << args[i] << endl;
	}
  }
  
  if (nsaddr == 0) {
	// env var
	int e_len = 0;
	for (int i=0; envp[i]; i++)
	  e_len += strlen(envp[i]) + 1;
	nsaddr = envz_entry(*envp, e_len, "CEPH_NAMESERVER");	
	if (nsaddr) {
	  while (nsaddr[0] != '=') nsaddr++;
	  nsaddr++;
	}
  }
  if (!nsaddr) {
	cerr << "i need ceph ns addr.. either CEPH_NAMESERVER env var or --ns blah" << endl;
	exit(-1);
  }
  // look up nsaddr
  tcpaddr_t nsa;
  if (tcpmessenger_lookup(nsaddr, nsa) < 0) {
	return 1;
  }

  cout << "ceph ns is " << nsaddr << " or " << nsa << endl;

  // args for fuse
  args = nargs;
  vec_to_argv(args, argc, argv);


  // start up tcpmessenger
  tcpmessenger_init();
  tcpmessenger_start();
  tcpmessenger_start_rankserver(nsa);
  
  Client *client = new Client(new TCPMessenger(MSG_ADDR_CLIENT_NEW));
  client->init();
	
  // start up fuse
  // use my argc, argv (make sure you pass a mount point!)
  cout << "mounting" << endl;
  client->mount();
  
  cout << "starting fuse on pid " << getpid() << endl;
  ceph_fuse_main(client, argc, argv);
  cout << "fuse finished on pid " << getpid() << endl;
  
  client->unmount();
  cout << "unmounted" << endl;
  client->shutdown();
  
  delete client;
  
  // wait for it to finish
  tcpmessenger_wait();
  tcpmessenger_shutdown();  // shutdown MPI

  return 0;
}

