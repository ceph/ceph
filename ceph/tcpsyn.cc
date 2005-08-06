
#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "config.h"

#include "mds/MDCluster.h"
#include "mds/MDS.h"
#include "osd/OSD.h"
#include "client/Client.h"
#include "client/SyntheticClient.h"

#include "msg/TCPMessenger.h"

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


#include "msg/mpistarter.cc"


int main(int argc, char **argv) 
{
  vector<char*> args;
  argv_to_vec(argc, argv, args);

  parse_config_options(args);

  parse_syn_options(args);

  int mkfs = 0;
  vector<char*> nargs;
  for (unsigned i=0; i<args.size(); i++) {
	//cout << "a " << args[i] << endl;
	if (strcmp(args[i], "--fastmkfs") == 0) {
	  mkfs = MDS_MKFS_FAST;
	}
	else if (strcmp(args[i], "--fullmkfs") == 0) {
	  mkfs = MDS_MKFS_FULL;
	}
	else {
	  // unknown arg, pass it on.
	  nargs.push_back(args[i]);
	}
  }

  args = nargs;
  if (!args.empty()) {
	for (unsigned i=0; i<args.size(); i++)
	  cout << "stray arg " << args[i] << endl;
  }
  assert(args.empty());


  // start up tcp messenger via MPI
  pair<int,int> mpiwho = mpi_bootstrap_tcp(argc, argv);
  int myrank = mpiwho.first;
  int world = mpiwho.second;

  if (myrank == 0)
	cerr << "nummds " << NUMMDS << "  numosd " << NUMOSD << "  numclient " << NUMCLIENT << endl;
  assert(NUMMDS + NUMOSD + (NUMCLIENT?1:0) <= world);

  MDCluster *mdc = new MDCluster(NUMMDS, NUMOSD);


  char hostname[100];
  gethostname(hostname,100);
  int pid = getpid();

  int started = 0;

  // create mds
  MDS *mds[NUMMDS];
  for (int i=0; i<NUMMDS; i++) {
	if (myrank != i) continue;
	cerr << "mds" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
	mds[i] = new MDS(mdc, i, new TCPMessenger(MSG_ADDR_MDS(i)));
	mds[i]->init();
	started++;
  }
  
  // create osd
  OSD *osd[NUMOSD];
  for (int i=0; i<NUMOSD; i++) {
	if (myrank != NUMMDS + i) continue;
	cerr << "osd" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
	osd[i] = new OSD(i, new TCPMessenger(MSG_ADDR_OSD(i)));
	osd[i]->init();
	started++;
  }
  
  // create client
  int client_nodes = world - NUMMDS - NUMOSD;
  int clients_per_node = 1;
  if (NUMCLIENT) clients_per_node = (NUMCLIENT-1) / client_nodes + 1;
  set<int> clientlist;
  Client *client[NUMCLIENT];
  SyntheticClient *syn[NUMCLIENT];
  for (int i=0; i<NUMCLIENT; i++) {
	//if (myrank != NUMMDS + NUMOSD + i % client_nodes) continue;
	if (myrank != NUMMDS + NUMOSD + i / clients_per_node) continue;
	clientlist.insert(i);
	client[i] = new Client(new TCPMessenger(MSG_ADDR_CLIENT(i)) );

	// logger?
	if (client_logger == 0) {
	  char s[80];
	  sprintf(s,"clnode.%d", myrank);
	  client_logger = new Logger(s, &client_logtype);
	}

	client[i]->init();
	started++;
  }

  int nclients = 0;
  for (set<int>::iterator it = clientlist.begin();
	   it != clientlist.end();
	   it++) {
	int i = *it;
	// use my argc, argv (make sure you pass a mount point!)
	//cout << "mounting" << endl;
	client[i]->mount(mkfs);
	
	//cout << "starting synthetic client on rank " << myrank << endl;
	syn[i] = new SyntheticClient(client[i]);
	
	syn[i]->start_thread();
	
	nclients++;
  }
  if (nclients) {
	cerr << "waiting for " << nclients << " clients to finish" << endl;
  }

  for (set<int>::iterator it = clientlist.begin();
	   it != clientlist.end();
	   it++) {
	int i = *it;

	//	  cout << "waiting for synthetic client" << i << " to finish" << endl;
	syn[i]->join_thread();
	delete syn[i];
	
	client[i]->unmount();
	//cout << "client" << i << " unmounted" << endl;
	client[i]->shutdown();
  }
  

  if (!started) {
	dout(1) << "IDLE" << endl;
	tcpmessenger_stop_rankserver();
  }

  // wait for everything to finish
  tcpmessenger_wait();
  
  tcpmessenger_shutdown(); 
  

  /*
  // cleanup
  for (int i=0; i<NUMMDS; i++) {
	if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_MDS(i),world)) continue;
	delete mds[i];
  }
  for (int i=0; i<NUMOSD; i++) {
	if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_OSD(i),world)) continue;
	delete osd[i];
  }
  for (int i=0; i<NUMCLIENT; i++) {
	if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_CLIENT(i),world)) continue;
	delete client[i];
  }
  */
  delete mdc;
  
  return 0;
}

