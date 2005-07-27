

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

#include "msg/FakeMessenger.h"

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


int main(int oargc, char **oargv) 
{
  cerr << "fakesyn starting" << endl;

  int argc;
  char **argv;
  parse_config_options(oargc, oargv,
					   argc, argv);

  int start = 0;

  // build new argc+argv for fuse
  typedef char* pchar;
  int nargc = 0;
  char **nargv = new pchar[argc];
  nargv[nargc++] = argv[0];

  list<int> syn_modes;
  list<int> syn_iargs;
  list<string> syn_sargs;

  int mkfs = 0;
  for (int i=1; i<argc; i++) {
	cout << "asdf" << endl;
	if (strcmp(argv[i], "--fastmkfs") == 0) {
	  mkfs = MDS_MKFS_FAST;
	}
	else if (strcmp(argv[i], "--fullmkfs") == 0) {
	  mkfs = MDS_MKFS_FULL;
	}

	else if (strcmp(argv[i],"--syn") == 0) {
	  ++i;
	  if (strcmp(argv[i],"writefile") == 0) {
		syn_modes.push_back( SYNCLIENT_MODE_WRITEFILE );
		syn_iargs.push_back( atoi(argv[++i]) );
		syn_iargs.push_back( atoi(argv[++i]) );
	  } else if (strcmp(argv[i],"readfile") == 0) {
		syn_modes.push_back( SYNCLIENT_MODE_READFILE );
		syn_iargs.push_back( atoi(argv[++i]) );
		syn_iargs.push_back( atoi(argv[++i]) );
	  } else if (strcmp(argv[i],"makedirs") == 0) {
		syn_modes.push_back( SYNCLIENT_MODE_MAKEDIRS );
		syn_iargs.push_back( atoi(argv[++i]) );
		syn_iargs.push_back( atoi(argv[++i]) );
		syn_iargs.push_back( atoi(argv[++i]) );
	  } else if (strcmp(argv[i],"fullwalk") == 0) {
		syn_modes.push_back( SYNCLIENT_MODE_FULLWALK );
		//syn_sargs.push_back( atoi(argv[++i]) );
	  } else if (strcmp(argv[i],"randomwalk") == 0) {
		syn_modes.push_back( SYNCLIENT_MODE_RANDOMWALK );
		syn_iargs.push_back( atoi(argv[++i]) );	   
	  } else if (strcmp(argv[i],"trace_include") == 0) {
		syn_modes.push_back( SYNCLIENT_MODE_TRACEINCLUDE );
		syn_iargs.push_back( atoi(argv[++i]) );
	  } else if (strcmp(argv[i],"trace_lib") == 0) {
		syn_modes.push_back( SYNCLIENT_MODE_TRACELIB );
		syn_iargs.push_back( atoi(argv[++i]) );
	  } else if (strcmp(argv[i],"trace_openssh") == 0) {
		syn_modes.push_back( SYNCLIENT_MODE_TRACEOPENSSH );
		syn_iargs.push_back( atoi(argv[++i]) );
	  } else if (strcmp(argv[i],"trace_opensshlib") == 0) {
		syn_modes.push_back( SYNCLIENT_MODE_TRACEOPENSSHLIB );
		syn_iargs.push_back( atoi(argv[++i]) );
	  } else if (strcmp(argv[i],"trace") == 0) {
		syn_modes.push_back( SYNCLIENT_MODE_TRACE );
		syn_sargs.push_back( argv[++i] );
		syn_iargs.push_back( atoi(argv[++i]) );
	  } else if (strcmp(argv[i],"until") == 0) {
		syn_modes.push_back( SYNCLIENT_MODE_UNTIL );
		syn_iargs.push_back( atoi(argv[++i]) );
	  } else {
		cerr << "unknown syn mode " << argv[i] << endl;
		return -1;
	  }
	}

	else {
	  // unknown arg, pass it on.
	  nargv[nargc++] = argv[i];
	}
  }

  MDCluster *mdc = new MDCluster(NUMMDS, NUMOSD);


  char hostname[100];
  gethostname(hostname,100);
  //int pid = getpid();

  // create mds
  MDS *mds[NUMMDS];
  for (int i=0; i<NUMMDS; i++) {
	//cerr << "mds" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
	mds[i] = new MDS(mdc, i, new FakeMessenger(MSG_ADDR_MDS(i)));
	start++;
  }
  
  // create osd
  OSD *osd[NUMOSD];
  for (int i=0; i<NUMOSD; i++) {
	//cerr << "osd" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
	osd[i] = new OSD(i, new FakeMessenger(MSG_ADDR_OSD(i)));
	start++;
  }
  
  // create client
  Client *client[NUMCLIENT];
  SyntheticClient *syn[NUMCLIENT];
  for (int i=0; i<NUMCLIENT; i++) {
	//cerr << "client" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
	client[i] = new Client(mdc, i, new FakeMessenger(MSG_ADDR_CLIENT(i)));
	start++;
  }


  // start message loop
  fakemessenger_startthread();
  
  // init
  for (int i=0; i<NUMMDS; i++) {
	mds[i]->init();
  }
  
  for (int i=0; i<NUMOSD; i++) {
	osd[i]->init();
  }
  
  // create client
  for (int i=0; i<NUMCLIENT; i++) {
	client[i]->init();
	
	// use my argc, argv (make sure you pass a mount point!)
	//cout << "mounting" << endl;
	client[i]->mount(mkfs);
	
	//cout << "starting synthetic client  " << endl;
	syn[i] = new SyntheticClient(client[i]);

	syn[i]->modes = syn_modes;
	syn[i]->sargs = syn_sargs;
	syn[i]->iargs = syn_iargs;
	syn[i]->start_thread();
  }
  for (int i=0; i<NUMCLIENT; i++) {
	
	cout << "waiting for synthetic client " << i << " to finish" << endl;
	syn[i]->join_thread();
	delete syn[i];
	
	client[i]->unmount();
	//cout << "unmounted" << endl;
	client[i]->shutdown();
  }
  
		
  // wait for it to finish
  fakemessenger_wait();
  
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

  free(argv);
  delete[] nargv;
  cout << "fakesyn done" << endl;
  return 0;
}

