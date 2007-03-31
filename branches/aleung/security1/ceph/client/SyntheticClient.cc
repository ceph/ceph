// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <iostream>
#include <sstream>
using namespace std;



#include "SyntheticClient.h"

#include "include/filepath.h"
#include "mds/MDS.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <utime.h>
#include <math.h>

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_client) cout << g_clock.now() << " synthetic" << client->get_nodeid() << " "

// traces
//void trace_include(SyntheticClient *syn, Client *cl, string& prefix);
//void trace_openssh(SyntheticClient *syn, Client *cl, string& prefix);


list<int> syn_modes;
list<int> syn_iargs;
list<string> syn_sargs;

void parse_syn_options(vector<char*>& args)
{
  vector<char*> nargs;

  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i],"--syn") == 0) {
      ++i;

      if (strcmp(args[i],"writefile") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_WRITEFILE );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"ior") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_IOR2 );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"mixed") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_MIXED );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"renewal") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_RENEWAL );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"wrshared") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_WRSHARED );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"writebatch") == 0) {
	syn_modes.push_back( SYNCLIENT_MODE_WRITEBATCH );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"readfile") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_READFILE );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"rw") == 0) {
        int a = atoi(args[++i]);
        int b = atoi(args[++i]);
        syn_modes.push_back( SYNCLIENT_MODE_WRITEFILE );
        syn_iargs.push_back( a );
        syn_iargs.push_back( b );
        syn_modes.push_back( SYNCLIENT_MODE_READFILE );
        syn_iargs.push_back( a );
        syn_iargs.push_back( b );

      } else if (strcmp(args[i],"makedirs") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_MAKEDIRS );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"makedirmess") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_MAKEDIRMESS );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"statdirs") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_STATDIRS );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"readdirs") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_READDIRS );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );

      } else if (strcmp(args[i],"makefiles") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_MAKEFILES );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"createshared") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_CREATESHARED );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"openshared") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_OPENSHARED );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );

      } else if (strcmp(args[i],"walk") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_FULLWALK );
        //syn_sargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"randomwalk") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_RANDOMWALK );
        syn_iargs.push_back( atoi(args[++i]) );       

      } else if (strcmp(args[i],"trace") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_TRACE );
        syn_sargs.push_back( args[++i] );
        syn_iargs.push_back( atoi(args[++i]) );

      } else if (strcmp(args[i],"until") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_UNTIL );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"sleepuntil") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_SLEEPUNTIL );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"only") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_ONLY );
        syn_iargs.push_back( atoi(args[++i]) );
        
      } else if (strcmp(args[i],"sleep") == 0) { 
        syn_modes.push_back( SYNCLIENT_MODE_SLEEP );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"randomsleep") == 0) { 
        syn_modes.push_back( SYNCLIENT_MODE_RANDOMSLEEP );
        syn_iargs.push_back( atoi(args[++i]) );

      } else if (strcmp(args[i],"opentest") == 0) { 
        syn_modes.push_back( SYNCLIENT_MODE_OPENTEST );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"optest") == 0) {
	syn_modes.push_back( SYNCLIENT_MODE_OPTEST );
        syn_iargs.push_back( atoi(args[++i]) );
      } else {
        cerr << "unknown syn arg " << args[i] << endl;
        assert(0);
      }
    }
    else {
      nargs.push_back(args[i]);
    }
  }

  args = nargs;
}


SyntheticClient::SyntheticClient(Client *client) 
{
  this->client = client;
  thread_id = 0;
  
  did_readdir = false;

  run_only = -1;

  this->modes = syn_modes;
  this->iargs = syn_iargs;
  this->sargs = syn_sargs;

  run_start = g_clock.now();
}




#define DBL 2

void *synthetic_client_thread_entry(void *ptr)
{
  SyntheticClient *sc = (SyntheticClient*)ptr;
  //int r = 
  sc->run();
  return 0;//(void*)r;
}

string SyntheticClient::get_sarg(int seq) 
{
  string a;
  if (!sargs.empty()) {
    a = sargs.front(); 
    sargs.pop_front();
  }
  if (a.length() == 0 || a == "~") {
    char s[20];
    sprintf(s,"syn.%d.%d", client->whoami, seq);
    a = s;
  } 
  //cout << "a is " << a << endl;
  return a;
}

int SyntheticClient::run()
{ 
  //run_start = g_clock.now();
  run_until = utime_t(0,0);
  dout(5) << "run" << endl;

  for (list<int>::iterator it = modes.begin();
       it != modes.end();
       it++) {
    int mode = *it;
    dout(3) << "mode " << mode << endl;

    switch (mode) {
    case SYNCLIENT_MODE_RANDOMSLEEP:
      {
        int iarg1 = iargs.front();
        iargs.pop_front();
        if (run_me()) {
          srand(time(0) + getpid() + client->whoami);
          sleep(rand() % iarg1);
        }
      }
      break;

    case SYNCLIENT_MODE_SLEEP:
      {
        int iarg1 = iargs.front();
        iargs.pop_front();
        if (run_me()) {
          dout(2) << "sleep " << iarg1 << endl;
          sleep(iarg1);
        }
      }
      break;

    case SYNCLIENT_MODE_ONLY:
      {
        run_only = iargs.front();
        iargs.pop_front();
        if (run_only == client->get_nodeid())
          dout(2) << "only " << run_only << endl;
      }
      break;

    case SYNCLIENT_MODE_UNTIL:
      {
        int iarg1 = iargs.front();
        iargs.pop_front();
        if (iarg1) {
          dout(2) << "until " << iarg1 << endl;
          utime_t dur(iarg1,0);
          run_until = run_start + dur;
        } else {
          dout(2) << "until " << iarg1 << " (no limit)" << endl;
          run_until = utime_t(0,0);
        }
      }
      break;

    case SYNCLIENT_MODE_SLEEPUNTIL:
      {
        int iarg1 = iargs.front();
        iargs.pop_front();
        if (iarg1) {
          dout(2) << "sleepuntil " << iarg1 << endl;
          utime_t at = g_clock.now() - run_start;
          if (at.sec() < iarg1) 
            sleep(iarg1 - at.sec());
        }
      }
      break;

    case SYNCLIENT_MODE_RANDOMWALK:
      {
        int iarg1 = iargs.front();
        iargs.pop_front();
        if (run_me()) {
          dout(2) << "randomwalk " << iarg1 << endl;
          random_walk(iarg1);
        }
      }
      break;

    case SYNCLIENT_MODE_MAKEDIRMESS:
      {
        string sarg1 = get_sarg(0);
        int iarg1 = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "makedirmess " << sarg1 << " " << iarg1 << endl;
          make_dir_mess(sarg1.c_str(), iarg1);
        }
      }
      break;
    case SYNCLIENT_MODE_MAKEDIRS:
      {
        string sarg1 = get_sarg(0);
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();
        int iarg3 = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "makedirs " << sarg1 << " " << iarg1 << " " << iarg2 << " " << iarg3 << endl;
          make_dirs(sarg1.c_str(), iarg1, iarg2, iarg3);
        }
      }
      break;
    case SYNCLIENT_MODE_STATDIRS:
      {
        string sarg1 = get_sarg(0);
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();
        int iarg3 = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "statdirs " << sarg1 << " " << iarg1 << " " << iarg2 << " " << iarg3 << endl;
          stat_dirs(sarg1.c_str(), iarg1, iarg2, iarg3);
        }
      }
      break;
    case SYNCLIENT_MODE_READDIRS:
      {
        string sarg1 = get_sarg(0);
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();
        int iarg3 = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "readdirs " << sarg1 << " " << iarg1 << " " << iarg2 << " " << iarg3 << endl;
          read_dirs(sarg1.c_str(), iarg1, iarg2, iarg3);
        }
      }
      break;


    case SYNCLIENT_MODE_MAKEFILES:
      {
        int num = iargs.front();  iargs.pop_front();
        int count = iargs.front();  iargs.pop_front();
        int priv = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "makefiles " << num << " " << count << " " << priv << endl;
          make_files(num, count, priv, false);
        }
      }
      break;
    case SYNCLIENT_MODE_MAKEFILES2:
      {
        int num = iargs.front();  iargs.pop_front();
        int count = iargs.front();  iargs.pop_front();
        int priv = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "makefiles2 " << num << " " << count << " " << priv << endl;
          make_files(num, count, priv, true);
        }
      }
      break;
    case SYNCLIENT_MODE_CREATESHARED:
      {
        string sarg1 = get_sarg(0);
        int num = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "createshared " << num << endl;
          create_shared(num);
        }
      }
      break;
    case SYNCLIENT_MODE_OPENSHARED:
      {
        string sarg1 = get_sarg(0);
        int num = iargs.front();  iargs.pop_front();
        int count = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "openshared " << num << endl;
          open_shared(num, count);
        }
      }
      break;

    case SYNCLIENT_MODE_FULLWALK:
      {
        string sarg1;// = get_sarg(0);
        if (run_me()) {
          dout(2) << "fullwalk" << sarg1 << endl;
          full_walk(sarg1);
        }
      }
      break;
    case SYNCLIENT_MODE_REPEATWALK:
      {
        string sarg1 = get_sarg(0);
        if (run_me()) {
          dout(2) << "repeatwalk " << sarg1 << endl;
          while (full_walk(sarg1) == 0) ;
        }
      }
      break;
      
    case SYNCLIENT_MODE_IOR2:
      {
	int count = iargs.front();  iargs.pop_front();
	string prefix = get_sarg(0);
	cout << "Prefix:" << prefix << endl;

	ostringstream oss;
	oss << "IOR2/tracepaths.cephtrace";
	string setupfile = oss.str();
	cout << "Setup trace:" << setupfile << endl;

	// choose random IOR2 file to open
	int filenum = rand() % 500;
	// cant be trace 0, its broken
	filenum++;
	//int filenum = client->whoami + 1;
	
	ostringstream ost;
	ost << "IOR2/IOR_trace_fileperproc.p" << filenum << "t.cephtrace";
	string tfile = ost.str();
	cout << "My " << count <<  " trace file:" << tfile << endl;
      
	
	// create trace object
	Trace ts(setupfile.c_str());
	Trace tr(tfile.c_str());
	
	client->mkdir(prefix.c_str(), 0755);
       
	if (run_me()) {
	  play_trace(ts, prefix);
	  
          for (int i=0; i<count; i++) {

	    if (time_to_stop()) break;
            play_trace(tr, prefix);

          }
	  
        }
	
      }
      break;
    case SYNCLIENT_MODE_MIXED:
      {
	string shared1 = "/shared1";
	string shared2 = "/shared2";
	string shared3 = "/shared3";
	string shared4 = "/shared4";
	string shared5 = "/shared5";
	string shared6 = "/shared6";

	string personal1 = get_sarg(0);
	string personal2 = get_sarg(1);
	string personal3 = get_sarg(2);
	string personal4 = get_sarg(3);

        int iterations = iargs.front();  iargs.pop_front();

	int size = 10;
	int inc = 128*1024;

	if (run_me()) {
	  
          for (int i=0; i<iterations; i++) {

	    if (time_to_stop()) break;
	    write_file(shared1, size, inc);
	    write_file(shared2, size, inc);
	    write_file(shared3, size, inc);
	    write_file(shared4, size, inc);
	    write_file(shared5, size, inc);
	    write_file(shared6, size, inc);
	    write_file(personal1, size, inc);
	    write_file(personal2, size, inc);
	    write_file(personal3, size, inc);
	    write_file(personal4, size, inc);
          }
	  
        }
      }
      break;
    case SYNCLIENT_MODE_RENEWAL:
      {
        int stop_int = iargs.front();  iargs.pop_front();
	utime_t stop_time(stop_int, 0);

	int size = 32;
	int inc = 1024*1024;

	if (run_me()) {
	  utime_t start_time = g_clock.now();
	  utime_t cur_time = g_clock.now();
	  int counter = 0;
	  string pfile;
          while (cur_time - start_time < stop_time) {
	    pfile = get_sarg(counter);
	    if (time_to_stop()) break;
	    cout << "Writting " << pfile << endl;
	    write_file(pfile, size, inc);
	    cur_time = g_clock.now();
	    cout << "Time check " << cur_time - start_time << endl;
	    counter++;
          }
	  
        }
      }
      break;
    case SYNCLIENT_MODE_WRITEFILE:
      {
        string sarg1 = get_sarg(0);
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();
        if (run_me())
          write_file(sarg1, iarg1, iarg2);
      }
      break;
    case SYNCLIENT_MODE_WRSHARED:
      {
        string sarg1 = "shared";
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();
        if (run_me())
          write_file(sarg1, iarg1, iarg2);
      }
      break;
    case SYNCLIENT_MODE_WRITEBATCH:
      {
          int iarg1 = iargs.front(); iargs.pop_front();
        int iarg2 = iargs.front(); iargs.pop_front();
        int iarg3 = iargs.front(); iargs.pop_front();

        if (run_me())
          write_batch(iarg1, iarg2, iarg3);
      }
      break;

    case SYNCLIENT_MODE_READFILE:
      {
        //string sarg1 = get_sarg(0);
	string sarg1 = "shared";
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();
        if (run_me())
          read_file(sarg1, iarg1, iarg2);
      }
      break;

    case SYNCLIENT_MODE_TRACE:
      {
        string tfile = get_sarg(0);
        sargs.push_front(string("~"));
        int iarg1 = iargs.front();  iargs.pop_front();
        string prefix = get_sarg(0);

        if (run_me()) {
          dout(2) << "trace " << tfile << " prefix " << prefix << " ... " << iarg1 << " times" << endl;
          
          Trace t(tfile.c_str());
          
          client->mkdir(prefix.c_str(), 0755);
          
          for (int i=0; i<iarg1; i++) {
            utime_t start = g_clock.now();
            
            if (time_to_stop()) break;
            play_trace(t, prefix);
            //if (time_to_stop()) break;
            //clean_dir(prefix);
            
            utime_t lat = g_clock.now();
            lat -= start;
            
            dout(1) << " trace " << tfile << " loop " << (i+1) << "/" << iarg1 << " done in " << (double)lat << " seconds" << endl;
            if (client_logger 
                && i > 0
                && i < iarg1-1
                ) {
              client_logger->finc("trsum", (double)lat);
              client_logger->inc("trnum");
            }
          }
        }
      }
      break;


    case SYNCLIENT_MODE_OPENTEST:
      {
        int count = iargs.front();  iargs.pop_front();
        if (run_me()) {
          for (int i=0; i<count; i++) {
            int fd = client->open("test", rand()%2 ? (O_WRONLY|O_CREAT):O_RDONLY);
            if (fd > 0) client->close(fd);
          }
        }
      }
      break;

    case SYNCLIENT_MODE_OPTEST:
      {
        int count = iargs.front();  iargs.pop_front();
        if (run_me()) {
	  client->mknod("test",0777);
	  struct stat st;
	  for (int i=0; i<count; i++) {
	    client->lstat("test", &st);
	    client->chmod("test", 0777);
          }
        }
      }
      break;
      
    default:
      assert(0);
    }
  }
  return 0;
}


int SyntheticClient::start_thread()
{
  assert(!thread_id);

  pthread_create(&thread_id, NULL, synthetic_client_thread_entry, this);
  assert(thread_id);
  return 0;
}

int SyntheticClient::join_thread()
{
  assert(thread_id);
  void *rv;
  pthread_join(thread_id, &rv);
  return 0;
}


bool roll_die(float p) 
{
  float r = (float)(rand() % 100000) / 100000.0;
  if (r < p) 
    return true;
  else 
    return false;
}

void SyntheticClient::init_op_dist()
{
  op_dist.clear();
  op_dist.add( MDS_OP_STAT, g_conf.fakeclient_op_stat );
  op_dist.add( MDS_OP_UTIME, g_conf.fakeclient_op_utime );
  op_dist.add( MDS_OP_CHMOD, g_conf.fakeclient_op_chmod );
  op_dist.add( MDS_OP_CHOWN, g_conf.fakeclient_op_chown );

  op_dist.add( MDS_OP_READDIR, g_conf.fakeclient_op_readdir );
  op_dist.add( MDS_OP_MKNOD, g_conf.fakeclient_op_mknod );
  op_dist.add( MDS_OP_LINK, g_conf.fakeclient_op_link );
  op_dist.add( MDS_OP_UNLINK, g_conf.fakeclient_op_unlink );
  op_dist.add( MDS_OP_RENAME, g_conf.fakeclient_op_rename );

  op_dist.add( MDS_OP_MKDIR, g_conf.fakeclient_op_mkdir );
  op_dist.add( MDS_OP_RMDIR, g_conf.fakeclient_op_rmdir );
  op_dist.add( MDS_OP_SYMLINK, g_conf.fakeclient_op_symlink );

  op_dist.add( MDS_OP_OPEN, g_conf.fakeclient_op_openrd );
  //op_dist.add( MDS_OP_READ, g_conf.fakeclient_op_read );
  //op_dist.add( MDS_OP_WRITE, g_conf.fakeclient_op_write );
  op_dist.add( MDS_OP_TRUNCATE, g_conf.fakeclient_op_truncate );
  op_dist.add( MDS_OP_FSYNC, g_conf.fakeclient_op_fsync );
  op_dist.add( MDS_OP_RELEASE, g_conf.fakeclient_op_close );  // actually, close()
  op_dist.normalize();
}

void SyntheticClient::up()
{
  cwd = cwd.prefixpath(cwd.depth()-1);
  dout(DBL) << "cd .. -> " << cwd << endl;
  clear_dir();
}


int SyntheticClient::play_trace(Trace& t, string& prefix)
{
  dout(4) << "play trace" << endl;
  t.start();

  utime_t start = g_clock.now();

  const char *p = prefix.c_str();

  map<__int64_t, __int64_t> open_files;

  while (!t.end()) {
    
    if (time_to_stop()) break;
    
    // op
    const char *op = t.get_string();
    dout(4) << "trace op " << op << endl;
    if (strcmp(op, "link") == 0) {
      const char *a = t.get_string(p);
      const char *b = t.get_string(p);
      client->link(a,b);      
    } else if (strcmp(op, "unlink") == 0) {
      const char *a = t.get_string(p);
      client->unlink(a);
    } else if (strcmp(op, "rename") == 0) {
      const char *a = t.get_string(p);
      const char *b = t.get_string(p);
      client->rename(a,b);      
    } else if (strcmp(op, "mkdir") == 0) {
      const char *a = t.get_string(p);
      __int64_t b = t.get_int();
      client->mkdir(a, b);
    } else if (strcmp(op, "rmdir") == 0) {
      const char *a = t.get_string(p);
      client->rmdir(a);
    } else if (strcmp(op, "symlink") == 0) {
      const char *a = t.get_string(p);
      const char *b = t.get_string(p);
      client->symlink(a,b);      
    } else if (strcmp(op, "readlink") == 0) {
      const char *a = t.get_string(p);
      char buf[100];
      client->readlink(a, buf, 100);
    } else if (strcmp(op, "lstat") == 0) {
      struct stat st;
      const char *a = t.get_string(p);
      client->lstat(a, &st);
    } else if (strcmp(op, "chmod") == 0) {
      const char *a = t.get_string(p);
      __int64_t b = t.get_int();
      client->chmod(a, b);
    } else if (strcmp(op, "chown") == 0) {
      const char *a = t.get_string(p);
      __int64_t b = t.get_int();
      __int64_t c = t.get_int();
      client->chown(a, b, c);
    } else if (strcmp(op, "utime") == 0) {
      const char *a = t.get_string(p);
      __int64_t b = t.get_int();
      __int64_t c = t.get_int();
      struct utimbuf u;
      u.actime = b;
      u.modtime = c;
      client->utime(a, &u);
    } else if (strcmp(op, "mknod") == 0) {
      const char *a = t.get_string(p);
      __int64_t b = t.get_int();
      client->mknod(a, b);
    } else if (strcmp(op, "getdir") == 0) {
      const char *a = t.get_string(p);
      map<string,inode_t> contents;
      client->getdir(a, contents);
    } else if (strcmp(op, "open") == 0) {
      const char *a = t.get_string(p);
      __int64_t b = t.get_int(); 
      __int64_t id = t.get_int();
      __int64_t fh = client->open(a, b);
      open_files[id] = fh;
    } else if (strcmp(op, "close") == 0) {
      __int64_t id = t.get_int();
      __int64_t fh = open_files[id];
      if (fh > 0) client->close(fh);
      open_files.erase(id);
    } else if (strcmp(op, "truncate") == 0) {
      const char *a = t.get_string(p);
      __int64_t b = t.get_int();
      client->truncate(a,b);
    } else if (strcmp(op, "read") == 0) {
      __int64_t id = t.get_int();
      __int64_t fh = open_files[id];
      int size = t.get_int();
      int off = t.get_int();
      char *buf = new char[size];
      client->read(fh, buf, size, off);
      delete[] buf;
    } else if (strcmp(op, "lseek") == 0) {
      __int64_t id = t.get_int();
      __int64_t fh = open_files[id];
      int off = t.get_int();
      int whence = t.get_int();
      client->lseek(fh, off, whence);
    }else if (strcmp(op, "write") == 0) {
      __int64_t id = t.get_int();
      __int64_t fh = open_files[id];
      int size = t.get_int();
      int off = t.get_int();
      char *buf = new char[size];
      memset(buf, 1, size);            // let's write 1's!
      client->write(fh, buf, size, off);
      delete[] buf;
    } else if (strcmp(op, "fsync") == 0) {
      assert(0);
    } else 
      assert(0);
  }

  // close open files
  for (map<__int64_t, __int64_t>::iterator fi = open_files.begin();
       fi != open_files.end();
       fi++) {
    dout(1) << "leftover close " << fi->second << endl;
    if (fi->second > 0) client->close(fi->second);
  }
  
  return 0;
}


int SyntheticClient::clean_dir(string& basedir)
{
  // read dir
  map<string, inode_t> contents;
  int r = client->getdir(basedir.c_str(), contents);
  if (r < 0) {
    dout(1) << "readdir on " << basedir << " returns " << r << endl;
    return r;
  }

  for (map<string, inode_t>::iterator it = contents.begin();
       it != contents.end();
       it++) {
    if (it->first == ".") continue;
    if (it->first == "..") continue;
    string file = basedir + "/" + it->first;

    if (time_to_stop()) break;

    struct stat st;
    int r = client->lstat(file.c_str(), &st);
    if (r < 0) {
      dout(1) << "stat error on " << file << " r=" << r << endl;
      continue;
    }

    if ((st.st_mode & INODE_TYPE_MASK) == INODE_MODE_DIR) {
      clean_dir(file);
      client->rmdir(file.c_str());
    } else {
      client->unlink(file.c_str());
    }
  }

  return 0;

}


int SyntheticClient::full_walk(string& basedir) 
{
  if (time_to_stop()) return -1;

  list<string> dirq;
  dirq.push_back(basedir);

  while (!dirq.empty()) {
    string dir = dirq.front();
    dirq.pop_front();

    // read dir
    map<string, inode_t> contents;
    int r = client->getdir(dir.c_str(), contents);
    if (r < 0) {
      dout(1) << "readdir on " << dir << " returns " << r << endl;
      continue;
    }
    
    for (map<string, inode_t>::iterator it = contents.begin();
	 it != contents.end();
	 it++) {
      if (it->first == ".") continue;
      if (it->first == "..") continue;
      string file = dir + "/" + it->first;
      
      struct stat st;
      int r = client->lstat(file.c_str(), &st);
      if (r < 0) {
	dout(1) << "stat error on " << file << " r=" << r << endl;
	continue;
      }
      
      if ((st.st_mode & INODE_TYPE_MASK) == INODE_MODE_DIR) {
	dirq.push_back(file);
      }
    }
  }

  return 0;
}

int SyntheticClient::make_dirs(const char *basedir, int dirs, int files, int depth)
{
  if (time_to_stop()) return 0;

  // make sure base dir exists
  int r = client->mkdir(basedir, 0755);
  if (r != 0) {
    dout(1) << "can't make base dir? " << basedir << endl;
    return -1;
  }

  // children
  char d[500];
  dout(3) << "make_dirs " << basedir << " dirs " << dirs << " files " << files << " depth " << depth << endl;
  for (int i=0; i<files; i++) {
    sprintf(d,"%s/file.%d", basedir, i);
    client->mknod(d, 0644);
  }

  if (depth == 0) return 0;

  for (int i=0; i<dirs; i++) {
    sprintf(d, "%s/dir.%d", basedir, i);
    make_dirs(d, dirs, files, depth-1);
  }
  
  return 0;
}

int SyntheticClient::stat_dirs(const char *basedir, int dirs, int files, int depth)
{
  if (time_to_stop()) return 0;

  // make sure base dir exists
  struct stat st;
  int r = client->lstat(basedir, &st);
  if (r != 0) {
    dout(1) << "can't make base dir? " << basedir << endl;
    return -1;
  }

  // children
  char d[500];
  dout(3) << "stat_dirs " << basedir << " dirs " << dirs << " files " << files << " depth " << depth << endl;
  for (int i=0; i<files; i++) {
    sprintf(d,"%s/file.%d", basedir, i);
    client->lstat(d, &st);
  }

  if (depth == 0) return 0;

  for (int i=0; i<dirs; i++) {
    sprintf(d, "%s/dir.%d", basedir, i);
    stat_dirs(d, dirs, files, depth-1);
  }
  
  return 0;
}
int SyntheticClient::read_dirs(const char *basedir, int dirs, int files, int depth)
{
  if (time_to_stop()) return 0;

  struct stat st;

  // children
  char d[500];
  dout(3) << "read_dirs " << basedir << " dirs " << dirs << " files " << files << " depth " << depth << endl;

  map<string,inode_t> contents;
  utime_t s = g_clock.now();
  int r = client->getdir(basedir, contents);
  utime_t e = g_clock.now();
  e -= s;
  if (client_logger) client_logger->finc("readdir", e);
  if (r < 0) {
    dout(0) << "read_dirs couldn't readdir " << basedir << ", stopping" << endl;
    return -1;
  }

  for (int i=0; i<files; i++) {
    sprintf(d,"%s/file.%d", basedir, i);
    utime_t s = g_clock.now();
    if (client->lstat(d, &st) < 0) {
      dout(2) << "read_dirs failed stat on " << d << ", stopping" << endl;
      return -1;
    }
    utime_t e = g_clock.now();
    e -= s;
    if (client_logger) client_logger->finc("stat", e);
  }

  if (depth > 0) 
    for (int i=0; i<dirs; i++) {
      sprintf(d, "%s/dir.%d", basedir, i);
      if (read_dirs(d, dirs, files, depth-1) < 0) return -1;
    }

  return 0;
}


int SyntheticClient::make_files(int num, int count, int priv, bool more)
{
  int whoami = client->get_nodeid();
  char d[255];

  if (priv) {
    for (int c=0; c<count; c++) {
      sprintf(d,"dir.%d.run%d", whoami, c);
      client->mkdir(d, 0755);
    }
  } else {
    // shared
    if (whoami == 0) {
      for (int c=0; c<count; c++) {
        sprintf(d,"dir.%d.run%d", 0, c);
        client->mkdir(d, 0755);
      }
    } else {
      sleep(5);
    }
  }
  
  // files
  struct stat st;
  for (int c=0; c<count; c++) {
    for (int n=0; n<num; n++) {
      sprintf(d,"dir.%d.run%d/file.client%d.%d", priv ? whoami:0, c, whoami, n);

      client->mknod(d, 0644);

      if (more) {
        client->lstat(d, &st);
        int fd = client->open(d, O_RDONLY);
        client->unlink(d);
        client->close(fd);
      }

      if (time_to_stop()) return 0;
    }
  }
  
  return 0;
}


int SyntheticClient::create_shared(int num)
{
  // files
  char d[255];
  for (int n=0; n<num; n++) {
    sprintf(d,"file.%d", n);
    client->mknod(d, 0644);
  }
  
  return 0;
}

int SyntheticClient::open_shared(int num, int count)
{
  // files
  char d[255];
  for (int c=0; c<count; c++) {
    // open
    list<int> fds;
    for (int n=0; n<num; n++) {
      sprintf(d,"file.%d", n);
      int fd = client->open(d,O_RDONLY);
      fds.push_back(fd);
    }

    while (!fds.empty()) {
      int fd = fds.front();
      fds.pop_front();
      client->close(fd);
    }
  }
  
  return 0;
}


int SyntheticClient::write_file(string& fn, int size, int wrsize)   // size is in MB, wrsize in bytes
{
  //__uint64_t wrsize = 1024*256;
  char *buf = new char[wrsize+100];   // 1 MB
  memset(buf, 7, wrsize);
  __uint64_t chunks = (__uint64_t)size * (__uint64_t)(1024*1024) / (__uint64_t)wrsize;

  int fd = client->open(fn.c_str(), O_RDWR|O_CREAT);
  dout(5) << "writing to " << fn << " fd " << fd << endl;
  if (fd < 0) return fd;

  for (unsigned i=0; i<chunks; i++) {
    if (time_to_stop()) {
      dout(0) << "stopping" << endl;
      break;
    }
    dout(2) << "writing block " << i << "/" << chunks << endl;
    
    // fill buf with a 16 byte fingerprint
    // 64 bits : file offset
    // 64 bits : client id
    // = 128 bits (16 bytes)
    __uint64_t *p = (__uint64_t*)buf;
    while ((char*)p < buf + wrsize) {
      *p = i*wrsize + (char*)p - buf;      
      p++;
      *p = client->get_nodeid();
      p++;
    }

    client->write(fd, buf, wrsize, i*wrsize);
  }
  
  client->close(fd);
  delete[] buf;

  return 0;
}

int SyntheticClient::write_batch(int nfile, int size, int wrsize)
{
  for (int i=0; i<nfile; i++) {
      string sarg1 = get_sarg(i);
    dout(0) << "Write file " << sarg1 << endl;
    write_file(sarg1, size, wrsize);
  }
  return 0;
}

int SyntheticClient::read_file(string& fn, int size, int rdsize)   // size is in MB, wrsize in bytes
{
  char *buf = new char[rdsize]; 
  memset(buf, 1, rdsize);
  __uint64_t chunks = (__uint64_t)size * (__uint64_t)(1024*1024) / (__uint64_t)rdsize;

  int fd = client->open(fn.c_str(), O_RDONLY);
  dout(5) << "reading from " << fn << " fd " << fd << endl;
  if (fd < 0) return fd;

  for (unsigned i=0; i<chunks; i++) {
    if (time_to_stop()) break;
    dout(2) << "reading block " << i << "/" << chunks << endl;
    int r = client->read(fd, buf, rdsize, i*rdsize);
    if (r < rdsize) {
      dout(1) << "read_file got r = " << r << ", probably end of file" << endl;
      break;
    }

    // verify fingerprint
    int bad = 0;
    __int64_t *p = (__int64_t*)buf;
    __int64_t readoff, readclient;
    while ((char*)p + 32 < buf + rdsize) {
      readoff = *p;
      __int64_t wantoff = i*rdsize + (__int64_t)((char*)p - buf);
      p++;
      readclient = *p;
      p++;
      if (readoff != wantoff ||
	  readclient != client->get_nodeid()) {
        if (!bad)
          dout(0) << "WARNING: wrong data from OSD, block says fileoffset=" << readoff << " client=" << readclient
		  << ", should be offset " << wantoff << " clietn " << client->get_nodeid()
		  << endl;
        bad++;
      }
    }
    if (bad) 
      dout(0) << " + " << (bad-1) << " other bad 16-byte bits in this block" << endl;
  }
  
  client->close(fd);
  delete[] buf;

  return 0;
}



int SyntheticClient::random_walk(int num_req)
{
  int left = num_req;

  //dout(1) << "random_walk() will do " << left << " ops" << endl;

  init_op_dist();  // set up metadata op distribution
 
  while (left > 0) {
    left--;

    if (time_to_stop()) break;

    // ascend?
    if (cwd.depth() && !roll_die(::pow((double).9, (double)cwd.depth()))) {
      dout(DBL) << "die says up" << endl;
      up();
      continue;
    }

    // descend?
    if (.9*roll_die(::pow((double).9,(double)cwd.depth())) && subdirs.size()) {
      string s = get_random_subdir();
      cwd.add_dentry( s );
      dout(DBL) << "cd " << s << " -> " << cwd << endl;
      clear_dir();
      continue;
    }

    int op = 0;
    filepath path;

    if (contents.empty() && roll_die(.3)) {
      if (did_readdir) {
        dout(DBL) << "empty dir, up" << endl;
        up();
      } else
        op = MDS_OP_READDIR;
    } else {
      op = op_dist.sample();
    }
    //dout(DBL) << "op is " << op << endl;

    int r = 0;

    // do op
    if (op == MDS_OP_UNLINK) {
      if (contents.empty())
        op = MDS_OP_READDIR;
      else 
        r = client->unlink( get_random_sub() );   // will fail on dirs
    }
     
    if (op == MDS_OP_RENAME) {
      if (contents.empty())
        op = MDS_OP_READDIR;
      else {
        r = client->rename( get_random_sub(), make_sub("ren") );
      }
    }
    
    if (op == MDS_OP_MKDIR) {
      r = client->mkdir( make_sub("mkdir"), 0755);
    }
    
    if (op == MDS_OP_RMDIR) {
      if (!subdirs.empty())
        r = client->rmdir( get_random_subdir() );
      else
        r = client->rmdir( cwd.c_str() );     // will pbly fail
    }
    
    if (op == MDS_OP_SYMLINK) {
    }
    
    if (op == MDS_OP_CHMOD) {
      if (contents.empty())
        op = MDS_OP_READDIR;
      else
        r = client->chmod( get_random_sub(), rand() & 0755 );
    }
    
    if (op == MDS_OP_CHOWN) {
      if (contents.empty())         r = client->chown( cwd.c_str(), rand(), rand() );
      else
        r = client->chown( get_random_sub(), rand(), rand() );
    }
     
    if (op == MDS_OP_LINK) {
    }
     
    if (op == MDS_OP_UTIME) {
      struct utimbuf b;
      memset(&b, 1, sizeof(b));
      if (contents.empty()) 
        r = client->utime( cwd.c_str(), &b );
      else
        r = client->utime( get_random_sub(), &b );
    }
    
    if (op == MDS_OP_MKNOD) {
      r = client->mknod( make_sub("mknod"), 0644);
    }
     
    if (op == MDS_OP_OPEN) {
      if (contents.empty())
        op = MDS_OP_READDIR;
      else {
        r = client->open( get_random_sub(), O_RDONLY );
        if (r > 0) {
          assert(open_files.count(r) == 0);
          open_files.insert(r);
        }
      }
    }

    if (op == MDS_OP_RELEASE) {   // actually, close
      if (open_files.empty())
        op = MDS_OP_STAT;
      else {
        int fh = get_random_fh();
        r = client->close( fh );
        if (r == 0) open_files.erase(fh);
      }
    }
    
    if (op == MDS_OP_STAT) {
      struct stat st;
      if (contents.empty()) {
        if (did_readdir) {
          if (roll_die(.1)) {
            dout(DBL) << "stat in empty dir, up" << endl;
            up();
          } else {
            op = MDS_OP_MKNOD;
          }
        } else
          op = MDS_OP_READDIR;
      } else
        r = client->lstat(get_random_sub(), &st);
    }

    if (op == MDS_OP_READDIR) {
      clear_dir();
      
      map<string, inode_t> c;
      r = client->getdir( cwd.c_str(), c );
      
      for (map<string, inode_t>::iterator it = c.begin();
           it != c.end();
           it++) {
        //dout(DBL) << " got " << it->first << endl;
        contents[it->first] = it->second;
        if (it->second.is_dir()) 
          subdirs.insert(it->first);
      }
      
      did_readdir = true;
    }
      
    // errors?
    if (r < 0) {
      // reevaluate cwd.
      //while (cwd.depth()) {
      //if (client->lookup(cwd)) break;   // it's in the cache
        
      //dout(DBL) << "r = " << r << ", client doesn't have " << cwd << ", cd .." << endl;
      dout(DBL) << "r = " << r << ", client may not have " << cwd << ", cd .." << endl;
      up();
      //}      
    }
  }

  // close files
  dout(DBL) << "closing files" << endl;
  while (!open_files.empty()) {
    int fh = get_random_fh();
    int r = client->close( fh );
    if (r == 0) open_files.erase(fh);
  }

  dout(DBL) << "done" << endl;
  return 0;
}




void SyntheticClient::make_dir_mess(const char *basedir, int n)
{
  vector<string> dirs;
  
  dirs.push_back(basedir);
  dirs.push_back(basedir);
  
  client->mkdir(basedir, 0755);

  // motivation:
  //  P(dir) ~ subdirs_of(dir) + 2
  // from 5-year metadata workload paper in fast'07

  // create dirs
  for (int i=0; i<n; i++) {
    // pick a dir
    int k = rand() % dirs.size();
    string parent = dirs[k];
    
    // pick a name
    std::stringstream ss;
    ss << parent << "/" << i;
    string dir;
    ss >> dir;

    // update dirs
    dirs.push_back(parent);
    dirs.push_back(dir);
    dirs.push_back(dir);

    // do it
    client->mkdir(dir.c_str(), 0755);
  }
    
  
}

