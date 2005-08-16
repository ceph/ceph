#ifndef __SYNTHETICCLIENT_H
#define __SYNTHETICCLIENT_H

#include <pthread.h>

#include "Client.h"
#include "include/Distribution.h"

#include "Trace.h"

#define SYNCLIENT_MODE_RANDOMWALK  1
#define SYNCLIENT_MODE_FULLWALK    2
#define SYNCLIENT_MODE_MAKEDIRS    3
#define SYNCLIENT_MODE_WRITEFILE   4
#define SYNCLIENT_MODE_READFILE    5
#define SYNCLIENT_MODE_UNTIL       6
#define SYNCLIENT_MODE_REPEATWALK  7
#define SYNCLIENT_MODE_WRITEBATCH  21

#define SYNCLIENT_MODE_TRACE       20
#define SYNCLIENT_MODE_TRACEOPENSSH 8
#define SYNCLIENT_MODE_TRACEOPENSSHLIB 9
#define SYNCLIENT_MODE_TRACEINCLUDE 10
#define SYNCLIENT_MODE_TRACELIB 11

#define SYNCLIENT_MODE_RANDOMSLEEP  12

void parse_syn_options(vector<char*>& args);

class SyntheticClient {
  Client *client;

  pthread_t thread_id;

  Distribution op_dist;

  void init_op_dist();
  int get_op();

  
  filepath             cwd;
  map<string, inode_t> contents;
  set<string>          subdirs;
  bool                 did_readdir;
  set<int>             open_files;

  void up();

  void clear_dir() {
	contents.clear();
	subdirs.clear();
	did_readdir = false;
  }

  int get_random_fh() {
	int r = rand() % open_files.size();
	set<int>::iterator it = open_files.begin();
	while (r--) it++;
	return *it;
  }


  filepath n1;
  const char *get_random_subdir() {
	assert(!subdirs.empty());
	int r = ((rand() % subdirs.size()) + (rand() % subdirs.size())) / 2;  // non-uniform distn
	set<string>::iterator it = subdirs.begin();
	while (r--) it++;

	n1 = cwd;
	n1.add_dentry( *it );
	return n1.get_path().c_str();
  }
  filepath n2;
  const char *get_random_sub() {
	assert(!contents.empty());
	int r = ((rand() % contents.size()) + (rand() % contents.size())) / 2;  // non-uniform distn
	if (cwd.depth() && cwd.last_bit().length()) 
	  r += cwd.last_bit().c_str()[0];                                         // slightly permuted
	r %= contents.size();

	map<string,inode_t>::iterator it = contents.begin();
	while (r--) it++;

	n2 = cwd;
	n2.add_dentry( it->first );
	return n2.get_path().c_str();
  }
  
  filepath sub;
  char sub_s[50];
  const char *make_sub(char *base) {
	sprintf(sub_s, "%s.%d", base, rand() % 100);
	string f = sub_s;
	sub = cwd;
	sub.add_dentry(f);
	return sub.c_str();
  }

 public:
  SyntheticClient(Client *client);

  int start_thread();
  int join_thread();

  int run();

  // run() will do one of these things:
  list<int> modes;
  list<string> sargs;
  list<int> iargs;
  utime_t run_start;
  utime_t run_until;

  string get_sarg(int seq);

  bool time_to_stop() {
	if (run_until.sec() && g_clock.now() > run_until) 
	  return true;
	else
	  return false;
  }

  string compose_path(string& prefix, char *rest) {
	return prefix + rest;
  }

  int full_walk(string& fromdir);
  int random_walk(int n);
  int make_dirs(const char *basedir, int dirs, int files, int depth);
  int write_file(string& fn, int mb, int chunk);
  int write_batch(int nfile, int mb, int chunk);
  int read_file(string& fn, int mb, int chunk);

  int clean_dir(string& basedir);

  int play_trace(Trace& t, string& prefix);

};

#endif
