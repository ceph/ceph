#ifndef __SYNTHETICCLIENT_H
#define __SYNTHETICCLIENT_H

#include <pthread.h>

#include "Client.h"
#include "include/Distribution.h"

#define SYNCLIENT_MODE_RANDOMWALK  1
#define SYNCLIENT_MODE_FULLWALK    2
#define SYNCLIENT_MODE_MAKEDIRS    3
#define SYNCLIENT_MODE_WRITEFILE   4

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

  const char *get_random_subdir() {
	assert(!subdirs.empty());
	int r = rand() % subdirs.size();
	set<string>::iterator it = subdirs.begin();
	while (r--) it++;
	return (*it).c_str();

  }
  const char *get_random_sub() {
	assert(!contents.empty());
	int r = rand() % contents.size();
	map<string,inode_t>::iterator it = contents.begin();
	while (r--) it++;
	return it->first.c_str();
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
  SyntheticClient(Client *client) {
	this->client = client;
	thread_id = 0;

	did_readdir = false;
  }

  int start_thread();
  int join_thread();

  int run();

  // run() will do one of these things:
  int mode;
  string sarg1;
  int iarg1, iarg2, iarg3;
  
  int full_walk(string& fromdir);
  int random_walk(int n);
  int make_dirs(const char *basedir, int dirs, int files, int depth);
  int write_file(string& fn, int mb, int chunk);
  int read_file(string& fn, int mb, int chunk);

};

#endif
