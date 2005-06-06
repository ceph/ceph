#ifndef __SYNTHETICCLIENT_H
#define __SYNTHETICCLIENT_H

#include <pthread.h>

#include "Client.h"
#include "include/Distribution.h"

class SyntheticClient {
  Client *client;
  int num_req;

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
  SyntheticClient(Client *client,
				  int num_req) {
	this->client = client;
	this->num_req = num_req;
	thread_id = 0;
  }

  int start_thread();
  int join_thread();

  int run();
};

#endif
