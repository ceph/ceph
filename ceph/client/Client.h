#ifndef __CLIENT_H
#define __CLIENT_H

#include "../include/Dispatcher.h"
#include "../include/lru.h"
#include "ClNode.h"

#include <map>
using namespace std;

class Messenger;
class Message;

class DentryCache;
class CInode;
class ClNode;
class MClientReply;
class MDCluster;

class Client : public Dispatcher {
 protected:
  MDCluster *mdcluster;
  Messenger *messenger;
  int whoami;

  multimap<inodeno_t, int> open_files;
  multimap<inodeno_t, int> open_files_sync;
  bool did_close_all;

  LRU    cache_lru;
  ClNode *root;
  ClNode *cwd;

  map<inodeno_t,ClNode*>  node_map;

  long tid, max_requests;

  vector<string> last_req_dn;

 public:
  Client(MDCluster *mdc, int id, Messenger *m, long req);
  ~Client();
  
  int init();
  int shutdown();

  ClNode *get_node(inodeno_t ino) {
	map<inodeno_t,ClNode*>::iterator it = node_map.find(ino);
	if (it == node_map.end()) return NULL;
	return it->second;
  }
  void add_node(ClNode *n) {
	if (node_map.count(n->ino) == 0)
	  node_map.insert(pair<inodeno_t,ClNode*>(n->ino, n));
  }
  void remove_node(ClNode *n) {
	if (cwd == n) cwd = root;
	node_map.erase(n->ino);
	cache_lru.lru_remove(n);
	delete n;
  }
  void detach_node(ClNode *n) {
	if (cwd == n) cwd = root;
	n->detach();
  }

  void done();

  virtual void dispatch(Message *m);

  virtual void assim_reply(MClientReply*);
  virtual void issue_request();

  virtual void send_request(string& p, int op, string& arg);
  void close_a_file();
  bool is_open(ClNode *n);
  bool is_sync(ClNode *n);

  void handle_sync_start(class MInodeSyncStart *m);
  void handle_sync_release(class MInodeSyncRelease *m);

  virtual void trim_cache();
};



#endif
