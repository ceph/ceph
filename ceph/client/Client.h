#ifndef __CLIENT_H
#define __CLIENT_H

#include "../include/Dispatcher.h"
#include "../include/lru.h"

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

  LRU    cache_lru;
  ClNode *root;
  ClNode *cwd;

  long tid, max_requests;

  vector<string> last_req_dn;

 public:
  Client(MDCluster *mdc, int id, Messenger *m, long req);
  ~Client();
  
  int init();
  int shutdown();

  void done();

  virtual void dispatch(Message *m);

  virtual void assim_reply(MClientReply*);
  virtual void issue_request();

  virtual void send_request(string& p, int op);

  virtual void trim_cache();
};



#endif
