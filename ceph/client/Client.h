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

class Client : public Dispatcher {
 protected:
  Messenger *messenger;
  int whoami;

  LRU    cache_lru;
  ClNode *root;
  ClNode *cwd;

  long tid;

 public:
  Client(int id, Messenger *m);
  ~Client();
  
  int init();
  int shutdown();

  virtual void dispatch(Message *m);

  virtual void assim_reply(MClientReply*);
  virtual void issue_request();

  virtual void send_request(string& p);
};



#endif
