#ifndef __CLIENT_H
#define __CLIENT_H

#include "Dispatcher.h"

class Messenger;
class Message;

class DentryCache;
class CInode;

class Client : public Dispatcher {
 protected:
  Messenger *messenger;
  int whoami;

  DentryCache *mdcache;

  CInode      *cwd;

  long tid;

 public:
  Client(int id, Messenger *m);
  ~Client();
  
  int init();
  int shutdown();

  virtual void dispatch(Message *m);

  virtual void issue_request();

};



#endif
