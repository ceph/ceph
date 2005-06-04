
#ifndef __OSD_H
#define __OSD_H

#include "msg/Dispatcher.h"

#include "common/Mutex.h"

class Messenger;
class MOSDRead;
class MOSDWrite;
class Message;
class ObjectStore;

class OSD : public Dispatcher {
 protected:
  Messenger *messenger;
  int whoami;

  ObjectStore *store;

  Mutex osd_lock;

 public:
  OSD(int id, Messenger *m);
  ~OSD();
  
  int init();
  int shutdown();

  virtual void dispatch(Message *m);

  void handle_ping(class MPing *m);
  void handle_op(class MOSDOp *m);
  void read(MOSDRead *m);
  void write(MOSDWrite *m);
};

#endif
