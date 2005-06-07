
#ifndef __OSD_H
#define __OSD_H

#include "msg/Dispatcher.h"

#include "common/Mutex.h"

class Messenger;
class MOSDRead;
class MOSDWrite;
class Message;
class ObjectStore;
class HostMonitor;

class OSD : public Dispatcher {
 protected:
  Messenger *messenger;
  int whoami;

  ObjectStore *store;
  HostMonitor *monitor;

  Mutex osd_lock;

 public:
  OSD(int id, Messenger *m);
  ~OSD();
  
  int init();
  int shutdown();

  virtual void dispatch(Message *m);

  void handle_ping(class MPing *m);
  void handle_op(class MOSDOp *m);
  void handle_read(MOSDRead *m);
  void handle_write(MOSDWrite *m);
};

#endif
