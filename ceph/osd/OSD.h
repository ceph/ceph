
#ifndef __OSD_H
#define __OSD_H

#include "msg/Dispatcher.h"

class Messenger;
class MOSDRead;
class MOSDWrite;
class Message;


class OSD : public Dispatcher {
 protected:
  Messenger *messenger;
  int whoami;

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
