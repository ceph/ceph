
#ifndef __OSD_H
#define __OSD_H

#include "Dispatcher.h"

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
  
  void init();

  virtual void dispatch(Message *m);

  void read(MOSDRead *m);
  void write(MOSDWrite *m);
};

#endif
