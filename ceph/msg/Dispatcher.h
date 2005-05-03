#ifndef __DISPATCHER_H
#define __DISPATCHER_H

#include "Message.h"

class Dispatcher {
 public:
  virtual void dispatch(Message *m) = 0;
};

#endif
