#ifndef __DISPATCHER_H
#define __DISPATCHER_H

class Message;

class Dispatcher {
 public:
  virtual void dispatch(Message *m) = 0;
};

#endif
