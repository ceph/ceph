#ifndef __MPIMESSENGER_H
#define __MPIMESSENGER_H

class MPIMessenger : public Messenger {
 protected:
  int whoami;

  class Logger *logger;
  
 public:
  MPIMessenger(long me);
  ~MPIMessenger();

  virtual int init(Dispatcher *dis);
  virtual int shutdown();
  virtual bool send_message(Message *m, long dest, int port=0, int fromport=0);
  virtual int wait_message(time_t seconds);

  virtual int loop();
};

#endif
