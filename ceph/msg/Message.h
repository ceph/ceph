
#ifndef __MESSAGE_H
#define __MESSAGE_H

#define MSG_PING       1
#define MSG_FWD        2
#define MSG_DISCOVER   3


virtual class Message {
 protected:
  char *serialized;
  int messagetype;

 public:
  Message() { 
	serialized = 0;
  };
  ~Message() {
	if (serialized) { delete serialized; serialized = 0; }
  };
  
  virtual void *serialize() = 0;
  
  friend class Messenger;
};


#endif
