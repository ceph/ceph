
#ifndef __MESSAGE_H
#define __MESSAGE_H

#define MSG_PING       1
#define MSG_FWD        2
#define MSG_DISCOVER   3

#define MSG_SUBSYS_SERVER   1
#define MSG_SUBSYS_BALANCER 2
#define MSG_SUBSYS_MDSTORE  3
#define MSG_SUBSYS_MDLOG    4





// abstract Message class

class Message {
 protected:
  char *serialized;
  unsigned long serial_len;
  int type;
  int subsys;

 public:
  Message() { 
	serialized = 0;
	serial_len = 0;
  };
  ~Message() {
	if (serialized) { delete serialized; serialized = 0; }
  };

  // type
  int get_type() {
	return type;
  }
  int get_subsys() {
	return subsys;
  }
  
  // serialization
  virtual unsigned long serialize() = 0;
  void *get_serialized() {
	return serialized;
  }
  unsigned long get_serialized_len() {
	return serial_len;
  }  
 
  friend class Messenger;
};


#endif
