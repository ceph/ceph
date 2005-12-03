#ifndef __THREAD_H
#define __THREAD_H

#include <pthread.h>

class Thread {
 private:
  pthread_t thread_id;

 public:
  Thread() : thread_id(0) {}

  pthread_t &get_thread_id() { return thread_id; }
  bool is_started() { return thread_id != 0; }

  virtual void *entry() = 0;

 private:
  static void *_entry_func(void *arg) {
	return ((Thread*)arg)->entry();
  }

 public:
  int create() {
	return pthread_create( &thread_id, NULL, _entry_func, (void*)this );
  }

  int join(void **prval = 0) {
	if (thread_id == 0) return -1;   // never started.
	int status = pthread_join(thread_id, prval);
	if (status == 0) thread_id = 0;
	return status;
  }
};

#endif
