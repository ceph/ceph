#ifndef __SYNTHETICCLIENT_H
#define __SYNTHETICCLIENT_H

#include "Client.h"
#include <pthread.h>

class SyntheticClient {
  Client *client;
  int num_req;

  pthread_t thread_id;

 public:
  SyntheticClient(Client *client,
				  int num_req) {
	this->client = client;
	this->num_req = num_req;
	thread_id = 0;
  }

  int start_thread();
  int join_thread();

  int run();
};

#endif
