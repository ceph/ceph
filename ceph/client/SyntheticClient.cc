
#include "SyntheticClient.h"

#include "include/config.h"


void *synthetic_client_thread_entry(void *ptr)
{
  SyntheticClient *sc = (SyntheticClient*)ptr;
  sc->run();
  return 0;
}


int SyntheticClient::start_thread()
{
  assert(!thread_id);

  pthread_create(&thread_id, NULL, synthetic_client_thread_entry, this);
}

int SyntheticClient::join_thread()
{
  void *rv;
  pthread_join(thread_id, &rv);
}


int SyntheticClient::run()
{
  
}
