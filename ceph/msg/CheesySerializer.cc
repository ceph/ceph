
#include "CheesySerializer.h"
#include "Message.h"
#include <iostream>
using namespace std;


void CheesySerializer::dispatch(Message *m)
{
  // i better be expecting it
  assert(waiting_for_reply);

  cout << "dispatch got " << reply << ", waking up waiter" << endl;
  reply = m;
  waiter.Post();
}


void CheesySerializer::send(Message *m, msg_addr_t dest, int port, int fromport)
{
  messenger->send_message(m, dest, port, fromport);
}

Message *CheesySerializer::sendrecv(Message *m, msg_addr_t dest, int port, int fromport)
{
  messenger->send_message(m, dest, port, fromport);
  waiting_for_reply = true;
  waiter.Wait();
  return reply;
}


// thread crap
void *cheesyserializer_starter(void *pthis)
{
  CheesySerializer *pt = (CheesySerializer*)pthis;
  pt->message_thread();
}

void CheesySerializer::message_thread()
{
  
}
