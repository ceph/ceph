
#include "CheesySerializer.h"
#include "Message.h"
#include <iostream>
using namespace std;


void CheesySerializer::dispatch(Message *m)
{
  // i better be expecting it
  assert(waiting_for_reply);

  cout << "serializer: dispatch got " << reply << ", waking up waiter" << endl;
  reply = m;
  waiter.Post();
}


void CheesySerializer::send(Message *m, msg_addr_t dest, int port, int fromport)
{
  cout << "serializer: send " << m << endl;
  messenger->send_message(m, dest, port, fromport);
}

Message *CheesySerializer::sendrecv(Message *m, msg_addr_t dest, int port, int fromport)
{
  cout << "serializer: sendrecv " << m << endl;
  messenger->send_message(m, dest, port, fromport);
  waiting_for_reply = true;
  cout << "serializer: sendrecv waiting " << endl;
  waiter.Wait();
  cout << "serializer: sendrecv got " << reply << endl;
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
