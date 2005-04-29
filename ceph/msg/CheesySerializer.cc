
#include "CheesySerializer.h"
#include "Message.h"
#include "Messenger.h"

#include <iostream>
using namespace std;


// ---------
// incoming messages

void CheesySerializer::dispatch(Message *m)
{
  long tid = m->get_tid();

  lock.Lock();

  // was i expecting it?
  if (call_sem.count(tid)) {
	// yes, this is a reply to a pending call.
	cout << "serializer: dispatch got reply for " << tid << " " << m << endl;
	call_reply[tid] = m;     // set reply
	call_sem[tid]->Post();
	lock.Unlock();
  } else {
	// no, this is an unsolicited message.
	lock.Unlock();	
	cout << "serializer: dispatch got unsolicited message" << m << endl;
	dispatcher->dispatch(m);
  }
}


// ---------
// outgoing messages

void CheesySerializer::send(Message *m, msg_addr_t dest, int port, int fromport)
{
  // just pass it on to the messenger
  cout << "serializer: send " << m << endl;
  messenger->send_message(m, dest, port, fromport);
}

Message *CheesySerializer::sendrecv(Message *m, msg_addr_t dest, int port, int fromport)
{
  Semaphore *sem = new Semaphore();

  // make up a transaction number that is unique (to me!)
  /* NOTE: since request+replies are matched up on tid's alone, it means that
	 two nodes using this mechanism can't do calls of each other or else their
	 tid's might overlap.  
	 This should be fine. only the Client uses this so far (not MDS).
	 If OSDs want to use this, though, this must be made smarter!!!
  */
  long tid = ++last_tid;
  m->set_tid(tid);

  cout << "serializer: sendrecv sending " << m << " on tid " << tid << endl;

  // add call records
  lock.Lock();
  assert(call_sem.count(tid) == 0);  // tid should be UNIQUE
  call_sem[tid] = sem;
  call_reply[tid] = 0;   // no reply yet
  lock.Unlock();

  // send
  messenger->send_message(m, dest, port, fromport);
  
  // wait
  cout << "serializer: sendrecv waiting for reply on tid " << tid << endl;
  sem->Wait();

  // pick up reply
  lock.Lock();
  Message *reply = call_reply[tid];
  assert(reply);
  call_reply.erase(tid);   // remove from call map
  call_sem.erase(tid);
  lock.Unlock();

  delete sem;
  
  cout << "serializer: sendrecv got reply " << reply << " on tid " << tid << endl;
  return reply;
}

