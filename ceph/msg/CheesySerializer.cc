
#include "CheesySerializer.h"
#include "Message.h"
#include "Messenger.h"

#include <iostream>
using namespace std;

#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "serializer: "

#define DEBUGLVL  13    // debug level of output

// ---------
// incoming messages

void CheesySerializer::dispatch(Message *m)
{
  long pcid = m->get_pcid();

  lock.Lock();

  // was i expecting it?
  if (call_sem.count(pcid)) {
	// yes, this is a reply to a pending call.
	dout(DEBUGLVL) << "dispatch got reply for " << pcid << " " << m << endl;
	call_reply[pcid] = m;     // set reply
	int r = call_sem[pcid]->Post();
	//cout << "post = " << r << endl;
	lock.Unlock();
  } else {
	// no, this is an unsolicited message.
	lock.Unlock();	
	dout(DEBUGLVL) << "dispatch got unsolicited message pcid " << pcid << " m " << m << endl;
	dispatcher->dispatch(m);
  }
}


// ---------
// outgoing messages

int CheesySerializer::send_message(Message *m, msg_addr_t dest, int port, int fromport)
{
  // just pass it on to the messenger
  dout(DEBUGLVL) << "send " << m << endl;
  m->set_pcid(0);
  messenger->send_message(m, dest, port, fromport);
}

Message *CheesySerializer::sendrecv(Message *m, msg_addr_t dest, int port)
{
  int fromport = 0;

  static Semaphore stsem;
  Semaphore *sem = &stsem;//new Semaphore();

  // make up a pcid that is unique (to me!)
  /* NOTE: since request+replies are matched up on pcid's alone, it means that
	 two nodes using this mechanism can't do calls of each other or else their
	 pcid's might overlap.  
	 This should be fine. only the Client uses this so far (not MDS).
	 If OSDs want to use this, though, this must be made smarter!!!
  */
  long pcid = ++last_pcid;
  m->set_pcid(pcid);

  dout(DEBUGLVL) << "sendrecv sending " << m << " on pcid " << pcid << endl;

  // add call records
  lock.Lock();
  assert(call_sem.count(pcid) == 0);  // pcid should be UNIQUE
  call_sem[pcid] = sem;
  call_reply[pcid] = 0;   // no reply yet
  lock.Unlock();

  // send
  messenger->send_message(m, dest, port, fromport);
  
  // wait
  dout(DEBUGLVL) << "sendrecv waiting for reply on pcid " << pcid << endl;
  //cout << "wait start, value = " << sem->Value() << endl;
  sem->Wait();


  // pick up reply
  lock.Lock();
  Message *reply = call_reply[pcid];
  assert(reply);
  call_reply.erase(pcid);   // remove from call map
  call_sem.erase(pcid);
  lock.Unlock();

  dout(DEBUGLVL) << "sendrecv got reply " << reply << " on pcid " << pcid << endl;
  //delete sem;
  
  return reply;
}

