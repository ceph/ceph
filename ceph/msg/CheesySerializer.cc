
#include "CheesySerializer.h"
#include "Message.h"
#include "Messenger.h"

#include <iostream>
using namespace std;

#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "serializer: "

#define DEBUGLVL  10    // debug level of output

// ---------
// incoming messages

void CheesySerializer::dispatch(Message *m)
{
  long pcid = m->get_pcid();
  Dispatcher *dispatcher = get_dispatcher();

  lock.Lock();

  // was i expecting it?
  if (call_cond.count(pcid)) {
	// yes, this is a reply to a pending call.
	dout(DEBUGLVL) << "dispatch got reply for " << pcid << " " << m << endl;
	call_reply[pcid] = m;     // set reply
	int r = call_cond[pcid]->Signal();
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
  //m->set_pcid(0);
  messenger->send_message(m, dest, port, fromport);
}

Message *CheesySerializer::sendrecv(Message *m, msg_addr_t dest, int port)
{
  int fromport = 0;

  Cond *cond = new Cond();

  // make up a pcid that is unique (to me!)
  /* NOTE: since request+replies are matched up on pcid's alone, it means that
	 two nodes using this mechanism can't do calls of each other or else their
	 pcid's might overlap.  
	 This should be fine. only the Client uses this so far (not MDS).
	 If OSDs want to use this, though, this must be made smarter!!!
  */
  long pcid = ++last_pcid;
  m->set_pcid(pcid);

  lock.Lock();

  dout(DEBUGLVL) << "sendrecv sending " << m << " on pcid " << pcid << endl;

  // add call records
  assert(call_cond.count(pcid) == 0);  // pcid should be UNIQUE
  call_cond[pcid] = cond;
  call_reply[pcid] = 0;   // no reply yet

  // send.  drop locks in case send_message is bad and blocks
  lock.Unlock();
  messenger->send_message(m, dest, port, fromport); 
  lock.Lock();

  // wait?
  if (call_reply[pcid] == 0) {
	dout(DEBUGLVL) << "sendrecv waiting for reply on pcid " << pcid << endl;
	//cout << "wait start, value = " << sem->Value() << endl;
	
	cond->Wait(lock);
  } else {
	dout(DEBUGLVL) << "sendrecv reply is already here on pcid " << pcid << endl;
  }

  // pick up reply
  Message *reply = call_reply[pcid];
  //assert(reply);
  call_reply.erase(pcid);   // remove from call map
  call_cond.erase(pcid);

  dout(DEBUGLVL) << "sendrecv got reply " << reply << " on pcid " << pcid << endl;
  //delete sem;

  lock.Unlock();
  
  return reply;
}


// -------------

int CheesySerializer::shutdown()
{
  dout(1) << "shutdown" << endl;

  // abort any pending sendrecv's
  lock.Lock();
  for (map<long,Cond*>::iterator it = call_cond.begin();
	   it != call_cond.end();
	   it++) {
	dout(1) << "shutdown waking up (hung) pcid " << it->first << endl;
	it->second->Signal();  // wake up!
  }	   
  lock.Unlock();

  // shutdown underlying messenger.
  messenger->shutdown();
}
