
#include <ext/rope>
#include "include/types.h"
#include "include/config.h"
#include "include/Message.h"
#include "include/Messenger.h"
#include <cassert>
#include <iostream>
using namespace std;

#define dout(l)  if (l <= DEBUG_LEVEL) cout


#include "messages/MPing.h"
#include "messages/MOSDRead.h"
#include "messages/MOSDReadReply.h"
#include "messages/MOSDWrite.h"
#include "messages/MOSDWriteReply.h"

#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"

#include "messages/MDirUpdate.h"

Message *
Messenger::decode(crope& ser)
{
  int type;

  // peek at type
  ser.copy(0, sizeof(int), (char*)&type);

  // make message
  Message *m;
  switch(type) {

	// -- with payload --

  case MSG_PING:
	m = new MPing();
	break;

  case MSG_OSD_READ:
	m = new MOSDRead();
	break;

  case MSG_OSD_READREPLY:
	m = new MOSDReadReply();
	break;

  case MSG_OSD_WRITE:
	m = new MOSDWrite();
	break;

  case MSG_OSD_WRITEREPLY:
	m = new MOSDWriteReply();
	break;

	// clients
  case MSG_CLIENT_REQUEST:
	m = new MClientRequest();
	break;

  case MSG_CLIENT_REPLY:
	m = new MClientReply();
	break;

	// mds
  case MSG_MDS_DIRUPDATE:
	m = new MDirUpdate();
	break;

	
	// -- simple messages without payload --

  default:
	dout(1) << "can't decode unknown message type " << type << endl;
	assert(0);
  }
  
  // decode
  m->decode_envelope(ser.substr(0, MSG_ENVELOPE_LEN));
  if (ser.length() > MSG_ENVELOPE_LEN)
	m->decode_payload(ser.substr(MSG_ENVELOPE_LEN,
								 ser.length() - MSG_ENVELOPE_LEN));
  
  // done!
  return m;
}
