
#include "include/types.h"
#include "include/Client.h"
#include "include/Messenger.h"
#include "include/Message.h"

#include "include/MDS.h"
#include "include/MDCache.h"

#include "messages/MClientRequest.h"

Client::Client(int id, Messenger *m)
{
  whoami = id;
  messenger = m;
  cwd = 0;
  mdcache = new DentryCache();
  tid = 0;
}

Client::~Client()
{
  if (messenger) { delete messenger; messenger = 0; }
  if (mdcache) { delete mdcache; mdcache = 0; }
}

int Client::init()
{
  messenger->set_dispatcher(this);
  return 0;
}

int Client::shutdown()
{
  mdcache->clear();
  messenger->shutdown();
  return 0;
}



// dispatch

void Client::dispatch(Message *m) 
{
  switch (m->get_type()) {
  case MSG_OSD_READREPLY:
  case MSG_OSD_WRITEREPLY:
	//assim_response(m);

	issue_request();	
	break;

  default:
	cout << "client" << whoami << " got unknown message " << m->get_type() << endl;
  }
}
	

/*void Client::assim_response()
{

}
*/


void Client::issue_request()
{
  MClientRequest *req = new MClientRequest(tid++, MDS_OP_STAT);
  req->ino = 1;
  messenger->send_message(req,
						  MSG_ADDR_MDS(0), MDS_PORT_SERVER,
						  0);
}
