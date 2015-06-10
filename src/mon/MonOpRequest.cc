#include <sstream>
#include "msg/Messenger.h"
#include "msg/Message.h"
#include "messages/MRoute.h"

#include "mon/MonOpRequest.h"
#include "mon/Session.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, const MonOpRequest *op) {
  *_dout << "optracker foo ";
  return *_dout;
}

void MonOpRequest::send_reply(Message *reply)
{
  if (!session) {
    dout(2) << "send_reply no session, dropping reply " << *reply
	    << " to " << request << " " << *request << dendl;
    reply->put();
    mark_event("reply: no session");
    return;
  }

  if (!session->con && !session->proxy_con) {
    dout(2) << "send_reply no connection, dropping reply " << *reply
	    << " to " << request << " " << *request << dendl;
    reply->put();
    mark_event("reply: no connection");
    return;
  }

  if (session->proxy_con) {
    dout(15) << "send_reply routing reply to " << con->get_peer_addr()
	     << " via " << session->proxy_con->get_peer_addr()
	     << " for request " << *request << dendl;
    session->proxy_con->send_message(new MRoute(session->proxy_tid, reply));
    mark_event("reply: send routed request");
  } else {
    session->con->send_message(reply);
    mark_event("reply: send");
  }
}
