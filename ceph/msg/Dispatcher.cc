
#include "Dispatcher.h"
#include "Messenger.h"

#include "mds/MDS.h"

int Dispatcher::send_message(Message *m, msg_addr_t dest, int dest_port)
{
  assert(0);
  //return dis_messenger->send_message(m, dest, dest_port, MDS_PORT_SERVER);  // on my port!
}
