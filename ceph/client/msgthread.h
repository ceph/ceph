#include "msg/Message.h"

// send the message, expecting no response.  threads other than the
// MPI thread use this function; if the MPI thread uses this function
// it could deadlock: this function could wait for the out queue to be
// emptied, but only the MPI thread can empty it.
void obfsmpi_send(Message *m)

// send the message to a server and wait for the response.  threads
// other than the MPI thread use this function.
Message *obfsmpi_sendrecv(Message *m)
