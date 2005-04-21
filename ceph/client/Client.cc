
#include "Client.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"


// cons/des

Client::Client()
{
  root = 0;
}


Client::~Client() 
{
  
  
}



// -------------------
// fs ops

// statfs

// getdir

typedef int (*fuse_dirfil_t) (fuse_dirh_t h, const char *name, int type,
                              ino_t ino);
