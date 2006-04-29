
#include "ObjectCacher.h"
#include "Objecter.h"


int ObjectCacher::readx(Objecter::OSDRead *rd, inodeno_t ino, Context *onfinish)
{
  assert(0);
  return 0;
}

int ObjectCacher::writex(Objecter::OSDWrite *wr, inodeno_t ino, Context *onack, Context *oncommit)
{
  assert(0);
  return 0;
}
 
  
// blocking.  atomic+sync.
int ObjectCacher::atomic_sync_readx(Objecter::OSDRead *rd, inodeno_t ino, Context *onfinish)
{
  assert(0);
  return 0;
}

int ObjectCacher::atomic_sync_writex(Objecter::OSDWrite *wr, inodeno_t ino, Context *onack, Context *oncommit)
{
  assert(0);
  return 0;
}
 
