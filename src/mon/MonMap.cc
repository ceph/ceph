
#include "MonMap.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


// read from/write to a file
int MonMap::write(const char *fn) 
{
  // encode
  bufferlist bl;
  encode(bl);
  
  return bl.write_file(fn);
}

int MonMap::read(const char *fn) 
{
  // read
  bufferlist bl;
  int r = bl.read_file(fn);
  if (r < 0)
    return r;
  decode(bl);
  return 0;
}
