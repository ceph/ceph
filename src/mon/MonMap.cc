
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

void MonMap::print_summary(ostream& out) const
{
  out << "e" << epoch << ": "
      << mon_addr.size() << " mons at "
      << mon_addr;
}
 
void MonMap::print(ostream& out) const
{
  out << "epoch " << epoch << "\n";
  out << "fsid " << fsid << "\n";
  out << "last_changed " << last_changed << "\n";
  out << "created " << created << "\n";
  unsigned i = 0;
  for (map<string,entity_addr_t>::const_iterator p = mon_addr.begin();
       p != mon_addr.end();
       p++)
    out << i++ << ": " << p->second << " mon." << p->first << "\n";
}
