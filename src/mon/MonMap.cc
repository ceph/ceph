
#include "MonMap.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "common/Formatter.h"

using ceph::Formatter;

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
  std::string error;
  int r = bl.read_file(fn, &error);
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
  for (map<entity_addr_t,string>::const_iterator p = addr_name.begin();
       p != addr_name.end();
       p++)
    out << i++ << ": " << p->first << " mon." << p->second << "\n";
}

void MonMap::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);
  f->dump_stream("fsid") <<  fsid;
  f->dump_stream("modified") << last_changed;
  f->dump_stream("created") << created;
  f->open_array_section("mons");
  int i = 0;
  for (map<entity_addr_t,string>::const_iterator p = addr_name.begin();
       p != addr_name.end();
       ++p, ++i) {
    f->open_object_section("mon");
    f->dump_int("rank", i);
    f->dump_string("name", p->second);
    f->dump_stream("addr") << p->first;
    f->close_section();
  }
  f->close_section();
}
