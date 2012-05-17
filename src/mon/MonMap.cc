
#include "MonMap.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "common/Formatter.h"

#include "include/ceph_features.h"
#include "include/addr_parsing.h"
#include "common/ceph_argparse.h"

using ceph::Formatter;

void MonMap::encode(bufferlist& blist, uint64_t features) const
{
  if ((features & CEPH_FEATURE_MONNAMES) == 0) {
    __u16 v = 1;
    ::encode(v, blist);
    ::encode_raw(fsid, blist);
    ::encode(epoch, blist);
    vector<entity_inst_t> mon_inst(mon_addr.size());
    for (unsigned n = 0; n < mon_addr.size(); n++)
      mon_inst[n] = get_inst(n);
    ::encode(mon_inst, blist);
    ::encode(last_changed, blist);
    ::encode(created, blist);
    return;
  }

  if ((features & CEPH_FEATURE_MONENC) == 0) {
    __u16 v = 2;
    ::encode(v, blist);
    ::encode_raw(fsid, blist);
    ::encode(epoch, blist);
    ::encode(mon_addr, blist);
    ::encode(last_changed, blist);
    ::encode(created, blist);
  }

  ENCODE_START(3, 3, blist);
  ::encode_raw(fsid, blist);
  ::encode(epoch, blist);
  ::encode(mon_addr, blist);
  ::encode(last_changed, blist);
  ::encode(created, blist);
  ENCODE_FINISH(blist);
}

void MonMap::decode(bufferlist::iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, p);
  ::decode_raw(fsid, p);
  ::decode(epoch, p);
  if (struct_v == 1) {
    vector<entity_inst_t> mon_inst;
    ::decode(mon_inst, p);
    for (unsigned i = 0; i < mon_inst.size(); i++) {
      char n[2];
      n[0] = '0' + i;
      n[1] = 0;
      string name = n;
      mon_addr[name] = mon_inst[i].addr;
    }
  } else {
    ::decode(mon_addr, p);
  }
  ::decode(last_changed, p);
  ::decode(created, p);
  DECODE_FINISH(p);
  calc_ranks();
}

void MonMap::generate_test_instances(list<MonMap*>& o)
{
  o.push_back(new MonMap);
  o.push_back(new MonMap);
  o.back()->epoch = 1;
  o.back()->last_changed = utime_t(123, 456);
  o.back()->created = utime_t(789, 101112);
  o.back()->add("one", entity_addr_t());
}

// read from/write to a file
int MonMap::write(const char *fn) 
{
  // encode
  bufferlist bl;
  encode(bl, CEPH_FEATURES_ALL);
  
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


int MonMap::build_from_host_list(std::string hostlist, std::string prefix)
{
  vector<entity_addr_t> addrs;
  if (parse_ip_port_vec(hostlist.c_str(), addrs)) {
    for (unsigned i=0; i<addrs.size(); i++) {
      char n[2];
      n[0] = 'a' + i;
      n[1] = 0;
      if (addrs[i].get_port() == 0)
	addrs[i].set_port(CEPH_MON_PORT);
      string name = prefix;
      name += n;
      add(name, addrs[i]);
    }
    return 0;
  }

  // maybe they passed us a DNS-resolvable name
  char *hosts = NULL;
  hosts = resolve_addrs(hostlist.c_str());
  if (!hosts)
    return -EINVAL;
  bool success = parse_ip_port_vec(hosts, addrs);
  free(hosts);
  if (!success)
    return -EINVAL;

  for (unsigned i=0; i<addrs.size(); i++) {
    char n[2];
    n[0] = 'a' + i;
    n[1] = 0;
    if (addrs[i].get_port() == 0)
      addrs[i].set_port(CEPH_MON_PORT);
    string name = prefix;
    name += n;
    add(name, addrs[i]);
  }
  return 0;
}
