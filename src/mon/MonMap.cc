
#include "MonMap.h"

#include <algorithm>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "common/Formatter.h"

#include "include/ceph_features.h"
#include "include/addr_parsing.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"

#include "common/dout.h"

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
  DECODE_START_LEGACY_COMPAT_LEN_16(3, 3, 3, p);
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
       ++p)
    out << i++ << ": " << p->first << " mon." << p->second << "\n";
}

void MonMap::dump(Formatter *f) const
{
  f->dump_unsigned("epoch", epoch);
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
    if (addrs.empty())
      return -ENOENT;
    for (unsigned i=0; i<addrs.size(); i++) {
      char n[2];
      n[0] = 'a' + i;
      n[1] = 0;
      if (addrs[i].get_port() == 0)
	addrs[i].set_port(CEPH_MON_PORT);
      string name = prefix;
      name += n;
      if (!contains(addrs[i]))
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

  if (addrs.empty())
    return -ENOENT;

  for (unsigned i=0; i<addrs.size(); i++) {
    char n[2];
    n[0] = 'a' + i;
    n[1] = 0;
    if (addrs[i].get_port() == 0)
      addrs[i].set_port(CEPH_MON_PORT);
    string name = prefix;
    name += n;
    if (!contains(addrs[i]) &&
	!contains(name))
      add(name, addrs[i]);
  }
  return 0;
}

void MonMap::set_initial_members(CephContext *cct,
				 list<std::string>& initial_members,
				 string my_name, const entity_addr_t& my_addr,
				 set<entity_addr_t> *removed)
{
  // remove non-initial members
  unsigned i = 0;
  while (i < size()) {
    string n = get_name(i);
    if (std::find(initial_members.begin(), initial_members.end(), n) != initial_members.end()) {
      lgeneric_dout(cct, 1) << " keeping " << n << " " << get_addr(i) << dendl;
      i++;
      continue;
    }

    lgeneric_dout(cct, 1) << " removing " << get_name(i) << " " << get_addr(i) << dendl;
    if (removed)
      removed->insert(get_addr(i));
    remove(n);
    assert(!contains(n));
  }

  // add missing initial members
  for (list<string>::iterator p = initial_members.begin(); p != initial_members.end(); ++p) {
    if (!contains(*p)) {
      if (*p == my_name) {
	lgeneric_dout(cct, 1) << " adding self " << *p << " " << my_addr << dendl;
	add(*p, my_addr);
      } else {
	entity_addr_t a;
	a.set_family(AF_INET);
	for (int n=1; ; n++) {
	  a.set_nonce(n);
	  if (!contains(a))
	    break;
	}
	lgeneric_dout(cct, 1) << " adding " << *p << " " << a << dendl;
	add(*p, a);
      }
      assert(contains(*p));
    }
  }
}


int MonMap::build_initial(CephContext *cct, ostream& errout)
{
  const md_config_t *conf = cct->_conf;
  // file?
  if (!conf->monmap.empty()) {
    int r;
    try {
      r = read(conf->monmap.c_str());
    }
    catch (const buffer::error &e) {
      r = -EINVAL;
    }
    if (r >= 0)
      return 0;
    errout << "unable to read/decode monmap from " << conf->monmap
	 << ": " << cpp_strerror(-r) << std::endl;
    return r;
  }

  // fsid from conf?
  if (!cct->_conf->fsid.is_zero()) {
    fsid = cct->_conf->fsid;
  }

  // -m foo?
  if (!conf->mon_host.empty()) {
    int r = build_from_host_list(conf->mon_host, "noname-");
    if (r < 0) {
      errout << "unable to parse addrs in '" << conf->mon_host << "'"
             << std::endl;
      return r;
    }
    created = ceph_clock_now(cct);
    last_changed = created;
    return 0;
  }

  // What monitors are in the config file?
  std::vector <std::string> sections;
  int ret = conf->get_all_sections(sections);
  if (ret) {
    errout << "Unable to find any monitors in the configuration "
         << "file, because there was an error listing the sections. error "
	 << ret << std::endl;
    return -ENOENT;
  }
  
  std::vector <std::string> mon_section = {"","mon","global"};
  for (std::vector <std::string>::const_iterator s = sections.begin();
       s != sections.end(); ++s) {
    if ((s->substr(0, 4) == "mon.") && (s->size() > 4)) {
       mon_section[0] = *s;
       const string &m_name = s->substr(4);
       std::string val;
       int res = conf->get_val_from_conf_file(mon_section, "mon addr", val, true);
       if (res) {
         errout << "failed to get an address for mon." << m_name << ": error "
          << res << std::endl;
         continue;
       }
       entity_addr_t addr;
       if (!addr.parse(val.c_str())) {
         errout << "unable to parse address for mon." << m_name
           << ": addr='" << val << "'" << std::endl;
         continue;
       }

       if (addr.get_port() == 0)
         addr.set_port(CEPH_MON_PORT);

       // the make sure this mon isn't already in the map
       if (contains(addr))
         remove(get_name(addr));
       if (contains(m_name))
         remove(m_name);

       add(m_name.c_str(), addr);
    }
  }

  if (size() == 0) {
    errout << "no monitors specified to connect to." << std::endl;
    return -ENOENT;
  }
  created = ceph_clock_now(cct);
  last_changed = created;
  return 0;
}
