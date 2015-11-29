
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

void mon_info_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(name, bl);
  ::encode(public_addr, bl);
  ENCODE_FINISH(bl);
}

void mon_info_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(name, p);
  ::decode(public_addr, p);
  DECODE_FINISH(p);
}

void mon_info_t::print(ostream& out) const
{
  out << "mon." << name
      << " public " << public_addr;
}

void MonMap::encode(bufferlist& blist, uint64_t con_features) const
{
  /* we keep the mon_addr map when encoding to ensure compatibility
   * with clients and other monitors that do not yet support the 'mons'
   * map. This map keeps its original behavior, containing a mapping of
   * monitor id (i.e., 'foo' in 'mon.foo') to the monitor's public
   * address -- which is obtained from the public address of each entry
   * in the 'mons' map.
   */
  map<string,entity_addr_t> mon_addr;
  for (map<string,mon_info_t>::const_iterator p = mon_info.begin();
       p != mon_info.end();
       ++p) {
    mon_addr[p->first] = p->second.public_addr;
  }

  if ((con_features & CEPH_FEATURE_MONNAMES) == 0) {
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

  if ((con_features & CEPH_FEATURE_MONENC) == 0) {
    __u16 v = 2;
    ::encode(v, blist);
    ::encode_raw(fsid, blist);
    ::encode(epoch, blist);
    ::encode(mon_addr, blist);
    ::encode(last_changed, blist);
    ::encode(created, blist);
  }

  ENCODE_START(5, 3, blist);
  ::encode_raw(fsid, blist);
  ::encode(epoch, blist);
  ::encode(mon_addr, blist);
  ::encode(last_changed, blist);
  ::encode(created, blist);
  ::encode(persistent_features, blist);
  ::encode(optional_features, blist);
  // this superseeds 'mon_addr'
  ::encode(mon_info, blist);
  ENCODE_FINISH(blist);
}

void MonMap::decode(bufferlist::iterator &p)
{
  map<string,entity_addr_t> mon_addr;
  DECODE_START_LEGACY_COMPAT_LEN_16(5, 3, 3, p);
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
  if (struct_v >= 4) {
    ::decode(persistent_features, p);
    ::decode(optional_features, p);
  }
  if (struct_v >= 5) {
    ::decode(mon_info, p);
  }
  DECODE_FINISH(p);
  sanitize_mons(mon_addr);
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

  MonMap *m = new MonMap;
  {
    m->epoch = 1;
    m->last_changed = utime_t(123, 456);

    entity_addr_t empty_addr_one;
    empty_addr_one.set_nonce(1);
    m->add("empty_addr_one", empty_addr_one);
    entity_addr_t empty_addr_two;
    empty_addr_two.set_nonce(2);
    m->add("empty_adrr_two", empty_addr_two);

    const char *local_pub_addr_s = "127.0.1.2";

    const char *end_p = local_pub_addr_s + strlen(local_pub_addr_s);
    entity_addr_t local_pub_addr;
    local_pub_addr.parse(local_pub_addr_s, &end_p);

    m->add("filled_pub_addr", local_pub_addr);

    m->add("empty_addr_zero", entity_addr_t());
  }
  o.push_back(m);
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
      << mon_info.size() << " mons at {";
  // the map that we used to print, as it was, no longer
  // maps strings to the monitor's public address, but to
  // mon_info_t instead. As such, print the map in a way
  // that keeps the expected format.
  bool has_printed = false;
  for (map<string,mon_info_t>::const_iterator p = mon_info.begin();
       p != mon_info.end();
       ++p) {
    if (has_printed)
      out << ",";
    out << p->first << "=" << p->second.public_addr;
    has_printed = true;
  }
  out << "}";
}
 
void MonMap::print(ostream& out) const
{
  out << "epoch " << epoch << "\n";
  out << "fsid " << fsid << "\n";
  out << "last_changed " << last_changed << "\n";
  out << "created " << created << "\n";
  unsigned i = 0;
  for (vector<string>::const_iterator p = ranks.begin();
       p != ranks.end();
       ++p) {
    out << i++ << ": " << get_addr(*p) << " mon." << *p << "\n";
  }
}

void MonMap::dump(Formatter *f) const
{
  f->dump_unsigned("epoch", epoch);
  f->dump_stream("fsid") <<  fsid;
  f->dump_stream("modified") << last_changed;
  f->dump_stream("created") << created;
  f->open_object_section("features");
  persistent_features.dump(f, "persistent");
  optional_features.dump(f, "optional");
  f->close_section();
  f->open_array_section("mons");
  int i = 0;
  for (vector<string>::const_iterator p = ranks.begin();
       p != ranks.end();
       ++p, ++i) {
    f->open_object_section("mon");
    f->dump_int("rank", i);
    f->dump_string("name", *p);
    f->dump_stream("addr") << get_addr(*p);
    f->dump_stream("public_addr") << get_addr(*p);
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
      if (!contains(addrs[i]))
	add(name, addrs[i]);
    }
    if (addrs.empty())
      return -ENOENT;
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
  std::vector <std::string> mon_names;
  for (std::vector <std::string>::const_iterator s = sections.begin();
       s != sections.end(); ++s) {
    if ((s->substr(0, 4) == "mon.") && (s->size() > 4)) {
      mon_names.push_back(s->substr(4));
    }
  }

  // Find an address for each monitor in the config file.
  for (std::vector <std::string>::const_iterator m = mon_names.begin();
       m != mon_names.end(); ++m) {
    std::vector <std::string> sections;
    std::string m_name("mon");
    m_name += ".";
    m_name += *m;
    sections.push_back(m_name);
    sections.push_back("mon");
    sections.push_back("global");
    std::string val;
    int res = conf->get_val_from_conf_file(sections, "mon addr", val, true);
    if (res) {
      errout << "failed to get an address for mon." << *m << ": error "
	   << res << std::endl;
      continue;
    }
    entity_addr_t addr;
    if (!addr.parse(val.c_str())) {
      errout << "unable to parse address for mon." << *m
	   << ": addr='" << val << "'" << std::endl;
      continue;
    }
    if (addr.get_port() == 0)
      addr.set_port(CEPH_MON_PORT);

    // the make sure this mon isn't already in the map
    if (contains(addr))
      remove(get_name(addr));
    if (contains(*m))
      remove(*m);

    add(m->c_str(), addr);
  }

  if (size() == 0) {
    errout << "no monitors specified to connect to." << std::endl;
    return -ENOENT;
  }
  created = ceph_clock_now(cct);
  last_changed = created;
  return 0;
}
