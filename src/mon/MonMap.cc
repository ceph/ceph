// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MonMap.h"

#include <algorithm>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef WITH_SEASTAR
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/net/dns.hh>
#include "crimson/common/config_proxy.h"
#endif

#include "common/Formatter.h"

#include "include/ceph_features.h"
#include "include/addr_parsing.h"
#include "common/ceph_argparse.h"
#include "common/ceph_json.h"
#include "common/dns_resolve.h"
#include "common/errno.h"
#include "common/dout.h"
#include "common/Clock.h"
#include "mon/health_check.h"

using std::list;
using std::map;
using std::ostream;
using std::ostringstream;
using std::set;
using std::string;
using std::vector;

using ceph::DNSResolver;
using ceph::Formatter;

#ifdef WITH_SEASTAR
namespace {
  seastar::logger& logger()
  {
    return crimson::get_logger(ceph_subsys_monc);
  }
}
#endif

void mon_info_t::encode(ceph::buffer::list& bl, uint64_t features) const
{
  uint8_t v = 5;
  uint8_t min_v = 1;
  if (!crush_loc.empty()) {
    // we added crush_loc in version 5, but need to let old clients decode it
    // so just leave the min_v at version 1. Monitors are protected
    // from misunderstandings about location because setting it is blocked
    // on FEATURE_PINGING
    min_v = 1;
  }
  if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
    v = 2;
  }
  ENCODE_START(v, min_v, bl);
  encode(name, bl);
  if (v < 3) {
    ceph_assert(min_v == 1);
    auto a = public_addrs.legacy_addr();
    if (a != entity_addr_t()) {
      encode(a, bl, features);
    } else {
      // note: we don't have a legacy addr here, so lie so that it looks
      // like one, just so that old clients get a valid-looking map.
      // they won't be able to talk to the v2 mons, but that's better
      // than nothing.
      encode(public_addrs.as_legacy_addr(), bl, features);
    }
  } else {
    encode(public_addrs, bl, features);
  }
  encode(priority, bl);
  encode(weight, bl);
  encode(crush_loc, bl);
  ENCODE_FINISH(bl);
}

void mon_info_t::decode(ceph::buffer::list::const_iterator& p)
{
  DECODE_START(5, p);
  decode(name, p);
  decode(public_addrs, p);
  if (struct_v >= 2) {
    decode(priority, p);
  }
  if (struct_v >= 4) {
    decode(weight, p);
  }
  if (struct_v >= 5) {
    decode(crush_loc, p);
  }
  DECODE_FINISH(p);
}

void mon_info_t::print(ostream& out) const
{
  out << "mon." << name
      << " addrs " << public_addrs
      << " priority " << priority
      << " weight " << weight
      << " crush location " << crush_loc;
}

void mon_info_t::dump(ceph::Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_stream("addr") << public_addrs;
  f->dump_int("priority", priority);
  f->dump_float("weight", weight);
  encode_json("crush_location", crush_loc, f);
}

void mon_info_t::generate_test_instances(list<mon_info_t*>& ls)
{
  ls.push_back(new mon_info_t);
  ls.push_back(new mon_info_t);
  ls.back()->name = "noname";
  ls.back()->public_addrs.parse("v1:1.2.3.4:567/890");
  ls.back()->priority = 1;
  ls.back()->weight = 1.0;
  ls.back()->crush_loc.emplace("root", "default");
  ls.back()->crush_loc.emplace("host", "foo");
}
namespace {
  struct rank_cmp {
    bool operator()(const mon_info_t &a, const mon_info_t &b) const {
      if (a.public_addrs.legacy_or_front_addr() == b.public_addrs.legacy_or_front_addr())
        return a.name < b.name;
      return a.public_addrs.legacy_or_front_addr() < b.public_addrs.legacy_or_front_addr();
    }
  };
}

void MonMap::calc_legacy_ranks()
{
  ranks.resize(mon_info.size());

  // Used to order entries according to public_addr, because that's
  // how the ranks are expected to be ordered by. We may expand this
  // later on, according to some other criteria, by specifying a
  // different comparator.
  //
  // Please note that we use a 'set' here instead of resorting to
  // std::sort() because we need more info than that's available in
  // the vector. The vector will thus be ordered by, e.g., public_addr
  // while only containing the names of each individual monitor.
  // The only way of achieving this with std::sort() would be to first
  // insert every mon_info_t entry into a vector 'foo', std::sort() 'foo'
  // with custom comparison functions, and then copy each invidual entry
  // to a new vector. Unless there's a simpler way, we don't think the
  // added complexity makes up for the additional memory usage of a 'set'.
  set<mon_info_t, rank_cmp> tmp;

  for (auto p = mon_info.begin(); p != mon_info.end(); ++p) {
    mon_info_t &m = p->second;
    tmp.insert(m);
  }

  // map the set to the actual ranks etc
  unsigned i = 0;
  for (auto p = tmp.begin(); p != tmp.end(); ++p, ++i) {
    ranks[i] = p->name;
  }
}

void MonMap::encode(ceph::buffer::list& blist, uint64_t con_features) const
{
  if ((con_features & CEPH_FEATURE_MONNAMES) == 0) {
    using ceph::encode;
    __u16 v = 1;
    encode(v, blist);
    ceph::encode_raw(fsid, blist);
    encode(epoch, blist);
    vector<entity_inst_t> mon_inst(ranks.size());
    for (unsigned n = 0; n < ranks.size(); n++) {
      mon_inst[n].name = entity_name_t::MON(n);
      mon_inst[n].addr = get_addrs(n).legacy_addr();
    }
    encode(mon_inst, blist, con_features);
    encode(last_changed, blist);
    encode(created, blist);
    return;
  }

  map<string,entity_addr_t> legacy_mon_addr;
  if (!HAVE_FEATURE(con_features, MONENC) ||
      !HAVE_FEATURE(con_features, SERVER_NAUTILUS)) {
    for (auto& [name, info] : mon_info) {
      // see note in mon_info_t::encode()
      auto addr = info.public_addrs.legacy_addr();
      if (addr == entity_addr_t()) {
        addr = info.public_addrs.as_legacy_addr();
      }
      legacy_mon_addr[name] = addr;
    }
  }

  if (!HAVE_FEATURE(con_features, MONENC)) {
    /* we keep the mon_addr map when encoding to ensure compatibility
       * with clients and other monitors that do not yet support the 'mons'
       * map. This map keeps its original behavior, containing a mapping of
       * monitor id (i.e., 'foo' in 'mon.foo') to the monitor's public
       * address -- which is obtained from the public address of each entry
       * in the 'mons' map.
       */
    using ceph::encode;
    __u16 v = 2;
    encode(v, blist);
    ceph::encode_raw(fsid, blist);
    encode(epoch, blist);
    encode(legacy_mon_addr, blist, con_features);
    encode(last_changed, blist);
    encode(created, blist);
    return;
  }

  if (!HAVE_FEATURE(con_features, SERVER_NAUTILUS)) {
    ENCODE_START(5, 3, blist);
    ceph::encode_raw(fsid, blist);
    encode(epoch, blist);
    encode(legacy_mon_addr, blist, con_features);
    encode(last_changed, blist);
    encode(created, blist);
    encode(persistent_features, blist);
    encode(optional_features, blist);
    encode(mon_info, blist, con_features);
    ENCODE_FINISH(blist);
    return;
  }

  ENCODE_START(9, 6, blist);
  ceph::encode_raw(fsid, blist);
  encode(epoch, blist);
  encode(last_changed, blist);
  encode(created, blist);
  encode(persistent_features, blist);
  encode(optional_features, blist);
  encode(mon_info, blist, con_features);
  encode(ranks, blist);
  encode(min_mon_release, blist);
  encode(removed_ranks, blist);
  uint8_t t = strategy;
  encode(t, blist);
  encode(disallowed_leaders, blist);
  encode(stretch_mode_enabled, blist);
  encode(tiebreaker_mon, blist);
  encode(stretch_marked_down_mons, blist);
  ENCODE_FINISH(blist);
}

void MonMap::decode(ceph::buffer::list::const_iterator& p)
{
  map<string,entity_addr_t> mon_addr;
  DECODE_START_LEGACY_COMPAT_LEN_16(9, 3, 3, p);
  ceph::decode_raw(fsid, p);
  decode(epoch, p);
  if (struct_v == 1) {
    vector<entity_inst_t> mon_inst;
    decode(mon_inst, p);
    for (unsigned i = 0; i < mon_inst.size(); i++) {
      char n[2];
      n[0] = '0' + i;
      n[1] = 0;
      string name = n;
      mon_addr[name] = mon_inst[i].addr;
    }
  } else if (struct_v < 6) {
    decode(mon_addr, p);
  }
  decode(last_changed, p);
  decode(created, p);
  if (struct_v >= 4) {
    decode(persistent_features, p);
    decode(optional_features, p);
  }
  if (struct_v < 5) {
    // generate mon_info from legacy mon_addr
    for (auto& [name, addr] : mon_addr) {
      mon_info_t &m = mon_info[name];
      m.name = name;
      m.public_addrs = entity_addrvec_t(addr);
    }
  } else {
    decode(mon_info, p);
  }
  if (struct_v < 6) {
    calc_legacy_ranks();
  } else {
    decode(ranks, p);
  }
  if (struct_v >= 7) {
    decode(min_mon_release, p);
  } else {
    min_mon_release = infer_ceph_release_from_mon_features(persistent_features);
  }
  if (struct_v >= 8) {
    decode(removed_ranks, p);
    uint8_t t;
    decode(t, p);
    strategy = static_cast<election_strategy>(t);
    decode(disallowed_leaders, p);
  }
  if (struct_v >= 9) {
    decode(stretch_mode_enabled, p);
    decode(tiebreaker_mon, p);
    decode(stretch_marked_down_mons, p);
  } else {
    stretch_mode_enabled = false;
    tiebreaker_mon = "";
    stretch_marked_down_mons.clear();
  }
  calc_addr_mons();
  DECODE_FINISH(p);
}

void MonMap::generate_test_instances(list<MonMap*>& o)
{
  o.push_back(new MonMap);
  o.push_back(new MonMap);
  o.back()->epoch = 1;
  o.back()->last_changed = utime_t(123, 456);
  o.back()->created = utime_t(789, 101112);
  o.back()->add("one", entity_addrvec_t());

  MonMap *m = new MonMap;
  {
    m->epoch = 1;
    m->last_changed = utime_t(123, 456);

    entity_addrvec_t empty_addr_one = entity_addrvec_t(entity_addr_t());
    empty_addr_one.v[0].set_nonce(1);
    m->add("empty_addr_one", empty_addr_one);
    entity_addrvec_t empty_addr_two = entity_addrvec_t(entity_addr_t());
    empty_addr_two.v[0].set_nonce(2);
    m->add("empty_addr_two", empty_addr_two);

    const char *local_pub_addr_s = "127.0.1.2";

    const char *end_p = local_pub_addr_s + strlen(local_pub_addr_s);
    entity_addrvec_t local_pub_addr;
    local_pub_addr.parse(local_pub_addr_s, &end_p);

    m->add(mon_info_t("filled_pub_addr", entity_addrvec_t(local_pub_addr), 1, 1));

    m->add("empty_addr_zero", entity_addrvec_t());
  }
  o.push_back(m);
}

// read from/write to a file
int MonMap::write(const char *fn) 
{
  // encode
  ceph::buffer::list bl;
  encode(bl, CEPH_FEATURES_ALL);
  
  return bl.write_file(fn);
}

int MonMap::read(const char *fn) 
{
  // read
  ceph::buffer::list bl;
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
  for (auto p = mon_info.begin(); p != mon_info.end(); ++p) {
    if (has_printed)
      out << ",";
    out << p->first << "=" << p->second.public_addrs;
    has_printed = true;
  }
  out << "}" << " removed_ranks: {" << removed_ranks << "}";
  out << " disallowed_leaders: {" << disallowed_leaders << "}";
}
 
void MonMap::print(ostream& out) const
{
  out << "epoch " << epoch << "\n";
  out << "fsid " << fsid << "\n";
  out << "last_changed " << last_changed << "\n";
  out << "created " << created << "\n";
  out << "min_mon_release " << to_integer<unsigned>(min_mon_release)
      << " (" << min_mon_release << ")\n";
  out << "election_strategy: " << strategy << "\n";
  if (stretch_mode_enabled) {
    out << "stretch_mode_enabled " << stretch_mode_enabled << "\n";
    out << "tiebreaker_mon " << tiebreaker_mon << "\n";
  }
  if (stretch_mode_enabled ||
      !disallowed_leaders.empty()) {
    out << "disallowed_leaders " << disallowed_leaders << "\n";
  }
  unsigned i = 0;
  for (auto p = ranks.begin(); p != ranks.end(); ++p) {
    const auto &mi = mon_info.find(*p);
    ceph_assert(mi != mon_info.end());
    out << i++ << ": " << mi->second.public_addrs << " mon." << *p;
    if (!mi->second.crush_loc.empty()) {
      out << "; crush_location " << mi->second.crush_loc;
    }
    out << "\n";
  }
}

void MonMap::dump(Formatter *f) const
{
  f->dump_unsigned("epoch", epoch);
  f->dump_stream("fsid") <<  fsid;
  last_changed.gmtime(f->dump_stream("modified"));
  created.gmtime(f->dump_stream("created"));
  f->dump_unsigned("min_mon_release", to_integer<unsigned>(min_mon_release));
  f->dump_string("min_mon_release_name", to_string(min_mon_release));
  f->dump_int ("election_strategy", strategy);
  f->dump_stream("disallowed_leaders") << disallowed_leaders;
  f->dump_bool("stretch_mode", stretch_mode_enabled);
  f->dump_string("tiebreaker_mon", tiebreaker_mon);
  f->dump_stream("removed_ranks") << removed_ranks;
  f->open_object_section("features");
  persistent_features.dump(f, "persistent");
  optional_features.dump(f, "optional");
  f->close_section();
  f->open_array_section("mons");
  int i = 0;
  for (auto p = ranks.begin(); p != ranks.end(); ++p, ++i) {
    f->open_object_section("mon");
    f->dump_int("rank", i);
    f->dump_string("name", *p);
    f->dump_object("public_addrs", get_addrs(*p));
    // compat: make these look like pre-nautilus entity_addr_t
    f->dump_stream("addr") << get_addrs(*p).get_legacy_str();
    f->dump_stream("public_addr") << get_addrs(*p).get_legacy_str();
    f->dump_unsigned("priority", get_priority(*p));
    f->dump_unsigned("weight", get_weight(*p));
    const auto &mi = mon_info.find(*p);
    // we don't need to assert this validity as all the get_* functions did
    f->dump_stream("crush_location") << mi->second.crush_loc;
    f->close_section();
  }
  f->close_section();
}

void MonMap::dump_summary(Formatter *f) const
{
  f->dump_unsigned("epoch", epoch);
  f->dump_string("min_mon_release_name", to_string(min_mon_release));
  f->dump_unsigned("num_mons", ranks.size());
}

// an ambiguous mon addr may be legacy or may be msgr2--we aren' sure.
// when that happens we need to try them both (unless we can
// reasonably infer from the port number which it is).
void MonMap::_add_ambiguous_addr(const string& name,
                                 entity_addr_t addr,
                                 int priority,
                                 int weight,
                                 bool for_mkfs)
{
  if (addr.get_type() != entity_addr_t::TYPE_ANY) {
    // a v1: or v2: prefix was specified
    if (addr.get_port() == 0) {
      // use default port
      if (addr.get_type() == entity_addr_t::TYPE_LEGACY) {
	addr.set_port(CEPH_MON_PORT_LEGACY);
      } else if (addr.get_type() == entity_addr_t::TYPE_MSGR2) {
	addr.set_port(CEPH_MON_PORT_IANA);
      } else {
	// wth
	return;
      }
      if (!contains(addr)) {
	add(name, entity_addrvec_t(addr), priority, weight);
      }
    } else {
      if (!contains(addr)) {
	add(name, entity_addrvec_t(addr), priority, weight);
      }
    }
  } else {
    // no v1: or v2: prefix specified
    if (addr.get_port() == CEPH_MON_PORT_LEGACY) {
      // legacy port implies legacy addr
      addr.set_type(entity_addr_t::TYPE_LEGACY);
      if (!contains(addr)) {
	if (!for_mkfs) {
	  add(name + "-legacy", entity_addrvec_t(addr), priority, weight);
	} else {
	  add(name, entity_addrvec_t(addr), priority, weight);
	}
      }
    } else if (addr.get_port() == CEPH_MON_PORT_IANA) {
      // iana port implies msgr2 addr
      addr.set_type(entity_addr_t::TYPE_MSGR2);
      if (!contains(addr)) {
	add(name, entity_addrvec_t(addr), priority, weight);
      }
    } else if (addr.get_port() == 0) {
      // no port; include both msgr2 and legacy ports
      if (!for_mkfs) {
	addr.set_type(entity_addr_t::TYPE_MSGR2);
	addr.set_port(CEPH_MON_PORT_IANA);
	if (!contains(addr)) {
	  add(name, entity_addrvec_t(addr), priority, weight);
	}
	addr.set_type(entity_addr_t::TYPE_LEGACY);
	addr.set_port(CEPH_MON_PORT_LEGACY);
	if (!contains(addr)) {
	  add(name + "-legacy", entity_addrvec_t(addr), priority, weight);
	}
      } else {
	entity_addrvec_t av;
	addr.set_type(entity_addr_t::TYPE_MSGR2);
	addr.set_port(CEPH_MON_PORT_IANA);
	av.v.push_back(addr);
	addr.set_type(entity_addr_t::TYPE_LEGACY);
	addr.set_port(CEPH_MON_PORT_LEGACY);
	av.v.push_back(addr);
	if (!contains(av)) {
	  add(name, av, priority, weight);
	}
      }
    } else {
      addr.set_type(entity_addr_t::TYPE_MSGR2);
      if (!contains(addr)) {
	add(name, entity_addrvec_t(addr), priority, weight);
      }
      if (!for_mkfs) {
	// try legacy on same port too
	addr.set_type(entity_addr_t::TYPE_LEGACY);
	if (!contains(addr)) {
	  add(name + "-legacy", entity_addrvec_t(addr), priority, weight);
	}
      }
    }
  }
}

void MonMap::init_with_addrs(const std::vector<entity_addrvec_t>& addrs,
                             bool for_mkfs,
                             std::string_view prefix)
{
  char id = 'a';
  for (auto& addr : addrs) {
    string name{prefix};
    name += id++;
    if (addr.v.size() == 1) {
      _add_ambiguous_addr(name, addr.front(), 0, 0, for_mkfs);
    } else {
      // they specified an addrvec, so let's assume they also specified
      // the addr *type* and *port*.  (we could possibly improve this?)
      add(name, addr, 0);
    }
  }
}

int MonMap::init_with_ips(const std::string& ips,
			  bool for_mkfs,
			  std::string_view prefix)
{
  vector<entity_addrvec_t> addrs;
  if (!parse_ip_port_vec(
	ips.c_str(), addrs,
	entity_addr_t::TYPE_ANY)) {
    return -EINVAL;
  }
  if (addrs.empty())
    return -ENOENT;
  init_with_addrs(addrs, for_mkfs, prefix);
  return 0;
}

int MonMap::init_with_hosts(const std::string& hostlist,
			    bool for_mkfs,
			    std::string_view prefix)
{
  // maybe they passed us a DNS-resolvable name
  char *hosts = resolve_addrs(hostlist.c_str());
  if (!hosts)
    return -EINVAL;

  vector<entity_addrvec_t> addrs;
  bool success = parse_ip_port_vec(
    hosts, addrs,
    entity_addr_t::TYPE_ANY);
  free(hosts);
  if (!success)
    return -EINVAL;
  if (addrs.empty())
    return -ENOENT;
  init_with_addrs(addrs, for_mkfs, prefix);
  calc_legacy_ranks();
  return 0;
}

void MonMap::set_initial_members(CephContext *cct,
				 list<std::string>& initial_members,
				 string my_name,
				 const entity_addrvec_t& my_addrs,
				 set<entity_addrvec_t> *removed)
{
  // remove non-initial members
  unsigned i = 0;
  while (i < size()) {
    string n = get_name(i);
    if (std::find(initial_members.begin(), initial_members.end(), n)
	!= initial_members.end()) {
      lgeneric_dout(cct, 1) << " keeping " << n << " " << get_addrs(i) << dendl;
      i++;
      continue;
    }

    lgeneric_dout(cct, 1) << " removing " << get_name(i) << " " << get_addrs(i)
			  << dendl;
    if (removed) {
      removed->insert(get_addrs(i));
    }
    remove(n);
    ceph_assert(!contains(n));
  }

  // add missing initial members
  for (auto& p : initial_members) {
    if (!contains(p)) {
      if (p == my_name) {
	lgeneric_dout(cct, 1) << " adding self " << p << " " << my_addrs
			      << dendl;
	add(p, my_addrs);
      } else {
	entity_addr_t a;
	a.set_type(entity_addr_t::TYPE_LEGACY);
	a.set_family(AF_INET);
	for (int n=1; ; n++) {
	  a.set_nonce(n);
	  if (!contains(a))
	    break;
	}
	lgeneric_dout(cct, 1) << " adding " << p << " " << a << dendl;
	add(p, entity_addrvec_t(a));
      }
      ceph_assert(contains(p));
    }
  }
  calc_legacy_ranks();
}

int MonMap::init_with_config_file(const ConfigProxy& conf,
                                  std::ostream& errout)
{
  std::vector<std::string> sections;
  int ret = conf.get_all_sections(sections);
  if (ret) {
    errout << "Unable to find any monitors in the configuration "
         << "file, because there was an error listing the sections. error "
	 << ret << std::endl;
    return -ENOENT;
  }
  std::vector<std::string> mon_names;
  for (const auto& section : sections) {
    if (section.substr(0, 4) == "mon." && section.size() > 4) {
      mon_names.push_back(section.substr(4));
    }
  }

  // Find an address for each monitor in the config file.
  for (const auto& mon_name : mon_names) {
    std::vector<std::string> sections;
    std::string m_name("mon");
    m_name += ".";
    m_name += mon_name;
    sections.push_back(m_name);
    sections.push_back("mon");
    sections.push_back("global");
    std::string val;
    int res = conf.get_val_from_conf_file(sections, "mon addr", val, true);
    if (res) {
      errout << "failed to get an address for mon." << mon_name
             << ": error " << res << std::endl;
      continue;
    }
    // the 'mon addr' field is a legacy field, so assume anything
    // there on a weird port is a v1 address, and do not handle
    // addrvecs.
    entity_addr_t addr;
    if (!addr.parse(val, entity_addr_t::TYPE_LEGACY)) {
      errout << "unable to parse address for mon." << mon_name
             << ": addr='" << val << "'" << std::endl;
      continue;
    }
    if (addr.get_port() == 0) {
      addr.set_port(CEPH_MON_PORT_LEGACY);
    }
    uint16_t priority = 0;
    if (!conf.get_val_from_conf_file(sections, "mon priority", val, false)) {
      try {
        priority = std::stoul(val);
      } catch (std::logic_error&) {
        errout << "unable to parse priority for mon." << mon_name
               << ": priority='" << val << "'" << std::endl;
        continue;
      }
    }
    uint16_t weight = 0;
    if (!conf.get_val_from_conf_file(sections, "mon weight", val, false)) {
      try {
        weight = std::stoul(val);
      } catch (std::logic_error&) {
        errout << "unable to parse weight for mon." << mon_name
               << ": weight='" << val << "'"
               << std::endl;
        continue;
      }
    }

    // make sure this mon isn't already in the map
    if (contains(addr))
      remove(get_name(addr));
    if (contains(mon_name))
      remove(mon_name);
    _add_ambiguous_addr(mon_name, addr, priority, weight, false);
  }
  return 0;
}

void MonMap::check_health(health_check_map_t *checks) const
{
  if (stretch_mode_enabled) {
    list<string> detail;
    for (auto& p : mon_info) {
      if (p.second.crush_loc.empty()) {
	ostringstream ss;
	ss << "mon " << p.first << " has no location set while in stretch mode";
	detail.push_back(ss.str());
      }
    }
    if (!detail.empty()) {
      ostringstream ss;
      ss << detail.size() << " monitor(s) have no location set while in stretch mode"
	 << "; this may cause issues with failover, OSD connections, netsplit handling, etc";
      auto& d = checks->add("MON_LOCATION_NOT_SET", HEALTH_WARN,
			    ss.str(), detail.size());
      d.detail.swap(detail);
    }
  }
}

#ifdef WITH_SEASTAR

seastar::future<> MonMap::read_monmap(const std::string& monmap)
{
  using namespace seastar;
  return open_file_dma(monmap, open_flags::ro).then([this] (file f) {
    return f.size().then([this, f = std::move(f)](size_t s) {
      return do_with(make_file_input_stream(f), [this, s](input_stream<char>& in) {
        return in.read_exactly(s).then([this](temporary_buffer<char> buf) {
          ceph::buffer::list bl;
          bl.push_back(ceph::buffer::ptr_node::create(
            ceph::buffer::create(std::move(buf))));
          decode(bl);
        });
      });
    });
  });
}

seastar::future<> MonMap::init_with_dns_srv(bool for_mkfs, const std::string& name)
{
  logger().debug("{}: for_mkfs={}, name={}", __func__, for_mkfs, name);
  string domain;
  string service = name;
  // check if domain is also provided and extract it from srv_name
  size_t idx = name.find("_");
  if (idx != name.npos) {
    domain = name.substr(idx + 1);
    service = name.substr(0, idx);
  }
  return seastar::net::dns::get_srv_records(
      seastar::net::dns_resolver::srv_proto::tcp,
      service, domain).then([this](seastar::net::dns_resolver::srv_records records) {
    return seastar::parallel_for_each(records, [this](auto record) {
      return seastar::net::dns::resolve_name(record.target).then(
          [record,this](seastar::net::inet_address a) {
	// the resolved address does not contain ceph specific info like nonce
	// nonce or msgr proto (legacy, msgr2), so set entity_addr_t manually
	entity_addr_t addr;
	addr.set_type(entity_addr_t::TYPE_ANY);
	addr.set_family(int(a.in_family()));
	addr.set_port(record.port);
	switch (a.in_family()) {
	case seastar::net::inet_address::family::INET:
	  addr.in4_addr().sin_addr = a;
	  break;
	case seastar::net::inet_address::family::INET6:
	  addr.in6_addr().sin6_addr = a;
	  break;
	}
        _add_ambiguous_addr(record.target,
                            addr,
                            record.priority,
                            record.weight,
                            false);
      }).handle_exception_type([t=record.target](const std::system_error& e) {
        logger().debug("{}: unable to resolve name for {}: {}",
                       "init_with_dns_srv", t, e);
      });
    });
  }).handle_exception_type([name](const std::system_error& e) {
    logger().debug("{}: unable to get monitor info from DNS SRV with {}: {}",
                 "init_with_dns_srv", name, e);
    // ignore DNS failures
    return seastar::make_ready_future<>();
  });
}

bool MonMap::maybe_init_with_mon_host(const std::string& mon_host,
                                      const bool for_mkfs)
{
  if (!mon_host.empty()) {
    if (auto ret = init_with_ips(mon_host, for_mkfs, "noname-"); ret == 0) {
      return true;
    }
    // TODO: resolve_addrs() is a blocking call
    if (auto ret = init_with_hosts(mon_host, for_mkfs, "noname-"); ret == 0) {
      return true;
    } else {
      throw std::runtime_error(cpp_strerror(ret));
    }
  }
  return false;
}

seastar::future<> MonMap::build_monmap(const crimson::common::ConfigProxy& conf,
				       bool for_mkfs)
{
  logger().debug("{}: for_mkfs={}", __func__, for_mkfs);
  // -m foo?
  if (maybe_init_with_mon_host(conf.get_val<std::string>("mon_host"), for_mkfs)) {
    return seastar::make_ready_future<>();
  }

  // What monitors are in the config file?
  ostringstream errout;
  if (auto ret = init_with_config_file(conf, errout); ret < 0) {
    throw std::runtime_error(errout.str());
  }
  if (size() > 0) {
    return seastar::make_ready_future<>();
  }
  // no info found from conf options lets try use DNS SRV records
  const string srv_name = conf.get_val<std::string>("mon_dns_srv_name");
  return init_with_dns_srv(for_mkfs, srv_name).then([this] {
    if (size() == 0) {
      throw std::runtime_error("no monitors specified to connect to.");
    }
  });
}

seastar::future<> MonMap::build_initial(const crimson::common::ConfigProxy& conf, bool for_mkfs)
{
  // mon_host_override?
  if (maybe_init_with_mon_host(conf.get_val<std::string>("mon_host_override"),
                               for_mkfs)) {
    return seastar::make_ready_future<>();
  }

  // file?
  if (const auto monmap = conf.get_val<std::string>("monmap");
      !monmap.empty()) {
    return read_monmap(monmap);
  } else {
    // fsid from conf?
    if (const auto new_fsid = conf.get_val<uuid_d>("fsid");
        !new_fsid.is_zero()) {
      fsid = new_fsid;
    }
    return build_monmap(conf, for_mkfs).then([this] {
      created = ceph_clock_now();
      last_changed = created;
      calc_legacy_ranks();
    });
  }
}

#else  // WITH_SEASTAR

int MonMap::init_with_monmap(const std::string& monmap, std::ostream& errout)
{
  int r;
  try {
    r = read(monmap.c_str());
  } catch (ceph::buffer::error&) {
    r = -EINVAL;
  }
  if (r >= 0)
    return 0;
  errout << "unable to read/decode monmap from " << monmap
         << ": " << cpp_strerror(-r) << std::endl;
  return r;
}

int MonMap::init_with_dns_srv(CephContext* cct,
                              std::string srv_name,
			      bool for_mkfs,
                              std::ostream& errout)
{
  lgeneric_dout(cct, 1) << __func__ << " srv_name: " << srv_name << dendl;

  string domain;
  // check if domain is also provided and extract it from srv_name
  size_t idx = srv_name.find("_");
  if (idx != string::npos) {
    domain = srv_name.substr(idx + 1);
    srv_name = srv_name.substr(0, idx);
  }

  map<string, DNSResolver::Record> records;
  if (DNSResolver::get_instance()->resolve_srv_hosts(cct, srv_name,
        DNSResolver::SRV_Protocol::TCP, domain, &records) != 0) {

    errout << "unable to get monitor info from DNS SRV with service name: "
           << "ceph-mon" << std::endl;
    return -1;
  } else {
    for (auto& record : records) {
      record.second.addr.set_type(entity_addr_t::TYPE_ANY);
      _add_ambiguous_addr(record.first,
                          record.second.addr,
                          record.second.priority,
                          record.second.weight,
                          false);
    }
    return 0;
  }
}

int MonMap::build_initial(CephContext *cct, bool for_mkfs, ostream& errout)
{
  lgeneric_dout(cct, 1) << __func__ << " for_mkfs: " << for_mkfs << dendl;
  const auto& conf = cct->_conf;

  // mon_host_override?
  auto mon_host_override = conf.get_val<std::string>("mon_host_override");
  if (!mon_host_override.empty()) {
    lgeneric_dout(cct, 1) << "Using mon_host_override " << mon_host_override << dendl;
    auto ret = init_with_ips(mon_host_override, for_mkfs, "noname-");
    if (ret == -EINVAL) {
      ret = init_with_hosts(mon_host_override, for_mkfs, "noname-");
    }
    if (ret < 0) {
      errout << "unable to parse addrs in '" << mon_host_override << "'"
	     << std::endl;
    }
    return ret;
  }

  // cct?
  auto addrs = cct->get_mon_addrs();
  if (addrs != nullptr && (addrs->size() > 0)) {
    init_with_addrs(*addrs, for_mkfs, "noname-");
    return 0;
  }

  // file?
  if (const auto monmap = conf.get_val<std::string>("monmap");
      !monmap.empty()) {
    return init_with_monmap(monmap, errout);
  }

  // fsid from conf?
  if (const auto new_fsid = conf.get_val<uuid_d>("fsid");
      !new_fsid.is_zero()) {
    fsid = new_fsid;
  }
  // -m foo?
  if (const auto mon_host = conf.get_val<std::string>("mon_host");
      !mon_host.empty()) {
    auto ret = init_with_ips(mon_host, for_mkfs, "noname-");
    if (ret == -EINVAL) {
      ret = init_with_hosts(mon_host, for_mkfs, "noname-");
    }
    if (ret < 0) {
      errout << "unable to parse addrs in '" << mon_host << "'"
	     << std::endl;
      return ret;
    }
  }
  if (size() == 0) {
    // What monitors are in the config file?
    if (auto ret = init_with_config_file(conf, errout); ret < 0) {
      return ret;
    }
  }
  if (size() == 0) {
    // no info found from conf options lets try use DNS SRV records
    string srv_name = conf.get_val<std::string>("mon_dns_srv_name");
    if (auto ret = init_with_dns_srv(cct, srv_name, for_mkfs, errout); ret < 0) {
      return -ENOENT;
    }
  }
  if (size() == 0) {
    errout << "no monitors specified to connect to." << std::endl;
    return -ENOENT;
  }
  strategy = static_cast<election_strategy>(conf.get_val<uint64_t>("mon_election_default_strategy"));
  created = ceph_clock_now();
  last_changed = created;
  calc_legacy_ranks();
  return 0;
}
#endif	// WITH_SEASTAR
