// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "MonMap.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/net/dns.hh>

#include "common/errno.h"

namespace ceph::mon {

using namespace seastar;

future<> MonMap::build_initial(const ConfigProxy& conf)
{
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
    return build_monmap(conf).then([this] {
      created = ceph_clock_now();
      last_changed = created;
      calc_legacy_ranks();
    });
  }
}

future<> MonMap::read_monmap(const std::string& monmap)
{
  return open_file_dma(monmap, open_flags::ro).then([this] (file f) {
    return f.size().then([this, f = std::move(f)](size_t s) {
      return do_with(make_file_input_stream(f), [this, s](input_stream<char>& in) {
        return in.read_exactly(s).then([this](temporary_buffer<char> buf) {
          bufferlist bl;
          bl.append(buffer::create(std::move(buf)));
          decode(bl);
        });
      });
    });
  });
}

seastar::future<> MonMap::build_monmap(const ConfigProxy& conf)
{
  // -m foo?
  if (const auto mon_host = conf.get_val<std::string>("mon_host");
      !mon_host.empty()) {
    if (auto ret = init_with_ips(mon_host, "noname-"); ret == 0) {
      return make_ready_future<>();
    }
    if (auto ret = init_with_hosts(mon_host, "noname-"); ret == 0) {
      return make_ready_future<>();
    } else {
      throw std::runtime_error(cpp_strerror(ret));
    }
  }

  // What monitors are in the config file?
  ostringstream errout;
  if (auto ret = init_with_config_file(conf, errout); ret < 0) {
    throw std::runtime_error(errout.str());
  }
  if (size() > 0) {
    return make_ready_future<>();
  }
  // no info found from conf options lets try use DNS SRV records
  const string srv_name = conf.get_val<std::string>("mon_dns_srv_name");
  return init_with_dns_srv(srv_name).then([this] {
    if (size() == 0) {
      throw std::runtime_error("no monitors specified to connect to.");
    }
  });
}

future<> MonMap::init_with_dns_srv(const std::string& name)
{
  string domain;
  string service = name;
  // check if domain is also provided and extract it from srv_name
  size_t idx = name.find("_");
  if (idx != name.npos) {
    domain = name.substr(idx + 1);
    service = name.substr(0, idx);
  }
  return net::dns_resolver{}.get_srv_records(
      net::dns_resolver::srv_proto::tcp,
      service, domain).then([this](net::dns_resolver::srv_records records) {
    parallel_for_each(records, [this](auto record) {
      return net::dns::resolve_name(record.target).then(
          [record,this](net::inet_address a) {
	// the resolved address does not contain ceph specific info like nonce
	// nonce or msgr proto (legacy, msgr2), so set entity_addr_t manually
	entity_addr_t addr;
	addr.set_type(entity_addr_t::TYPE_LEGACY);
	addr.set_family(int(a.in_family()));
	addr.set_port(record.port);
	switch (a.in_family()) {
	case net::inet_address::family::INET:
	  addr.in4_addr().sin_addr = a;
	  break;
	case net::inet_address::family::INET6:
	  addr.in6_addr().sin6_addr = a;
	  break;
	}
	add(mon_info_t{record.target, addr, record.priority});
      });
    });
  });
}

} // namespace ceph::mon
