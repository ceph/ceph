// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "CrushLocation.h"

#include <vector>
#include <boost/algorithm/string/trim.hpp>
#include <seastar/util/process.hh>
#include <seastar/util/later.hh>

#include "crush/CrushWrapper.h"
#include "crimson/common/log.h"
#include "crimson/common/config_proxy.h"

static seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_crush);
}

using namespace crimson::common;

namespace crimson::crush {

seastar::future<> CrushLocation::update_from_conf()
{
  auto crush_location = local_conf().get_val<std::string>("crush_location");
  if (crush_location.length()) {
    _parse(crush_location);
  }

  return seastar::now();
}

void CrushLocation::_parse(const std::string& s)
{
  std::multimap<std::string, std::string> new_crush_location;
  std::vector<std::string> lvec;
  get_str_vec(s, ";, \t", lvec);
  int r = CrushWrapper::parse_loc_multimap(lvec, &new_crush_location);
  if (r < 0) {
    logger().error("CrushWrapper::parse_loc_multimap error, keeping original\
      crush_location {}", *this);
    return;
  }

  loc.swap(new_crush_location);
  logger().info("{}: crush_location is {}", __func__, *this);
  return;
}

seastar::future<> CrushLocation::update_from_hook()
{
  auto crush_location_hook = local_conf().get_val<std::string>("crush_location_hook");
  if (crush_location_hook.length() == 0)
    return seastar::now();

  return seastar::file_exists(
    crush_location_hook
  ).then([this] (bool result) {
    if (!result) {
      std::stringstream errmsg;
      errmsg << "the user define crush location hook: "
             << local_conf().get_val<std::string>("crush_location_hook")
             << " is not exists.";
      logger().error("{}", errmsg.str());
      throw std::runtime_error(errmsg.str());
    }

    return seastar::file_accessible(
      local_conf().get_val<std::string>("crush_location_hook"),
      seastar::access_flags::execute
    ).then([this] (bool result) {
      if (!result) {
        std::stringstream errmsg;
        errmsg << "the user define crush location hook: "
               << local_conf().get_val<std::string>("crush_location_hook")
               << " is not executable.";
        logger().error("{}", errmsg.str());
        throw std::runtime_error(errmsg.str());
      }

      seastar::experimental::spawn_parameters params = {
        .argv = {
          local_conf().get_val<std::string>("crush_location_hook"),
          "--cluster",
          local_conf()->cluster,
          "--id",
          local_conf()->name.get_id(),
          "--type",
          local_conf()->name.get_type_str()
        }
      };
      return seastar::experimental::spawn_process(
        local_conf().get_val<std::string>("crush_location_hook"),
        std::move(params)
      ).then([this] (auto process) {
        auto stdout = process.stdout();
        return do_with(
          std::move(process),
          std::move(stdout),
          [this](auto& process, auto& stdout)
        {
          return stdout.read().then([] (seastar::temporary_buffer<char> buf) {
            auto out = std::string(buf.get(), buf.size());
            boost::algorithm::trim_if(out, boost::algorithm::is_any_of(" \n\r\t"));
            return seastar::make_ready_future<std::string>(std::move(out));
          }).then([&process, this] (auto out) {
            return process.wait(
            ).then([out = std::move(out), this] (auto wstatus) {
              auto* exit_signal = std::get_if<seastar::experimental::process::wait_signaled>(&wstatus);
              if (exit_signal != nullptr) {
                std::stringstream errmsg;
                errmsg << "the user define crush location hook: "
                       << local_conf().get_val<std::string>("crush_location_hook")
                       << " terminated, terminated signal is "
                       << exit_signal->terminating_signal;
                logger().error("{}", errmsg.str());
                throw std::runtime_error(errmsg.str());
              }

              auto* exit_status = std::get_if<seastar::experimental::process::wait_exited>(&wstatus);
              if (exit_status->exit_code != 0) {
                std::stringstream errmsg;
                errmsg << "the user define crush location hook: "
                       << local_conf().get_val<std::string>("crush_location_hook")
                       << " execute failed, exit_code is " << exit_status->exit_code;
                logger().error("{}", errmsg.str());
                throw std::runtime_error(errmsg.str());
              } else {
                _parse(out);
              }
              return seastar::now();
            });
          });
        });
      });
    });
  });
}

seastar::future<> CrushLocation::init_on_startup()
{
  if (local_conf().get_val<std::string>("crush_location").length()) {
    return update_from_conf();
  }
  if (local_conf().get_val<std::string>("crush_location_hook").length()) {
    return update_from_hook();
  }

  // start with a sane default
  char hostname[HOST_NAME_MAX + 1];
  int r = gethostname(hostname, sizeof(hostname));
  if (r < 0)
    strcpy(hostname, "unknown_host");
  // use short hostname
  for (unsigned i=0; hostname[i]; ++i) {
    if (hostname[i] == '.') {
      hostname[i] = '\0';
      break;
    }
  }

  loc.clear();
  loc.insert(std::make_pair<std::string, std::string>("host", hostname));
  loc.insert(std::make_pair<std::string, std::string>("root", "default"));
  return seastar::now();
}

std::multimap<std::string,std::string> CrushLocation::get_location() const
{
  return loc;
}

std::ostream& operator<<(std::ostream& os, const CrushLocation& loc)
{
  bool first = true;
  for (auto& [type, pos] : loc.get_location()) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    os << '"' << type << '=' << pos << '"';
  }
  return os;
}

}
