// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <fmt/format.h>

#include <string>

#include <boost/url/parse.hpp>
#include <boost/url/url_view.hpp>

namespace rgw {

bool parse_url_authority(const std::string& url,
                         std::string& host,
                         std::string& user,
                         std::string& password) {
  auto r = boost::urls::parse_uri(url);
  if (!r) {
    return false;
  }
  const auto& v = r.value();
  if (v.has_port()) {
    host = fmt::format("{}:{}", v.host(), v.port());
  } else {
    host = std::string(v.host());
  }
  user = std::string(v.user());
  password = std::string(v.password());
  return true;
}

bool parse_url_userinfo(const std::string& url,
                        std::string& user,
                        std::string& password) {
  auto r = boost::urls::parse_uri(url);
  if (!r) {
    return false;
  }
  const auto& v = r.value();
  user = std::string(v.user());
  password = std::string(v.password());
  return true;
}
} // namespace rgw
