// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>
#include <regex>

namespace rgw {

namespace {
  const auto USER_GROUP_IDX = 3;
  const auto PASSWORD_GROUP_IDX = 4;
  const auto HOST_GROUP_IDX = 5;

  const std::string schema_re = "([[:alpha:]]+:\\/\\/)";
  const std::string user_pass_re = "(([^:\\s]+):([^@\\s]+)@)?";
  const std::string host_port_re = "([[:alnum:].:-]+)";
  const std::string vhost_re = "(/[[:print:]]+)?";
}

bool parse_url_authority(const std::string& url, std::string& host, std::string& user, std::string& password) {
  const std::string re = schema_re + user_pass_re + host_port_re + vhost_re;
  const std::regex url_regex(re, std::regex::icase);
  std::smatch url_match_result;

  if (std::regex_match(url, url_match_result, url_regex)) {
    host = url_match_result[HOST_GROUP_IDX];
    user = url_match_result[USER_GROUP_IDX];
    password = url_match_result[PASSWORD_GROUP_IDX];
    return true;
  }

  return false;
}

bool parse_url_userinfo(const std::string& url, std::string& user, std::string& password) {
  const std::string re = schema_re + user_pass_re + host_port_re + vhost_re;
  const std::regex url_regex(re);
  std::smatch url_match_result;

  if (std::regex_match(url, url_match_result, url_regex)) {
    user = url_match_result[USER_GROUP_IDX];
    password = url_match_result[PASSWORD_GROUP_IDX];
    return true;
  }

  return false;
}
}

