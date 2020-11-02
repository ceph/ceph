// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/Utils.h"
#include "common/dout.h"
#include "common/errno.h"
#include <boost/lexical_cast.hpp>
#include <regex>

namespace librbd {
namespace migration {
namespace util {

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::util::" << __func__ << ": "

int parse_url(CephContext* cct, const std::string& url, UrlSpec* url_spec) {
  ldout(cct, 10) << "url=" << url << dendl;
  *url_spec = UrlSpec{};

  // parse the provided URL (scheme, user, password, host, port, path,
  // parameters, query, and fragment)
  std::regex url_regex(
    R"(^(?:([^:/]*)://)?(?:(\w+)(?::(\w+))?@)?([^/;\?:#]+)(?::([^/;\?#]+))?)"
    R"((?:/([^;\?#]*))?(?:;([^\?#]+))?(?:\?([^#]+))?(?:#(\w+))?$)");
  std::smatch match;
  if(!std::regex_match(url, match, url_regex)) {
    lderr(cct) << "invalid url: '" << url << "'" << dendl;
    return -EINVAL;
  }

  auto& scheme = match[1];
  if (scheme == "http" || scheme == "") {
    url_spec->scheme = URL_SCHEME_HTTP;
  } else if (scheme == "https") {
    url_spec->scheme = URL_SCHEME_HTTPS;
    url_spec->port = "443";
  } else {
    lderr(cct) << "invalid url scheme: '" << url << "'" << dendl;
    return -EINVAL;
  }

  url_spec->host = match[4];
  auto& port = match[5];
  if (port.matched) {
    try {
      boost::lexical_cast<uint16_t>(port);
    } catch (boost::bad_lexical_cast&) {
      lderr(cct) << "invalid url port: '" << url << "'" << dendl;
      return -EINVAL;
    }
    url_spec->port = port;
  }

  auto& path = match[6];
  if (path.matched) {
    url_spec->path += path;
  }
  return 0;
}

} // namespace util
} // namespace migration
} // namespace librbd
